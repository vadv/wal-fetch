"""Private PostgreSQL resources, command transcript and bounded diagnostics."""
import hashlib
import json
import os
from pathlib import Path
import re
import shutil
import signal
import socket
import subprocess
import tempfile
import time


def lsn(value):
    hi, lo = value.split('/')
    return (int(hi, 16) << 32) + int(lo, 16)


def wait_for(predicate, seconds=5):
    deadline = time.monotonic() + seconds
    while time.monotonic() < deadline:
        if predicate():
            return
        time.sleep(0.03)
    raise AssertionError('condition did not become true')


def free_port():
    with socket.socket() as sock:
        sock.bind(('127.0.0.1', 0))
        return sock.getsockname()[1]


class Fixture:
    def __init__(self):
        if os.geteuid() == 0:
            raise RuntimeError('Run recovery tests as an unprivileged user, not root')
        root = Path(os.environ.get('WAL_FETCH_TEST_WORK', tempfile.gettempdir())).resolve()
        root.mkdir(parents=True, exist_ok=True)
        self.segment_mb = int(os.environ.get('WAL_FETCH_TEST_SEGMENT_MB', '16'))
        self.pg_version = os.environ.get('WAL_FETCH_TEST_PG_VERSION', '16')
        self.no_slot = os.environ.get('WAL_FETCH_TEST_NO_SLOT', '0') == '1'
        self.run = Path(tempfile.mkdtemp(prefix='unix-it-', dir=root))
        (self.run / 's').mkdir()
        self.binary = Path(__file__).resolve().parents[2] / 'wal-fetch'
        self.evidence = Path(os.environ.get('WAL_FETCH_TEST_EVIDENCE', root / (self.run.name + '-diagnostics')))
        self.env = {k: v for k, v in os.environ.items() if not k.startswith('PG')}
        self.env.update(TMPDIR=str(self.run), LC_ALL='C', PGPASSWORD='disposable-e2e-password',
                        PGSSLMODE='disable', WAL_FETCH_TIMEOUT='30')
        self.transcript = (self.run / 'commands.log').open('w')
        self.clusters = []
        self.scopes = []
        self.result = {'run': str(self.run), 'status': 'RUNNING', 'no_slot': self.no_slot}
        print(f'RUN={self.run}', flush=True)

    def new_scope(self, name):
        scope = self.run / name
        scope.mkdir()
        self.scopes.append(scope)
        return scope

    def redact(self, text):
        return text.replace(self.env['PGPASSWORD'], '[REDACTED]').replace('wrong-password', '[REDACTED]')

    def command(self, args, *, check=True, timeout=60, env=None):
        self.transcript.write(self.redact('$ ' + ' '.join(map(str, args))) + '\n')
        self.transcript.flush()
        p = subprocess.run(list(map(str, args)), env=self.env if env is None else env, text=True,
                           stdout=subprocess.PIPE, stderr=subprocess.STDOUT, timeout=timeout)
        self.transcript.write(self.redact(p.stdout) + f'\n[exit {p.returncode}]\n')
        self.transcript.flush()
        if check and p.returncode:
            raise RuntimeError(self.redact(f'{args[0]} failed: {p.stdout}'))
        return p

    def configure(self, data, label):
        port = free_port()
        sock = self.run / 's' / label[0]
        sock.mkdir()
        with (data / 'postgresql.conf').open('a') as f:
            f.write(f"\nport={port}\nlisten_addresses='127.0.0.1'\nunix_socket_directories='{sock}'\n"
                    "autovacuum=off\narchive_mode=off\nwal_level=replica\nmax_wal_senders=8\nwal_keep_size='128MB'\n"
                    "checkpoint_timeout='1h'\nmax_wal_size='1GB'\nlog_replication_commands=on\nlogging_collector=off\n"
                    "log_line_prefix='%m [%p] %a '\npassword_encryption='scram-sha-256'\n")
        (data / 'pg_hba.conf').write_text(
            'local all postgres trust\nlocal replication all trust\nlocal all wal_reader reject\n'
            'host replication wal_reader 127.0.0.1/32 scram-sha-256\n'
            'host all wal_reader 127.0.0.1/32 reject\n')
        scope = self.new_scope(label + '-client')
        c = dict(data=data, label=label, port=port, sock=sock, scope=scope, log=self.run / f'{label}.log')
        self.clusters.append(c)
        return c

    def sql(self, c, query):
        return self.command(['psql', '-XAt', '-v', 'ON_ERROR_STOP=1', '-h', c['sock'],
                            '-p', c['port'], '-U', 'postgres', '-d', 'postgres', '-c', query]).stdout.strip()

    def start(self, c, wait=True):
        self.command(['pg_ctl', '-D', c['data'], '-l', c['log'], '-w' if wait else '-W',
                 '-t', '30', 'start'], timeout=40)

    def stop(self, c):
        if (c['data'] / 'postmaster.pid').exists():
            try:
                self.command(['pg_ctl', '-D', c['data'], '-m', 'fast', '-w', '-t', '20', 'stop'],
                             check=False, timeout=25)
            except subprocess.TimeoutExpired:
                pass
            if (c['data'] / 'postmaster.pid').exists():
                self.command(['pg_ctl', '-D', c['data'], '-m', 'immediate', '-w', '-t', '10', 'stop'],
                             check=False, timeout=15)

    def fetch(self, c, name, dest, *, check=True, env_patch=None, timeout=40, scope=None, idle='30s',
              log_file=None, slot_failure_limit=None):
        env = dict(self.env, PGHOST='127.0.0.1', PGPORT=str(c['port']), PGUSER='wal_reader')
        env.update(env_patch or {})
        env = {k: v for k, v in env.items() if v is not None}
        limit = env.pop('WAL_FETCH_TIMEOUT', '30')
        args = [self.binary, '-pgdata', scope or c['scope'], '-idle-timeout', idle, '-timeout', limit + 's']
        if log_file is not None:
            args += ['-log-file', str(log_file)]
        if slot_failure_limit is not None:
            args += ['-slot-failure-limit', str(slot_failure_limit)]
        if self.no_slot:
            args += ['-no-slot']
        if self.segment_mb != 16:
            args += ['-wal-segment-size', f'{self.segment_mb}MB']
        return self.command(args + [name, dest], check=check, timeout=timeout, env=env)

    def create_source(self):
        version = self.command(['initdb', '--version']).stdout.strip()
        if not re.search(rf'PostgreSQL\) {re.escape(self.pg_version)}\.', version):
            raise RuntimeError(f'PostgreSQL {self.pg_version} CLI tools must be on PATH')
        self.result['postgres_version'] = version
        self.result['binary_sha256'] = hashlib.sha256(self.binary.read_bytes()).hexdigest()
        source_dir = self.run / 'source'
        self.command(['initdb', '-D', source_dir, '-U', 'postgres', '-A', 'trust',
                      '--no-locale', f'--wal-segsize={self.segment_mb}'])
        source = self.configure(source_dir, 'source')
        self.start(source)
        self.sql(source, "CREATE ROLE wal_reader LOGIN REPLICATION PASSWORD 'disposable-e2e-password'; "
                    "CREATE TABLE e2e_marker(id int PRIMARY KEY, value text); "
                    "INSERT INTO e2e_marker VALUES(1,'before backup');")
        return source

    def audit_replication(self):
        audited = []
        for cluster in self.clusters:
            if not cluster['log'].exists():
                continue
            own = [line for line in cluster['log'].read_text().splitlines() if ' wal-fetch-unix ' in line]
            if not own:
                continue
            assert any('IDENTIFY_SYSTEM' in line for line in own), own
            assert not any('SHOW ' in line or 'SELECT ' in line for line in own), own
            if self.no_slot:
                assert not any(re.search(r'CREATE_REPLICATION_SLOT|READ_REPLICATION_SLOT|\bSLOT\b', line)
                               for line in own), own
            audited.append(cluster['label'])
        if not audited:
            return
        if self.no_slot:
            for cluster in self.clusters:
                if (cluster['data'] / 'postmaster.pid').exists():
                    assert self.sql(cluster, 'SELECT count(*) FROM pg_replication_slots') == '0'
            logs = list(self.run.glob('*/.wal-fetch/server.log'))
            if (self.run / 'custom-server.log').exists():
                logs.append(self.run / 'custom-server.log')
            for logpath in logs:
                log = logpath.read_text()
                assert ('retention lost' not in log and 'retention floor advanced' not in log
                        and 'temporary slot created' not in log)
            self.result['slotless_zero_slots_and_no_slot_commands'] = True
        self.result.update(no_SHOW_or_ordinary_SQL=True, audited_clusters=audited)

    def cleanup(self):
        cleanup_errors = []
        for scope in self.scopes:
            try:
                pidfile = scope / '.wal-fetch/server.pid'
                if pidfile.exists():
                    pid = int(pidfile.read_text())
                    if Path(f'/proc/{pid}/exe').resolve() == self.binary.resolve():
                        os.kill(pid, signal.SIGTERM)
            except ProcessLookupError:
                pass
            except Exception as exc:
                cleanup_errors.append(self.redact(f'{scope.name}: {exc}'))
        deadline = time.monotonic() + 5
        while any((scope / '.wal-fetch/server.sock').exists() for scope in self.scopes) and time.monotonic() < deadline:
            time.sleep(0.05)
        for cluster in reversed(self.clusters):
            try:
                self.stop(cluster)
            except Exception as exc:
                cleanup_errors.append(self.redact(f'{cluster["label"]}: {exc}'))
        self.result['all_owned_clusters_stopped'] = all(
            not (c['data'] / 'postmaster.pid').exists() for c in self.clusters)
        self.result['all_owned_unix_sockets_removed'] = all(
            not (scope / '.wal-fetch/server.sock').exists() for scope in self.scopes)
        if not self.result['all_owned_clusters_stopped'] or not self.result['all_owned_unix_sockets_removed']:
            cleanup_errors.append('owned process cleanup incomplete')
        if cleanup_errors:
            self.result.update(status='FAIL', cleanup_errors=cleanup_errors)

    def save_evidence(self):
        cleanup_errors = self.result.get('cleanup_errors', [])
        self.transcript.close()
        # An allowlist, redaction and bounded tails keep CI artifacts small and safe.
        out = self.evidence / self.run.name
        out.mkdir(parents=True, exist_ok=True)
        names = ('commands.log', 'source.log', 'leader.log', 'replica.log', 'custom-server.log',
                 'restore-requests.log', 'replica-controldata.txt', 'ordinary-sql-rejected.log')
        logs = [(self.run / name, name) for name in names]
        logs += [(scope / '.wal-fetch/server.log', scope.name + '-server.log') for scope in self.scopes]
        for source_log, name in logs:
            if source_log.is_file():
                with source_log.open('rb') as stream:
                    stream.seek(max(0, source_log.stat().st_size - 65536))
                    (out / name).write_text(self.redact(stream.read().decode('utf-8', errors='replace')))
        # Never remove a live cluster's data. Failed cleanup is nonzero, with evidence.
        if self.result['all_owned_clusters_stopped'] and self.result['all_owned_unix_sockets_removed']:
            try:
                shutil.rmtree(self.run)
            except OSError as exc:
                self.result.update(status='FAIL', cleanup_errors=cleanup_errors + [self.redact(str(exc))])
        (out / 'result.json').write_text(json.dumps(self.result, indent=2) + '\n')
        print(json.dumps(self.result, indent=2), flush=True)
        print(f'DIAGNOSTICS={out}', flush=True)
