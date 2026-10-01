#!/usr/bin/env python3
"""Disposable PG16 recovery test; no installs or preexisting clusters used."""
import concurrent.futures
import signal
import hashlib
import json
import os
from pathlib import Path
import re
import shutil
import socket
import subprocess
import tempfile
import time

ARTIFACT = Path(__file__).resolve().parent
ROOT = Path(os.environ.get('WAL_FETCH_TEST_WORK', tempfile.gettempdir()))
ROOT.mkdir(parents=True, exist_ok=True)
if os.geteuid() == 0:
    raise SystemExit('Run integration.py as an unprivileged user, not root')
SEGMENT_MB = int(os.environ.get('WAL_FETCH_TEST_SEGMENT_MB', '16'))
NO_SLOT = os.environ.get('WAL_FETCH_TEST_NO_SLOT', '0') == '1'
RUN = Path(tempfile.mkdtemp(prefix='unix-it-', dir=ROOT))
SOCKETS = RUN / 's'
SOCKETS.mkdir()
ENV = {k: v for k, v in os.environ.items() if not k.startswith('PG')}
ENV.update(TMPDIR=str(RUN), LC_ALL='C', PGPASSWORD='disposable-e2e-password', PGSSLMODE='disable', WAL_FETCH_TIMEOUT='30')
EVIDENCE = Path(os.environ.get('WAL_FETCH_TEST_EVIDENCE', ROOT / (RUN.name + '-diagnostics')))
helper = ARTIFACT / 'wal-fetch'
transcript = (RUN / 'commands.log').open('w')
clusters = []
scopes = []

def redact(text):
    return text.replace(ENV['PGPASSWORD'], '[REDACTED]').replace('wrong-password', '[REDACTED]')

def interrupted(signum, _frame):
    raise KeyboardInterrupt(f'interrupted by signal {signum}')

signal.signal(signal.SIGTERM, interrupted)

def command(args, *, check=True, timeout=60, env=None):
    transcript.write(redact('$ ' + ' '.join(map(str, args))) + '\n')
    transcript.flush()
    p = subprocess.run(list(map(str, args)), env=ENV if env is None else env, text=True,
                       stdout=subprocess.PIPE, stderr=subprocess.STDOUT, timeout=timeout)
    transcript.write(redact(p.stdout) + f'\n[exit {p.returncode}]\n')
    transcript.flush()
    if check and p.returncode:
        raise RuntimeError(redact(f'{args[0]} failed: {p.stdout}'))
    return p

def free_port():
    with socket.socket() as s:
        s.bind(('127.0.0.1', 0))
        return s.getsockname()[1]

def configure(data, label):
    port = free_port()
    sock = SOCKETS / label[0]
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
    scope = RUN / (label + '-client')
    scope.mkdir()
    scopes.append(scope)
    c = dict(data=data, label=label, port=port, sock=sock, scope=scope, log=RUN / f'{label}.log')
    clusters.append(c)
    return c

def sql(c, query):
    return command(['psql', '-XAt', '-v', 'ON_ERROR_STOP=1', '-h', c['sock'],
                    '-p', c['port'], '-U', 'postgres', '-d', 'postgres', '-c', query]).stdout.strip()

def start(c, wait=True):
    command(['pg_ctl', '-D', c['data'], '-l', c['log'], '-w' if wait else '-W',
             '-t', '30', 'start'], timeout=40)

def stop(c):
    if (c['data'] / 'postmaster.pid').exists():
        try:
            command(['pg_ctl', '-D', c['data'], '-m', 'fast', '-w', '-t', '20', 'stop'], check=False, timeout=25)
        except subprocess.TimeoutExpired:
            pass
        if (c['data'] / 'postmaster.pid').exists():
            command(['pg_ctl', '-D', c['data'], '-m', 'immediate', '-w', '-t', '10', 'stop'], check=False, timeout=15)

def lsn(value):
    hi, lo = value.split('/')
    return (int(hi, 16) << 32) + int(lo, 16)

print(f'RUN={RUN}', flush=True)
result = {'run': str(RUN), 'status': 'RUNNING', 'no_slot': NO_SLOT}
try:
    version = command(['initdb', '--version']).stdout.strip()
    if not re.search(r'PostgreSQL\) 16\.', version):
        raise RuntimeError('PostgreSQL 16 CLI tools must be on PATH')
    result['postgres_version'] = version
    result['binary_sha256'] = hashlib.sha256(helper.read_bytes()).hexdigest()
    source_dir = RUN / 'source'
    command(['initdb', '-D', source_dir, '-U', 'postgres', '-A', 'trust', '--no-locale', f'--wal-segsize={SEGMENT_MB}'])
    source = configure(source_dir, 'source')
    start(source)
    sql(source, "CREATE ROLE wal_reader LOGIN REPLICATION PASSWORD 'disposable-e2e-password'; "
                "CREATE TABLE e2e_marker(id int PRIMARY KEY, value text); "
                "INSERT INTO e2e_marker VALUES(1,'before backup');")
    def fetch(c, name, dest, *, check=True, env_patch=None, timeout=40, scope=None, idle='30s', log_file=None):
        env = dict(ENV, PGHOST='127.0.0.1', PGPORT=str(c['port']), PGUSER='wal_reader')
        env.update(env_patch or {})
        env = {k: v for k, v in env.items() if v is not None}
        limit = env.pop('WAL_FETCH_TIMEOUT', '30')
        args = [helper, '-pgdata', scope or c['scope'], '-idle-timeout', idle, '-timeout', limit + 's']
        if log_file is not None: args += ['-log-file', str(log_file)]
        if NO_SLOT: args += ['-no-slot']
        if SEGMENT_MB != 16: args += ['-wal-segment-size', f'{SEGMENT_MB}MB']
        return command(args + [name, dest], check=check, timeout=timeout, env=env)

    sql(source, 'CREATE TABLE wal_fixture AS SELECT generate_series(1,10000);')
    closed = sql(source, 'SELECT pg_walfile_name(pg_current_wal_insert_lsn());')
    sql(source, 'SELECT pg_switch_wal();')
    completed = RUN / 'completed.wal'
    with concurrent.futures.ThreadPoolExecutor(max_workers=8) as pool:
        list(pool.map(lambda i: fetch(source, closed, RUN / f'concurrent-{i}.wal'), range(8)))
    server_pid = int((source['scope'] / '.wal-fetch/server.pid').read_text())
    assert sql(source, 'SELECT count(*) FROM pg_replication_slots') == ('0' if NO_SLOT else '1')
    result['concurrent_coldstart_one_server_expected_slots'] = True
    fetch(source, closed, completed)
    assert completed.read_bytes() == (source_dir / 'pg_wal' / closed).read_bytes()
    result['completed_exact_compare'] = True
    sql(source, 'CHECKPOINT;')
    quiet = sql(source, "SELECT pg_current_wal_flush_lsn(), pg_walfile_name(pg_current_wal_flush_lsn()), pg_size_bytes(current_setting('wal_segment_size'));")
    quiet_flush, quiet_name, quiet_size = quiet.split('|')
    active = RUN / 'active.wal'
    begin = time.monotonic()
    fetch(source, quiet_name, active, env_patch={'WAL_FETCH_TIMEOUT': '5'}, timeout=10)
    result['quiet_active_seconds'] = time.monotonic() - begin
    after = sql(source, 'SELECT pg_current_wal_flush_lsn();')
    assert quiet_flush == after, (quiet_flush, after)
    raw = (source_dir / 'pg_wal' / quiet_name).read_bytes()
    got = active.read_bytes()
    offset = lsn(quiet_flush) % int(quiet_size)
    assert offset > 0 and len(got) == len(raw) == int(quiet_size)
    assert got[:offset] == raw[:offset] and got[offset:] == bytes(len(got)-offset)
    result['quiet_active_prefix_and_zero_tail'] = True
    pgpass = RUN / 'pgpass'
    pgpass.write_text(f'127.0.0.1:{source["port"]}:replication:wal_reader:disposable-e2e-password\n')
    pgpass.chmod(0o600)
    passfile_result = RUN / 'passfile.wal'
    fetch(source, closed, passfile_result, env_patch={'PGPASSWORD': None, 'PGPASSFILE': str(pgpass)})
    assert passfile_result.read_bytes() == completed.read_bytes()
    result['replication_pgpass'] = True
    unchanged = RUN / 'unchanged'
    for name, env_patch in [(closed, {'PGPASSWORD': 'wrong-password'}), ('000000010000000000000000', {}), ('000000010000000000000FFF', {}), ('00000003.history', {})]:
        unchanged.write_bytes(b'ORIGINAL')
        failure = fetch(source, name, unchanged, check=False, env_patch=env_patch)
        assert failure.returncode != 0 and unchanged.read_bytes() == b'ORIGINAL', (name, failure.stdout)
        assert 'disposable-e2e-password' not in failure.stdout
    result['missing_auth_atomicity'] = True
    bad_scope = RUN / 'bad-auth'; bad_scope.mkdir(); scopes.append(bad_scope)
    assert fetch(source, closed, unchanged, check=False, scope=bad_scope,
                 env_patch={'PGPASSWORD':'wrong-password'}).returncode != 0
    assert 'wrong-password' not in (bad_scope / '.wal-fetch/server.log').read_text()
    result['fresh_server_auth_failure_sanitized'] = True
    mismatch_env = dict(ENV, PGHOST='127.0.0.1', PGPORT=str(source['port']), PGUSER='wal_reader')
    mismatch = command([helper, '-pgdata', source['scope'], '-idle-timeout', '30s', '-wal-segment-size', '64MB' if SEGMENT_MB != 64 else '16MB', closed, unchanged], env=mismatch_env, check=False)
    assert mismatch.returncode and 'configuration mismatch' in mismatch.stdout
    result['different_segment_config_rejected'] = True
    mismatch = command([helper, '-pgdata', source['scope'], '-idle-timeout', '30s',
                        '-wal-segment-size', f'{SEGMENT_MB}MB',
                        '-no-slot=' + str(not NO_SLOT).lower(), closed, unchanged], env=mismatch_env, check=False)
    assert mismatch.returncode and 'configuration mismatch' in mismatch.stdout
    result['different_slot_mode_rejected'] = True

    def wait_for(predicate, seconds=5):
        deadline = time.monotonic() + seconds
        while time.monotonic() < deadline:
            if predicate(): return
            time.sleep(0.03)
        raise AssertionError('condition did not become true')

    log_scope = RUN / 'log-client'; log_scope.mkdir(); scopes.append(log_scope)
    custom_log = RUN / 'custom-server.log'
    fetch(source, closed, RUN / 'log-success', scope=log_scope, log_file=os.path.relpath(custom_log))
    assert (RUN / 'log-success').read_bytes() == completed.read_bytes()
    probe = '000000010000000000000FFF'
    assert fetch(source, probe, unchanged, check=False, scope=log_scope, log_file=custom_log).returncode != 0
    mismatch = fetch(source, closed, unchanged, check=False, scope=log_scope, log_file=RUN / 'other.log')
    assert mismatch.returncode and 'configuration mismatch' in mismatch.stdout
    os.kill(int((log_scope / '.wal-fetch/server.pid').read_text()), signal.SIGTERM)
    wait_for(lambda: not (log_scope / '.wal-fetch/server.sock').exists())
    log_text = custom_log.read_text()
    assert re.search(r'\d{4}/\d{2}/\d{2} \d{2}:\d{2}:\d{2} wal-fetch\[\d+\]', log_text), log_text
    assert f'fetched name={closed} bytes={SEGMENT_MB << 20}' in log_text
    assert 'rejected error=' in log_text and f'name={probe}' in log_text
    assert 'server started' in log_text and 'server stopped' in log_text
    assert ENV['PGPASSWORD'] not in log_text and 'password=' not in log_text and 'host=' not in log_text
    assert custom_log.stat().st_mode & 0o777 == 0o600
    assert not (log_scope / '.wal-fetch/server.log').exists()
    result['custom_log_success_failure_identity_and_no_secrets'] = True

    if not NO_SLOT:
        # Slot progress is measured on the real server, not inferred from logs.
        def slot_state(scope):
            log = (scope / '.wal-fetch/server.log').read_text()
            name = re.findall(r'temporary slot created name=(wal_fetch_[0-9a-f]+)', log)[-1]
            row = sql(source, f"SELECT active_pid, restart_lsn FROM pg_replication_slots WHERE slot_name='{name}'")
            return name, row

        sql(source, 'CHECKPOINT;')
        active_name = sql(source, 'SELECT pg_walfile_name(pg_current_wal_flush_lsn())')
        fetch(source, active_name, RUN / 'slot-active')
        slot, row = slot_state(source['scope'])
        owner, initial = row.split('|')
        size = SEGMENT_MB << 20
        first = lsn(initial) // size
        for _ in range(4):
            sql(source, 'INSERT INTO wal_fixture VALUES (42);')
            sql(source, 'SELECT pg_switch_wal();')
        def wal_name(seg):
            pos = seg * size
            return f'00000001{pos >> 32:08X}{(pos & 0xffffffff) // size:08X}'
        floors = []
        for seg in range(first, first + 4):
            fetch(source, wal_name(seg), RUN / f'progress-{seg}.wal')
            wait_for(lambda: lsn(slot_state(source['scope'])[1].split('|')[1]) >= max(lsn(initial), seg * size))
            current_slot, row = slot_state(source['scope'])
            current_owner, floor = row.split('|')
            assert current_slot == slot and current_owner == owner, (current_slot, current_owner)
            floors.append(lsn(floor))
        assert floors == sorted(floors) and floors[-1] == (first + 3) * size, floors
        fetch(source, wal_name(first), RUN / 'repeat-old')
        assert lsn(slot_state(source['scope'])[1].split('|')[1]) == floors[-1]
        fetch(source, '00000003.history', unchanged, check=False)
        assert slot_state(source['scope'])[0] == slot and slot_state(source['scope'])[1]
        result['same_owner_CopyDone_reuse_and_three_segment_advance'] = floors
        result['old_repeat_and_missing_history_preserve_slot'] = True

        before_rejection = slot_state(source['scope'])
        for probe in [wal_name(first + 50), '00000002' + wal_name(first)[8:]]:
            unchanged.write_bytes(b'ORIGINAL')
            failed = fetch(source, probe, unchanged, check=False)
            assert failed.returncode != 0 and unchanged.read_bytes() == b'ORIGINAL'
            assert slot_state(source['scope']) == before_rejection, probe
        result['future_and_nonancestor_requests_preserve_slot_owner_and_floor'] = True

        # Local protocol negative cases on a real owner: no publication ACK, no advance.
        # This canonical digest matches the documented effective connection identity
        # in our controlled environment; it contains no credential in the wire request.
        identity_values = ['127.0.0.1', source['port'], 'wal_reader', ENV['PGPASSWORD'],
                           'replication', {'application_name':'wal-fetch-unix', 'replication':'yes'},
                           size, '30s', str(source['scope'].resolve()), NO_SLOT,
                           str(source['scope'] / '.wal-fetch/server.log'), False]
        for key in ('PGSSLMODE','PGSSLROOTCERT','PGSSLCERT','PGSSLKEY','PGSSLCRL','PGSSLSNI','PGCHANNELBINDING','PGSERVICE','PGSERVICEFILE'):
            identity_values.extend((key, ENV.get(key, '')))
        identity = hashlib.sha256(json.dumps(identity_values, sort_keys=True, separators=(',', ':')).encode()).hexdigest()
        sql(source, 'INSERT INTO wal_fixture VALUES (43);')
        candidate_name = wal_name(first + 4)
        for mode in ('no-ack', 'wrong-id', 'wrong-version', 'not-published', 'injected-lsn', 'malformed'):
            with socket.socket(socket.AF_UNIX) as conn:
                conn.settimeout(5)
                conn.connect(str(source['scope'] / '.wal-fetch/server.sock'))
                req = {'version':1, 'identity':identity, 'name':candidate_name, 'deadline':time.time_ns()+5_000_000_000}
                conn.sendall(json.dumps(req).encode()+b'\n')
                stream = conn.makefile('rb')
                header = json.loads(stream.readline())
                assert header['ok'], header
                payload = stream.read(header['length'])
                assert len(payload) == size and stream.read(1) == b''
                assert hashlib.sha256(payload).hexdigest() == header['sha256']
                ack = {'version':1, 'id':header['id'], 'published':True}
                if mode == 'wrong-id': ack['id'] = '0'*32
                if mode == 'wrong-version': ack['version'] = 2
                if mode == 'not-published': ack['published'] = False
                if mode == 'injected-lsn': ack['lsn'] = 'FFFFFFFF/FFFFFFFF'
                if mode != 'no-ack': conn.sendall(b'{broken}\n' if mode == 'malformed' else json.dumps(ack).encode()+b'\n')
                stream.close()
            fetch(source, '00000003.history', unchanged, check=False) # serial barrier
            assert lsn(slot_state(source['scope'])[1].split('|')[1]) == floors[-1]
        fetch(source, candidate_name, RUN / 'valid-publication-after-invalid-acks')
        wait_for(lambda: lsn(slot_state(source['scope'])[1].split('|')[1]) == (first + 4) * size)
        result['invalid_or_missing_ACK_never_advances'] = True


        # An ERROR on the actual owning session deletes TEMPORARY; next call is fresh.
        unchanged.write_bytes(b'ORIGINAL')
        failed = fetch(source, '000000010000000000000000', unchanged, check=False)
        assert failed.returncode and unchanged.read_bytes() == b'ORIGINAL'
        wait_for(lambda: sql(source, f"SELECT count(*) FROM pg_replication_slots WHERE slot_name='{slot}'") == '0')
        fetch(source, closed, RUN / 'after-owner-error')
        new_slot, row = slot_state(source['scope'])
        assert new_slot != slot
        sql(source, f"SELECT pg_terminate_backend({row.split('|')[0]})")
        assert fetch(source, closed, unchanged, check=False).returncode != 0
        fetch(source, closed, RUN / 'after-owner-disconnect')
        assert 'retention lost' in (source['scope'] / '.wal-fetch/server.log').read_text()
        result['owner_error_disconnect_and_honest_reconnect'] = True

    # Server cannot race a fallback write: it has never received destination.
    pid = int((source['scope'] / '.wal-fetch/server.pid').read_text())
    os.kill(pid, signal.SIGSTOP)
    try:
        unchanged.write_bytes(b'ORIGINAL')
        failed = fetch(source, closed, unchanged, check=False, env_patch={'WAL_FETCH_TIMEOUT':'0.1'})
        assert failed.returncode != 0 and unchanged.read_bytes() == b'ORIGINAL'
        unchanged.write_bytes(b'FALLBACK')
    finally: os.kill(pid, signal.SIGCONT)
    time.sleep(0.25)
    assert unchanged.read_bytes() == b'FALLBACK'
    result['client_timeout_no_late_destination_write'] = True

    # SIGKILL leaves a stale socket; lifetime flock releases and one replacement starts.
    os.kill(pid, signal.SIGKILL)
    with concurrent.futures.ThreadPoolExecutor(max_workers=6) as pool:
        list(pool.map(lambda i: fetch(source, closed, RUN / f'stale-{i}.wal'), range(6)))
    assert int((source['scope'] / '.wal-fetch/server.pid').read_text()) != pid
    result['stale_socket_concurrent_respawn'] = True

    if not NO_SLOT:
        # Idle never cancels active or queued work. Pause ONLY our private WAL sender.
        idle_scope = RUN / 'idle-client'; idle_scope.mkdir(); scopes.append(idle_scope)
        fetch(source, closed, RUN / 'idle-warm', scope=idle_scope, idle='300ms')
        idle_pid = int((idle_scope / '.wal-fetch/server.pid').read_text())
        idle_slot, row = slot_state(idle_scope)
        backend = int(row.split('|')[0])
        os.kill(backend, signal.SIGSTOP)
        try:
            with concurrent.futures.ThreadPoolExecutor(max_workers=2) as pool:
                jobs = [pool.submit(fetch, source, closed, RUN / f'idle-active-{i}', scope=idle_scope, idle='300ms') for i in range(2)]
                time.sleep(0.8)
                assert (idle_scope / '.wal-fetch/server.sock').exists()
                os.kill(backend, signal.SIGCONT)
                for job in jobs: job.result()
        finally:
            try: os.kill(backend, signal.SIGCONT)
            except ProcessLookupError: pass
        wait_for(lambda: not (idle_scope / '.wal-fetch/server.sock').exists())
        # Closing the connection precedes PostgreSQL processing its disconnect.
        wait_for(lambda: sql(source, f"SELECT count(*) FROM pg_replication_slots WHERE slot_name='{idle_slot}'") == '0')
        fetch(source, closed, RUN / 'idle-respawn', scope=idle_scope, idle='300ms')
        assert int((idle_scope / '.wal-fetch/server.pid').read_text()) != idle_pid
        result['idle_active_queue_cleanup_and_autorespawn'] = True

    else:
        owner_query = "SELECT pid FROM pg_stat_replication WHERE application_name='wal-fetch-unix'"
        owner = sql(source, owner_query)
        assert owner.isdigit(), owner
        fetch(source, closed, RUN / 'reuse-slotless')
        assert sql(source, owner_query) == owner
        sql(source, f'SELECT pg_terminate_backend({owner})')
        assert fetch(source, closed, unchanged, check=False).returncode != 0
        fetch(source, closed, RUN / 'reconnect-slotless')
        assert sql(source, owner_query) != owner
        assert sql(source, 'SELECT count(*) FROM pg_replication_slots') == '0'
        result['slotless_same_owner_CopyDone_reuse_and_reconnect'] = True

    base = RUN / 'base'
    command(['pg_basebackup', '-h', source['sock'], '-p', source['port'], '-U', 'postgres',
             '-D', base, '-X', 'stream', '-c', 'fast'], timeout=90)
    # Backup termination already switches source WAL; this checkpoint makes the
    # promotion occur later than backup redo, so recovery needs an ancestor WAL.
    sql(source, 'CHECKPOINT;')
    source_target = sql(source, 'SELECT checkpoint_lsn FROM pg_control_checkpoint();')
    stop(source)
    leader_dir, replica_dir = RUN / 'leader', RUN / 'replica'
    shutil.copytree(base, leader_dir)
    shutil.copytree(base, replica_dir)
    for p in (source_dir / 'pg_wal').iterdir():
        if p.is_file() and re.fullmatch(r'[0-9A-F]{24}', p.name):
            shutil.copy2(p, leader_dir / 'pg_wal' / p.name)
    leader = configure(leader_dir, 'leader')
    (leader_dir / 'postgresql.auto.conf').write_text(
        f"restore_command='false'\nrecovery_target_lsn='{source_target}'\nrecovery_target_action='promote'\n")
    (leader_dir / 'recovery.signal').touch()
    start(leader)
    # pg_ctl readiness may mean hot standby; promotion must finish before writes.
    wait_for(lambda: sql(leader, 'SELECT pg_is_in_recovery();') == 'f', seconds=30)
    sql(leader, "INSERT INTO e2e_marker VALUES(2,'after promotion'); CHECKPOINT;")
    capture = sql(leader, "SELECT timeline_id, checkpoint_lsn, pg_current_wal_flush_lsn(), "
                         "pg_size_bytes(current_setting('wal_segment_size')), "
                         "current_setting('archive_mode') FROM pg_control_checkpoint();")
    tli_str, target, flush, size_str, archive_mode = capture.split('|')
    tli, size = int(tli_str), int(size_str)
    assert tli > 1 and archive_mode == 'off', capture
    history_name = f'{tli:08X}.history'
    history = (leader_dir / 'pg_wal' / history_name).read_text()
    (RUN / 'leader.history').write_text(history)
    branch = [line.split() for line in history.splitlines() if line and not line.startswith('#')][-1]
    old_tli, fork = int(branch[0]), branch[1]
    assert lsn(target) // size == lsn(fork) // size, (target, fork)
    assert lsn(flush) // size == lsn(target) // size and lsn(flush) % size != 0, (flush, target)
    history_result = RUN / 'fetched.history'
    fetch(leader, history_name, history_result)
    assert history_result.read_bytes() == (leader_dir / 'pg_wal' / history_name).read_bytes()
    result['history_exact_compare'] = True
    fork_pos = lsn(fork)
    ancestor_pos = fork_pos - 1
    ancestor_name = f'{old_tli:08X}{ancestor_pos >> 32:08X}{(ancestor_pos & 0xffffffff) // size:08X}'
    ancestor_result = RUN / 'ancestor.wal'
    fetch(leader, ancestor_name, ancestor_result)
    raw = (source_dir / 'pg_wal' / ancestor_name).read_bytes()
    got = ancestor_result.read_bytes()
    valid = ancestor_pos % size + 1
    assert len(got) == size and got[:valid] == raw[:valid] and got[valid:] == bytes(size-valid)
    result['ancestor_prefix_and_zero_tail'] = True
    result.update(timeline=tli, parent_timeline=old_tli, fork_lsn=fork, checkpoint_lsn=target,
                  captured_flush_lsn=flush, wal_segment_size=size, archive_mode=archive_mode)
    reject = command(['psql', '-XAt', '-h', '127.0.0.1', '-p', leader['port'],
                      '-U', 'wal_reader', '-d', 'postgres', '-c', 'SELECT 1'], check=False)
    assert reject.returncode != 0 and 'pg_hba.conf rejects connection' in reject.stdout, reject.stdout
    (RUN / 'ordinary-sql-rejected.log').write_text(reject.stdout)
    replica = configure(replica_dir, 'replica')
    removed = []
    for p in (replica_dir / 'pg_wal').iterdir():
        if p.is_file() and re.fullmatch(r'[0-9A-F]{24}(?:\.partial)?|[0-9A-F]{8}\.history', p.name):
            removed.append(p.name)
            p.unlink()
    result['replica_removed_local_wal'] = removed
    assert not (replica_dir / 'pg_wal' / history_name).exists()
    assert not (replica_dir / 'standby.signal').exists()
    requests = RUN / 'restore-requests.log'
    wrapper = RUN / 'restore-wrapper.sh'
    wrapper.write_text('#!/bin/sh\n'
        f'printf "REQUEST %s %s\\n" "$1" "$2" >> "{requests}"\n'
        f'PGHOST=127.0.0.1 PGPORT={leader["port"]} PGUSER=wal_reader "{helper}" {"-no-slot" if NO_SLOT else ""} -pgdata "{leader["scope"]}" -idle-timeout 30s -wal-segment-size {SEGMENT_MB}MB "$1" "$2"\n'
        'status=$?\n'
        f'printf "RESULT %s %s\\n" "$1" "$status" >> "{requests}"\n'
        'exit "$status"\n')
    wrapper.chmod(0o700)
    (replica_dir / 'postgresql.auto.conf').write_text(
        f"restore_command='\"{wrapper}\" %f %p'\nrecovery_target_timeline='{tli}'\n"
        f"recovery_target_lsn='{target}'\nrecovery_target_action='shutdown'\n"
        "primary_conninfo=''\nrecovery_prefetch=off\n")
    (replica_dir / 'recovery.signal').touch()
    (RUN / 'replica-before-start.json').write_text(json.dumps({
        'history_exists': False, 'standby_signal_exists': False, 'primary_conninfo': '',
        'auto_conf': (replica_dir / 'postgresql.auto.conf').read_text(),
        'pg_wal_files': sorted(p.name for p in (replica_dir / 'pg_wal').iterdir())}, indent=2))
    start(replica, wait=False)
    deadline = time.monotonic() + 75
    while time.monotonic() < deadline:
        log = replica['log'].read_text() if replica['log'].exists() else ''
        if 'database system is shut down' in log and not (replica_dir / 'postmaster.pid').exists():
            break
        time.sleep(0.2)
    else:
        raise RuntimeError('replica did not shut down within 75s; inspect replica.log')
    control = command(['pg_controldata', replica_dir]).stdout
    (RUN / 'replica-controldata.txt').write_text(control)
    logs = replica['log'].read_text()
    reqs = requests.read_text()
    assert re.search(r'Database cluster state:\s+shut down in recovery', control), control
    assert re.search(r'Latest checkpoint location:\s+' + re.escape(target) + r'\s', control), control
    assert re.search(r"Latest checkpoint's TimeLineID:\s+" + str(tli) + r'\s', control), control
    assert 'recovery stopping after WAL location (LSN)' in logs and target in logs, logs
    assert 'shutdown at recovery target' in logs, logs
    assert f'RESULT {history_name} 0' in reqs, reqs
    assert any(re.fullmatch(rf'RESULT {old_tli:08X}[0-9A-F]{{16}} 0', line) for line in reqs.splitlines()), reqs
    assert any(re.fullmatch(rf'RESULT {tli:08X}[0-9A-F]{{16}} 0', line) for line in reqs.splitlines()), reqs
    assert 'started streaming WAL' not in logs, logs
    after = sql(leader, 'SELECT pg_current_wal_flush_lsn();')
    assert lsn(after) // size == lsn(target) // size, (after, target)
    for cluster in (source, leader):
        own = [line for line in cluster['log'].read_text().splitlines() if ' wal-fetch-unix ' in line]
        assert any('IDENTIFY_SYSTEM' in line for line in own)
        assert not any('SHOW ' in line or 'SELECT ' in line for line in own), own
        if NO_SLOT:
            assert not any(re.search(r'CREATE_REPLICATION_SLOT|READ_REPLICATION_SLOT|\bSLOT\b', line) for line in own), own
    if NO_SLOT:
        assert sql(leader, 'SELECT count(*) FROM pg_replication_slots') == '0'
        for logpath in RUN.glob('*/.wal-fetch/server.log'):
            log = logpath.read_text()
            assert 'retention lost' not in log and 'retention floor advanced' not in log and 'temporary slot created' not in log
        result['slotless_zero_slots_and_no_slot_commands'] = True
    result['no_SHOW_or_ordinary_SQL'] = True
    result.update(status='PASS', final_leader_flush_lsn=after,
                  new_history_fetched=True, ancestor_wal_fetched=True,
                  new_timeline_wal_fetched=True, first_new_timeline_active_segment=True,
                  ordinary_sql_rejected=True, recovery_target_shutdown=True)
except (Exception, KeyboardInterrupt) as exc:
    result.update(status='FAIL', error=redact(str(exc)))
finally:
    cleanup_errors = []
    for scope in scopes:
        try:
            pidfile = scope / '.wal-fetch/server.pid'
            if pidfile.exists():
                pid = int(pidfile.read_text())
                if Path(f'/proc/{pid}/exe').resolve() == helper.resolve():
                    os.kill(pid, signal.SIGTERM)
        except ProcessLookupError:
            pass
        except Exception as exc:
            cleanup_errors.append(redact(f'{scope.name}: {exc}'))
    deadline = time.monotonic() + 5
    while any((scope / '.wal-fetch/server.sock').exists() for scope in scopes) and time.monotonic() < deadline:
        time.sleep(0.05)
    for cluster in reversed(clusters):
        try:
            stop(cluster)
        except Exception as exc:
            cleanup_errors.append(redact(f'{cluster["label"]}: {exc}'))
    result['all_owned_clusters_stopped'] = all(not (c['data'] / 'postmaster.pid').exists() for c in clusters)
    result['all_owned_unix_sockets_removed'] = all(not (scope / '.wal-fetch/server.sock').exists() for scope in scopes)
    if not result['all_owned_clusters_stopped'] or not result['all_owned_unix_sockets_removed']:
        cleanup_errors.append('owned process cleanup incomplete')
    if cleanup_errors:
        result.update(status='FAIL', cleanup_errors=cleanup_errors)
    transcript.close()
    # An allowlist, redaction and bounded tails keep CI artifacts small and safe.
    out = EVIDENCE / RUN.name
    out.mkdir(parents=True, exist_ok=True)
    names = ('commands.log', 'source.log', 'leader.log', 'replica.log', 'custom-server.log',
             'restore-requests.log', 'replica-controldata.txt', 'ordinary-sql-rejected.log')
    logs = [(RUN / name, name) for name in names]
    logs += [(scope / '.wal-fetch/server.log', scope.name + '-server.log') for scope in scopes]
    for source_log, name in logs:
        if source_log.is_file():
            with source_log.open('rb') as stream:
                stream.seek(max(0, source_log.stat().st_size - 65536))
                (out / name).write_text(redact(stream.read().decode('utf-8', errors='replace')))
    # Never remove a live cluster's data. Failed cleanup is nonzero, with evidence.
    if result['all_owned_clusters_stopped'] and result['all_owned_unix_sockets_removed']:
        try:
            shutil.rmtree(RUN)
        except OSError as exc:
            result.update(status='FAIL', cleanup_errors=cleanup_errors + [redact(str(exc))])
    (out / 'result.json').write_text(json.dumps(result, indent=2) + '\n')
    print(json.dumps(result, indent=2), flush=True)
    print(f'DIAGNOSTICS={out}', flush=True)

raise SystemExit(0 if result['status'] == 'PASS' else 1)
