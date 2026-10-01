"""WAL bytes, replication credentials, errors and custom logging."""
import concurrent.futures
import os
import re
import signal
import time

from fixture import lsn, wait_for


def test_completed_wal(fx, source, closed):
    source_dir = source['data']
    completed = fx.run / 'completed.wal'
    with concurrent.futures.ThreadPoolExecutor(max_workers=8) as pool:
        list(pool.map(lambda i: fx.fetch(source, closed, fx.run / f'concurrent-{i}.wal'), range(8)))
    int((source['scope'] / '.wal-fetch/server.pid').read_text())
    assert fx.sql(source, 'SELECT count(*) FROM pg_replication_slots') == ('0' if fx.no_slot else '1')
    fx.result['concurrent_coldstart_one_server_expected_slots'] = True
    fx.fetch(source, closed, completed)
    assert completed.read_bytes() == (source_dir / 'pg_wal' / closed).read_bytes()
    fx.result['completed_exact_compare'] = True


def test_quiet_active_wal(fx, source):
    source_dir = source['data']
    fx.sql(source, 'CHECKPOINT;')
    quiet = fx.sql(source, "SELECT pg_current_wal_flush_lsn(), "
                   "pg_walfile_name(pg_current_wal_flush_lsn()), "
                   "pg_size_bytes(current_setting('wal_segment_size'));")
    quiet_flush, quiet_name, quiet_size = quiet.split('|')
    active = fx.run / 'active.wal'
    begin = time.monotonic()
    fx.fetch(source, quiet_name, active, env_patch={'WAL_FETCH_TIMEOUT': '5'}, timeout=10)
    fx.result['quiet_active_seconds'] = time.monotonic() - begin
    after = fx.sql(source, 'SELECT pg_current_wal_flush_lsn();')
    assert quiet_flush == after, (quiet_flush, after)
    raw = (source_dir / 'pg_wal' / quiet_name).read_bytes()
    got = active.read_bytes()
    offset = lsn(quiet_flush) % int(quiet_size)
    assert offset > 0 and len(got) == len(raw) == int(quiet_size)
    assert got[:offset] == raw[:offset] and got[offset:] == bytes(len(got)-offset)
    fx.result['quiet_active_prefix_and_zero_tail'] = True


def test_credentials_and_errors(fx, source, closed, session):
    completed = source['data'] / 'pg_wal' / closed
    pgpass = fx.run / 'pgpass'
    pgpass.write_text(f'127.0.0.1:{source["port"]}:replication:wal_reader:disposable-e2e-password\n')
    pgpass.chmod(0o600)
    passfile_result = fx.run / 'passfile.wal'
    fx.fetch(source, closed, passfile_result, env_patch={'PGPASSWORD': None, 'PGPASSFILE': str(pgpass)})
    assert passfile_result.read_bytes() == completed.read_bytes()
    fx.result['replication_pgpass'] = True
    unchanged = fx.run / 'unchanged'
    for name, env_patch in [
        (closed, {'PGPASSWORD': 'wrong-password'}),
        ('000000010000000000000000', {}),
        ('000000010000000000000FFF', {}),
        ('00000003.history', {}),
    ]:
        unchanged.write_bytes(b'ORIGINAL')
        failure = fx.fetch(source, name, unchanged, check=False, env_patch=env_patch)
        assert failure.returncode != 0 and unchanged.read_bytes() == b'ORIGINAL', (name, failure.stdout)
        assert 'disposable-e2e-password' not in failure.stdout
    fx.result['missing_auth_atomicity'] = True
    bad_scope = fx.new_scope('bad-auth')
    assert fx.fetch(source, closed, unchanged, check=False, scope=bad_scope,
                 env_patch={'PGPASSWORD':'wrong-password'}).returncode != 0
    assert 'wrong-password' not in (bad_scope / '.wal-fetch/server.log').read_text()
    fx.result['fresh_server_auth_failure_sanitized'] = True
    mismatch_env = dict(fx.env, PGHOST='127.0.0.1', PGPORT=str(source['port']), PGUSER='wal_reader')
    mismatch = fx.command(
        [fx.binary, '-pgdata', source['scope'], '-idle-timeout', '30s',
         '-wal-segment-size', '64MB' if fx.segment_mb != 64 else '16MB', closed, unchanged],
        env=mismatch_env, check=False)
    assert mismatch.returncode and 'configuration mismatch' in mismatch.stdout
    fx.result['different_segment_config_rejected'] = True
    mismatch = fx.command([fx.binary, '-pgdata', source['scope'], '-idle-timeout', '30s',
                        '-wal-segment-size', f'{fx.segment_mb}MB',
                        '-no-slot=' + str(not fx.no_slot).lower(), closed, unchanged], env=mismatch_env, check=False)
    assert mismatch.returncode and 'configuration mismatch' in mismatch.stdout
    fx.result['different_slot_mode_rejected'] = True


def test_custom_log(fx, source, closed):
    completed = source['data'] / 'pg_wal' / closed
    unchanged = fx.run / 'unchanged'
    log_scope = fx.new_scope('log-client')
    custom_log = fx.run / 'custom-server.log'
    fx.fetch(source, closed, fx.run / 'log-success', scope=log_scope, log_file=os.path.relpath(custom_log))
    assert (fx.run / 'log-success').read_bytes() == completed.read_bytes()
    probe = '000000010000000000000FFF'
    assert fx.fetch(source, probe, unchanged, check=False, scope=log_scope, log_file=custom_log).returncode != 0
    mismatch = fx.fetch(source, closed, unchanged, check=False, scope=log_scope, log_file=fx.run / 'other.log')
    assert mismatch.returncode and 'configuration mismatch' in mismatch.stdout
    os.kill(int((log_scope / '.wal-fetch/server.pid').read_text()), signal.SIGTERM)
    wait_for(lambda: not (log_scope / '.wal-fetch/server.sock').exists())
    log_text = custom_log.read_text()
    assert re.search(r'\d{4}/\d{2}/\d{2} \d{2}:\d{2}:\d{2} wal-fetch\[\d+\]', log_text), log_text
    assert f'fetched name={closed} bytes={fx.segment_mb << 20}' in log_text
    assert 'rejected error=' in log_text and f'name={probe}' in log_text
    assert 'server started' in log_text and 'server stopped' in log_text
    assert fx.env['PGPASSWORD'] not in log_text and 'password=' not in log_text and 'host=' not in log_text
    assert custom_log.stat().st_mode & 0o777 == 0o600
    assert not (log_scope / '.wal-fetch/server.log').exists()
    fx.result['custom_log_success_failure_identity_and_no_secrets'] = True
