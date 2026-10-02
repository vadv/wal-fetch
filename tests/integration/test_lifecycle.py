"""TEMP retention/ACK rules, session failures, timeouts and Unix-server lifetime."""
import concurrent.futures
import hashlib
import json
import os
import re
import signal
import socket
import time

import pytest

from fixture import lsn, wait_for


def slot_state(fx, source, scope=None):
    scope = scope or source['scope']
    log = (scope / '.wal-fetch/server.log').read_text()
    name = re.findall(r'temporary slot created name=(wal_fetch_[0-9a-f]+)', log)[-1]
    row = fx.sql(source, f"SELECT active_pid, restart_lsn FROM pg_replication_slots WHERE slot_name='{name}'")
    return name, row


def wal_name(segment, size):
    pos = segment * size
    return f'00000001{pos >> 32:08X}{(pos & 0xffffffff) // size:08X}'


def slot_progress(fx, source):
    unchanged = fx.run / 'unchanged'
    fx.sql(source, 'CHECKPOINT;')
    active_name = fx.sql(source, 'SELECT pg_walfile_name(pg_current_wal_flush_lsn())')
    fx.fetch(source, active_name, fx.run / 'slot-active')
    slot, row = slot_state(fx, source)
    owner, initial = row.split('|')
    size = fx.segment_mb << 20
    first = lsn(initial) // size
    for _ in range(4):
        fx.sql(source, 'INSERT INTO wal_fixture VALUES (42);')
        fx.sql(source, 'SELECT pg_switch_wal();')
    floors = []
    for seg in range(first, first + 4):
        fx.fetch(source, wal_name(seg, size), fx.run / f'progress-{seg}.wal')
        wait_for(lambda: lsn(slot_state(fx, source)[1].split('|')[1]) >= max(lsn(initial), seg * size))
        current_slot, row = slot_state(fx, source)
        current_owner, floor = row.split('|')
        assert current_slot == slot and current_owner == owner, (current_slot, current_owner)
        floors.append(lsn(floor))
    assert floors == sorted(floors) and floors[-1] == (first + 3) * size, floors
    fx.fetch(source, wal_name(first, size), fx.run / 'repeat-old')
    assert lsn(slot_state(fx, source)[1].split('|')[1]) == floors[-1]
    fx.fetch(source, '00000003.history', unchanged, check=False)
    assert slot_state(fx, source)[0] == slot and slot_state(fx, source)[1]
    fx.result['same_owner_CopyDone_reuse_and_three_segment_advance'] = floors
    fx.result['old_repeat_and_missing_history_preserve_slot'] = True

    before_rejection = slot_state(fx, source)
    for probe in [wal_name(first + 50, size), '00000002' + wal_name(first, size)[8:]]:
        unchanged.write_bytes(b'ORIGINAL')
        failed = fx.fetch(source, probe, unchanged, check=False)
        assert failed.returncode != 0 and unchanged.read_bytes() == b'ORIGINAL'
        assert slot_state(fx, source) == before_rejection, probe
    fx.result['future_and_nonancestor_requests_preserve_slot_owner_and_floor'] = True

    return first


@pytest.mark.skipif(os.environ.get('WAL_FETCH_TEST_NO_SLOT') == '1', reason='requires a TEMP slot')
def test_slot_progress_and_publication_acknowledgements(fx, source, closed):
    first = slot_progress(fx, source)
    size = fx.segment_mb << 20
    unchanged = fx.run / 'unchanged'
    # Local protocol negative cases on a real owner: no publication ACK, no advance.
    # This canonical digest matches the documented effective connection identity
    # in our controlled environment; it contains no credential in the wire request.
    identity_values = ['127.0.0.1', source['port'], 'wal_reader', fx.env['PGPASSWORD'],
                       'replication', {'application_name':'wal-fetch-unix', 'replication':'yes'},
                       size, '30s', str(source['scope'].resolve()), fx.no_slot,
                       str(source['scope'] / '.wal-fetch/server.log'), False, 5]
    for key in ('PGSSLMODE', 'PGSSLROOTCERT', 'PGSSLCERT', 'PGSSLKEY', 'PGSSLCRL',
                'PGSSLSNI', 'PGCHANNELBINDING', 'PGSERVICE', 'PGSERVICEFILE'):
        identity_values.extend((key, fx.env.get(key, '')))
    identity = hashlib.sha256(json.dumps(identity_values, sort_keys=True, separators=(',', ':')).encode()).hexdigest()
    fx.sql(source, 'INSERT INTO wal_fixture VALUES (43);')
    candidate_name = wal_name(first + 4, size)
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
            if mode == 'wrong-id':
                ack['id'] = '0'*32
            if mode == 'wrong-version':
                ack['version'] = 2
            if mode == 'not-published':
                ack['published'] = False
            if mode == 'injected-lsn':
                ack['lsn'] = 'FFFFFFFF/FFFFFFFF'
            if mode != 'no-ack':
                conn.sendall(b'{broken}\n' if mode == 'malformed' else json.dumps(ack).encode()+b'\n')
            stream.close()
        fx.fetch(source, '00000003.history', unchanged, check=False) # serial barrier
        assert lsn(slot_state(fx, source)[1].split('|')[1]) == (first + 3) * size
    fx.fetch(source, candidate_name, fx.run / 'valid-publication-after-invalid-acks')
    wait_for(lambda: lsn(slot_state(fx, source)[1].split('|')[1]) == (first + 4) * size)
    fx.result['invalid_or_missing_ACK_never_advances'] = True


@pytest.mark.skipif(os.environ.get('WAL_FETCH_TEST_NO_SLOT') == '1', reason='requires a TEMP slot')
def test_owner_failure(fx, source, closed, session):
    unchanged = fx.run / 'unchanged'
    slot, _ = slot_state(fx, source)
    # An ERROR on the actual owning session deletes TEMPORARY; next call is fresh.
    unchanged.write_bytes(b'ORIGINAL')
    failed = fx.fetch(source, '000000010000000000000000', unchanged, check=False)
    assert failed.returncode and unchanged.read_bytes() == b'ORIGINAL'
    wait_for(lambda: fx.sql(source, f"SELECT count(*) FROM pg_replication_slots WHERE slot_name='{slot}'") == '0')
    fx.fetch(source, closed, fx.run / 'after-owner-error')
    new_slot, row = slot_state(fx, source)
    assert new_slot != slot
    fx.sql(source, f"SELECT pg_terminate_backend({row.split('|')[0]})")
    assert fx.fetch(source, closed, unchanged, check=False).returncode != 0
    fx.fetch(source, closed, fx.run / 'after-owner-disconnect')
    assert 'retention lost' in (source['scope'] / '.wal-fetch/server.log').read_text()
    fx.result['owner_error_disconnect_and_honest_reconnect'] = True


def test_client_timeout(fx, source, closed, session):
    unchanged = fx.run / 'unchanged'
    # Server cannot race a fallback write: it has never received destination.
    pid = int((source['scope'] / '.wal-fetch/server.pid').read_text())
    os.kill(pid, signal.SIGSTOP)
    try:
        unchanged.write_bytes(b'ORIGINAL')
        failed = fx.fetch(source, closed, unchanged, check=False, env_patch={'WAL_FETCH_TIMEOUT':'0.1'})
        assert failed.returncode != 0 and unchanged.read_bytes() == b'ORIGINAL'
        unchanged.write_bytes(b'FALLBACK')
    finally:
        os.kill(pid, signal.SIGCONT)
    time.sleep(0.25)
    assert unchanged.read_bytes() == b'FALLBACK'
    fx.result['client_timeout_no_late_destination_write'] = True


def test_stale_socket(fx, source, closed, session):
    pid = int((source['scope'] / '.wal-fetch/server.pid').read_text())
    # SIGKILL leaves a stale socket; lifetime flock releases and one replacement starts.
    os.kill(pid, signal.SIGKILL)
    with concurrent.futures.ThreadPoolExecutor(max_workers=6) as pool:
        list(pool.map(lambda i: fx.fetch(source, closed, fx.run / f'stale-{i}.wal'), range(6)))
    assert int((source['scope'] / '.wal-fetch/server.pid').read_text()) != pid
    fx.result['stale_socket_concurrent_respawn'] = True


def test_log_rotation(fx, source, closed, session):
    scope = source['scope']
    log = scope / '.wal-fetch/server.log'
    pid = int((scope / '.wal-fetch/server.pid').read_text())
    # Rename first, then signal; the server reopens the same path in place.
    rotated = scope / '.wal-fetch/server.log.1'
    log.rename(rotated)
    os.kill(pid, signal.SIGUSR1)
    wait_for(lambda: log.exists() and 'log file reopened' in log.read_text())
    fx.fetch(source, closed, fx.run / 'rotated.wal')
    fresh = log.read_text()
    assert f'fetched name={closed} bytes={fx.segment_mb << 20}' in fresh
    assert f'fetched name={closed}' not in rotated.read_text()
    assert (log.stat().st_mode & 0o777) == 0o600
    fx.result['sigusr1_reopens_log_without_restart'] = True


@pytest.mark.skipif(os.environ.get('WAL_FETCH_TEST_NO_SLOT') == '1', reason='requires a TEMP slot')
def test_idle_and_queue(fx, source, closed):
    # Idle never cancels active or queued work. Pause ONLY our private WAL sender.
    idle_scope = fx.new_scope('idle-client')
    fx.fetch(source, closed, fx.run / 'idle-warm', scope=idle_scope, idle='300ms')
    idle_pid = int((idle_scope / '.wal-fetch/server.pid').read_text())
    idle_slot, row = slot_state(fx, source, idle_scope)
    backend = int(row.split('|')[0])
    os.kill(backend, signal.SIGSTOP)
    try:
        with concurrent.futures.ThreadPoolExecutor(max_workers=2) as pool:
            jobs = [pool.submit(fx.fetch, source, closed, fx.run / f'idle-active-{i}',
                                scope=idle_scope, idle='300ms') for i in range(2)]
            time.sleep(0.8)
            assert (idle_scope / '.wal-fetch/server.sock').exists()
            os.kill(backend, signal.SIGCONT)
            for job in jobs:
                job.result()
    finally:
        try:
            os.kill(backend, signal.SIGCONT)
        except ProcessLookupError:
            pass
    wait_for(lambda: not (idle_scope / '.wal-fetch/server.sock').exists())
    # Closing the connection precedes PostgreSQL processing its disconnect.
    wait_for(lambda: fx.sql(source, f"SELECT count(*) FROM pg_replication_slots WHERE slot_name='{idle_slot}'") == '0')
    fx.fetch(source, closed, fx.run / 'idle-respawn', scope=idle_scope, idle='300ms')
    assert int((idle_scope / '.wal-fetch/server.pid').read_text()) != idle_pid
    fx.result['idle_active_queue_cleanup_and_autorespawn'] = True


@pytest.mark.skipif(os.environ.get('WAL_FETCH_TEST_NO_SLOT') != '1', reason='requires -no-slot')
def test_slotless_session_reuse(fx, source, closed, session):
    unchanged = fx.run / 'unchanged'
    owner_query = "SELECT pid FROM pg_stat_replication WHERE application_name='wal-fetch-unix'"
    owner = fx.sql(source, owner_query)
    assert owner.isdigit(), owner
    fx.fetch(source, closed, fx.run / 'reuse-slotless')
    assert fx.sql(source, owner_query) == owner
    fx.sql(source, f'SELECT pg_terminate_backend({owner})')
    assert fx.fetch(source, closed, unchanged, check=False).returncode != 0
    fx.fetch(source, closed, fx.run / 'reconnect-slotless')
    assert fx.sql(source, owner_query) != owner
    assert fx.sql(source, 'SELECT count(*) FROM pg_replication_slots') == '0'
    fx.result['slotless_same_owner_CopyDone_reuse_and_reconnect'] = True


@pytest.mark.skipif(os.environ.get('WAL_FETCH_TEST_NO_SLOT') == '1', reason='requires a TEMP slot')
@pytest.mark.parametrize('missing', ['future', 'removed'])
def test_repeated_wal_misses_release_slot(fx, source, closed, session, missing):
    name = wal_name(1000, fx.segment_mb << 20) if missing == 'future' else wal_name(0, fx.segment_mb << 20)
    dest = fx.run / 'unavailable.wal'
    original_slot = slot_state(fx, source)[0]

    def reject(filename):
        dest.write_bytes(b'ORIGINAL')
        failed = fx.fetch(source, filename, dest, check=False)
        assert failed.returncode != 0 and dest.read_bytes() == b'ORIGINAL'
        if filename == name and missing == 'removed':
            assert 'SQLSTATE 58P01' in failed.stdout, failed.stdout

    # A published WAL resets four misses; history probes do not change the count.
    for _ in range(4):
        reject(name)
    reject('00000003.history')
    if missing == 'future':
        assert slot_state(fx, source)[0] == original_slot and slot_state(fx, source)[1]
    else:
        wait_for(lambda: fx.sql(source, 'SELECT count(*) FROM pg_replication_slots') == '0')
    fx.fetch(source, closed, fx.run / 'reset-failures.wal')
    reset_slot = slot_state(fx, source)[0]
    for _ in range(4):
        reject(name)
    reject('00000003.history')
    if missing == 'future':
        assert slot_state(fx, source)[0] == reset_slot and slot_state(fx, source)[1]
    reject(name)
    wait_for(lambda: fx.sql(source, 'SELECT count(*) FROM pg_replication_slots') == '0')

    # Retries, including PostgreSQL errors on the owning connection, stay slotless.
    creates = source['log'].read_text().count('CREATE_REPLICATION_SLOT')
    for retry in (name, '00000003.history', wal_name(0, fx.segment_mb << 20), name):
        reject(retry)
        assert fx.sql(source, 'SELECT count(*) FROM pg_replication_slots') == '0'
    assert source['log'].read_text().count('CREATE_REPLICATION_SLOT') == creates

    # The successful probe publishes exact bytes; only the following call gets a slot.
    fx.fetch(source, closed, dest)
    assert dest.read_bytes() == (source['data'] / 'pg_wal' / closed).read_bytes()
    assert fx.sql(source, 'SELECT count(*) FROM pg_replication_slots') == '0'
    fx.fetch(source, closed, fx.run / 'retention-restored.wal')
    assert fx.sql(source, 'SELECT count(*) FROM pg_replication_slots') == '1'
    assert slot_state(fx, source)[0] != reset_slot
    fx.result['repeated_misses_release_probe_and_rearm'] = True


@pytest.mark.skipif(os.environ.get('WAL_FETCH_TEST_NO_SLOT') == '1', reason='requires a TEMP slot')
@pytest.mark.parametrize('limit', [0, 2])
def test_slot_failure_limit_option(fx, source, closed, limit):
    dest = fx.run / 'limit-option.wal'
    fx.fetch(source, closed, dest, slot_failure_limit=limit)
    original = slot_state(fx, source)
    mismatch = fx.fetch(source, closed, dest, check=False)
    assert mismatch.returncode and 'configuration mismatch' in mismatch.stdout
    for _ in range(6 if limit == 0 else limit):
        failure = fx.fetch(source, wal_name(1000, fx.segment_mb << 20), dest,
                           check=False, slot_failure_limit=limit)
        assert failure.returncode != 0
    if limit == 0:
        assert slot_state(fx, source) == original and original[1]
    else:
        wait_for(lambda: fx.sql(source, 'SELECT count(*) FROM pg_replication_slots') == '0')
    fx.result['slot_failure_limit_override_or_disabled'] = True
