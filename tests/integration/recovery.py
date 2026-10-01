"""Promote a backup, fetch its timeline, then restore to its checkpoint."""
import json
import re
import shutil
import time

from fixture import lsn, wait_for


def promote_backup(fx, source):
    source_dir = source['data']
    base = fx.run / 'base'
    fx.command(['pg_basebackup', '-h', source['sock'], '-p', source['port'], '-U', 'postgres',
             '-D', base, '-X', 'stream', '-c', 'fast'], timeout=90)
    # Backup termination already switches source WAL; this checkpoint makes the
    # promotion occur later than backup redo, so recovery needs an ancestor WAL.
    fx.sql(source, 'CHECKPOINT;')
    source_target = fx.sql(source, 'SELECT checkpoint_lsn FROM pg_control_checkpoint();')
    fx.stop(source)
    leader_dir, replica_dir = fx.run / 'leader', fx.run / 'replica'
    shutil.copytree(base, leader_dir)
    shutil.copytree(base, replica_dir)
    for p in (source_dir / 'pg_wal').iterdir():
        if p.is_file() and re.fullmatch(r'[0-9A-F]{24}', p.name):
            shutil.copy2(p, leader_dir / 'pg_wal' / p.name)
    leader = fx.configure(leader_dir, 'leader')
    (leader_dir / 'postgresql.auto.conf').write_text(
        f"restore_command='false'\nrecovery_target_lsn='{source_target}'\nrecovery_target_action='promote'\n")
    (leader_dir / 'recovery.signal').touch()
    fx.start(leader)
    # pg_ctl readiness may mean hot standby; promotion must finish before writes.
    wait_for(lambda: fx.sql(leader, 'SELECT pg_is_in_recovery();') == 'f', seconds=30)
    fx.sql(leader, "INSERT INTO e2e_marker VALUES(2,'after promotion'); CHECKPOINT;")
    return leader, replica_dir


def capture_target(fx, leader):
    leader_dir = leader['data']
    capture = fx.sql(leader, "SELECT timeline_id, checkpoint_lsn, pg_current_wal_flush_lsn(), "
                         "pg_size_bytes(current_setting('wal_segment_size')), "
                         "current_setting('archive_mode') FROM pg_control_checkpoint();")
    tli_str, checkpoint, flush, size_str, archive_mode = capture.split('|')
    tli, size = int(tli_str), int(size_str)
    assert tli > 1 and archive_mode == 'off', capture
    history_name = f'{tli:08X}.history'
    history = (leader_dir / 'pg_wal' / history_name).read_text()
    (fx.run / 'leader.history').write_text(history)
    branch = [line.split() for line in history.splitlines() if line and not line.startswith('#')][-1]
    old_tli, fork = int(branch[0]), branch[1]
    assert lsn(checkpoint) // size == lsn(fork) // size, (checkpoint, fork)
    assert lsn(flush) // size == lsn(checkpoint) // size and lsn(flush) % size != 0, (flush, checkpoint)
    target = dict(timeline=tli, parent_timeline=old_tli, fork_lsn=fork, checkpoint_lsn=checkpoint,
                  captured_flush_lsn=flush, wal_segment_size=size, archive_mode=archive_mode)
    fx.result.update(target)
    return target


def history_and_ancestor(fx, source, leader, target):
    source_dir, leader_dir = source['data'], leader['data']
    size = target['wal_segment_size']
    old_tli, fork = target['parent_timeline'], target['fork_lsn']
    history_name = f"{target['timeline']:08X}.history"
    history_result = fx.run / 'fetched.history'
    fx.fetch(leader, history_name, history_result)
    assert history_result.read_bytes() == (leader_dir / 'pg_wal' / history_name).read_bytes()
    fx.result['history_exact_compare'] = True
    fork_pos = lsn(fork)
    ancestor_pos = fork_pos - 1
    ancestor_name = f'{old_tli:08X}{ancestor_pos >> 32:08X}{(ancestor_pos & 0xffffffff) // size:08X}'
    ancestor_result = fx.run / 'ancestor.wal'
    fx.fetch(leader, ancestor_name, ancestor_result)
    raw = (source_dir / 'pg_wal' / ancestor_name).read_bytes()
    got = ancestor_result.read_bytes()
    valid = ancestor_pos % size + 1
    assert len(got) == size and got[:valid] == raw[:valid] and got[valid:] == bytes(size-valid)
    fx.result['ancestor_prefix_and_zero_tail'] = True


def ordinary_sql_rejected(fx, leader):
    reject = fx.command(['psql', '-XAt', '-h', '127.0.0.1', '-p', leader['port'],
                      '-U', 'wal_reader', '-d', 'postgres', '-c', 'SELECT 1'], check=False)
    assert reject.returncode != 0 and 'pg_hba.conf rejects connection' in reject.stdout, reject.stdout
    (fx.run / 'ordinary-sql-rejected.log').write_text(reject.stdout)


def restore_replica(fx, leader, replica_dir, target):
    tli, checkpoint = target['timeline'], target['checkpoint_lsn']
    history_name = f'{tli:08X}.history'
    replica = fx.configure(replica_dir, 'replica')
    removed = []
    for p in (replica_dir / 'pg_wal').iterdir():
        if p.is_file() and re.fullmatch(r'[0-9A-F]{24}(?:\.partial)?|[0-9A-F]{8}\.history', p.name):
            removed.append(p.name)
            p.unlink()
    fx.result['replica_removed_local_wal'] = removed
    assert not (replica_dir / 'pg_wal' / history_name).exists()
    assert not (replica_dir / 'standby.signal').exists()
    requests = fx.run / 'restore-requests.log'
    wrapper = fx.run / 'restore-wrapper.sh'
    wrapper.write_text('#!/bin/sh\n'
        f'printf "REQUEST %s %s\\n" "$1" "$2" >> "{requests}"\n'
        f'PGHOST=127.0.0.1 PGPORT={leader["port"]} PGUSER=wal_reader "{fx.binary}" '
        f'{"-no-slot" if fx.no_slot else ""} -pgdata "{leader["scope"]}" '
        f'-idle-timeout 30s -wal-segment-size {fx.segment_mb}MB "$1" "$2"\n'
        'status=$?\n'
        f'printf "RESULT %s %s\\n" "$1" "$status" >> "{requests}"\n'
        'exit "$status"\n')
    wrapper.chmod(0o700)
    (replica_dir / 'postgresql.auto.conf').write_text(
        f"restore_command='\"{wrapper}\" %f %p'\nrecovery_target_timeline='{tli}'\n"
        f"recovery_target_lsn='{checkpoint}'\nrecovery_target_action='shutdown'\n"
        "primary_conninfo=''\nrecovery_prefetch=off\n")
    (replica_dir / 'recovery.signal').touch()
    (fx.run / 'replica-before-start.json').write_text(json.dumps({
        'history_exists': False, 'standby_signal_exists': False, 'primary_conninfo': '',
        'auto_conf': (replica_dir / 'postgresql.auto.conf').read_text(),
        'pg_wal_files': sorted(p.name for p in (replica_dir / 'pg_wal').iterdir())}, indent=2))
    fx.start(replica, wait=False)
    deadline = time.monotonic() + 75
    while time.monotonic() < deadline:
        log = replica['log'].read_text() if replica['log'].exists() else ''
        if 'database system is shut down' in log and not (replica_dir / 'postmaster.pid').exists():
            break
        time.sleep(0.2)
    else:
        raise RuntimeError('replica did not shut down within 75s; inspect replica.log')
    return replica


def verify_checkpoint(fx, leader, replica, target):
    replica_dir = replica['data']
    checkpoint, tli = target['checkpoint_lsn'], target['timeline']
    old_tli, size = target['parent_timeline'], target['wal_segment_size']
    history_name = f'{tli:08X}.history'
    requests = fx.run / 'restore-requests.log'
    control = fx.command(['pg_controldata', replica_dir]).stdout
    (fx.run / 'replica-controldata.txt').write_text(control)
    logs = replica['log'].read_text()
    reqs = requests.read_text()
    assert re.search(r'Database cluster state:\s+shut down in recovery', control), control
    assert re.search(r'Latest checkpoint location:\s+' + re.escape(checkpoint) + r'\s', control), control
    assert re.search(r"Latest checkpoint's TimeLineID:\s+" + str(tli) + r'\s', control), control
    assert 'recovery stopping after WAL location (LSN)' in logs and checkpoint in logs, logs
    assert 'shutdown at recovery target' in logs, logs
    assert f'RESULT {history_name} 0' in reqs, reqs
    assert any(re.fullmatch(rf'RESULT {old_tli:08X}[0-9A-F]{{16}} 0', line) for line in reqs.splitlines()), reqs
    assert any(re.fullmatch(rf'RESULT {tli:08X}[0-9A-F]{{16}} 0', line) for line in reqs.splitlines()), reqs
    assert 'started streaming WAL' not in logs, logs
    after = fx.sql(leader, 'SELECT pg_current_wal_flush_lsn();')
    assert lsn(after) // size == lsn(checkpoint) // size, (after, checkpoint)
    fx.result.update(final_leader_flush_lsn=after, new_history_fetched=True, ancestor_wal_fetched=True,
                     new_timeline_wal_fetched=True, first_new_timeline_active_segment=True,
                     ordinary_sql_rejected=True, recovery_target_shutdown=True)


def replication_commands_only(fx, source, leader):
    for cluster in (source, leader):
        own = [line for line in cluster['log'].read_text().splitlines() if ' wal-fetch-unix ' in line]
        assert any('IDENTIFY_SYSTEM' in line for line in own)
        assert not any('SHOW ' in line or 'SELECT ' in line for line in own), own
        if fx.no_slot:
            assert not any(re.search(r'CREATE_REPLICATION_SLOT|READ_REPLICATION_SLOT|\bSLOT\b', line)
                           for line in own), own
    if fx.no_slot:
        assert fx.sql(leader, 'SELECT count(*) FROM pg_replication_slots') == '0'
        for logpath in fx.run.glob('*/.wal-fetch/server.log'):
            log = logpath.read_text()
            assert ('retention lost' not in log and 'retention floor advanced' not in log
                    and 'temporary slot created' not in log)
        fx.result['slotless_zero_slots_and_no_slot_commands'] = True
    fx.result['no_SHOW_or_ordinary_SQL'] = True
