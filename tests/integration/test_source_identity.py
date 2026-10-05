"""Keep the source cluster pinned while allowing its timeline to change."""
from test_recovery import promote_backup


def use_source_port(fx, cluster, source):
    cluster['port'] = source['port']
    with (cluster['data'] / 'postgresql.conf').open('a') as config:
        config.write(f"\nport={cluster['port']}\n")


def test_different_system_id_is_rejected(fx, source, closed, session):
    system_id = fx.sql(source, 'SELECT system_identifier FROM pg_control_system()')
    pidfile = source['scope'] / '.wal-fetch/server.pid'
    pid = pidfile.read_text()
    original = (source['data'] / 'pg_wal' / closed).read_bytes()
    fx.stop(source)

    data = fx.run / 'replacement'
    fx.command(['initdb', '-D', data, '-U', 'postgres', '-A', 'trust',
                '--no-locale', f'--wal-segsize={fx.segment_mb}'])
    replacement = fx.configure(data, 'replacement')
    use_source_port(fx, replacement, source)
    fx.start(replacement)
    fx.sql(replacement, "CREATE ROLE wal_reader LOGIN REPLICATION PASSWORD 'disposable-e2e-password'; "
                       "CREATE TABLE replacement_marker AS SELECT generate_series(1,10000);")
    fx.sql(replacement, 'SELECT pg_switch_wal()')
    assert fx.sql(replacement, 'SELECT system_identifier FROM pg_control_system()') != system_id
    assert (data / 'pg_wal' / closed).read_bytes() != original

    dest = fx.run / 'rejected.wal'
    for attempt, name in enumerate([closed, closed, '00000002.history', closed]):
        dest.write_bytes(b'ARCHIVE FALLBACK')
        failed = fx.fetch(replacement, name, dest, check=False, scope=source['scope'])
        assert failed.returncode != 0, failed.stdout
        assert dest.read_bytes() == b'ARCHIVE FALLBACK'
        if attempt:
            assert 'source system identifier changed' in failed.stdout, failed.stdout
        assert pidfile.read_text() == pid
    fx.result['different_system_id_rejected_without_respawn_or_publication'] = True


def test_same_system_timeline_change_restarts_server(fx, source, session):
    system_id = fx.sql(source, 'SELECT system_identifier FROM pg_control_system()')
    pidfile = source['scope'] / '.wal-fetch/server.pid'
    pid = pidfile.read_text()
    leader, _ = promote_backup(fx, source)
    fx.stop(leader)
    use_source_port(fx, leader, source)
    fx.start(leader)
    assert fx.sql(leader, 'SELECT system_identifier FROM pg_control_system()') == system_id
    timeline = int(fx.sql(leader, 'SELECT timeline_id FROM pg_control_checkpoint()'))
    assert timeline > 1

    name = f'{timeline:08X}.history'
    dest = fx.run / name
    fx.fetch(leader, name, dest, scope=source['scope'])
    assert dest.read_bytes() == (leader['data'] / 'pg_wal' / name).read_bytes()
    assert pidfile.read_text() != pid
    name = fx.sql(leader, 'SELECT pg_walfile_name(pg_current_wal_insert_lsn())')
    fx.sql(leader, 'SELECT pg_switch_wal()')
    fx.fetch(leader, name, dest, scope=source['scope'])
    assert dest.read_bytes() == (leader['data'] / 'pg_wal' / name).read_bytes()
    fx.result['same_system_timeline_change_autorespawns_and_delivers_wal'] = True
