#!/usr/bin/env python3
"""Run private PostgreSQL 16 recovery scenarios; no existing services are used."""
import signal
import traceback

import delivery
import lifecycle
import recovery
from fixture import Fixture


def interrupted(signum, _frame):
    raise KeyboardInterrupt(f'interrupted by signal {signum}')


def main():
    signal.signal(signal.SIGTERM, interrupted)
    fx = Fixture()
    try:
        source = fx.create_source()

        closed = delivery.completed_wal(fx, source)
        delivery.quiet_active_wal(fx, source)
        delivery.credentials_and_errors(fx, source, closed)
        delivery.custom_log(fx, source, closed)

        if not fx.no_slot:
            first = lifecycle.slot_progress(fx, source)
            lifecycle.publication_acknowledgements(fx, source, first)
            lifecycle.owner_failure(fx, source, closed)

        lifecycle.client_timeout(fx, source, closed)
        lifecycle.stale_socket(fx, source, closed)
        if fx.no_slot:
            lifecycle.slotless_session_reuse(fx, source, closed)
        else:
            lifecycle.idle_and_queue(fx, source, closed)

        leader, replica_dir = recovery.promote_backup(fx, source)
        target = recovery.capture_target(fx, leader)
        recovery.history_and_ancestor(fx, source, leader, target)
        recovery.ordinary_sql_rejected(fx, leader)
        replica = recovery.restore_replica(fx, leader, replica_dir, target)
        recovery.verify_checkpoint(fx, leader, replica, target)
        recovery.replication_commands_only(fx, source, leader)
        fx.result['status'] = 'PASS'
    except (Exception, KeyboardInterrupt) as exc:
        frames = traceback.extract_tb(exc.__traceback__)
        scenario = frames[1].name if len(frames) > 1 else 'main'
        fx.result.update(status='FAIL', failed_scenario=scenario, error_type=type(exc).__name__,
                         error=fx.redact(f'{scenario}: {type(exc).__name__}: {exc}'))
    finally:
        fx.cleanup()
        fx.save_evidence()
    return 0 if fx.result['status'] == 'PASS' else 1


if __name__ == '__main__':
    raise SystemExit(main())
