"""Each test owns its PostgreSQL processes, helper servers and diagnostics."""
import signal

import pytest

from fixture import Fixture


@pytest.hookimpl(hookwrapper=True)
def pytest_runtest_makereport(item, call):
    outcome = yield
    report = outcome.get_result()
    setattr(item, 'rep_' + report.when, report)


def interrupted(signum, _frame):
    raise KeyboardInterrupt(f'interrupted by signal {signum}')


@pytest.fixture
def fx(request):
    resources = Fixture()
    resources.result['nodeid'] = request.node.nodeid
    previous_handler = signal.signal(signal.SIGTERM, interrupted)
    try:
        yield resources
    finally:
        reports = [getattr(request.node, 'rep_' + phase, None) for phase in ('setup', 'call')]
        failed = next((report for report in reports if report is not None and report.failed), None)
        resources.result['status'] = 'PASS' if reports[1] is not None and reports[1].passed else 'FAIL'
        if failed is not None:
            resources.result['error'] = resources.redact(str(failed.longrepr))[-65536:]
        try:
            try:
                resources.audit_replication()
            finally:
                resources.cleanup()
        except BaseException as exc:
            resources.result.update(status='FAIL', error=resources.redact(str(exc))[-65536:])
            raise
        finally:
            try:
                resources.save_evidence()
            finally:
                signal.signal(signal.SIGTERM, previous_handler)
        if resources.result.get('cleanup_errors'):
            pytest.fail('owned process cleanup or artifact removal failed; see diagnostics')


@pytest.fixture
def source(fx):
    # fx has already yielded, so even a partial setup is cleaned up.
    return fx.create_source()


@pytest.fixture
def closed(fx, source):
    fx.sql(source, 'CREATE TABLE wal_fixture AS SELECT generate_series(1,10000);')
    name = fx.sql(source, 'SELECT pg_walfile_name(pg_current_wal_insert_lsn());')
    fx.sql(source, 'SELECT pg_switch_wal();')
    return name


@pytest.fixture
def session(fx, source, closed):
    """Start one helper session without running another test as setup."""
    fx.fetch(source, closed, fx.run / 'session.wal')
