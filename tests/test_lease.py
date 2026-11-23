import io
import json
import threading
import time
import unittest
import urllib.error
from unittest import mock

import clef_lease
from clef_lease import Lease, identity

RUN = 'f' * 32
KEY = 'k' * 64
TOKEN = 'a' * 64
BEARER = 'b' * 64
BASE = 'http://100.64.1.2:8769'
PREFIX = BASE + '/v1/leases/' + RUN
ROUTES = {
    ('PUT', ''): 'create',
    ('GET', ''): 'status',
    ('DELETE', ''): 'withdraw',
    ('POST', '/heartbeat'): 'heartbeat',
    ('POST', '/ready'): 'ready',
}


def http_error(code, where=''):
    """An HTTPError carrying a decoy body: nothing here may ever reach a message."""
    # The decoy body echoes every credential: if the client ever surfaced the
    # error body, the secret-sweep tests would catch it.
    body = json.dumps({'error': 'denied', 'echo': KEY + TOKEN + BEARER}).encode()
    return urllib.error.HTTPError(PREFIX + where, code, 'error', {},
                                 io.BytesIO(body))


class Response:
    def __init__(self, payload):
        self.payload = payload

    def read(self):
        return json.dumps(self.payload).encode()

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        return False


class Endpoint:
    """Offline stand-in for the registrar. Records requests, never opens a socket."""

    def __init__(self, run=RUN):
        self.run = run
        self.phase = None
        self.create_phase = 'staging'
        self.ready_phase = 'active'
        self.heartbeat_override = None
        self.reply_run = None
        self.requests = []
        self.errors = []
        self.faults = {}
        self.sticky = {}
        self.hooks = {}
        self.heartbeats = 0
        self.ready_calls = 0
        self.workers = []
        self.body = None
        self.lock = threading.Lock()

    def opener(self):
        return self

    def route(self, method, url):
        prefix = BASE + '/v1/leases/' + self.run
        if not url.startswith(prefix):
            self.errors.append('unexpected url ' + url)
            raise http_error(400)
        tail = url[len(prefix):]
        name = ROUTES.get((method, tail))
        if name is None:
            self.errors.append('unexpected route %s %r' % (method, tail))
            raise http_error(404)
        return name

    def open(self, request, timeout=None):
        record = {
            'method': request.get_method(),
            'url': request.full_url,
            'headers': {k.lower(): v for k, v in request.headers.items()},
            'body': None if request.data is None else json.loads(request.data.decode()),
            'timeout': timeout,
        }
        record['route'] = self.route(record['method'], record['url'])
        if (record['headers'].get('authorization') != 'Bearer ' + KEY
                or record['headers'].get('x-lease-token') != TOKEN):
            self.errors.append('missing/wrong credentials on ' + record['route'])
            raise http_error(403)
        with self.lock:
            self.requests.append(record)
            queue = self.faults.get(record['route'])
            fault = queue.pop(0) if queue else self.sticky.get(record['route'])
        hook = self.hooks.get(record['route'])
        if hook is not None:
            hook(record)
        if fault is not None:
            raise fault
        return Response(self.handle(record))

    def handle(self, record):
        route = record['route']
        if route == 'create':
            self.body = record['body']
            self.phase = self.create_phase
            return self.public()
        if route == 'status':
            if self.phase is None:
                raise http_error(404)
            return self.public()
        if route == 'heartbeat':
            with self.lock:
                self.heartbeats += 1
                self.workers.append(record['body']['workers'])
            if self.heartbeat_override is not None:
                return self.public(self.heartbeat_override)
            if self.phase in ('expired', 'withdrawn'):
                raise http_error(410, '/heartbeat')
            return self.public()
        if route == 'ready':
            with self.lock:
                self.ready_calls += 1
            self.phase = self.ready_phase
            return self.public()
        if route == 'withdraw':
            self.phase = 'withdrawn'
            return self.public()
        raise AssertionError('unrouted ' + route)

    def public(self, phase=None):
        run = self.reply_run or self.run
        return {'run_id': run, 'provider': 'clef-nb-' + run,
                'phase': self.phase if phase is None else phase,
                'expires': 1234.0}

    def seen(self, route):
        with self.lock:
            return [r for r in self.requests if r['route'] == route]


class NoNetwork(unittest.TestCase):
    def test_suite_cannot_reach_the_network(self):
        # Same guard LeaseCase installs, asserted here: urlopen is a raise-only
        # stub, so no test in this file can open a socket even by mistake.
        patched = mock.patch.object(clef_lease.urllib.request, 'urlopen',
                                    side_effect=AssertionError('live network call'))
        with patched:
            with self.assertRaises(AssertionError) as caught:
                clef_lease.urllib.request.urlopen(PREFIX)
        self.assertIn('live network call', str(caught.exception))


def settle(seconds=0.1):
    time.sleep(seconds)


def wait_for(predicate, timeout=2.0):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if predicate():
            return True
        time.sleep(0.002)
    return predicate()


class LeaseCase(unittest.TestCase):
    """Base case: blocks live urlopen and owns the fake clock + leases."""

    def setUp(self):
        patched = mock.patch.object(
            clef_lease.urllib.request, 'urlopen',
            side_effect=AssertionError('test attempted a live network call'))
        patched.start()
        self.addCleanup(patched.stop)
        self.endpoint = Endpoint()
        self.endpoints = [self.endpoint]
        self.clock = [1000.0]
        self.leases = []

    def tearDown(self):
        for lease in self.leases:
            lease.halt.set()
        for lease in self.leases:
            for thread in (lease.thread, lease.admit_thread):
                if thread is not None and thread is not threading.current_thread():
                    thread.join(timeout=5)
        for endpoint in self.endpoints:
            self.assertEqual(endpoint.errors, [])

    def reset_endpoint(self):
        self.endpoint = Endpoint()
        self.endpoints.append(self.endpoint)
        return self.endpoint

    def assert_clean(self, *texts):
        blob = ' | '.join(str(text) for text in texts)
        for name, secret in (('registry key', KEY), ('lease token', TOKEN),
                             ('bearer', BEARER)):
            self.assertNotIn(secret, blob, name + ' leaked into a message')

    def diagnostics(self, lease):
        return (repr(lease), str(lease), lease.last_error)



    def make_lease(self, interval=20.0, ident=None, healthy=None):
        lease = Lease(BASE + '/', KEY,
                      ident or {'run_id': RUN, 'token': TOKEN,
                                'hostname': 'clef-nb-' + RUN},
                      self.endpoint.opener,
                      healthy or (lambda: 2),
                      interval=interval, now=lambda: self.clock[0])
        self.leases.append(lease)
        return lease

    def admitted(self, interval=20.0, **kwargs):
        lease = self.make_lease(interval=interval, **kwargs)
        lease.create(BASE + '/v1', 2, BEARER)
        return lease


class Identity(unittest.TestCase):
    def test_unique_instances(self):
        a, b = identity(), identity()
        self.assertNotEqual(a['run_id'], b['run_id'])
        self.assertNotEqual(a['token'], b['token'])
        self.assertRegex(a['run_id'], r'^[0-9a-f]{32}$')
        self.assertRegex(a['token'], r'^[0-9a-f]{64}$')
        self.assertEqual(a['hostname'], 'clef-nb-' + a['run_id'])
        self.assertEqual(set(a), {'run_id', 'token', 'hostname'})


class Create(LeaseCase):
    def test_create_sends_contract_request(self):
        lease = self.make_lease()
        result = lease.create(BASE + '/v1', 2, BEARER)

        request = self.endpoint.seen('create')[0]
        self.assertEqual(request['method'], 'PUT')
        self.assertEqual(request['url'], PREFIX)
        self.assertEqual(request['headers']['authorization'], 'Bearer ' + KEY)
        self.assertEqual(request['headers']['x-lease-token'], TOKEN)
        self.assertEqual(request['headers']['content-type'], 'application/json')
        self.assertEqual(request['body'],
                         {'base': BASE + '/v1', 'workers': 2, 'bearer': BEARER})
        self.assertEqual(result['phase'], 'staging')
        self.assertEqual(result['provider'], 'clef-nb-' + RUN)
        self.assertEqual(result['run_id'], RUN)

    def test_base_trailing_slash_is_normalised(self):
        lease = self.make_lease()
        self.assertEqual(lease.base, BASE)
        self.assertEqual(lease._url(), PREFIX)

    def test_create_is_the_only_write_before_heartbeat(self):
        self.admitted()
        self.assertEqual(len(self.endpoint.seen('create')), 1)
        self.assertEqual(len(self.endpoint.seen('heartbeat')), 0)


class Terminal(LeaseCase):
    def test_renewal_410_fails_closed_without_recreating(self):
        lease = self.admitted(interval=0.02)
        self.endpoint.faults['heartbeat'] = [http_error(410, '/heartbeat')]
        self.assertTrue(wait_for(lambda: lease.lost))
        self.assertTrue(lease.halt.is_set())
        self.assertFalse(lease.withdrawn)
        self.assertIn('410', str(lease.last_error))
        heartbeats = len(self.endpoint.seen('heartbeat'))
        settle()
        self.assertEqual(len(self.endpoint.seen('heartbeat')), heartbeats)
        self.assertEqual(len(self.endpoint.seen('create')), 1)
        self.assertEqual(len(self.endpoint.seen('ready')), 0)
        self.assertEqual(self.endpoint.seen('create')[0]['headers']['x-lease-token'], TOKEN)


class Confirmation(LeaseCase):
    def test_failed_renewal_does_not_advance_confirmation(self):
        lease = self.admitted(interval=0.02)
        self.clock[0] = 1010.0
        self.assertTrue(wait_for(lambda: lease.last_confirmed == 1010.0))
        self.endpoint.sticky['heartbeat'] = http_error(503, '/heartbeat')
        attempts = len(self.endpoint.seen('heartbeat'))
        self.clock[0] = 1050.0
        self.assertTrue(wait_for(
            lambda: len(self.endpoint.seen('heartbeat')) >= attempts + 2))
        self.assertEqual(lease.last_confirmed, 1010.0)
        self.assertFalse(lease.lost)
        self.assertFalse(lease.withdrawn)
        self.assertIsNotNone(lease.last_error)

    def test_recovered_renewal_advances_confirmation(self):
        lease = self.admitted(interval=0.02)
        self.endpoint.sticky['heartbeat'] = http_error(503, '/heartbeat')
        attempts = len(self.endpoint.seen('heartbeat'))
        self.assertTrue(wait_for(
            lambda: len(self.endpoint.seen('heartbeat')) >= attempts + 2))
        self.clock[0] = 1060.0
        del self.endpoint.sticky['heartbeat']
        self.assertTrue(wait_for(lambda: lease.last_confirmed == 1060.0))
        self.assertFalse(lease.lost)
        self.assertIsNone(lease.last_error)

    def test_persistent_failure_past_window_is_lost_not_withdrawn(self):
        lease = self.admitted(interval=0.02)
        self.endpoint.sticky['heartbeat'] = http_error(403, '/heartbeat')
        attempts = len(self.endpoint.seen('heartbeat'))
        self.assertTrue(wait_for(
            lambda: len(self.endpoint.seen('heartbeat')) >= attempts + 1))
        self.clock[0] = 1000.0 + 119.0
        settle()
        self.assertFalse(lease.lost, 'window is 120s, not shorter')
        self.clock[0] = 1000.0 + 121.0
        self.assertTrue(wait_for(lambda: lease.lost))
        self.assertFalse(lease.withdrawn)
        self.assertEqual(len(self.endpoint.seen('create')), 1)
        self.assertEqual(len(self.endpoint.seen('ready')), 0)


class CreateRetry(LeaseCase):
    def test_transport_failure_is_verified_with_get_then_retried(self):
        lease = self.make_lease()
        self.endpoint.faults['create'] = [urllib.error.URLError('reset'),
                                          urllib.error.URLError('reset')]
        result = lease.create(BASE + '/v1', 2, BEARER)
        creates = self.endpoint.seen('create')
        self.assertEqual(len(creates), 3)
        self.assertEqual(len(self.endpoint.seen('status')), 2)
        self.assertEqual({r['url'] for r in creates}, {PREFIX})
        self.assertEqual({r['headers']['x-lease-token'] for r in creates}, {TOKEN})
        self.assertEqual({json.dumps(r['body'], sort_keys=True) for r in creates},
                         {json.dumps({'base': BASE + '/v1', 'workers': 2,
                                      'bearer': BEARER}, sort_keys=True)})
        self.assertEqual(result['phase'], 'staging')
        self.assertTrue(lease.thread.is_alive())

    def test_503_is_retried_without_a_status_probe(self):
        lease = self.make_lease()
        self.endpoint.faults['create'] = [http_error(503), http_error(503)]
        lease.create(BASE + '/v1', 2, BEARER)
        self.assertEqual(len(self.endpoint.seen('create')), 3)
        self.assertEqual(len(self.endpoint.seen('status')), 0)

    def test_exhausted_transport_failure_is_not_confirmation(self):
        lease = self.make_lease()
        self.endpoint.sticky['create'] = urllib.error.URLError('reset')
        with self.assertRaises(RuntimeError) as caught:
            lease.create(BASE + '/v1', 2, BEARER)
        self.assertIn('not confirmed', str(caught.exception))
        self.assertEqual(len(self.endpoint.seen('create')), 3)
        self.assertIsNone(lease.thread)
        self.assertFalse(lease.withdrawn)
        self.assertFalse(lease.lost)

    def test_terminal_status_is_never_retried(self):
        for code in (400, 403, 409, 410):
            with self.subTest(code=code):
                lease = self.make_lease()
                before = len(self.endpoint.seen('create'))
                self.endpoint.faults['create'] = [http_error(code)]
                with self.assertRaises(clef_lease.LeaseFault) as caught:
                    lease.create(BASE + '/v1', 2, BEARER)
                self.assertEqual(caught.exception.status, code)
                self.assertEqual(len(self.endpoint.seen('create')) - before, 1)
                self.assertIsNone(lease.thread)

    def test_applied_but_lost_create_verifies_with_get(self):
        lease = self.make_lease()

        def applied(record):
            self.endpoint.body = record['body']
            self.endpoint.phase = 'staging'
            raise urllib.error.URLError('connection reset after write')

        self.endpoint.hooks['create'] = applied
        result = lease.create(BASE + '/v1', 2, BEARER)
        self.assertEqual(len(self.endpoint.seen('create')), 1)
        self.assertEqual(len(self.endpoint.seen('status')), 1)
        self.assertEqual(result['phase'], 'staging')
        self.assertEqual(result['run_id'], RUN)
        self.assertTrue(lease.thread.is_alive())
        self.assertIsNone(lease.last_error)


class Withdrawal(LeaseCase):
    def test_withdraw_confirms_and_stops_renewal(self):
        lease = self.admitted(interval=0.02)
        self.assertTrue(wait_for(lambda: self.endpoint.seen('heartbeat')))
        result = lease.withdraw()
        request = self.endpoint.seen('withdraw')[0]
        self.assertEqual(request['method'], 'DELETE')
        self.assertEqual(request['url'], PREFIX)
        self.assertIsNone(request['body'])
        self.assertEqual(request['headers']['authorization'], 'Bearer ' + KEY)
        self.assertEqual(result['phase'], 'withdrawn')
        self.assertTrue(lease.withdrawn)
        self.assertTrue(lease.halt.is_set())
        self.assertFalse(lease.thread.is_alive())
        heartbeats = len(self.endpoint.seen('heartbeat'))
        settle()
        self.assertEqual(len(self.endpoint.seen('heartbeat')), heartbeats)
        self.assertEqual(len(self.endpoint.seen('create')), 1)

    def test_withdraw_retries_503_without_a_status_probe(self):
        lease = self.admitted()
        self.endpoint.faults['withdraw'] = [http_error(503)]
        lease.withdraw()
        self.assertEqual(len(self.endpoint.seen('withdraw')), 2)
        self.assertEqual(len(self.endpoint.seen('status')), 0)
        self.assertTrue(lease.withdrawn)

    def test_applied_but_lost_delete_is_confirmed_by_status(self):
        lease = self.admitted()

        def applied(record):
            self.endpoint.phase = 'withdrawn'
            raise urllib.error.URLError('connection reset after delete')

        self.endpoint.hooks['withdraw'] = applied
        result = lease.withdraw()
        self.assertEqual(len(self.endpoint.seen('withdraw')), 1)
        self.assertEqual(len(self.endpoint.seen('status')), 1)
        self.assertEqual(result['phase'], 'withdrawn')
        self.assertTrue(lease.withdrawn)
        self.assertIsNone(lease.last_error)

    def test_unconfirmed_withdrawal_is_never_reported_as_withdrawn(self):
        lease = self.admitted(interval=0.02)
        self.endpoint.sticky['withdraw'] = urllib.error.URLError('network down')
        with self.assertRaises(RuntimeError) as caught:
            lease.withdraw()
        self.assertIn('not confirmed', str(caught.exception))
        self.assertEqual(len(self.endpoint.seen('withdraw')), 3)
        self.assertEqual(len(self.endpoint.seen('create')), 1)
        self.assertFalse(lease.withdrawn)
        self.assertFalse(lease.halt.is_set())
        self.assertTrue(lease.thread.is_alive())
        heartbeats = len(self.endpoint.seen('heartbeat'))
        self.assertTrue(wait_for(
            lambda: len(self.endpoint.seen('heartbeat')) > heartbeats))

    def test_withdraw_410_is_not_retried(self):
        lease = self.admitted()
        self.endpoint.faults['withdraw'] = [http_error(410)]
        with self.assertRaises(clef_lease.LeaseFault) as caught:
            lease.withdraw()
        self.assertEqual(caught.exception.status, 410)
        self.assertEqual(len(self.endpoint.seen('withdraw')), 1)
        self.assertFalse(lease.withdrawn)

    def test_stop_twice_is_idempotent(self):
        lease = self.admitted()
        lease.withdraw()
        lease.withdraw()
        self.assertEqual(len([r for r in self.endpoint.seen('withdraw')]), 2)
        self.assertTrue(lease.withdrawn)
        self.assertEqual(len(self.endpoint.seen('create')), 1)


class LostAdmission(LeaseCase):
    def test_lost_ready_response_is_resolved_by_status(self):
        lease = self.admitted()

        def applied(record):
            self.endpoint.phase = 'active'
            raise urllib.error.URLError('read timeout after the probe')

        self.endpoint.hooks['ready'] = applied
        result = lease.ready()
        self.assertEqual(result['phase'], 'active')
        self.assertEqual(result['run_id'], RUN)
        self.assertEqual(len(self.endpoint.seen('ready')), 1)
        self.assertEqual(len(self.endpoint.seen('status')), 1)
        self.assertEqual(len(self.endpoint.seen('create')), 1)
        self.assertEqual(lease.phase, 'active')
        self.assertEqual(lease.ident['run_id'], RUN)
        self.assertFalse(lease.lost)

    def test_lost_ready_response_is_never_reproved(self):
        lease = self.admitted()

        def unconfirmed(record):
            self.endpoint.phase = 'staging'
            raise urllib.error.URLError('read timeout during the probe')

        self.endpoint.hooks['ready'] = unconfirmed
        with self.assertRaises(RuntimeError) as caught:
            lease.ready()
        self.assertIn('not confirmed', str(caught.exception))
        self.assertEqual(len(self.endpoint.seen('ready')), 1)
        self.assertEqual(len(self.endpoint.seen('create')), 1)
        self.assertNotEqual(lease.phase, 'active')

    def test_lost_ready_with_unreachable_status_fails_closed(self):
        lease = self.admitted()
        self.endpoint.hooks['ready'] = lambda record: (_ for _ in ()).throw(
            urllib.error.URLError('read timeout'))
        self.endpoint.sticky['status'] = urllib.error.URLError('status unreachable')
        with self.assertRaises(RuntimeError) as caught:
            lease.ready()
        self.assertIn('unknown', str(caught.exception))
        self.assertEqual(len(self.endpoint.seen('status')), 3)
        self.assertEqual(len(self.endpoint.seen('ready')), 1)
        self.assertNotEqual(lease.phase, 'active')

    def test_ready_410_is_terminal_and_never_recreated(self):
        lease = self.admitted()
        self.endpoint.faults['ready'] = [http_error(410, '/ready')]
        with self.assertRaises(clef_lease.LeaseFault) as caught:
            lease.ready()
        self.assertEqual(caught.exception.status, 410)
        self.assertTrue(lease.lost)
        self.assertFalse(lease.withdrawn)
        self.assertEqual(len(self.endpoint.seen('ready')), 1)
        self.assertEqual(len(self.endpoint.seen('create')), 1)
        self.assertEqual(len(self.endpoint.seen('status')), 0)


class ReAdmission(LeaseCase):
    def test_staging_renewal_starts_one_admission_while_renewing(self):
        lease = self.admitted(interval=0.02)
        entered = threading.Event()
        release = threading.Event()

        def hold(record):
            entered.set()
            release.wait(5)

        self.endpoint.hooks['ready'] = hold
        try:
            self.assertTrue(wait_for(lambda: lease.admitting))
            self.assertTrue(entered.wait(2))
            heartbeats = len(self.endpoint.seen('heartbeat'))
            self.assertTrue(wait_for(
                lambda: len(self.endpoint.seen('heartbeat')) >= heartbeats + 3),
                'renewal stopped during background re-admission')
            self.assertGreater(len(self.endpoint.seen('heartbeat')), 3)
            self.assertEqual(len(self.endpoint.seen('ready')), 1)
        finally:
            release.set()
        self.assertTrue(wait_for(lambda: not lease.admitting))
        self.assertEqual(lease.phase, 'active')
        self.assertEqual(len(self.endpoint.seen('ready')), 1)
        self.assertEqual(len(self.endpoint.seen('create')), 1)

    def test_repeated_staging_does_not_spawn_a_second_worker(self):
        lease = self.admitted(interval=0.02)
        entered = threading.Event()
        release = threading.Event()

        def hold(record):
            entered.set()
            release.wait(5)

        self.endpoint.hooks['ready'] = hold
        try:
            self.assertTrue(entered.wait(2))
            self.assertTrue(wait_for(
                lambda: len(self.endpoint.seen('heartbeat')) >= 5))
            self.assertEqual(len(self.endpoint.seen('ready')), 1)
            self.assertEqual(
                len([t for t in threading.enumerate() if t.name == 'clef-lease-admit']), 1)
        finally:
            release.set()
        self.assertTrue(wait_for(lambda: not lease.admitting))

    def test_failed_readmission_is_recorded_and_renewal_continues(self):
        lease = self.admitted(interval=0.02)
        self.endpoint.ready_phase = 'staging'
        self.assertTrue(wait_for(lambda: len(self.endpoint.seen('ready')) >= 1))
        self.assertTrue(wait_for(lambda: not lease.admitting))
        self.assertIn('staging', str(lease.last_error))
        self.assertNotEqual(lease.phase, 'active')
        self.assertFalse(lease.lost)
        heartbeats = len(self.endpoint.seen('heartbeat'))
        self.assertTrue(wait_for(
            lambda: len(self.endpoint.seen('heartbeat')) >= heartbeats + 2))
        self.assertEqual(len(self.endpoint.seen('create')), 1)

    def test_no_admission_after_withdrawal(self):
        lease = self.admitted(interval=0.02)
        lease.withdraw()
        self.endpoint.phase = 'staging'
        settle(0.15)
        self.assertEqual(len(self.endpoint.seen('ready')), 0)
        self.assertFalse(lease.admitting)


class Secrets(LeaseCase):
    def test_errors_never_contain_credentials(self):
        cases = {}

        def run(name, prepare, action):
            self.reset_endpoint()
            lease = self.make_lease()
            prepare(self.endpoint)
            try:
                action(lease)
            except Exception as exc:
                self.assert_clean(str(exc), repr(exc), *self.diagnostics(lease))
                cases[name] = str(exc)
            else:
                self.fail(name + ' did not fail')

        run('create 403', lambda e: e.faults.__setitem__('create', [http_error(403)]),
            lambda l: l.create(BASE + '/v1', 2, BEARER))
        run('create 409', lambda e: e.faults.__setitem__('create', [http_error(409)]),
            lambda l: l.create(BASE + '/v1', 2, BEARER))
        run('create exhausted',
            lambda e: e.sticky.__setitem__('create', urllib.error.URLError('down')),
            lambda l: l.create(BASE + '/v1', 2, BEARER))
        run('ready 410',
            lambda e: e.faults.__setitem__('ready', [http_error(410, '/ready')]),
            lambda l: (l.create(BASE + '/v1', 2, BEARER), l.ready()))
        run('ready foreign runtime',
            lambda e: setattr(e, 'reply_run', 'e' * 32),
            lambda l: (l.create(BASE + '/v1', 2, BEARER), l.ready()))
        run('lost ready unconfirmed',
            lambda e: setattr(e, 'ready_phase', 'staging'),
            lambda l: (l.create(BASE + '/v1', 2, BEARER), l.ready()))
        run('withdraw 410',
            lambda e: e.faults.__setitem__('withdraw', [http_error(410)]),
            lambda l: (l.create(BASE + '/v1', 2, BEARER), l.withdraw()))
        run('withdraw unconfirmed',
            lambda e: e.sticky.__setitem__('withdraw', urllib.error.URLError('down')),
            lambda l: (l.create(BASE + '/v1', 2, BEARER), l.withdraw()))
        self.assertEqual(len(cases), 8)

    def test_hostile_phase_text_is_sanitised(self):
        lease = self.admitted()
        self.endpoint.ready_phase = TOKEN
        with self.assertRaises(RuntimeError) as caught:
            lease.ready()
        self.assert_clean(str(caught.exception), repr(caught.exception),
                          *self.diagnostics(lease))

    def test_hostile_phase_text_from_create_is_sanitised(self):
        lease = self.make_lease()
        self.endpoint.create_phase = KEY
        lease.create(BASE + '/v1', 2, BEARER)
        self.assertEqual(lease.phase, 'unrecognised-phase')
        self.assert_clean(*self.diagnostics(lease))

    def test_hostile_renewal_phase_is_clean_in_diagnostics(self):
        lease = self.admitted(interval=0.02)
        self.endpoint.heartbeat_override = BEARER
        self.assertTrue(wait_for(lambda: self.endpoint.seen('heartbeat')))
        settle()
        self.assertEqual(lease.phase, 'unrecognised-phase')
        self.assert_clean(*self.diagnostics(lease))

    def test_failed_readmission_diagnostic_is_clean(self):
        lease = self.admitted(interval=0.02)
        self.endpoint.ready_phase = KEY
        self.assertTrue(wait_for(lambda: lease.last_error))
        self.assertTrue(wait_for(lambda: not lease.admitting))
        self.assert_clean(*self.diagnostics(lease))


class Teardown(LeaseCase):
    def test_confirmed_withdrawal_wins_over_a_late_admission(self):
        lease = self.admitted(interval=0.02)
        entered = threading.Event()
        release = threading.Event()

        def hold(record):
            entered.set()
            release.wait(5)

        self.endpoint.hooks['ready'] = hold
        self.assertTrue(wait_for(lambda: lease.admitting))
        self.assertTrue(entered.wait(2))
        lease.withdraw()
        self.assertTrue(lease.withdrawn)
        release.set()
        self.assertTrue(wait_for(lambda: not lease.admitting))
        settle()
        self.assertTrue(lease.withdrawn)
        self.assertEqual(lease.phase, 'withdrawn')
        self.assertFalse(lease.lost)
        self.assertEqual(len(self.endpoint.seen('create')), 1)
        self.assertEqual(len(self.endpoint.seen('ready')), 1)

    def test_concurrent_withdrawals_do_not_deadlock(self):
        lease = self.admitted(interval=0.02)
        results = []

        def stop():
            results.append(lease.withdraw()['phase'])

        threads = [threading.Thread(target=stop) for _ in range(2)]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join(10)
            self.assertFalse(thread.is_alive(), 'withdraw deadlocked')
        self.assertEqual(results, ['withdrawn', 'withdrawn'])
        self.assertTrue(lease.withdrawn)
        self.assertEqual(len(self.endpoint.seen('create')), 1)


class Admission(LeaseCase):
    def test_ready_posts_empty_body_and_requires_active(self):
        lease = self.admitted()
        result = lease.ready()
        request = self.endpoint.seen('ready')[0]
        self.assertEqual(request['method'], 'POST')
        self.assertEqual(request['url'], PREFIX + '/ready')
        self.assertEqual(request['body'], {})
        self.assertEqual(request['headers']['authorization'], 'Bearer ' + KEY)
        self.assertEqual(request['headers']['x-lease-token'], TOKEN)
        self.assertEqual(request['timeout'], 650)
        self.assertEqual(result['phase'], 'active')
        self.assertEqual(lease.phase, 'active')
        self.assertFalse(lease.lost)

    def test_ready_rejects_inactive_phase(self):
        lease = self.admitted()
        self.endpoint.ready_phase = 'staging'
        with self.assertRaises(RuntimeError) as caught:
            lease.ready()
        self.assertIn('staging', str(caught.exception))
        self.assertNotEqual(lease.phase, 'active')

    def test_ready_rejects_another_runtime(self):
        lease = self.admitted()
        self.endpoint.reply_run = 'e' * 32
        with self.assertRaises(RuntimeError) as caught:
            lease.ready()
        self.assertIn('another runtime', str(caught.exception))
        self.assertNotEqual(lease.phase, 'active')

    def test_heartbeat_continues_while_ready_is_blocked(self):
        lease = self.admitted(interval=0.02)
        entered = threading.Event()
        release = threading.Event()

        def hold(record):
            entered.set()
            release.wait(5)

        self.endpoint.hooks['ready'] = hold
        outcome = []
        probe = threading.Thread(target=lambda: outcome.append(lease.ready()))
        probe.start()
        try:
            self.assertTrue(entered.wait(2))
            during = len(self.endpoint.seen('heartbeat'))
            self.assertTrue(wait_for(
                lambda: len(self.endpoint.seen('heartbeat')) >= during + 3),
                'renewal stopped while the readiness probe was in flight')
        finally:
            release.set()
            probe.join(5)
        self.assertEqual(outcome[0]['phase'], 'active')
        self.assertEqual(lease.phase, 'active')


class Renewal(LeaseCase):
    def test_create_starts_exactly_one_renewal_thread(self):
        lease = self.make_lease()
        lease.create(BASE + '/v1', 2, BEARER)
        self.assertIsNotNone(lease.thread)
        self.assertTrue(lease.thread.is_alive())
        with self.assertRaises(RuntimeError):
            lease.create(BASE + '/v1', 2, BEARER)
        self.assertEqual(len(self.endpoint.seen('create')), 1)
        self.assertEqual(sum(1 for t in threading.enumerate()
                             if t.name == 'clef-lease-renew'), 1)

    def test_heartbeat_renews_on_the_interval_with_healthy_count(self):
        # 'admitting' is a phase the client mirrors but must not act on, so this
        # stays deterministic even though a staging phase would now re-admit.
        self.endpoint.create_phase = 'admitting'
        lease = self.admitted(interval=0.02)
        self.assertTrue(wait_for(lambda: len(self.endpoint.seen('heartbeat')) >= 2))
        request = self.endpoint.seen('heartbeat')[0]
        self.assertEqual(request['method'], 'POST')
        self.assertEqual(request['url'], PREFIX + '/heartbeat')
        self.assertEqual(request['headers']['authorization'], 'Bearer ' + KEY)
        self.assertEqual(request['headers']['x-lease-token'], TOKEN)
        self.assertEqual(request['body'], {'workers': 2})
        self.assertEqual(request['timeout'], 30)
        self.assertFalse(lease.withdrawn)
        self.assertFalse(lease.lost)
        self.assertEqual(lease.phase, 'admitting')
        self.assertEqual(len(self.endpoint.seen('ready')), 0)

    def test_heartbeat_reports_healthy_worker_count(self):
        lease = self.admitted(interval=0.02, healthy=lambda: 1)
        self.assertTrue(wait_for(lambda: self.endpoint.seen('heartbeat')))
        self.assertEqual(self.endpoint.workers[0], 1)

    def test_heartbeat_observes_terminal_phase_as_lost(self):
        lease = self.admitted(interval=0.02)
        self.endpoint.heartbeat_override = 'expired'
        self.assertTrue(wait_for(lambda: lease.lost))
        self.assertTrue(lease.halt.is_set())
        self.assertFalse(lease.withdrawn)
        count = len(self.endpoint.seen('heartbeat'))
        settle()
        self.assertEqual(len(self.endpoint.seen('heartbeat')), count)
        self.assertEqual(len(self.endpoint.seen('create')), 1)



class Recovery(LeaseCase):
    """The notebook needs a status read, and a lost window must be able to end.

    `lost` means "no confirmed renewal for a while". If confirmation returns, the runtime is
    not lost any more — otherwise a transient outage leaves it permanently unadmittable while
    it keeps running and serving nothing.
    """

    def test_status_reads_phase_without_claiming_a_withdrawal(self):
        lease = self.admitted()
        payload = lease.status()
        self.assertEqual(payload['phase'], 'staging')
        self.assertEqual(lease.phase, 'staging')
        self.assertEqual(len(self.endpoint.seen('create')), 1, 'status must not re-create')
        self.assertFalse(lease.withdrawn)

        self.endpoint.phase = 'withdrawn'
        self.assertEqual(lease.status()['phase'], 'withdrawn')
        self.assertFalse(lease.withdrawn, 'a read cannot confirm a withdrawal')
        self.assert_clean(*self.diagnostics(lease))

    def test_status_raises_a_clean_fault_when_unreachable(self):
        lease = self.admitted()
        self.endpoint.sticky['status'] = http_error(503)
        with self.assertRaises(clef_lease.LeaseFault) as caught:
            lease.status()
        self.assert_clean(str(caught.exception), *self.diagnostics(lease))

    def test_confirmed_renewal_ends_the_lost_window(self):
        lease = self.admitted(interval=0.01)
        self.endpoint.sticky['heartbeat'] = http_error(503, '/heartbeat')
        self.clock[0] += 200
        self.assertTrue(wait_for(lambda: lease.lost))
        self.assertFalse(lease.withdrawn, 'a lost window is not a withdrawal')

        del self.endpoint.sticky['heartbeat']
        self.assertTrue(wait_for(lambda: not lease.lost), 'a confirmed renewal ends the window')
        self.assertFalse(lease.withdrawn)

    def test_lost_then_staging_re_admits_instead_of_sticking(self):
        lease = self.admitted(interval=0.01)
        self.endpoint.sticky['heartbeat'] = http_error(503, '/heartbeat')
        self.clock[0] += 200
        self.assertTrue(wait_for(lambda: lease.lost))

        del self.endpoint.sticky['heartbeat']
        self.endpoint.phase = 'staging'      # registrar restarted and demoted this runtime
        self.assertTrue(wait_for(lambda: self.endpoint.ready_calls >= 1),
                        'a recovered registrar must trigger re-admission')
        self.assertTrue(wait_for(lambda: lease.phase == 'active'))
        self.assertFalse(lease.lost)
