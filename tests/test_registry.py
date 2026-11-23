"""Offline tests for the Clef notebook lease registrar.

Every test in this file runs without network access:

* the registry core is driven through a recording fake client;
* the Plexus adapter is driven through a fake ``urllib.request.urlopen``;
* the HTTP wrapper is exercised over a loopback socket bound to 127.0.0.1:0.

No test contacts a tailnet address, and ``main()`` is never executed.
"""

import io
import json
import tempfile
import threading
import unittest
import urllib.error
import urllib.request
from copy import deepcopy
from http.server import ThreadingHTTPServer
from pathlib import Path
from unittest import mock

from clef_registry import (Fault, Handler, Plexus, Registry, acquire_singleton,
                           start, validate_bind)

A = 'a' * 32
B = 'b' * 32
C = 'c' * 32
TOKEN = 'c' * 64
OTHER = 'd' * 64
BEARER = 'e' * 64


def body(ip='100.64.1.2', workers=1, bearer=BEARER):
    return {'base': f'http://{ip}:8443/v1', 'workers': workers, 'bearer': bearer}


class Fake:
    """Recording stand-in for the Plexus adapter.

    ``apply=False`` models a gateway that acknowledges a pool PUT without
    persisting it (a silent no-op write).
    """

    def __init__(self):
        self.providers = {}
        self.alias = None
        self.apply = True
        self.calls = []
        self.provider_error = None
        self.pool_error = None
        self.prove_error = None
        self.prove_events = {}

    def provider(self, slug, cfg):
        self.calls.append(('provider', slug, deepcopy(cfg)))
        if self.provider_error:
            raise self.provider_error
        self.providers[slug] = deepcopy(cfg)

    def pool(self, cfg):
        self.calls.append(('pool', deepcopy(cfg)))
        if self.pool_error:
            raise self.pool_error
        if self.apply:
            self.alias = deepcopy(cfg)
        if self.alias != cfg:
            raise RuntimeError('pool readback mismatch')

    def prove(self, slug, cfg, run_id):
        self.calls.append(('prove', slug, run_id))
        gate = self.prove_events.get(slug)
        if gate is not None:
            entered, release = gate
            entered.set()
            if not release.wait(5):
                raise RuntimeError('test did not release the probe')
        if self.prove_error:
            raise self.prove_error
        return None

    def kinds(self, kind):
        return [call for call in self.calls if call[0] == kind]


class Base(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.clock = [1000.0]
        self.client = Fake()
        self.r = Registry(self.path, self.client, now=lambda: self.clock[0])

    @property
    def path(self):
        return Path(self.tmp.name) / 'state' / 'notebooks.json'


class Identity(Base):
    def test_identity_unique_and_idempotent(self):
        first = self.r.create(A, TOKEN, body())
        self.assertEqual(first['run_id'], A)
        self.assertEqual(first['provider'], 'clef-nb-' + A)
        self.assertEqual(first['phase'], 'staging')
        self.assertEqual(first['expires'], 1180.0)
        self.r.create(A, TOKEN, body())
        self.r.create(B, TOKEN, body('100.64.1.3'))
        self.assertEqual(len(self.r.records), 2)
        with self.assertRaises(Fault) as e:
            self.r.create(A, TOKEN, body('100.64.1.4'))
        self.assertEqual(e.exception.status, 409)

    def test_identity_wrong_owner(self):
        self.r.create(A, TOKEN, body())
        with self.assertRaises(Fault) as e:
            self.r.renew(A, OTHER, 1)
        self.assertEqual(e.exception.status, 403)

    def test_unknown_lease_is_404(self):
        with self.assertRaises(Fault) as e:
            self.r.renew(A, TOKEN, 1)
        self.assertEqual(e.exception.status, 404)

    def test_rejects_malformed_identity(self):
        cases = ((A.upper(), TOKEN), ('a' * 31, TOKEN), (A, TOKEN[:-1]),
                 (A, 'zz' * 32), (A, ''), ('', TOKEN))
        for run, token in cases:
            with self.assertRaises(Fault, msg=(run, token)) as e:
                self.r.create(run, token, body())
            self.assertEqual(e.exception.status, 400)
        self.assertEqual(self.r.records, {})
        self.assertEqual(self.client.calls, [])

    def test_public_view_excludes_secrets(self):
        view = self.r.create(A, TOKEN, body())
        self.assertEqual(set(view), {'run_id', 'provider', 'phase', 'expires'})
        rendered = json.dumps(view)
        self.assertNotIn(BEARER, rendered)
        self.assertNotIn(TOKEN, rendered)
        self.assertNotIn('token_hash', rendered)
        self.assertNotIn('api_key', rendered)

    def test_state_is_private_and_reloads(self):
        self.r.create(A, TOKEN, body())
        self.assertEqual(self.path.stat().st_mode & 0o777, 0o600)
        self.assertEqual(self.path.parent.stat().st_mode & 0o777, 0o700)
        again = Registry(self.path, self.client, now=lambda: self.clock[0])
        self.assertEqual(again.records[A]['body'], body())
        self.assertEqual(again.records[A]['phase'], 'staging')
        self.assertEqual(again.records[A]['expires'], 1180.0)

    def test_create_after_expiry_is_terminal(self):
        self.r.create(A, TOKEN, body())
        self.clock[0] += 181
        with self.assertRaises(Fault) as e:
            self.r.create(A, TOKEN, body())
        self.assertEqual(e.exception.status, 410)


class Lifecycle(Base):
    def test_two_ready_instances_and_stop_one(self):
        self.r.create(A, TOKEN, body())
        self.r.create(B, TOKEN, body('100.64.1.3'))
        self.r.ready(A, TOKEN)
        self.r.ready(B, TOKEN)
        self.r.withdraw(A, TOKEN)
        targets = self.client.alias['target_groups'][0]['targets']
        self.assertEqual({t['provider']: t['enabled'] for t in targets},
                         {'clef-nb-' + A: False, 'clef-nb-' + B: True})
        self.assertEqual(self.client.providers['clef-nb-' + B]['maxConcurrency'], 1)

    def test_pool_shape(self):
        self.r.create(A, TOKEN, body())
        self.r.create(B, TOKEN, body('100.64.1.3', workers=2))
        self.r.ready(B, TOKEN)
        self.r.ready(A, TOKEN)
        cfg = self.client.alias
        self.assertEqual(cfg['type'], 'decisions')
        self.assertEqual(cfg['priority'], 'selector')
        self.assertFalse(cfg['sticky_session'])
        self.assertFalse(cfg['use_image_fallthrough'])
        self.assertFalse(cfg['enforce_limits'])
        self.assertFalse(cfg['synthetic_safeguard_approval'])
        self.assertEqual(cfg['additional_aliases'], [])
        self.assertEqual(cfg['metadata'], {'source': 'custom', 'overrides': {
            'name': 'clef-flash-notebooks', 'context_length': 16384}})
        self.assertEqual(len(cfg['target_groups']), 1)
        group = cfg['target_groups'][0]
        self.assertEqual(group['name'], 'NOTEBOOKS')
        self.assertEqual(group['selector'], 'random')
        self.assertEqual(group['targets'], [
            {'provider': 'clef-nb-' + A, 'model': 'clef-flash', 'enabled': True},
            {'provider': 'clef-nb-' + B, 'model': 'clef-flash', 'enabled': True},
        ])

    def test_provider_config_shape(self):
        self.r.create(A, TOKEN, body(workers=2))
        self.r.ready(A, TOKEN)
        cfg = self.client.providers['clef-nb-' + A]
        self.assertEqual(cfg['api_base_url'], {'systemone': 'http://100.64.1.2:8443/v1'})
        self.assertEqual(cfg['display_name'], 'Clef notebook ' + A)
        self.assertEqual(cfg['api_key'], BEARER)
        self.assertIs(cfg['enabled'], True)
        self.assertEqual(cfg['maxConcurrency'], 2)
        self.assertEqual(cfg['timeoutMs'], 600000)
        self.assertEqual(cfg['models'], {'clef-flash': {
            'type': 'decisions', 'access_via': ['systemone'],
            'pricing': {'source': 'simple', 'input': 0, 'output': 0}}})
        self.assertEqual([c[1:] for c in self.client.kinds('prove')],
                         [('clef-nb-' + A, A)])
        self.assertEqual(self.r.records[A]['phase'], 'active')

    def test_expired_probe_cannot_republish(self):
        self.r.create(A, TOKEN, body())

        def expire(*args):
            self.clock[0] += 181
            self.r.sweep()

        self.client.prove = expire
        with self.assertRaises(Fault):
            self.r.ready(A, TOKEN)
        self.assertNotEqual(self.r.records[A]['phase'], 'active')
        self.assertIsNone(self.client.alias)

    def test_noop_pool_write_is_not_success(self):
        self.r.create(A, TOKEN, body())
        self.client.apply = False
        with self.assertRaises(RuntimeError):
            self.r.ready(A, TOKEN)
        self.assertNotEqual(self.r.records[A]['phase'], 'active')
        self.assertIsNone(self.client.alias)

    def test_renew_cannot_revive(self):
        self.r.create(A, TOKEN, body())
        self.r.withdraw(A, TOKEN)
        self.r.withdraw(A, TOKEN)
        with self.assertRaises(Fault) as e:
            self.r.renew(A, TOKEN, 1)
        self.assertEqual(e.exception.status, 410)

    def test_withdraw_is_idempotent_and_keeps_other_instance(self):
        self.r.create(A, TOKEN, body())
        self.r.create(B, TOKEN, body('100.64.1.3'))
        self.r.ready(A, TOKEN)
        self.r.ready(B, TOKEN)
        before_b = deepcopy(self.client.providers['clef-nb-' + B])
        before_a = deepcopy(self.client.providers['clef-nb-' + A])
        first = self.r.withdraw(A, TOKEN)
        second = self.r.withdraw(A, TOKEN)
        self.assertEqual(first['phase'], 'withdrawn')
        self.assertEqual(second['phase'], 'withdrawn')
        self.assertEqual(self.r.records[A]['phase'], 'withdrawn')
        self.assertEqual(self.r.records[B]['phase'], 'active')
        self.assertEqual(self.client.providers['clef-nb-' + B], before_b)
        self.assertEqual(self.client.providers['clef-nb-' + A], before_a)
        targets = {t['provider']: t['enabled']
                   for t in self.client.alias['target_groups'][0]['targets']}
        self.assertEqual(targets, {'clef-nb-' + A: False, 'clef-nb-' + B: True})

    def test_renew_extends_expiry_without_publishing(self):
        self.r.create(A, TOKEN, body())
        self.clock[0] += 100
        view = self.r.renew(A, TOKEN, 1)
        self.assertEqual(view['expires'], 1280.0)
        self.assertEqual(view['phase'], 'staging')
        self.assertEqual(self.client.kinds('pool'), [])

    def test_ready_is_idempotent_for_active_lease(self):
        self.r.create(A, TOKEN, body())
        self.r.ready(A, TOKEN)
        self.r.ready(A, TOKEN)
        self.assertEqual(len(self.client.kinds('prove')), 1)
        self.assertEqual(self.r.records[A]['phase'], 'active')


class Capacity(Base):
    def _two_workers(self, run=A, ip='100.64.1.2'):
        self.r.create(run, TOKEN, body(ip, workers=2))
        self.r.ready(run, TOKEN)

    def test_shrink_writes_only_this_provider_cap(self):
        self._two_workers(A)
        self._two_workers(B, '100.64.1.3')
        before_a = deepcopy(self.client.providers['clef-nb-' + A])
        before_b = deepcopy(self.client.providers['clef-nb-' + B])
        pool_writes = len(self.client.kinds('pool'))
        view = self.r.renew(A, TOKEN, 1)
        self.assertEqual(view['phase'], 'active')
        self.assertEqual(self.r.records[A]['workers'], 1)
        self.assertFalse(self.r.records[A].get('cap_dirty'))
        written = self.client.providers['clef-nb-' + A]
        self.assertEqual(written['maxConcurrency'], 1)
        self.assertEqual({k: v for k, v in written.items() if k != 'maxConcurrency'},
                         {k: v for k, v in before_a.items() if k != 'maxConcurrency'})
        self.assertEqual(self.client.providers['clef-nb-' + B], before_b)
        # Capacity is a provider field, never a routing change.
        self.assertEqual(len(self.client.kinds('pool')), pool_writes)

    def test_capacity_write_failure_still_renews_then_sweep_retries(self):
        self._two_workers(A)
        self.clock[0] += 100
        self.client.provider_error = RuntimeError('provider readback mismatch')
        with self.assertRaises(Fault) as e:
            self.r.renew(A, TOKEN, 1)
        self.assertEqual(e.exception.status, 503)
        self.assertEqual(e.exception.message, 'capacity update not confirmed; retry safely')
        # The renewal is durable even though the capacity write failed.
        self.assertEqual(self.r.records[A]['expires'], 1280.0)
        self.assertEqual(self.r.records[A]['workers'], 2)
        self.assertEqual(self.r.records[A]['cap_dirty'], 1)
        self.assertEqual(self.client.providers['clef-nb-' + A]['maxConcurrency'], 2)
        reloaded = Registry(self.path, self.client, now=lambda: self.clock[0])
        self.assertEqual(reloaded.records[A]['cap_dirty'], 1)
        # A failed capacity write must not cost the lease its renewal.
        self.assertEqual(reloaded.records[A]['phase'], 'active')

    def test_sweep_retries_dirty_capacity_and_clears_it(self):
        self._two_workers(A)
        self.client.provider_error = RuntimeError('provider readback mismatch')
        with self.assertRaises(Fault):
            self.r.renew(A, TOKEN, 1)
        self.client.provider_error = None
        self.r.sweep()
        self.assertEqual(self.r.records[A]['workers'], 1)
        self.assertFalse(self.r.records[A].get('cap_dirty'))
        self.assertEqual(self.client.providers['clef-nb-' + A]['maxConcurrency'], 1)
        self.assertEqual(self.r.records[A]['phase'], 'active')

    def test_sweep_keeps_dirty_capacity_on_failure(self):
        self._two_workers(A)
        self.client.provider_error = RuntimeError('provider readback mismatch')
        with self.assertRaises(Fault):
            self.r.renew(A, TOKEN, 1)
        with self.assertRaises(RuntimeError):
            self.r.sweep()
        self.assertEqual(self.r.records[A]['cap_dirty'], 1)
        self.assertEqual(self.r.records[A]['workers'], 2)
        self.assertEqual(self.client.providers['clef-nb-' + A]['maxConcurrency'], 2)
        reloaded = Registry(self.path, self.client, now=lambda: self.clock[0])
        self.assertEqual(reloaded.records[A]['cap_dirty'], 1)

    def test_capacity_increase_requires_new_runtime(self):
        self.r.create(A, TOKEN, body(workers=1))
        self.r.ready(A, TOKEN)
        writes = len(self.client.kinds('provider'))
        self.clock[0] += 50
        with self.assertRaises(Fault) as e:
            self.r.renew(A, TOKEN, 2)
        self.assertEqual(e.exception.status, 409)
        self.assertEqual(e.exception.message, 'capacity increase requires a new runtime')
        self.assertEqual(self.r.records[A]['workers'], 1)
        self.assertEqual(len(self.client.kinds('provider')), writes)
        # Expiry is persisted before the capacity decision, so the renewal stands.
        self.assertEqual(self.r.records[A]['expires'], 1230.0)

    def test_zero_healthy_workers_withdraws(self):
        self.r.create(A, TOKEN, body())
        self.r.ready(A, TOKEN)
        view = self.r.renew(A, TOKEN, 0)
        self.assertEqual(view['phase'], 'withdrawn')
        self.assertEqual(self.r.records[A]['phase'], 'withdrawn')
        targets = self.client.alias['target_groups'][0]['targets']
        self.assertEqual(targets, [{'provider': 'clef-nb-' + A,
                                    'model': 'clef-flash', 'enabled': False}])

    def test_invalid_worker_count_is_400(self):
        self.r.create(A, TOKEN, body())
        for n in (3, -1, True, 1.0, '1', None):
            with self.assertRaises(Fault, msg=repr(n)) as e:
                self.r.renew(A, TOKEN, n)
            self.assertEqual(e.exception.status, 400)

    def test_shrink_while_staging_updates_cap_before_admission(self):
        self.r.create(A, TOKEN, body(workers=2))
        view = self.r.renew(A, TOKEN, 1)
        self.assertEqual(view['phase'], 'staging')
        self.assertEqual(self.r.records[A]['workers'], 1)
        self.assertEqual(self.client.providers['clef-nb-' + A]['maxConcurrency'], 1)
        self.r.ready(A, TOKEN)
        self.assertEqual(self.client.providers['clef-nb-' + A]['maxConcurrency'], 1)


class Validation(Base):
    def _reject(self, payload, status=400):
        with self.assertRaises(Fault, msg=repr(payload)) as e:
            self.r.create(A, TOKEN, payload)
        self.assertEqual(e.exception.status, status)
        self.assertEqual(self.r.records, {})
        self.assertEqual(self.client.calls, [])

    def test_base_must_be_literal_tailnet_http_v1(self):
        bad = (
            'http://192.168.1.5:8443/v1',        # RFC1918, not tailnet
            'http://10.0.0.4:8443/v1',           # RFC1918, not tailnet
            'http://100.128.0.1:8443/v1',        # inside 100/8 but outside 100.64/10
            'http://plexus.example.invalid:8443/v1',  # a name, not a literal IP
            'https://100.64.1.2:8443/v1',        # wrong scheme
            'ftp://100.64.1.2:8443/v1',          # wrong scheme
            'http://100.64.1.2:80/v1',           # unsupported port
            'http://100.64.1.2:8769/v1',         # registrar port, not a notebook port
            'http://100.64.1.2:8443',            # missing /v1 path
            'http://100.64.1.2:8443/v1/',        # trailing slash
            'http://100.64.1.2:8443/v2',         # wrong API version
            'http://100.64.1.2:8443/v1?x=1',     # query
            'http://100.64.1.2:8443/v1#frag',    # fragment
            'http://user:pw@100.64.1.2:8443/v1',  # credentials
            'http://[fd7a:115c:a1e0::1]:8443/v1',  # IPv6
            'http://100.64.1.2:8443//v1',
            12345,
            None,
        )
        for base in bad:
            payload = body()
            payload['base'] = base
            self._reject(payload)

    def test_valid_tailnet_bases_are_accepted(self):
        for i, ip in enumerate(('100.64.0.1', '100.64.1.2', '100.127.255.254')):
            view = self.r.create(f'{i:032x}', TOKEN, body(ip))
            self.assertEqual(view['phase'], 'staging')
            self.r.records.clear()

    def test_workers_bounds(self):
        for n in (0, 3, -1, True, False, 1.0, 2.0, '1', None, [1]):
            payload = body()
            payload['workers'] = n
            self._reject(payload)

    def test_bearer_shape(self):
        for bearer in ('', 'e' * 63, 'e' * 65, 'E' * 64, 'z' * 64, None, 5, ['e' * 64]):
            payload = body()
            payload['bearer'] = bearer
            self._reject(payload)

    def test_body_fields_are_exact(self):
        self._reject({'base': body()['base'], 'workers': 1})
        self._reject({'base': body()['base'], 'workers': 1,
                      'bearer': BEARER, 'extra': 'x'})
        self._reject([])
        self._reject('not a body')
        self._reject(None)

    def test_rejection_happens_before_any_client_call(self):
        payload = body('192.168.1.5')
        with self.assertRaises(Fault):
            self.r.create(A, TOKEN, payload)
        self.assertEqual(self.client.calls, [])
        self.assertFalse(self.path.exists())


class Recovery(Base):
    def test_restart_demotes_before_recovery(self):
        self.r.create(A, TOKEN, body())
        self.r.ready(A, TOKEN)
        reloaded = Registry(self.r.path, self.client, now=lambda: self.clock[0])
        reloaded.recover()
        self.assertEqual(reloaded.records[A]['phase'], 'staging')
        self.assertFalse(self.client.alias['target_groups'][0]['targets'][0]['enabled'])
        # The demotion is durable, not just in memory.
        durable = json.loads(self.path.read_text())
        self.assertEqual(durable[A]['phase'], 'staging')

    def test_recovery_disables_every_published_target_in_one_write(self):
        self.r.create(A, TOKEN, body())
        self.r.create(B, TOKEN, body('100.64.1.3'))
        self.r.ready(A, TOKEN)
        self.r.ready(B, TOKEN)
        reloaded = Registry(self.r.path, self.client, now=lambda: self.clock[0])
        writes = len(self.client.kinds('pool'))
        reloaded.recover()
        self.assertEqual(len(self.client.kinds('pool')), writes + 1)
        targets = self.client.alias['target_groups'][0]['targets']
        self.assertEqual([(t['provider'], t['enabled']) for t in targets],
                         [('clef-nb-' + A, False), ('clef-nb-' + B, False)])
        self.assertEqual({r['phase'] for r in reloaded.records.values()}, {'staging'})

    def test_stale_heartbeat_cannot_enable_after_recovery(self):
        self.r.create(A, TOKEN, body())
        self.r.ready(A, TOKEN)
        reloaded = Registry(self.r.path, self.client, now=lambda: self.clock[0])
        reloaded.recover()
        view = reloaded.renew(A, TOKEN, 1)
        self.assertEqual(view['phase'], 'staging')
        self.assertFalse(self.client.alias['target_groups'][0]['targets'][0]['enabled'])
        self.assertEqual(len(self.client.kinds('prove')), 1)

    def test_recovery_requires_renewed_readiness(self):
        self.r.create(A, TOKEN, body())
        self.r.ready(A, TOKEN)
        reloaded = Registry(self.r.path, self.client, now=lambda: self.clock[0])
        reloaded.recover()
        view = reloaded.ready(A, TOKEN)
        self.assertEqual(view['phase'], 'active')
        self.assertEqual(len(self.client.kinds('prove')), 2)
        self.assertTrue(self.client.alias['target_groups'][0]['targets'][0]['enabled'])

    def test_stale_pending_admission_cannot_enable(self):
        self.r.create(A, TOKEN, body())
        entered, release = threading.Event(), threading.Event()
        self.client.prove_events['clef-nb-' + A] = (entered, release)
        errors = []

        def run():
            try:
                self.r.ready(A, TOKEN)
            except Exception as e:  # noqa: BLE001 - recorded and asserted below
                errors.append(e)

        thread = threading.Thread(target=run)
        thread.start()
        self.assertTrue(entered.wait(5))
        self.r.recover()  # as if the registrar restarted mid-admission
        release.set()
        thread.join(5)
        self.assertFalse(thread.is_alive())
        self.assertEqual(len(errors), 1)
        self.assertIsInstance(errors[0], Fault)
        self.assertEqual(errors[0].status, 409)
        self.assertNotEqual(self.r.records[A]['phase'], 'active')
        self.assertFalse(self.r.records[A]['published'])
        self.assertIsNone(self.client.alias)

    def test_recovery_clears_probe_state(self):
        self.r.create(A, TOKEN, body())
        self.r.records[A]['probing'] = True
        self.r.records[A]['probe_generation'] = 'deadbeef'
        self.r.persist()
        reloaded = Registry(self.r.path, self.client, now=lambda: self.clock[0])
        reloaded.recover()
        self.assertNotIn('probing', reloaded.records[A])
        self.assertNotIn('probe_generation', reloaded.records[A])


class Races(Base):
    def _block(self, run=A):
        entered, release = threading.Event(), threading.Event()
        self.client.prove_events['clef-nb-' + run] = (entered, release)
        return entered, release

    def _thread(self, target, *args):
        errors = []

        def run():
            try:
                target(*args)
            except Exception as e:  # noqa: BLE001 - asserted by the caller
                errors.append(e)

        thread = threading.Thread(target=run)
        thread.start()
        return thread, errors

    def test_duplicate_ready_while_probing_is_409(self):
        self.r.create(A, TOKEN, body())
        entered, release = self._block()
        thread, errors = self._thread(self.r.ready, A, TOKEN)
        self.assertTrue(entered.wait(5))
        with self.assertRaises(Fault) as e:
            self.r.ready(A, TOKEN)
        self.assertEqual(e.exception.status, 409)
        self.assertEqual(e.exception.message, 'readiness probe already in progress')
        release.set()
        thread.join(5)
        self.assertEqual(errors, [])
        self.assertEqual(self.r.records[A]['phase'], 'active')
        self.assertEqual(len(self.client.kinds('prove')), 1)

    def test_heartbeat_continues_during_probe(self):
        self.r.create(A, TOKEN, body(workers=2))
        entered, release = self._block()
        thread, errors = self._thread(self.r.ready, A, TOKEN)
        self.assertTrue(entered.wait(5))
        self.clock[0] += 60
        view = self.r.renew(A, TOKEN, 2)
        self.assertEqual(view['phase'], 'staging')
        self.assertEqual(self.r.records[A]['expires'], 1240.0)
        release.set()
        thread.join(5)
        self.assertEqual(errors, [])
        self.assertEqual(self.r.records[A]['phase'], 'active')

    def test_capacity_change_during_probe_rejected(self):
        self.r.create(A, TOKEN, body(workers=2))
        entered, release = self._block()
        thread, errors = self._thread(self.r.ready, A, TOKEN)
        self.assertTrue(entered.wait(5))
        with self.assertRaises(Fault) as e:
            self.r.renew(A, TOKEN, 1)
        self.assertEqual(e.exception.status, 409)
        self.assertEqual(e.exception.message,
                         'capacity changed while readiness proof is pending')
        self.assertEqual(self.r.records[A]['workers'], 2)
        self.assertEqual(self.client.providers['clef-nb-' + A]['maxConcurrency'], 2)
        release.set()
        thread.join(5)
        self.assertEqual(errors, [])
        self.assertEqual(self.r.records[A]['workers'], 2)

    def test_two_probes_complete_in_opposite_order(self):
        self.r.create(A, TOKEN, body())
        self.r.create(B, TOKEN, body('100.64.1.3'))
        a_in, a_release = self._block(A)
        b_in, b_release = self._block(B)
        ta, ea = self._thread(self.r.ready, A, TOKEN)
        tb, eb = self._thread(self.r.ready, B, TOKEN)
        self.assertTrue(a_in.wait(5))
        self.assertTrue(b_in.wait(5))
        # B finishes its probe first even though A started first.
        b_release.set()
        tb.join(5)
        a_release.set()
        ta.join(5)
        self.assertEqual((ea, eb), ([], []))
        self.assertEqual(self.r.records[A]['phase'], 'active')
        self.assertEqual(self.r.records[B]['phase'], 'active')
        targets = self.client.alias['target_groups'][0]['targets']
        self.assertEqual([(t['provider'], t['enabled']) for t in targets],
                         [('clef-nb-' + A, True), ('clef-nb-' + B, True)])
        self.assertEqual(self.client.providers['clef-nb-' + A]['maxConcurrency'], 1)
        self.assertEqual(self.client.providers['clef-nb-' + B]['maxConcurrency'], 1)

    def test_stop_during_other_instance_probe(self):
        self.r.create(A, TOKEN, body())
        self.r.ready(A, TOKEN)
        provider_a = deepcopy(self.client.providers['clef-nb-' + A])
        self.r.create(B, TOKEN, body('100.64.1.3'))
        entered, release = self._block(B)
        thread, errors = self._thread(self.r.ready, B, TOKEN)
        self.assertTrue(entered.wait(5))
        self.r.withdraw(A, TOKEN)
        release.set()
        thread.join(5)
        self.assertEqual(errors, [])
        self.assertEqual(self.r.records[A]['phase'], 'withdrawn')
        self.assertEqual(self.r.records[B]['phase'], 'active')
        targets = {t['provider']: t['enabled']
                   for t in self.client.alias['target_groups'][0]['targets']}
        self.assertEqual(targets, {'clef-nb-' + A: False, 'clef-nb-' + B: True})
        # Stopping A rewrote neither provider object.
        self.assertEqual(self.client.providers['clef-nb-' + A], provider_a)
        self.assertEqual(self.client.providers['clef-nb-' + B]['maxConcurrency'], 1)

    def test_stop_wins_over_inflight_ready(self):
        self.r.create(A, TOKEN, body())
        entered, release = self._block()
        thread, errors = self._thread(self.r.ready, A, TOKEN)
        self.assertTrue(entered.wait(5))
        self.r.withdraw(A, TOKEN)
        release.set()
        thread.join(5)
        self.assertEqual(len(errors), 1)
        self.assertIsInstance(errors[0], Fault)
        self.assertEqual(errors[0].status, 410)
        self.assertEqual(self.r.records[A]['phase'], 'withdrawn')
        # The lease was never published, so there was no target to disable.
        self.assertFalse(self.r.records[A]['published'])
        self.assertIsNone(self.client.alias)

    def test_probe_failure_publishes_nothing(self):
        cases = (RuntimeError('instance health mismatch'),
                 RuntimeError('invalid choice answer'),
                 TimeoutError('probe timed out'),
                 urllib.error.HTTPError('https://plexus.test/v1/systemone', 401,
                                        'unauthorized', None, io.BytesIO(b'')))
        for err in cases:
            with self.subTest(error=repr(err)):
                try:
                    with tempfile.TemporaryDirectory() as tmp:
                        client = Fake()
                        client.prove_error = err
                        registry = Registry(Path(tmp) / 'state.json', client,
                                            now=lambda: self.clock[0])
                        registry.create(A, TOKEN, body())
                        with self.assertRaises(type(err)):
                            registry.ready(A, TOKEN)
                        self.assertEqual(registry.records[A]['phase'], 'staging')
                        self.assertFalse(registry.records[A]['published'])
                        self.assertIsNone(client.alias)
                        self.assertEqual([c[0] for c in client.calls],
                                         ['provider', 'prove'])
                        # The probe flag must not leak: a later attempt can succeed.
                        client.prove_error = None
                        self.assertEqual(registry.ready(A, TOKEN)['phase'], 'active')
                finally:
                    close = getattr(err, 'close', None)
                    if close:
                        close()


def provider_payload(run=A, ip='100.64.1.2', workers=1):
    return {'api_base_url': {'systemone': f'http://{ip}:8443/v1'},
            'display_name': 'Clef notebook ' + run,
            'api_key': BEARER, 'enabled': True,
            'allow_100_percent_utilization': True,
            'models': {'clef-flash': {'type': 'decisions', 'access_via': ['systemone'],
                                      'pricing': {'source': 'simple', 'input': 0,
                                                  'output': 0}}},
            'maxConcurrency': workers, 'timeoutMs': 600000,
            'headers': {}, 'extraBody': {},
            'model_autosync': {'enabled': False, 'intervalMinutes': 60}}


def provider_readback(payload, **over):
    """What the deployed build returns on GET, including read-only extras."""
    got = deepcopy(payload)
    got['disable_cooldown'] = False
    got['stall_cooldown'] = None
    got['estimateTokens'] = False
    got['useClaudeMasking'] = False
    got['auto_compat'] = None
    got.update(over)
    return got


def pool_payload(*providers, **over):
    providers = providers or ('clef-nb-' + A,)
    cfg = {'target_groups': [{'name': 'NOTEBOOKS', 'selector': 'random',
                             'targets': [{'provider': p, 'model': 'clef-flash',
                                          'enabled': True} for p in providers]}],
           'priority': 'selector', 'type': 'decisions', 'sticky_session': False,
           'use_image_fallthrough': False, 'enforce_limits': False,
           'synthetic_safeguard_approval': False, 'additional_aliases': [],
           'metadata': {'source': 'custom', 'overrides': {
               'name': 'clef-flash-notebooks', 'context_length': 16384}}}
    cfg.update(over)
    return cfg


class FakeResponse:
    def __init__(self, payload):
        self._raw = b'' if payload is None else json.dumps(payload).encode()

    def read(self):
        return self._raw

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        return False


class FakeHTTPError(urllib.error.HTTPError):
    """HTTPError without urllib's file machinery, so tests stay warning-free."""

    def __init__(self, url, code):
        Exception.__init__(self, f'HTTP Error {code}: error')
        self.url, self.code, self.msg, self.hdrs, self.fp = url, code, 'error', None, None


class Gateway:
    """Strict fake of ``urllib.request.urlopen``.

    Any request that was not explicitly routed fails the test instead of
    returning a universal 200.
    """

    def __init__(self):
        self.routes = {}
        self.log = []

    def route(self, method, url, *responses):
        if len(responses) == 1 and isinstance(responses[0], list):
            responses = tuple(responses[0])
        self.routes[(method, url)] = list(responses)

    def calls(self, method=None):
        return [e for e in self.log if method is None or e['method'] == method]

    def paths(self):
        return [(e['method'], e['url']) for e in self.log]

    def __call__(self, request, timeout=None):
        method = request.get_method()
        url = request.full_url
        self.log.append({'method': method, 'url': url,
                         'body': json.loads(request.data) if request.data else None,
                         'headers': {k.lower(): v for k, v in request.header_items()},
                         'timeout': timeout})
        key = (method, url)
        if key not in self.routes:
            raise AssertionError(f'unexpected request {method} {url}')
        queue = self.routes[key]
        reply = queue[0] if len(queue) == 1 else queue.pop(0)
        if isinstance(reply, BaseException):
            raise reply
        status, payload = reply
        if status >= 400:
            raise FakeHTTPError(url, status)
        return FakeResponse(payload)


class AdapterCase(unittest.TestCase):
    base = 'https://plexus.test'

    def setUp(self):
        self.gw = Gateway()
        self.sleeps = []
        patcher = mock.patch('urllib.request.urlopen', self.gw)
        patcher.start()
        self.addCleanup(patcher.stop)
        self.p = Plexus(self.base, 'admin-key', sleep=self.sleeps.append)

    def provider_url(self, slug='clef-nb-' + A):
        return f'{self.base}/v0/management/providers/{slug}'

    def pool_url(self):
        return f'{self.base}/v0/management/aliases/clef-flash-notebooks'


class AdapterProvider(AdapterCase):
    def test_creates_missing_provider_and_verifies_readback(self):
        cfg = provider_payload()
        url = self.provider_url()
        self.gw.route('GET', url, (404, None), (200, provider_readback(cfg)))
        self.gw.route('PUT', url, (200, {'success': True, 'slug': 'clef-nb-' + A}))
        self.p.provider('clef-nb-' + A, cfg)
        self.assertEqual([m for m, _ in self.gw.paths()], ['GET', 'PUT', 'GET'])
        put = self.gw.calls('PUT')[0]
        self.assertEqual(put['body'], cfg)
        self.assertEqual(put['headers']['x-admin-key'], 'admin-key')
        self.assertNotIn('authorization', put['headers'])
        for entry in self.gw.log:
            self.assertNotIn('authorization', entry['headers'])

    def test_refuses_foreign_provider_without_writing(self):
        cfg = provider_payload()
        foreign = provider_readback(provider_payload(run=B))
        url = self.provider_url()
        self.gw.route('GET', url, (200, foreign))
        with self.assertRaises(RuntimeError) as e:
            self.p.provider('clef-nb-' + A, cfg)
        self.assertEqual(str(e.exception), 'existing provider not owned by this lease')
        self.assertEqual(self.gw.calls('PUT'), [])

    def test_ownership_is_rechecked_on_every_write(self):
        cfg = provider_payload()
        url = self.provider_url()
        self.gw.route('GET', url, (404, None), (200, provider_readback(cfg)))
        self.gw.route('PUT', url, (200, {'success': True, 'slug': 'clef-nb-' + A}))
        self.p.provider('clef-nb-' + A, cfg)
        # Someone replaced our provider with a foreign one of the same slug.
        self.gw.route('GET', url, (200, provider_readback(provider_payload(run=B))))
        with self.assertRaises(RuntimeError):
            self.p.provider('clef-nb-' + A, cfg)
        self.assertEqual(len(self.gw.calls('PUT')), 1)

    def test_masked_api_key_readback_is_accepted(self):
        cfg = provider_payload()
        url = self.provider_url()
        masked = provider_readback(cfg, api_key='sk-****************')
        self.gw.route('GET', url, (404, None), (200, masked))
        self.gw.route('PUT', url, (200, {'success': True}))
        self.p.provider('clef-nb-' + A, cfg)

    def test_masked_api_key_does_not_hide_config_drift(self):
        cfg = provider_payload(workers=1)
        url = self.provider_url()
        drifted = provider_readback(cfg, api_key='sk-****', maxConcurrency=2)
        self.gw.route('GET', url, (404, None), (200, drifted))
        self.gw.route('PUT', url, (200, {'success': True}))
        with self.assertRaises(RuntimeError) as e:
            self.p.provider('clef-nb-' + A, cfg)
        self.assertEqual(str(e.exception), 'provider readback mismatch')

    def test_unmasked_api_key_mismatch_is_rejected(self):
        cfg = provider_payload()
        url = self.provider_url()
        wrong = provider_readback(cfg, api_key='f' * 64)
        self.gw.route('GET', url, (404, None), (200, wrong))
        self.gw.route('PUT', url, (200, {'success': True}))
        with self.assertRaises(RuntimeError) as e:
            self.p.provider('clef-nb-' + A, cfg)
        self.assertEqual(str(e.exception), 'provider readback mismatch')

    def test_each_verified_field_is_checked(self):
        cfg = provider_payload()
        url = self.provider_url()
        drifts = ({'api_base_url': {'systemone': 'http://100.64.9.9:8443/v1'}},
                  {'models': {}},
                  {'timeoutMs': 1},
                  {'enabled': False})
        for drift in drifts:
            with self.subTest(drift=drift):
                self.gw.routes.clear()
                self.gw.route('GET', url, (404, None), (200, provider_readback(cfg, **drift)))
                self.gw.route('PUT', url, (200, {'success': True}))
                with self.assertRaises(RuntimeError) as e:
                    self.p.provider('clef-nb-' + A, cfg)
                self.assertEqual(str(e.exception), 'provider readback mismatch')

    def test_put_acknowledgement_is_not_readback(self):
        cfg = provider_payload()
        url = self.provider_url()
        self.gw.route('GET', url, (404, None))  # an already-missing object
        self.gw.route('PUT', url, (200, {'success': True, 'slug': 'clef-nb-' + A}))
        with self.assertRaises(urllib.error.HTTPError):
            self.p.provider('clef-nb-' + A, cfg)
        # A fresh GET followed the PUT: the acknowledgement itself is not proof.
        self.assertEqual([m for m, _ in self.gw.paths()], ['GET', 'PUT', 'GET'])

    def test_rejects_unsafe_slug(self):
        with self.assertRaises(ValueError):
            self.p.provider('../providers/other', provider_payload())
        self.assertEqual(self.gw.log, [])


class AdapterPool(AdapterCase):
    def test_creates_missing_pool_and_verifies_readback(self):
        cfg = pool_payload()
        url = self.pool_url()
        self.gw.route('GET', url, (404, None), (200, deepcopy(cfg)))
        self.gw.route('PUT', url, (200, {'success': True, 'slug': 'clef-flash-notebooks'}))
        self.p.pool(cfg)
        self.assertEqual([m for m, _ in self.gw.paths()], ['GET', 'PUT', 'GET'])
        self.assertEqual(self.gw.calls('PUT')[0]['body'], cfg)

    def test_refuses_unrelated_pool_alias_without_writing(self):
        cfg = pool_payload()
        url = self.pool_url()
        mutations = (
            {'type': 'chat'},
            {'additional_aliases': ['clef-flash']},
            {'use_image_fallthrough': True},
            {'enforce_limits': True},
        )
        for mutation in mutations:
            with self.subTest(mutation=mutation):
                current = deepcopy(cfg)
                current.update(mutation)
                self.gw.routes.clear()
                self.gw.route('GET', url, (200, current))
                with self.assertRaises(RuntimeError) as e:
                    self.p.pool(cfg)
                self.assertEqual(str(e.exception), 'pool ownership mismatch')
                self.assertEqual(self.gw.calls('PUT'), [])
        # Structural variants: extra group, renamed group, other selector.
        structural = (
            {'target_groups': cfg['target_groups'] + [
                {'name': 'FALLBACK', 'selector': 'random', 'targets': []}]},
            {'target_groups': [{'name': 'POOL', 'selector': 'random',
                                'targets': cfg['target_groups'][0]['targets']}]},
            {'target_groups': [{'name': 'NOTEBOOKS', 'selector': 'usage',
                                'targets': cfg['target_groups'][0]['targets']}]},
        )
        for current in structural:
            with self.subTest(current=current):
                self.gw.routes.clear()
                self.gw.route('GET', url, (200, current))
                with self.assertRaises(RuntimeError) as e:
                    self.p.pool(cfg)
                self.assertEqual(str(e.exception), 'pool ownership mismatch')
                self.assertEqual(self.gw.calls('PUT'), [])

    def test_refuses_unknown_pool_target_without_writing(self):
        cfg = pool_payload()
        url = self.pool_url()
        targets = (
            {'provider': 'clef-llama-remote', 'model': 'clef-flash', 'enabled': True},
            {'alias': 'cloudflare/clef-flash', 'enabled': True},
            {'target_alias_slug': 'cloudflare/clef-flash', 'enabled': True},
            {'provider': 'clef-nb-' + A, 'model': 'clef-flash', 'enabled': True,
             'weight': 2},
            {'provider': 'clef-nb-' + A, 'model': 'gpt-4o', 'enabled': True},
        )
        for target in targets:
            with self.subTest(target=target):
                current = deepcopy(cfg)
                current['target_groups'][0]['targets'] = [target]
                self.gw.routes.clear()
                self.gw.route('GET', url, (200, current))
                with self.assertRaises(RuntimeError) as e:
                    self.p.pool(cfg)
                self.assertEqual(str(e.exception), 'pool contains unknown target')
                self.assertEqual(self.gw.calls('PUT'), [])

    def test_noop_pool_put_is_detected(self):
        cfg = pool_payload()
        url = self.pool_url()
        stale = deepcopy(cfg)
        stale['target_groups'][0]['targets'] = []
        self.gw.route('GET', url, (404, None), (200, stale))
        self.gw.route('PUT', url, (200, {'success': True}))
        with self.assertRaises(RuntimeError) as e:
            self.p.pool(cfg)
        self.assertEqual(str(e.exception), 'pool readback mismatch')

    def test_pool_readback_verifies_every_field(self):
        cfg = pool_payload()
        url = self.pool_url()
        for key, value in (('sticky_session', True), ('priority', 'performance'),
                           ('metadata', {'source': 'custom', 'overrides': {
                               'name': 'clef-flash-notebooks', 'context_length': 32768}})):
            with self.subTest(key=key):
                got = deepcopy(cfg)
                got[key] = value
                self.gw.routes.clear()
                self.gw.route('GET', url, (404, None), (200, got))
                self.gw.route('PUT', url, (200, {'success': True}))
                with self.assertRaises(RuntimeError) as e:
                    self.p.pool(cfg)
                self.assertEqual(str(e.exception), 'pool readback mismatch')

    def test_missing_only_swallows_404(self):
        self.gw.route('GET', self.pool_url(), (500, None))
        with self.assertRaises(urllib.error.HTTPError):
            self.p.missing('/v0/management/aliases/clef-flash-notebooks')
        self.gw.route('GET', self.pool_url(), (404, None))
        self.assertTrue(self.p.missing('/v0/management/aliases/clef-flash-notebooks'))

    def test_base_url_validation(self):
        for bad in ('ftp://plexus.test', 'plexus.test', 'https://user:pw@plexus.test',
                    'https://plexus.test/?x=1', 'https://plexus.test/#f', '', None):
            with self.assertRaises(ValueError, msg=repr(bad)):
                Plexus(bad, 'admin-key')
        self.assertEqual(Plexus('https://plexus.test/', 'k').base, 'https://plexus.test')


def probe_answers(**over):
    """A well-formed /v1/systemone answer block, keyed by our probe questions."""
    answers = {
        'fruit': {'type': 'choice', 'choice': 'fruit', 'confidence': 0.91,
                  'probabilities': {'fruit': 0.91, 'vegetable': 0.05, 'other': 0.04}},
        'sure': {'type': 'noul', 'noul': 0.93},
        'score': {'type': 'score', 'score': 2.0,
                  'legend': {'0': 'No food is named.', '1': 'Food, no classification.',
                             '2': 'Food and explicit classification.'},
                  'probabilities': {'0': 0.0, '1': 0.1, '2': 0.9}},
    }
    answers.update(over)
    return {'answers': answers,
            'usage': {'input_tokens': 140, 'output_tokens': 0},
            'model': 'clef-flash'}


class AdapterProbe(AdapterCase):
    slug = 'clef-nb-' + A

    def setUp(self):
        super().setUp()
        self.cfg = provider_payload()
        self.health_url = 'http://100.64.1.2:8443/health'
        self.key_url = f'{self.base}/v0/management/keys/clef-probe-{A}'
        self.systemone_url = f'{self.base}/v1/systemone'
        self.health = {'run_id': A, 'active': 0,
                       'workers': [{'alive': True, 'healthy': True}]}

    def _routes(self, *, health=None, key_get=(200, {'allowedProviders': ['clef-nb-' + A]}),
                systemone=(200, probe_answers()), delete=(200, {'success': True}),
                absence=(404, None)):
        self.gw.route('GET', self.health_url, (200, health or self.health))
        self.gw.route('PUT', self.key_url, (200, {'success': True}))
        self.gw.route('GET', self.key_url, key_get, absence)
        self.gw.route('POST', self.systemone_url, systemone)
        self.gw.route('DELETE', self.key_url, delete)

    def test_probes_through_scoped_direct_key_then_deletes_it(self):
        self._routes()
        self.assertIsNone(self.p.prove(self.slug, self.cfg, A))
        self.assertEqual(
            [(m, u) for m, u in self.gw.paths()],
            [('GET', self.health_url), ('PUT', self.key_url), ('GET', self.key_url),
             ('POST', self.systemone_url), ('DELETE', self.key_url),
             ('GET', self.key_url)])
        put = self.gw.calls('PUT')[0]
        secret = put['body']['secret']
        self.assertTrue(secret.startswith('sk-'))
        self.assertEqual(put['body']['allowedProviders'], [self.slug])
        self.assertEqual(put['body']['comment'], 'temporary Clef readiness probe')
        self.assertEqual(put['headers']['x-admin-key'], 'admin-key')
        post = self.gw.calls('POST')[0]
        self.assertEqual(post['url'], self.systemone_url)
        self.assertEqual(post['headers']['authorization'], 'Bearer ' + secret)
        self.assertNotEqual(post['headers']['authorization'], 'Bearer ' + BEARER)
        self.assertEqual(post['timeout'], 120)
        self.assertEqual(post['body']['model'], f'direct/{self.slug}/clef-flash')
        self.assertEqual(post['body']['state']['run_id'], A)
        self.assertEqual(set(post['body']['questions']), {'fruit', 'sure', 'score'})
        self.assertEqual(post['body']['questions']['fruit']['type'], 'choice')
        self.assertEqual(post['body']['questions']['sure']['type'], 'noul')
        self.assertEqual(post['body']['questions']['score']['type'], 'score')
        self.assertIn('criteria', post['body']['questions']['fruit'])
        self.assertEqual(self.gw.calls('GET')[0]['timeout'], 10)
        self.assertEqual(self.gw.calls('DELETE')[0]['body'], None)

    def test_health_mismatch_blocks_key_creation(self):
        bad_health = (
            {'run_id': B, 'workers': [{'alive': True, 'healthy': True}]},
            {'run_id': A, 'workers': []},
            {'run_id': A, 'workers': [{'alive': True, 'healthy': True}] * 2},
            {'run_id': A, 'workers': [{'alive': True, 'healthy': False}]},
            {'run_id': A, 'workers': [{'alive': False, 'healthy': True}]},
            {'run_id': A},
        )
        for health in bad_health:
            with self.subTest(health=health):
                self.gw.routes.clear()
                self.gw.log.clear()
                self._routes(health=health)
                with self.assertRaises(RuntimeError) as e:
                    self.p.prove(self.slug, self.cfg, A)
                self.assertEqual(str(e.exception), 'instance health mismatch')
                self.assertEqual(self.gw.calls('PUT'), [])
                self.assertEqual(self.gw.calls('POST'), [])

    def test_invalid_answer_shapes_fail_and_still_delete_key(self):
        cases = ({'fruit': {'type': 'choice', 'choice': 'meat'}},
                 {'fruit': {'type': 'noul', 'noul': 0.5}},
                 {'fruit': {'type': 'choice'}},
                 {'sure': {'type': 'noul', 'noul': 1.5}},
                 {'sure': {'type': 'score', 'score': 1.0}},
                 {'sure': {'type': 'noul', 'noul': 'yes'}},
                 {'score': {'type': 'score', 'score': 3.0}},
                 {'score': {'type': 'score', 'score': -1}},
                 {'score': {'type': 'score', 'score': float('nan')}},
                 {'score': {'type': 'score', 'score': '2'}},
                 None)  # the fruit answer is missing entirely
        for mutation in cases:
            with self.subTest(mutation=mutation):
                self.gw.routes.clear()
                self.gw.log.clear()
                answers = probe_answers(**(mutation or {}))
                if mutation is None:
                    del answers['answers']['fruit']
                self._routes(systemone=(200, answers))
                with self.assertRaises(RuntimeError) as e:
                    self.p.prove(self.slug, self.cfg, A)
                self.assertIn(str(e.exception),
                              ('invalid choice answer', 'invalid numeric answer'))
                # The temporary key is deleted even on a failed probe.
                self.assertEqual([c['method'] for c in self.gw.calls()][-2:],
                                 ['DELETE', 'GET'])
                self.assertEqual(self.gw.paths()[-1], ('GET', self.key_url))

    def test_upstream_401_is_not_retried(self):
        self._routes(systemone=(401, {'error': 'unauthorized'}))
        with self.assertRaises(urllib.error.HTTPError) as e:
            self.p.prove(self.slug, self.cfg, A)
        self.assertEqual(e.exception.code, 401)
        self.assertEqual(len(self.gw.calls('POST')), 1)
        self.assertEqual(self.sleeps, [])
        self.assertEqual(self.gw.calls('DELETE')[0]['url'], self.key_url)

    def test_transient_503_is_retried_with_bounded_timeout(self):
        self._routes(systemone=[(503, None), (503, None), (200, probe_answers())])
        self.p.prove(self.slug, self.cfg, A)
        posts = self.gw.calls('POST')
        self.assertEqual(len(posts), 3)
        self.assertEqual({p['timeout'] for p in posts}, {120})
        self.assertEqual(self.sleeps, [3, 3])

    def test_timeout_is_bounded_retried_and_never_publishes(self):
        self._routes(systemone=[TimeoutError('timed out')])
        with self.assertRaises(TimeoutError):
            self.p.prove(self.slug, self.cfg, A)
        posts = self.gw.calls('POST')
        self.assertEqual(len(posts), 3)
        self.assertEqual({p['timeout'] for p in posts}, {120})
        self.assertEqual(self.sleeps, [3, 3])
        self.assertEqual(self.gw.calls('DELETE')[0]['url'], self.key_url)

    def test_probe_key_scope_mismatch_fails(self):
        self._routes(key_get=(200, {'allowedProviders': ['clef-nb-' + B]}))
        with self.assertRaises(RuntimeError) as e:
            self.p.prove(self.slug, self.cfg, A)
        self.assertEqual(str(e.exception), 'probe key scope mismatch')
        self.assertEqual(self.gw.calls('POST'), [])
        self.assertEqual(self.gw.calls('DELETE')[0]['url'], self.key_url)

    def test_probe_key_deletion_must_be_absent(self):
        # DELETE succeeds but the key is still readable: a live credential.
        self.gw.route('GET', self.health_url, (200, self.health))
        self.gw.route('PUT', self.key_url, (200, {'success': True}))
        self.gw.route('GET', self.key_url, (200, {'allowedProviders': [self.slug]}))
        self.gw.route('POST', self.systemone_url, (200, probe_answers()))
        self.gw.route('DELETE', self.key_url, (200, {'success': True}))
        with self.assertRaises(RuntimeError) as e:
            self.p.prove(self.slug, self.cfg, A)
        self.assertEqual(str(e.exception), 'probe key deletion not confirmed')

    def test_already_absent_probe_key_is_tolerated(self):
        self._routes(delete=(404, None))
        self.p.prove(self.slug, self.cfg, A)

    def test_probe_key_delete_failure_is_reported(self):
        self._routes(delete=(500, None))
        with self.assertRaises(urllib.error.HTTPError) as e:
            self.p.prove(self.slug, self.cfg, A)
        self.assertEqual(e.exception.code, 500)


class Http(Base):
    """Exercise the real Handler over a loopback socket (127.0.0.1:0 only)."""

    def setUp(self):
        super().setUp()
        self.server = ThreadingHTTPServer(('127.0.0.1', 0), Handler)
        self.server.daemon_threads = True
        self.server.registry = self.r
        self.server.bootstrap = TOKEN
        self.thread = threading.Thread(target=self.server.serve_forever,
                                       kwargs={'poll_interval': 0.01}, daemon=True)
        self.thread.start()
        self.addCleanup(self.server.server_close)
        self.addCleanup(self.server.shutdown)
        self.base = f'http://127.0.0.1:{self.server.server_port}'

    def request(self, method, path, payload=None, *, auth=TOKEN, lease=TOKEN,
                raw=None):
        data = raw if raw is not None else (
            None if payload is None else json.dumps(payload).encode())
        req = urllib.request.Request(self.base + path, data=data, method=method)
        if auth is not None:
            req.add_header('authorization', 'Bearer ' + auth)
        if lease is not None:
            req.add_header('x-lease-token', lease)
        if data is not None:
            req.add_header('content-type', 'application/json')
        try:
            with urllib.request.urlopen(req, timeout=10) as f:
                return f.status, json.loads(f.read() or b'{}'), f.headers['content-type']
        except urllib.error.HTTPError as e:
            with e:
                raw_body = e.read()
            return e.code, json.loads(raw_body or b'{}'), e.headers['content-type']

    def lease_path(self, run=A):
        return '/v1/leases/' + run

    def test_health_is_unauthenticated(self):
        status, view, ctype = self.request('GET', '/health', auth=None, lease=None)
        self.assertEqual((status, view), (200, {'status': 'ok'}))
        self.assertEqual(ctype, 'application/json')

    def test_lifecycle_over_http(self):
        status, view, ctype = self.request('PUT', self.lease_path(), body())
        self.assertEqual(status, 200)
        self.assertEqual(view['run_id'], A)
        self.assertEqual(view['provider'], 'clef-nb-' + A)
        self.assertEqual(view['phase'], 'staging')
        self.assertEqual(ctype, 'application/json')
        status, view, _ = self.request('POST', self.lease_path() + '/heartbeat',
                                       {'workers': 1})
        self.assertEqual((status, view['phase']), (200, 'staging'))
        status, view, _ = self.request('POST', self.lease_path() + '/ready', {})
        self.assertEqual((status, view['phase']), (200, 'active'))
        status, view, _ = self.request('GET', self.lease_path())
        self.assertEqual((status, view['phase']), (200, 'active'))
        status, view, _ = self.request('DELETE', self.lease_path())
        self.assertEqual((status, view['phase']), (200, 'withdrawn'))
        # A terminal lease stays readable so a client can resolve a lost ready reply.
        status, view, _ = self.request('GET', self.lease_path())
        self.assertEqual((status, view['phase']), (200, 'withdrawn'))
        status, view, _ = self.request('POST', self.lease_path() + '/heartbeat',
                                       {'workers': 1})
        self.assertEqual(status, 410)
        self.assertEqual(view, {'error': 'lease ended; create a new runtime'})

    def test_missing_or_wrong_bootstrap_is_403(self):
        for bootstrap in (None, 'f' * 64, ''):
            with self.subTest(bootstrap=bootstrap):
                status, view, _ = self.request('PUT', self.lease_path(), body(),
                                               auth=bootstrap)
                self.assertEqual((status, view), (403, {'error': 'unauthorized'}))
        self.assertEqual(self.r.records, {})
        self.assertEqual(self.client.calls, [])

    def test_invalid_lease_token_is_403(self):
        for token in (None, '', 'zz', 'c' * 63):
            with self.subTest(token=token):
                status, view, _ = self.request('PUT', self.lease_path(), body(),
                                               lease=token)
                self.assertEqual((status, view), (403, {'error': 'invalid lease token'}))

    def test_unknown_and_malformed_routes_are_404(self):
        cases = (('GET', '/v1/leases/' + 'f' * 32, None),
                 ('GET', '/v1/leases/nope', None),
                 ('GET', '/v1/other', None),
                 ('GET', '/v1/leases', None),
                 ('PUT', self.lease_path() + '/ready', None),
                 ('GET', self.lease_path() + '/ready', None),
                 ('POST', self.lease_path() + '/bogus', None),
                 ('PUT', '/v1/leases/' + A + '/heartbeat', {'workers': 1}))
        for method, path, payload in cases:
            with self.subTest(case=(method, path)):
                status, view, _ = self.request(method, path, payload)
                self.assertEqual(status, 404)
                self.assertEqual(set(view), {'error'})
        self.assertEqual(self.client.calls, [])

    def test_invalid_json_is_400(self):
        status, view, _ = self.request('PUT', self.lease_path(), raw=b'{not json')
        self.assertEqual((status, view), (400, {'error': 'invalid request'}))
        self.assertEqual(self.r.records, {})

    def test_body_shape_errors_are_400(self):
        status, view, _ = self.request('PUT', self.lease_path(),
                                       {'base': body()['base'], 'workers': 1})
        self.assertEqual(status, 400)
        self.assertEqual(view, {'error': 'expected base, workers and bearer'})
        status, view, _ = self.request('PUT', self.lease_path(),
                                       dict(body(), extra='x'))
        self.assertEqual(status, 400)
        self.r.create(A, TOKEN, body())
        for payload in ({'workers': 3}, {'workers': '1'}, {}, {'workers': 1, 'x': 2}):
            with self.subTest(payload=payload):
                status, view, _ = self.request('POST', self.lease_path() + '/heartbeat',
                                               payload)
                self.assertEqual(status, 400)
        status, view, _ = self.request('POST', self.lease_path() + '/ready', {'x': 1})
        self.assertEqual(status, 404)

    def test_conflicting_body_is_409(self):
        self.request('PUT', self.lease_path(), body())
        status, view, _ = self.request('PUT', self.lease_path(), body('100.64.1.9'))
        self.assertEqual((status, view),
                         (409, {'error': 'run id already has different configuration'}))

    def test_oversized_body_is_400(self):
        status, view, _ = self.request('PUT', self.lease_path(), raw=b'x' * 5000)
        self.assertEqual((status, view), (400, {'error': 'invalid body size'}))
        self.assertEqual(self.r.records, {})

    def test_upstream_failures_never_echo_payloads_or_secrets(self):
        leak = f'provider readback mismatch api_key={BEARER} bootstrap={TOKEN}'
        status, view, _ = self.request('PUT', self.lease_path(), body())
        self.assertEqual(status, 200)
        self.client.provider_error = RuntimeError(leak)
        status, view, ctype = self.request('POST', self.lease_path() + '/ready', {})
        self.assertEqual((status, view),
                         (503, {'error': 'registration not confirmed; retry safely'}))
        self.assertEqual(ctype, 'application/json')
        rendered = json.dumps(view)
        for secret in (BEARER, TOKEN, 'api_key', 'sk-'):
            self.assertNotIn(secret, rendered)
        self.client.provider_error = None
        self.client.pool_error = RuntimeError(leak)
        status, view, _ = self.request('POST', self.lease_path() + '/ready', {})
        self.assertEqual((status, view),
                         (503, {'error': 'registration not confirmed; retry safely'}))
        self.assertNotIn(BEARER, json.dumps(view))
        self.assertEqual(self.r.records[A]['phase'], 'staging')

    def test_responses_never_include_credentials(self):
        seen = [self.request('PUT', self.lease_path(), body())[1]]
        seen.append(self.request('PUT', self.lease_path(), body())[1])
        seen.append(self.request('POST', self.lease_path() + '/heartbeat',
                                 {'workers': 1})[1])
        seen.append(self.request('POST', self.lease_path() + '/ready', {})[1])
        seen.append(self.request('GET', self.lease_path())[1])
        seen.append(self.request('DELETE', self.lease_path())[1])
        for payload in seen:
            self.assertEqual(set(payload), {'run_id', 'provider', 'phase', 'expires'})
            rendered = json.dumps(payload)
            for secret in (BEARER, TOKEN, 'token_hash', 'api_key', 'Bearer'):
                self.assertNotIn(secret, rendered)


class Recorder:
    """Fake adapter that records reconcile writes for ordering assertions."""

    def __init__(self, events, error=None):
        self.events = events
        self.error = error

    def provider(self, slug, cfg):
        self.events.append(('provider', slug))

    def pool(self, cfg):
        self.events.append('pool')
        if self.error:
            raise self.error

    def prove(self, slug, cfg, run_id):
        self.events.append('prove')


class Service(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.dir = Path(self.tmp.name)
        self.lock_path = self.dir / 'registrar.lock'
        self.state_path = self.dir / 'state' / 'notebooks.json'

    def server_factory(self, events):
        class FakeServer:
            def __init__(self, address, handler):
                self.address, self.handler = address, handler
                self.registry = None
                self.bootstrap = None
                self.daemon_threads = False
                events.append(('listen', address))

        return FakeServer

    def seed_published(self):
        client = Fake()
        registry = Registry(self.state_path, client, now=lambda: 1000.0)
        registry.create(A, TOKEN, body())
        registry.ready(A, TOKEN)
        return registry

    def test_lock_precedes_recovery_and_listening(self):
        self.seed_published()
        events = []
        client = Recorder(events)
        handle, registry, server = start(self.lock_path, self.state_path, client,
                                         '100.121.214.107', TOKEN, port=8769,
                                         server_factory=self.server_factory(events))
        self.addCleanup(handle.close)
        # Recovery demoted the lease (pool write) BEFORE the listener existed.
        self.assertEqual(events, ['pool', ('listen', ('100.121.214.107', 8769))])
        self.assertEqual(registry.records[A]['phase'], 'staging')
        self.assertIs(server.registry, registry)
        self.assertEqual(server.bootstrap, TOKEN)
        self.assertIs(server.handler, Handler)
        self.assertTrue(server.daemon_threads)

    def test_second_registrar_refuses_before_touching_state(self):
        self.seed_published()
        first_events = []
        handle, _, _ = start(self.lock_path, self.state_path, Recorder(first_events),
                             '100.121.214.107', TOKEN,
                             server_factory=self.server_factory(first_events))
        self.addCleanup(handle.close)
        second_events = []
        with self.assertRaises(SystemExit) as e:
            start(self.lock_path, self.state_path, Recorder(second_events),
                  '100.121.214.107', TOKEN,
                  server_factory=self.server_factory(second_events))
        self.assertEqual(str(e.exception), 'another registrar owns the pool')
        # No demotion, no pool write, no listener: the singleton is checked first.
        self.assertEqual(second_events, [])

    def test_failed_recovery_never_opens_the_listener(self):
        self.seed_published()
        events = []
        client = Recorder(events, error=RuntimeError('pool readback mismatch'))
        with self.assertRaises(RuntimeError):
            start(self.lock_path, self.state_path, client, '100.121.214.107', TOKEN,
                  server_factory=self.server_factory(events))
        self.assertEqual(events, ['pool'])
        self.assertFalse([e for e in events if isinstance(e, tuple)])

    def test_lock_is_released_after_a_failed_start(self):
        self.seed_published()
        events = []
        with self.assertRaises(RuntimeError):
            start(self.lock_path, self.state_path,
                  Recorder(events, error=RuntimeError('pool readback mismatch')),
                  '100.121.214.107', TOKEN, server_factory=self.server_factory(events))
        events.clear()
        handle, _, _ = start(self.lock_path, self.state_path, Recorder(events),
                             '100.121.214.107', TOKEN,
                             server_factory=self.server_factory(events))
        self.addCleanup(handle.close)
        self.assertIn(('listen', ('100.121.214.107', 8769)), events)

    def test_acquire_singleton_reacquirable_after_close(self):
        handle = acquire_singleton(self.lock_path)
        with self.assertRaises(SystemExit):
            acquire_singleton(self.lock_path)
        handle.close()
        again = acquire_singleton(self.lock_path)
        again.close()
        self.assertEqual(self.lock_path.parent.stat().st_mode & 0o777, 0o700)
        self.assertEqual(self.lock_path.stat().st_mode & 0o777, 0o600)

    def test_validate_bind_refuses_non_tailnet_listeners(self):
        for bad in ('0.0.0.0', '127.0.0.1', '192.168.1.2', '100.128.0.1',
                    'fd7a:115c:a1e0::1', 'plexus.test', '', None):
            with self.assertRaises(SystemExit, msg=repr(bad)):
                validate_bind(bad)
        self.assertEqual(validate_bind('100.121.214.107'), '100.121.214.107')


class Terminal(Base):
    def test_changed_body_create_cannot_hijack_tombstone(self):
        self.r.create(A, TOKEN, body())
        self.r.ready(A, TOKEN)
        self.r.withdraw(A, TOKEN)
        before = deepcopy(self.r.records[A])
        for payload in (body(), body('100.64.9.9'), body(workers=2)):
            with self.subTest(payload=payload):
                with self.assertRaises(Fault) as e:
                    self.r.create(A, TOKEN, payload)
                self.assertEqual(e.exception.status, 410)
        self.assertEqual(self.r.records[A], before)
        self.assertEqual(self.r.records[A]['phase'], 'withdrawn')

    def test_duplicate_concurrent_stop_is_safe(self):
        self.r.create(A, TOKEN, body())
        self.r.ready(A, TOKEN)
        results, errors = [], []

        def stop():
            try:
                results.append(self.r.withdraw(A, TOKEN))
            except Exception as e:  # noqa: BLE001 - asserted below
                errors.append(e)

        threads = [threading.Thread(target=stop) for _ in range(2)]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join(5)
            self.assertFalse(thread.is_alive(), 'concurrent stop deadlocked')
        self.assertEqual(errors, [])
        self.assertEqual([r['phase'] for r in results], ['withdrawn', 'withdrawn'])
        targets = self.client.alias['target_groups'][0]['targets']
        self.assertEqual(targets, [{'provider': 'clef-nb-' + A,
                                    'model': 'clef-flash', 'enabled': False}])

    def test_expiry_disables_only_that_target(self):
        self.r.create(A, TOKEN, body())
        self.r.create(B, TOKEN, body('100.64.1.3'))
        self.r.ready(A, TOKEN)
        self.r.ready(B, TOKEN)
        before_b = deepcopy(self.client.providers['clef-nb-' + B])
        self.clock[0] += 150
        self.r.renew(B, TOKEN, 1)  # B keeps heartbeating while A goes silent
        self.clock[0] += 40
        self.r.sweep()
        self.assertEqual(self.r.records[A]['phase'], 'expired')
        self.assertEqual(self.r.records[B]['phase'], 'active')
        targets = {t['provider']: t['enabled']
                   for t in self.client.alias['target_groups'][0]['targets']}
        self.assertEqual(targets, {'clef-nb-' + A: False, 'clef-nb-' + B: True})
        self.assertEqual(self.client.providers['clef-nb-' + B], before_b)
        self.assertEqual(len(self.client.kinds('prove')), 2)


if __name__ == '__main__':
    unittest.main()