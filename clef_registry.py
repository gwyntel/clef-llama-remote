"""Single-writer control plane for Clef notebook leases.

The registrar owns exactly one Plexus alias (``clef-flash-notebooks``) and one
Plexus provider per notebook runtime. Notebooks never receive Plexus
credentials: they present a per-run lease token to this service, and only this
service writes provider/pool state.

Failure paths deliberately keep tombstones and dirty reconciliation state
instead of pretending a failed external write rolled back.
"""

from __future__ import annotations

import fcntl
import hashlib
import hmac
import ipaddress
import json
import math
import os
import re
import secrets
import threading
import time
import urllib.error
import urllib.parse
import urllib.request
from copy import deepcopy
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path

POOL = 'clef-flash-notebooks'
TTL = 180
ID = re.compile(r'[0-9a-f]{32}\Z')
TOKEN = re.compile(r'[0-9a-f]{64}\Z')
PROVIDER = re.compile(r'clef-nb-[0-9a-f]{32}\Z')


class Fault(Exception):
    """A client-visible failure with an HTTP status code."""

    def __init__(self, status, message):
        self.status, self.message = status, message
        super().__init__(message)


def validate_body(body):
    """Validate a lease creation body before any network or state write.

    The base must be a literal tailnet IPv4 address on a supported System One
    port: no names (DNS could resolve anywhere), no credentials, query or
    fragment, and no alternate API version.
    """
    if not isinstance(body, dict) or set(body) != {'base', 'workers', 'bearer'}:
        raise Fault(400, 'expected base, workers and bearer')
    u = urllib.parse.urlsplit(body['base']) if isinstance(body['base'], str) else None
    try:
        ok = (u is not None and u.scheme == 'http' and u.path == '/v1' and not u.query
              and not u.fragment and not u.username and not u.password
              and u.port in (8080, 8443) and u.hostname
              and ipaddress.ip_address(u.hostname) in ipaddress.ip_network('100.64.0.0/10'))
    except (ValueError, TypeError):
        ok = False
    if not ok:
        raise Fault(400, 'expected literal tailnet IPv4 and supported System One port')
    if not isinstance(body['bearer'], str) or not TOKEN.fullmatch(body['bearer']):
        raise Fault(400, 'bearer must be 32-byte hex')
    n = body['workers']
    if type(n) is not int or n not in (1, 2):
        raise Fault(400, 'workers must be 1 or 2')
    return deepcopy(body)


def save(path, obj):
    """Atomically persist registrar state with owner-only permissions."""
    path.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
    tmp = path.with_suffix('.tmp')
    fd = os.open(tmp, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o600)
    with os.fdopen(fd, 'w') as f:
        json.dump(obj, f, sort_keys=True)
        f.flush()
        os.fsync(f.fileno())
    os.replace(tmp, path)


def provider_cfg(rec, workers=None):
    """Full Plexus provider config for a lease.

    ``workers`` overrides the recorded healthy count so a capacity write never
    mutates the lease record before the external write is confirmed.
    """
    concurrency = rec['workers'] if workers is None else workers
    return {
        'api_base_url': {'systemone': rec['body']['base']},
        'display_name': 'Clef notebook ' + rec['id'],
        'api_key': rec['bearer'], 'enabled': True,
        'allow_100_percent_utilization': True,
        'models': {'clef-flash': {'type': 'decisions', 'access_via': ['systemone'],
                                 'pricing': {'source': 'simple', 'input': 0, 'output': 0}}},
        'maxConcurrency': concurrency, 'timeoutMs': 600000,
        'headers': {}, 'extraBody': {},
        'model_autosync': {'enabled': False, 'intervalMinutes': 60},
    }


class Registry:
    def __init__(self, path, client, now=time.time):
        self.path, self.client, self.now = Path(path), client, now
        self.lock = threading.RLock()
        self.records = json.loads(self.path.read_text()) if self.path.exists() else {}
        self.dirty = False

    def persist(self):
        save(self.path, self.records)

    def own(self, run, token, terminal=False):
        r = self.records.get(run)
        if not r:
            raise Fault(404, 'unknown lease')
        if not hmac.compare_digest(r['token_hash'], hashlib.sha256(token.encode()).hexdigest()):
            raise Fault(403, 'wrong lease token')
        if not terminal and (r['phase'] in ('expired', 'withdrawn') or r['expires'] <= self.now()):
            raise Fault(410, 'lease ended; create a new runtime')
        return r

    def create(self, run, token, body):
        if not ID.fullmatch(run) or not TOKEN.fullmatch(token):
            raise Fault(400, 'invalid run id or lease token')
        body = validate_body(body)
        with self.lock:
            if run in self.records:
                r = self.own(run, token)
                if r['body'] != body:
                    raise Fault(409, 'run id already has different configuration')
                return self.public(r)
            r = {'id': run, 'body': body, 'workers': body['workers'],
                 'token_hash': hashlib.sha256(token.encode()).hexdigest(),
                 'bearer': body['bearer'], 'expires': self.now() + TTL,
                 'phase': 'staging', 'published': False}
            self.records[run] = r
            self.persist()
            return self.public(r)

    def public(self, r):
        return {'run_id': r['id'], 'provider': 'clef-nb-' + r['id'],
                'phase': r['phase'], 'expires': r['expires']}

    def _pool(self):
        """Rewrite the dedicated pool alias, verified by readback.

        Only published leases appear: active ones enabled, ended ones as
        disabled tombstones. An empty pool is never written, because the alias
        schema has no valid empty group.
        """
        entries = [r for r in self.records.values() if r['published']]
        if not entries:
            self.dirty = False
            return
        targets = [{'provider': 'clef-nb-' + r['id'], 'model': 'clef-flash',
                    'enabled': r['phase'] == 'active'}
                   for r in sorted(entries, key=lambda x: x['id'])]
        cfg = {'target_groups': [{'name': 'NOTEBOOKS', 'selector': 'random',
                                  'targets': targets}],
               'priority': 'selector', 'type': 'decisions', 'sticky_session': False,
               'use_image_fallthrough': False, 'enforce_limits': False,
               'synthetic_safeguard_approval': False, 'additional_aliases': [],
               'metadata': {'source': 'custom',
                            'overrides': {'name': POOL, 'context_length': 16384}}}
        self.dirty = True
        self.client.pool(cfg)
        self.dirty = False

    def ready(self, run, token):
        with self.lock:
            r = self.own(run, token)
            if r['phase'] == 'active':
                self._pool()
                return self.public(r)
            if r.get('probing'):
                raise Fault(409, 'readiness probe already in progress')
            r['probing'] = True
            cfg = provider_cfg(r)
            generation = secrets.token_hex(16)
            r['probe_generation'] = generation
            self.persist()
        try:
            slug = 'clef-nb-' + run
            self.client.provider(slug, cfg)
            self.client.prove(slug, cfg, run)
            with self.lock:
                r = self.own(run, token)
                # A recovery (service restart) or a competing admission invalidates
                # this generation, so a stale proof can never enable a target.
                if r.get('probe_generation') != generation or r['phase'] != 'staging':
                    raise Fault(409, 'readiness generation changed')
                # Intent is durable before an external write: restart must demote it.
                r['phase'] = 'admitting'
                r['published'] = True
                self.persist()
                r['phase'] = 'active'
                try:
                    self._pool()
                    self.persist()
                except Exception:
                    r['phase'] = 'staging'
                    self.persist()
                    self.dirty = True
                    try:
                        self._pool()  # Verified demotion after an ambiguously applied PUT.
                    except Exception:
                        self.dirty = True  # sweep retries; caller never receives READY.
                    raise
                return self.public(r)
        finally:
            with self.lock:
                rec = self.records.get(run)
                if rec is not None:
                    rec.pop('probing', None)
                    self.persist()

    def withdraw(self, run, token):
        with self.lock:
            r = self.own(run, token, terminal=True)
            r['phase'] = 'withdrawn'
            self.persist()
            self._pool()
            return self.public(r)

    def recover(self):
        """Fail closed after a restart, before the HTTP listener opens.

        Every published target is demoted to staging and the pool is rewritten
        with all targets disabled; a stale heartbeat cannot re-enable one, and
        a probe that started before the restart can no longer publish because
        its generation was cleared.
        """
        with self.lock:
            for r in self.records.values():
                r.pop('probing', None)
                r.pop('probe_generation', None)
                if r['phase'] in ('active', 'admitting'):
                    r['phase'] = 'staging'
            self.persist()
            self._pool()

    def sweep(self):
        """Expire dead leases, retry dirty capacity writes, reconcile the pool."""
        with self.lock:
            changed = False
            for r in self.records.values():
                if r['phase'] not in ('expired', 'withdrawn') and r['expires'] <= self.now():
                    r['phase'] = 'expired'
                    changed = True
            if changed:
                self.persist()
            errors = []
            for r in list(self.records.values()):
                pending = r.get('cap_dirty')
                if not pending:
                    continue
                # r['workers'] stays at the last CONFIRMED count until this succeeds,
                # so routing never over-subscribes a runtime whose shrink is unproven.
                try:
                    self.client.provider('clef-nb-' + r['id'], provider_cfg(r, pending))
                except Exception as e:  # keep dirty; retry next sweep
                    errors.append(e)
                    continue
                r['workers'] = pending
                r.pop('cap_dirty', None)
                self.persist()
            if changed or self.dirty:
                self._pool()  # pool reconciliation must not be blocked by a cap retry
            if errors:
                raise errors[0]

    def renew(self, run, token, workers):
        """Heartbeat: extend the lease, then reconcile the healthy worker cap.

        The expiry extension is persisted BEFORE any capacity write so a failed
        capacity update can never cost the lease its renewal.
        """
        if type(workers) is not int or workers not in (0, 1, 2):
            raise Fault(400, 'invalid healthy worker count')
        with self.lock:
            r = self.own(run, token)
            if workers == 0:
                return self.withdraw(run, token)
            if r.get('probing') and workers != r['workers']:
                raise Fault(409, 'capacity changed while readiness proof is pending')
            r['expires'] = self.now() + TTL
            self.persist()
            if r['phase'] == 'active' and workers != r['workers'] and workers > r['workers']:
                # Permit shrink only. Recovering a worker needs a fresh admission proof.
                raise Fault(409, 'capacity increase requires a new runtime')
            if workers < r['workers']:
                try:
                    self.client.provider('clef-nb-' + run, provider_cfg(r, workers))
                except Exception:
                    r['cap_dirty'] = workers
                    self.persist()
                    raise Fault(503, 'capacity update not confirmed; retry safely')
                r['workers'] = workers
                r.pop('cap_dirty', None)
                self.persist()
            return self.public(r)


def validate_plexus_base(base):
    """Reject unusable Plexus base URLs before any request is attempted."""
    if not isinstance(base, str):
        raise ValueError('plexus base must be an http(s) URL')
    u = urllib.parse.urlsplit(base)
    if (u.scheme not in ('http', 'https') or not u.netloc
            or u.username or u.password or u.query or u.fragment):
        raise ValueError('plexus base must be an http(s) URL without credentials, '
                         'query or fragment')
    return base.rstrip('/')


class Plexus:
    """Management + inference client for the deployed Plexus gateway.

    Ownership is enforced by content, not by a local cache: every write re-reads
    the existing object and refuses to overwrite anything this lease does not
    own. Readback is always a fresh GET, because a PUT only acknowledges.
    """

    def __init__(self, base, admin, sleep=time.sleep):
        self.base = validate_plexus_base(base)
        self.admin = admin
        self.sleep = sleep

    def call(self, method, path, body=None, bearer=None, timeout=30):
        headers = {'accept': 'application/json'}
        headers['authorization' if bearer else 'x-admin-key'] = (
            'Bearer ' + bearer if bearer else self.admin)
        data = None if body is None else json.dumps(body).encode()
        if data is not None:
            headers['content-type'] = 'application/json'
        request = urllib.request.Request(self.base + path, data=data,
                                         headers=headers, method=method)
        with urllib.request.urlopen(request, timeout=timeout) as f:
            raw = f.read()
        return json.loads(raw) if raw else {}

    def missing(self, path):
        try:
            self.call('GET', path)
            return False
        except urllib.error.HTTPError as e:
            if e.code == 404:
                return True
            raise

    def provider(self, slug, cfg):
        if not PROVIDER.fullmatch(slug):
            raise ValueError('invalid provider slug')
        path = '/v0/management/providers/' + slug
        if not self.missing(path):
            current = self.call('GET', path)
            # display_name encodes the owning run id: it is the ownership marker.
            if current.get('display_name') != cfg['display_name']:
                raise RuntimeError('existing provider not owned by this lease')
        self.call('PUT', path, cfg)
        got = self.call('GET', path)
        for key in ('api_base_url', 'models', 'maxConcurrency', 'timeoutMs', 'enabled'):
            if got.get(key) != cfg[key]:
                raise RuntimeError('provider readback mismatch')
        # Key comparison only when the deployed build returns it unmasked.
        published = got.get('api_key')
        if isinstance(published, str) and published and '*' not in published:
            if published != cfg['api_key']:
                raise RuntimeError('provider readback mismatch')

    def pool(self, cfg):
        path = '/v0/management/aliases/' + POOL
        if not self.missing(path):
            current = self.call('GET', path)
            groups = current.get('target_groups')
            if (current.get('type') != 'decisions' or current.get('additional_aliases')
                    or current.get('use_image_fallthrough') or current.get('enforce_limits')
                    or not isinstance(groups, list) or len(groups) != 1
                    or groups[0].get('name') != 'NOTEBOOKS'
                    or groups[0].get('selector') != 'random'):
                raise RuntimeError('pool ownership mismatch')
            known = {t['provider'] for t in cfg['target_groups'][0]['targets']}
            for target in groups[0].get('targets') or []:
                if (set(target) - {'provider', 'model', 'enabled'}
                        or target.get('provider') not in known
                        or target.get('model') != 'clef-flash'):
                    raise RuntimeError('pool contains unknown target')
        self.call('PUT', path, cfg)
        got = self.call('GET', path)
        for key in cfg:
            if got.get(key) != cfg[key]:
                raise RuntimeError('pool readback mismatch')

    def prove(self, slug, cfg, run, attempts=3, timeout=120):
        """Prove this instance end-to-end THROUGH Plexus, then remove the key.

        The probe uses a temporary inference key scoped to this one provider and
        ``direct/<slug>/clef-flash``, so no alias or fallback can mask a failure.
        Typed transport/capability checks only: no confidence threshold gates
        admission.
        """
        root = cfg['api_base_url']['systemone'][:-3]
        request = urllib.request.Request(root + '/health')
        with urllib.request.urlopen(request, timeout=10) as f:
            health = json.load(f)
        workers = health.get('workers', [])
        if (health.get('run_id') != run or len(workers) != cfg['maxConcurrency']
                or not all(w.get('alive') and w.get('healthy') for w in workers)):
            raise RuntimeError('instance health mismatch')
        name = 'clef-probe-' + run
        path = '/v0/management/keys/' + name
        secret = 'sk-' + secrets.token_hex(32)
        # PUT is idempotent, so a key left by a crashed probe of this same owned
        # lease is replaced rather than reused.
        try:
            self.call('PUT', path, {'secret': secret,
                                    'comment': 'temporary Clef readiness probe',
                                    'allowedProviders': [slug]})
            got = self.call('GET', path)
            if got.get('allowedProviders') != [slug]:
                raise RuntimeError('probe key scope mismatch')
            payload = {'model': f'direct/{slug}/clef-flash',
                       'state': {'run_id': run,
                                 'sample': {'food': 'apple', 'classification': 'fruit'}},
                       'questions': {
                           'fruit': {'type': 'choice',
                                     'instructions': 'Classify `sample.food` using the named food only.',
                                     'criteria': {
                                         'fruit': 'A fruit.',
                                         'vegetable': 'A non-fruit vegetable.',
                                         'other': 'Neither category or insufficient information.'}},
                           'sure': {'type': 'noul',
                                    'instructions': 'Does `sample.food` name a fruit?',
                                    'criteria': {'true': 'The named food is a fruit.',
                                                 'false': 'The named food is not a fruit.'}},
                           'score': {'type': 'score',
                                     'instructions': 'Which processing state describes `sample`?',
                                     'criteria': ['No food is named.',
                                                  'A food is named but no classification is supplied.',
                                                  'A food and an explicit classification are supplied.']}}}
            answer = None
            for attempt in range(attempts):
                try:
                    answer = self.call('POST', '/v1/systemone', payload,
                                       bearer=secret, timeout=timeout)
                    break
                except urllib.error.HTTPError as e:
                    # Only propagation-shaped failures are retryable; 401/403/409
                    # and malformed requests fail immediately.
                    if e.code not in (404, 500, 503) or attempt == attempts - 1:
                        raise
                except (TimeoutError, ConnectionError, urllib.error.URLError):
                    if attempt == attempts - 1:
                        raise
                self.sleep(3)
            answers = answer.get('answers', {})
            choice, noul, score = (answers.get(k, {}) for k in ('fruit', 'sure', 'score'))
            if (choice.get('type') != 'choice'
                    or choice.get('choice') not in ('fruit', 'vegetable', 'other')):
                raise RuntimeError('invalid choice answer')
            for obj, kind in ((noul, 'noul'), (score, 'score')):
                value = obj.get(kind)
                if (obj.get('type') != kind or type(value) not in (int, float)
                        or not math.isfinite(value)
                        or not 0 <= value <= (1 if kind == 'noul' else 2)):
                    raise RuntimeError('invalid numeric answer')
        finally:
            try:
                self.call('DELETE', path)
            except urllib.error.HTTPError as e:
                if e.code != 404:
                    raise
            if not self.missing(path):
                raise RuntimeError('probe key deletion not confirmed')


class Handler(BaseHTTPRequestHandler):
    """Tailnet-only lease API for notebooks.

    Responses carry lease state only: never the bearer, lease token, token hash,
    API key, or any upstream exception payload.
    """

    def log_message(self, format, *args):
        pass  # no credentials, bodies, URLs or raw exceptions in access logs

    def reply(self, status, payload):
        raw = json.dumps(payload).encode()
        self.send_response(status)
        self.send_header('content-type', 'application/json')
        self.send_header('content-length', str(len(raw)))
        self.end_headers()
        self.wfile.write(raw)

    def do_GET(self):
        if self.path == '/health':
            self.reply(200, {'status': 'ok'})
            return
        self.handle_request()

    def do_POST(self):
        self.handle_request()

    def do_PUT(self):
        self.handle_request()

    def do_DELETE(self):
        self.handle_request()

    def handle_request(self):
        try:
            auth = self.headers.get('authorization', '')
            if not hmac.compare_digest(auth, 'Bearer ' + self.server.bootstrap):
                raise Fault(403, 'unauthorized')
            parts = self.path.split('/')
            if (len(parts) not in (4, 5) or parts[1:3] != ['v1', 'leases']
                    or not ID.fullmatch(parts[3])):
                raise Fault(404, 'unknown route')
            run = parts[3]
            token = self.headers.get('x-lease-token', '')
            if not TOKEN.fullmatch(token):
                raise Fault(403, 'invalid lease token')
            length = int(self.headers.get('content-length', '0'))
            if not 0 <= length <= 4096:
                raise Fault(400, 'invalid body size')
            raw = self.rfile.read(length) if length else b''
            body = json.loads(raw) if raw else {}
            registry = self.server.registry
            if len(parts) == 4 and self.command == 'PUT':
                result = registry.create(run, token, body)
            elif len(parts) == 4 and self.command == 'GET':
                with registry.lock:
                    result = registry.public(registry.own(run, token, terminal=True))
            elif len(parts) == 4 and self.command == 'DELETE':
                result = registry.withdraw(run, token)
            elif len(parts) == 5 and parts[4] == 'heartbeat' and self.command == 'POST':
                if not isinstance(body, dict) or set(body) != {'workers'}:
                    raise Fault(400, 'expected workers')
                result = registry.renew(run, token, body['workers'])
            elif (len(parts) == 5 and parts[4] == 'ready'
                    and self.command == 'POST' and body == {}):
                result = registry.ready(run, token)
            else:
                raise Fault(404, 'unknown route')
            self.reply(200, result)
        except Fault as e:
            self.reply(e.status, {'error': e.message})
        except (ValueError, TypeError, UnicodeError):
            self.reply(400, {'error': 'invalid request'})
        except Exception:
            # Deliberately generic: upstream payloads may contain credentials.
            self.reply(503, {'error': 'registration not confirmed; retry safely'})


def validate_bind(host):
    """The registrar must listen on a literal tailnet IPv4, never a wildcard."""
    try:
        address = ipaddress.ip_address(host)
    except (ValueError, TypeError):
        raise SystemExit('registry must bind a literal tailnet IPv4')
    if address.version != 4 or address not in ipaddress.ip_network('100.64.0.0/10'):
        raise SystemExit('registry must bind a literal tailnet IPv4')
    return host


def acquire_singleton(path):
    """Take the process-wide registrar lock, held for the process lifetime.

    This runs BEFORE recovery: a second process must not demote or rewrite the
    pool merely because it started and then found the port occupied.
    """
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True, mode=0o700)
    fd = os.open(path, os.O_RDWR | os.O_CREAT, 0o600)
    handle = os.fdopen(fd, 'a')
    try:
        fcntl.flock(handle, fcntl.LOCK_EX | fcntl.LOCK_NB)
    except BlockingIOError:
        handle.close()
        raise SystemExit('another registrar owns the pool')
    return handle


def start(lock_path, state_path, client, host, bootstrap, *, port=8769,
          server_factory=ThreadingHTTPServer):
    """Lock, fail closed, then bind. Never binds a listener before recovery."""
    handle = acquire_singleton(lock_path)
    try:
        registry = Registry(state_path, client)
        registry.recover()  # fail closed BEFORE the HTTP listener exists
        server = server_factory((host, port), Handler)
        server.daemon_threads = True
        server.registry = registry
        server.bootstrap = bootstrap
    except BaseException:
        handle.close()
        raise
    return handle, registry, server


def sweep_forever(registry, interval=5):
    while True:
        try:
            registry.sweep()
        except Exception:
            print('registry reconciliation failed; retrying', flush=True)
        time.sleep(interval)


def main():
    host = validate_bind(os.environ.get('CLEF_REGISTRY_HOST', ''))
    base = os.environ['PLEXUS_BASE_URL']
    home = Path.home() / '.hermes'
    admin = (home / 'secrets' / 'plexus_admin_key').read_text().strip()
    bootstrap = (home / 'secrets' / 'clef_registry_key').read_text().strip()
    if not TOKEN.fullmatch(bootstrap):
        raise SystemExit('registry token must be 32-byte hex')
    client = Plexus(base, admin)
    handle, registry, server = start(
        home / 'state' / 'clef-notebook-registry.lock',
        home / 'state' / 'clef-notebook-registry.json',
        client, host, bootstrap)
    threading.Thread(target=sweep_forever, args=(registry,), daemon=True).start()
    try:
        server.serve_forever()
    finally:
        server.server_close()
        handle.close()


if __name__ == '__main__':
    main()
