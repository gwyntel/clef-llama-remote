"""Per-runtime lease client for the Clef notebook pool.

Embedded verbatim into one notebook code cell by ``tools/build_v10.py``.
Standard library only, self-contained, no import-time side effects.
"""

import json
import secrets
import threading
import time
import urllib.error
import urllib.request
import uuid

WINDOW = 120.0      # seconds without a confirmed renewal before the lease counts as lost
ATTEMPTS = 3        # create/withdraw attempts, including the first
TERMINAL = (400, 403, 404, 409, 410)   # never retried, never resurrected
PHASES = ('staging', 'admitting', 'active', 'expired', 'withdrawn')
UNRECOGNISED = 'unrecognised-phase'


def identity():
    """A fresh runtime identity: unique run id, 256-bit lease token, node hostname."""
    run = uuid.uuid4().hex
    return {'run_id': run, 'token': secrets.token_hex(32), 'hostname': 'clef-nb-' + run}


class LeaseFault(RuntimeError):
    """A lease operation failure. Never carries a credential, a body or a header."""

    def __init__(self, status, reason):
        self.status = status
        self.reason = reason
        super().__init__(reason if status is None else '%s (status=%s)' % (reason, status))


class Lease:
    """Client for one runtime's registrar lease.

    ``opener`` is a zero-argument factory returning an object with
    ``.open(request, timeout=...)``; ``interval``/``now`` are injectable so tests
    never wait on wall-clock time.
    """

    def __init__(self, base, key, ident, opener, healthy,
                 interval=20.0, now=time.monotonic):
        self.base = base.rstrip('/')
        self.key = key
        self.ident = dict(ident)
        self.opener = opener
        self.healthy = healthy
        self.interval = float(interval)
        self.now = now
        self.halt = threading.Event()
        self.admit_lock = threading.Lock()
        self.thread = None
        self.admit_thread = None
        self.admitting = False
        self.lost = False
        self.withdrawn = False
        self.phase = None
        self.last_error = None
        self.last_confirmed = now()

    def __repr__(self):
        return ('Lease(run_id=%s, phase=%s, lost=%s, withdrawn=%s)'
                % (self.ident['run_id'], self.phase, self.lost, self.withdrawn))

    __str__ = __repr__

    # -- transport ---------------------------------------------------------

    def _url(self, suffix=''):
        return self.base + '/v1/leases/' + self.ident['run_id'] + suffix

    @staticmethod
    def _phase_text(phase):
        """Registrar text is untrusted: only known phases may reach a diagnostic."""
        return phase if phase in PHASES else UNRECOGNISED

    def _reason(self, method, suffix, detail):
        what = method + (' ' + suffix if suffix else '')
        return 'registrar %s for run %s failed: %s' % (
            what, self.ident['run_id'], detail)

    def call(self, method, suffix='', body=None, timeout=30):
        """One HTTP attempt. Raises LeaseFault, never a raw urllib exception."""
        data = None if body is None else json.dumps(body).encode()
        headers = {'authorization': 'Bearer ' + self.key,
                   'x-lease-token': self.ident['token']}
        if data is not None:
            headers['content-type'] = 'application/json'
            headers['content-length'] = str(len(data))
        request = urllib.request.Request(self._url(suffix), data=data,
                                        headers=headers, method=method)
        try:
            with self.opener().open(request, timeout=timeout) as response:
                raw = response.read()
        except urllib.error.HTTPError as exc:
            code = getattr(exc, 'code', None)
            try:
                exc.close()     # neither the body nor the socket may be retained
            except Exception:
                pass
            raise LeaseFault(code, self._reason(method, suffix, 'status %s' % code))
        except Exception as exc:
            raise LeaseFault(None, self._reason(method, suffix,
                                                'transport ' + type(exc).__name__))
        try:
            payload = json.loads(raw) if raw else {}
        except (TypeError, ValueError):
            raise LeaseFault(None, self._reason(method, suffix, 'undecodable response'))
        if not isinstance(payload, dict):
            raise LeaseFault(None, self._reason(method, suffix, 'unexpected response shape'))
        return payload

    # -- write with bounded, idempotent retries -----------------------------

    def _write(self, method, suffix, body, label, verify, attempts=ATTEMPTS, timeout=30):
        """Bounded retries that reuse the same run id, token and body.

        503 means "not confirmed, retry safely"; a transport failure is ambiguous
        and is resolved by an authenticated status read before any retry.
        400/403/404/409/410 are never retried and never resurrect a lease.
        """
        last = None
        for _ in range(attempts):
            try:
                payload = self.call(method, suffix, body, timeout=timeout)
                self.last_error = None
                return payload
            except LeaseFault as exc:
                last = exc
                self.last_error = exc.reason
                if exc.status is not None and exc.status != 503:
                    raise
                if exc.status == 503:
                    continue
                resolved = verify()
                if resolved is not None:
                    self.last_error = None
                    return resolved
        reason = '%s not confirmed after %d attempts for run %s' % (
            label, attempts, self.ident['run_id'])
        self.last_error = reason
        raise LeaseFault(None if last is None else last.status, reason)

    def _verify_withdrawn(self):
        try:
            payload = self.call('GET')
        except LeaseFault as exc:
            if exc.status is None:
                return None
            raise
        if payload.get('phase') == 'withdrawn':
            return payload
        return None             # still live: repeat the identical, idempotent DELETE

    def _verify_created(self):
        try:
            return self.call('GET')
        except LeaseFault as exc:
            if exc.status is None or exc.status == 404:
                return None     # not applied (or still unknown): repeat the identical PUT
            raise

    # -- lifecycle ---------------------------------------------------------

    def create(self, base, workers, bearer):
        if self.thread is not None and self.thread.is_alive():
            raise RuntimeError('lease already has a renewal thread for run '
                               + self.ident['run_id'])
        payload = self._write('PUT', '', {'base': base, 'workers': workers,
                                         'bearer': bearer},
                             'registration', self._verify_created)
        self.phase = self._phase_text(payload.get('phase'))
        self.last_confirmed = self.now()
        self._start_watch()
        return payload

    def status(self):
        """Authenticated status read for diagnostics.

        Read-only by construction: it reports the registrar's phase and never claims a
        withdrawal (only a confirmed DELETE may do that) and never re-creates a lease.
        """
        payload = self.call('GET')
        self.phase = self._phase_text(payload.get('phase'))
        return payload

    def _start_watch(self):
        """Start the single dedicated renewal thread for this run id."""
        if self.thread is not None and self.thread.is_alive():
            raise RuntimeError('lease already has a renewal thread for run '
                               + self.ident['run_id'])
        self.thread = threading.Thread(target=self._watch, name='clef-lease-renew',
                                       daemon=True)
        self.thread.start()

    def _watch(self):
        while not self.halt.wait(self.interval):
            self._renew()

    def _renew(self):
        try:
            workers = self.healthy()
        except Exception as exc:
            self._unconfirmed('healthy worker count unavailable (%s)'
                              % type(exc).__name__)
            return
        try:
            payload = self.call('POST', '/heartbeat', {'workers': workers})
        except LeaseFault as exc:
            if exc.status == 410:
                # Terminal: the registrar tombstoned this lease. Never resurrect it.
                self.lost = True
                self.last_error = exc.reason
                self.halt.set()
                return
            self._unconfirmed(exc.reason)
            return
        phase = self._phase_text(payload.get('phase'))
        self.phase = phase
        if phase in ('withdrawn', 'expired'):
            self.lost = True
            self.last_error = 'lease ended: phase %s for run %s' % (
                phase, self.ident['run_id'])
            self.halt.set()
            return
        self.last_confirmed = self.now()
        # Confirmation returned, so this runtime is no longer in an unconfirmed window. Leaving
        # `lost` sticky would strand a runtime that outlived a transient outage: it would keep
        # serving while the registrar's demotion kept it out of the pool, forever.
        self.lost = False
        self.last_error = None
        if phase == 'staging':
            # Registrar restarted or demoted us: re-prove admission in the background
            # while this renewal thread keeps the lease alive.
            self._start_admission()

    def _start_admission(self):
        """Start at most one background admission worker for this lease."""
        with self.admit_lock:
            if self.admitting or self.withdrawn or self.lost:
                return False
            self.admitting = True
            self.admit_thread = threading.Thread(
                target=self._admit, name='clef-lease-admit', daemon=True)
            self.admit_thread.start()
            return True

    def _admit(self):
        try:
            self.ready()
        except Exception as exc:
            reason = getattr(exc, 'reason', None) or type(exc).__name__
            self.last_error = 're-admission failed: %s' % reason
        finally:
            with self.admit_lock:
                self.admitting = False

    def _unconfirmed(self, reason):
        """Record loss of confirmation without ever reporting a withdrawal."""
        self.last_error = reason
        if self.now() - self.last_confirmed > WINDOW:
            self.lost = True

    def withdraw(self):
        """Withdraw this runtime. ``withdrawn`` only becomes true on a confirmed DELETE.

        One exception, and it is not a weakening: if create() never completed, there is no
        lease to withdraw and the registrar's 404 is the truth, so the runtime can free its
        resources instead of reporting INCOMPLETE forever. A 404 after a *confirmed* create
        means the registrar lost state, which must still fail closed.
        """
        never_created = self.phase is None
        try:
            payload = self._write('DELETE', '', None, 'withdrawal', self._verify_withdrawn)
        except LeaseFault as exc:
            if exc.status == 404 and never_created:
                self.withdrawn = True
                self.phase = 'withdrawn'
                self.last_error = None
                self.halt.set()
                return {'run_id': self.ident['run_id'],
                        'provider': 'clef-nb-' + self.ident['run_id'],
                        'phase': 'withdrawn'}
            raise
        phase = self._phase_text(payload.get('phase'))
        if phase != 'withdrawn':
            self.last_error = 'withdrawal not confirmed; phase %s for run %s' % (
                phase, self.ident['run_id'])
            raise LeaseFault(None, self.last_error)
        self.withdrawn = True
        self.phase = 'withdrawn'
        self.last_error = None
        self.halt.set()
        thread = self.thread
        if thread is not None and thread is not threading.current_thread():
            thread.join(timeout=35)
        return payload

    # -- admission ---------------------------------------------------------

    def ready(self, timeout=650):
        """Ask the registrar to prove and publish this runtime into the pool."""
        try:
            payload = self.call('POST', '/ready', {}, timeout=timeout)
        except LeaseFault as exc:
            self.last_error = exc.reason
            if exc.status == 410:
                self.lost = True        # terminal: the lease is gone, never resurrect
                raise
            if exc.status is not None:
                raise
            payload = self._resolve_ready()
        return self._admitted(payload)

    def _resolve_ready(self):
        """Resolve a lost ready() response by status read, never by a second proof."""
        for _ in range(ATTEMPTS):
            try:
                return self.call('GET')
            except LeaseFault as exc:
                if exc.status is not None:
                    raise
        reason = 'admission outcome unknown after a lost response for run %s' % (
            self.ident['run_id'])
        self.last_error = reason
        raise LeaseFault(None, reason)

    def _admitted(self, payload):
        if self.withdrawn:
            # Stop already won: a late probe result must never re-publish this runtime.
            self.last_error = 'lease already withdrawn; ignoring a late admission'
            raise LeaseFault(None, self.last_error)
        if payload.get('run_id') != self.ident['run_id']:
            self.last_error = 'admission response belongs to another runtime'
            raise LeaseFault(None, self.last_error)
        phase = self._phase_text(payload.get('phase'))
        self.phase = phase
        if phase != 'active':
            self.last_error = 'admission not confirmed; phase %s for run %s' % (
                phase, self.ident['run_id'])
            raise LeaseFault(None, self.last_error)
        self.lost = False
        self.last_error = None
        return payload
