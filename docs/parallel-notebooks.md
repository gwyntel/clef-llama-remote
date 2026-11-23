# Parallel Clef notebooks — registrar setup, activation and acceptance

Status: **staged, not activated.** Nothing in this document has been run on the control host.
Service activation, secret provisioning, parent-alias linking, Colab uploads and any pause of
existing automation are separate, explicitly authorised actions.

## Why a registrar exists

Two notebooks cannot safely share `cloudflare/clef-flash` or a single provider. Both discoveries
are unavoidable:

- `GET → edit → PUT` on the same alias is not concurrency-safe. A re-`GET` after the fact detects
  damage; it does not prevent a lost update. One serialized writer is the only correct shape.
- A notebook cannot prove its own readiness. "The workers answer `/health`" is a local
  observation; what matters is that Plexus can reach this node and get typed decisions back
  through the same route real traffic uses.

So each runtime publishes a **lease** and the registrar — a small always-on service on the control
host — owns every shared object.

## Ownership

| Object | Owner | Lifetime |
|---|---|---|
| `clef-nb-<32 hex>` provider | registrar | retained after stop; its pool target disabled; slug never reused |
| `clef-nb-<same uuid>` Tailscale node | that runtime | deleted only when the recorded device id matches its own node id/IP |
| `clef-flash-notebooks` alias | registrar only | durable pool, no hosted fallbacks, no additional aliases |
| `cloudflare/clef-flash`, `clef-flash-local` | their existing owners | one-time approved pool-pointer insertion only |
| `clef-probe-<uuid>` inference key | registrar | created for the readiness probe; deleted and absence verified in `finally` |
| lease record | registrar | active → withdrawn/expired tombstone; never resurrected |

Invariants worth stating because each one is a way this can go wrong:

- No startup scan or delete of other Tailscale nodes, including old v9 names.
- No v10 notebook receives `PLEXUS_ADMIN_KEY` or `PLEXUS_API_KEY`.
- The registrar stores only the SHA-256 of the lease token; a wrong token is `403`.
- TTL 180 s, renewed every 20 s on a dedicated thread (including during a long readiness probe).
  Expiry or withdrawal is terminal for that UUID.
- `maxConcurrency` is the healthy worker count of **that** runtime, never a sum across runtimes.
  Zero healthy workers withdraws the lease.
- Pool selector is `random` with `sticky_session: false`; parent selectors, order and stickiness
  are untouched.
- No global cooldown clearing, key-policy expansion, provider-template overwrite, service restart
  or model/context retuning anywhere in this feature.

## Setup (approval required)

1. **Registry token** — 32 bytes of hex, `0600`, in a `0700` directory:

   ```bash
   install -d -m 700 ~/.hermes/secrets
   [ -f ~/.hermes/secrets/clef_registry_key ] || \
     python3 -c 'import secrets;print(secrets.token_hex(32))' > ~/.hermes/secrets/clef_registry_key
   chmod 600 ~/.hermes/secrets/clef_registry_key
   ```

   Never print it, never commit it, never store it next to the notebook in Drive. It is copied
   once into Colab's secret panel as `CLEF_REGISTRY_KEY`.

2. **Management authority** — the registrar reads the existing
   `~/.hermes/secrets/plexus_admin_key` (0600). No new Plexus credential is created.

3. **Endpoints file** — the unit takes both endpoints from a gitignored `0600` file, because
   this repository is public and the tailnet addresses are household details:

   ```bash
   U=$(tailscale ip -4)          # this host's literal tailnet IPv4
   umask 077
   cat > ~/.hermes/secrets/clef-notebook-registry.env <<EOF
   CLEF_REGISTRY_HOST=$U
   PLEXUS_BASE_URL=https://<plexus-base>
   EOF
   chmod 600 ~/.hermes/secrets/clef-notebook-registry.env
   echo "bind=$U"            # prints the address, never a secret
   ```

   The bind address must be a literal tailnet IPv4, never a wildcard or LAN address, and it must
   match the URL supplied to notebooks (`CLEF_REGISTRY_URL`). If it changes, update this file and
   the Colab-side value. Changing ACL/grants is out of scope and needs its own authorisation.

4. **Singleton check** — the registrar takes `fcntl.flock` on
   `~/.hermes/state/clef-notebook-registry.lock` **before** `recover()`, so a second process
   cannot demote or write the pool before discovering the port is taken.

## Activation (approval required)

```bash
install -m 644 deploy/clef-notebook-registry.service ~/.config/systemd/user/
systemctl --user daemon-reload
systemd-analyze verify ~/.config/systemd/user/clef-notebook-registry.service
systemctl --user start clef-notebook-registry.service
ss -ltnp | grep 8769                      # expect exactly <CT tailnet IPv4>:8769
curl -s http://<CT tailnet IPv4>:8769/health   # expect {"status":"ok"}
journalctl --user -u clef-notebook-registry -n 40 --no-pager
```

`recover()` runs before the listener opens: every previously published target is written disabled
and persisted. If that demotion cannot be confirmed the process exits non-zero and systemd
restarts it — it never serves an enabled pool it could not reconcile.

There is no Plexus, gateway or container restart in this procedure.

## Notebook configuration

| Variable | Where | Value |
|---|---|---|
| `TAILSCALE_API_KEY` | Colab secret panel | existing device/key-scoped token |
| `CLEF_REGISTRY_KEY` | Colab secret panel | the 32-byte hex registry token |
| `CLEF_REGISTRY_URL` | environment, `.env`, or Colab secret | `http://<CT tailnet IPv4>:8769` |

The userspace HTTP CONNECT proxy that v9 armed (`127.0.0.1:1055`) is preserved and is the only
route off the VM. There is no Funnel or public exposure at any point.

## One-time parent linking (approval required; concurrency gate, not a retry loop)

The pool is usable standalone. Linking it into an existing parent alias is **not** a
retry-until-lucky PUT: legacy writers still rewrite those aliases.

1. Obtain a window in which old notebooks will not run their start/stop/routing cells. They keep
   serving; do not delete their runtimes.
2. With permission, pause only the CT alias-mutating invocations (`clef-local-reconciler.timer`,
   `clef-rung-guard.timer`, judge-dispatch cron) and wait for in-flight writer processes to exit.
3. Snapshot both parent aliases (`GET /v0/management/aliases/<slug>` to a timestamped file).
4. Insert the pointer **once**, writing `alias` (read-back comes back as
   `target_alias_slug`/`alias` depending on build):

   ```python
   from copy import deepcopy

   def add_pool_pointer(parent):
       out = deepcopy(parent)
       groups = out['target_groups']
       local = [g for g in groups if g.get('name') == 'LOCAL']
       if len(local) != 1:
           raise RuntimeError('expected exactly one existing LOCAL group')
       refs = [t for g in groups for t in g['targets']
               if (t.get('alias') or t.get('target_alias_slug')) == 'clef-flash-notebooks']
       if refs:
           if len(refs) != 1 or refs[0].get('enabled') is not True:
               raise RuntimeError('unexpected existing pool pointer')
           return out
       local[0]['targets'].append({'alias': 'clef-flash-notebooks', 'enabled': True})
       return out
   ```

5. `GET → transform → PUT` the full object stripped only of read-only `slug`, then a fresh `GET`.
   Assert that removing exactly that one inserted pointer makes the object equal to the
   pre-write snapshot: group order, selectors, sticky flags, metadata, every legacy target and
   `additional_aliases` unchanged.
6. Resume the same automation state (the same units that were paused, no more), then `GET` both
   parents again.

If the window cannot be obtained, leave the pool standalone and record integration as pending.
Do not claim a strict no-clobber guarantee against uncontrolled legacy writers.

Before trusting nested routing, prove it on the **deployed** Plexus: a temporary one-target test
alias pointing at `clef-flash-notebooks`, a temporary key scoped to it,
`POST /v1/systemone`, then delete the alias and key and verify absence. Do not force the route by
disabling legacy or paid fallbacks.

## Acceptance status (2026-11-23)

Performed with a single live runtime:

- **Admission** — lease active and renewing; provider `clef-nb-<uuid>` with the correct base,
  `maxConcurrency` equal to the healthy worker count, `timeoutMs`, decisions model; the temporary
  probe key created, used and deleted with absence verified.
- **Standalone pool** — the pool alias exists with exactly one enabled target.
- **Nested routing on the deployed gateway** — temp one-target alias -> pool -> provider served a
  real `/v1/systemone` request (9/9 checks, temp objects deleted and verified absent).
- **Parent linking** — one pointer per parent, snapshot-diff verified, integrated route served by
  the notebook through `clef-flash-local`.
- **Dual-T4 two-worker acceptance** — **explicitly waived by the operator on 2026-11-23. NOT
  PERFORMED.** The two-worker path is therefore unproven on real hardware; everything it depends
  on (worker plan, per-worker cap, GPU-residency proof) is covered only by the single-worker run
  and the offline tests. Do not read this as passing.
- **Graceful stop / hard-death expiry / registrar restart** curves from the step list above were
  not exercised against a live runtime in this session; they remain the first things to verify
  the next time a runtime is started for testing.

## Two-notebook acceptance (real GPUs, approval required)

Leave every older notebook alone. Start A and B as **separate new GPU runtimes**. After every
transition, read the registrar's lease state and the Plexus pool/provider objects.

1. A loads and proves inference. Its provider slug is unique; no legacy provider, alias or node
   changed.
2. B loads slowly. A keeps answering direct System One requests. B is absent from the enabled pool
   until its probe passes.
3. B ready. Both targets enabled exactly once; each provider's `maxConcurrency` equals its own
   healthy worker count. Send concurrent direct probes to A and B, then pool requests; attribute
   service from the proxy `/health` `run_id` and per-instance counters, never from
   `model=clef-flash` alone.
4. Run A's graceful stop twice. A's target disabled, A's own node gone, B still answers, B's base,
   key, concurrency and target unchanged. Nothing is "restored" to a shared provider.
5. Start C, then delete **C's runtime only**. Within 180 s plus the 5 s sweep cadence and
   propagation grace: C's target disabled, A/B untouched. Report observed timing, not the bound.
6. Restart the registrar while B lives (explicit service-restart approval). Pool targets are
   disabled first; B renews, re-runs admission and returns. No stale admission is resurrected.
7. Single- and dual-GPU behaviour: one worker for one visible GPU, two for two, no silent 2→1
   startup downgrade. **If dual T4 hardware is unavailable, dual-GPU acceptance is blocked — not
   passed by mocks.**
8. Real judge-shaped long-input smoke: the existing caller's full question block and v9 batch
   settings, on the new instance route, with no private-data upload to hosted fallbacks. A fruit
   probe proves connectivity, not long-input capacity. Record `usage.input_tokens` and the
   tail-marker discrimination result.
9. Diff fresh legacy snapshots against the baseline. Only the approved pool pointer and state
   changes made by their existing owners are allowed. Any other v10-originated mutation fails
   acceptance.

Evidence (fingerprints, GPU type/count, slugs, request ids, readback flags, failure/stop timing,
remaining blockers) belongs in this file or `EVIDENCE.md`. No secrets and no private tailnet
details in a public commit.

## Withdrawal and decommission

- **Stop one runtime:** run its shutdown cell. It withdraws the lease, waits 10 s for the routing
  reload, drains local in-flight requests (600 s cap), then stops workers, proxy and its own
  tailnet node.
- **Retire the pool from a parent:** remove the single `clef-flash-notebooks` pointer in the same
  quiescent window as insertion, and verify the parent matches its snapshot minus that pointer.
- **Stop the registrar:** `systemctl --user stop clef-notebook-registry.service` (and
  `disable`) — with approval. No new notebook can join while it is down; active routes keep
  working until their leases would expire, and recovery is fail-closed.
- **Do not** delete providers or keys to "clean up". Tombstones are deliberate: they keep the pool
  from becoming an invalid empty group and are the restart-recovery record. Garbage collection
  must prove ownership, absence of alias references and no active traffic, and is a separate
  feature.

## Known limitations

1. **Legacy writers.** Unique identities protect notebooks from each other, but no new client can
   make an uncontrolled whole-alias writer atomic. Parent linking needs the one approved quiet
   window above.
2. **Always-on component.** If the registrar is down, new notebooks cannot join.
3. **Ephemeral lease timing.** TTL is not instant failover: a hard death can cost failed attempts
   until expiry, and Plexus's own health/cooldown behaviour may mask it sooner. Zero failed
   requests on hard eviction is not promised.
4. **Provider/key read schemas vary by Plexus release.** The registrar verifies ownership from the
   provider's `display_name` (which encodes the run UUID) plus non-secret config readback, and
   compares `api_key` only when the readback is unmasked. It never guesses success from a `PUT`.
5. **`clef-flash-local` already carries a hosted fallback** (`FALLBACK → cloudflare/clef-flash`).
   That privacy/routing policy is preserved and reported, not "fixed" here.

## Residual risks accepted in this version

1. **The bootstrap token is the trust boundary.** A holder can admit a node from its *own*
   tailnet address only: the registrar binds the advertised base to the caller's source address
   and allows one live lease per source. It cannot publish another node's provider, another
   runtime's lease, or a parent alias. Treat the token as an admission credential, not a read
   key.
2. **Tombstones accumulate by design.** Every run leaves a provider and a disabled pool target;
   the state file grows with each run. Garbage collection must prove ownership, absence of alias
   references and no active traffic, and is a separate feature.
3. **One lock, held across Plexus calls.** A slow gateway delays other leases' heartbeats. The
   180 s TTL absorbs normal latency; if a gateway stall outlasts it, leases expire and the pool
   is disabled — fail-closed and recoverable when the gateway recovers, but visible as an outage.
4. **A foreign pool target blocks further pool writes.** If anything else adds a target to
   `clef-flash-notebooks`, the registrar refuses to rewrite the alias (`pool contains unknown
   target`) rather than erase it. Remove that target by hand.
5. **Lost state fails closed.** If the registrar starts with no records while the alias still
   has enabled targets, it refuses to serve until someone reconciles the pool. That is deliberate:
   an empty record set must never look like "nothing is published".
6. **Secret lengths are printed at notebook start** (never values), as v9 did. If that is too
   much, drop the `len=` from the secrets summary.

## Activation record (2026-11-23)

Provisioned and verified on CT 102 (`100.121.214.107`, the registrar host):

| Check | Result |
|---|---|
| `~/.hermes/secrets/clef_registry_key` | generated, 64 hex, mode 0600, never printed |
| `~/.hermes/secrets/clef-notebook-registry.env` | `CLEF_REGISTRY_HOST` + `PLEXUS_BASE_URL`, mode 0600 |
| Unit installed + enabled | `clef-notebook-registry.service` (survives reboot) |
| Listener | `ss -ltnp` -> `100.121.214.107:8769` only, no wildcard, no LAN |
| Health | `GET /health` -> `{"status": "ok"}` |
| State | `~/.hermes/state/clef-notebook-registry.json` `{}`, mode 0600; lock 0600 |
| Journal | `registry: recovery complete (0 lease record(s)); listening on 100.121.214.107:8769` — recovery before the listener, no demotion failure |

The startup line exists so this acceptance is checkable from the journal rather than inferred.

## Paused automation (paused 2026-11-23 for v10 testing)

Paused deliberately so notebook registration can be observed without alias writers competing.
**Resume every item below, in this order, when testing ends.**

| What | Was | Now | Resume with |
|---|---|---|---|
| Cron `9d07766b6529` "Clef judge dispatch (T4 + Mac)" | active, every 15 min | **paused** | `hermes cron resume 9d07766b6529` |
| `clef-local-reconciler.timer` | active | **stopped** | `systemctl --user start clef-local-reconciler.timer` |
| `clef-rung-guard.timer` | active | **stopped** | `systemctl --user start clef-rung-guard.timer` |
| `clef-judge.service` | active + enabled | **stopped + disabled** | `systemctl --user enable --now clef-judge.service` |

Note: the two timers are stopped but still *enabled*, so a reboot would restart them. If the pause
must survive a reboot, also `systemctl --user disable` them (and re-enable on resume).

## Routing proof on the deployed gateway (2026-11-23, 15/15 checks)

Run against the live Plexus with a self-contained chain that mirrors the real shape — outer alias
-> single alias pointer -> one-target alias -> provider -> stand-in `/v1/systemone` — plus a
scoped inference key. Nothing shared was touched: every object was created with a `zz-probe-`
prefix, deleted, and its absence re-verified (alias, provider and key all 404 afterwards).

Confirmed: pointer written as `alias` reads back as an alias reference; a two-level alias chain
serves decisions (`served=standin-t4`); `direct/<provider>/<model>` works (the path the
registrar's admission probe uses); and a scoped key refuses a provider outside its allow-list
with **403**.

**Two live findings this proof produced, both fixed in code:**

1. **`/v1/systemone` requires `allowRawPassthrough` on the inference key.** Without it the route
   is not permitted. The registrar's probe key now sets it.
2. **A freshly minted key answers 401 for several seconds** (measured: mint -> 401, same key ->
   200 after ~8 s) because the gateway caches the key list. Treating that 401 as an auth failure
   would have rejected **every** admission. The probe now treats 401 on a just-minted key as
   propagation, with a bounded budget (8 cheap attempts, 3 s apart) while transport failures stay
   capped at 3 so a hung request cannot exceed the client's 650 s ready timeout.

## Parent linking — DONE (2026-11-23)

Applied in the quiescent window (judge cron paused, both alias-writing timers stopped, no live
v9 runtime — the only `clef-llama-remote` node was `offline, last seen 43m ago`).

Method, per parent: snapshot -> append one `{"alias": "clef-flash-notebooks", "enabled": true}`
to the existing `LOCAL` group -> `PUT` the full object minus read-only `slug` -> fresh `GET`.

| Parent | Result |
|---|---|
| `clef-flash-local` | pointer present exactly once, enabled; removing it restores the pre-write snapshot |
| `cloudflare/clef-flash` | pointer present exactly once, enabled; removing it restores the pre-write snapshot |

Both verified by: strip the inserted pointer from the fresh read, normalize read-only `slug` and
pointer read/write naming, then compare with the pre-write snapshot. Group order, selectors,
`sticky_session`, `additional_aliases` (`clef-flash`, `talkie-judge`), SUB/FALLBACK groups and
every legacy target are unchanged. Snapshots: `parent-snapshots/20261123T0802*-*.json`.

Final LOCAL groups: `clef-llama-remote` (on, dead — see below), `clef-flash-mac` (off),
`clef-flash-notebooks` (on).

**Integrated route proven after linking**: `POST /v1/systemone` with `model=clef-flash-local`
returned HTTP 200 in 0.5 s, `usage.input_tokens=223`, and the response's `model` field was
`/kaggle/temp/clef/models/.../Cloudflare_clef-flash-Q4_K_M.gguf` — i.e. the request fell through
the dead legacy rung and was served by the notebook through the pool.

**Pre-existing drift, not caused by v10:** the legacy provider `clef-llama-remote` was repointed
by a v9 run to `http://100.124.240.60:8443/v1` with `maxConcurrency: 1` (it was
`100.101.46.86:8443`, 2, at planning time), and that node is now offline. The LOCAL rung is still
`enabled`, so each `clef-flash-local` request pays one failed attempt before failing over. v10
never wrote that provider; repairing it is a separate routing decision.

## Parent linking — original procedure (kept for reference)

`clef-flash-notebooks` does not exist until the first notebook is admitted (the registrar only
writes the pool once it has a published lease; it will not create an empty alias, because that
shape is invalid). At activation time the alias returned 404 and there were no `clef-nb-*`
providers.

Inserting the pool pointer *before* the alias exists would point a production alias at a
non-existent target, which is precisely the unproven routing change this feature must not make.
So the linking is deferred until a v10 runtime has been admitted, at which point:

1. snapshot both parents (still required — the timers are paused but that is not proof);
2. re-assert the alias exists and has exactly one enabled target;
3. run the insertion algorithm above on each parent, and assert that removing exactly the
   inserted pointer restores the pre-write snapshot byte-for-byte;
4. re-GET both parents after resuming the writers.
