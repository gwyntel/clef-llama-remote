#!/usr/bin/env python3
"""Deterministic transform: sanitized Drive v9 -> output-free Colab/Kaggle v10.

Every edit is anchored to an exact symbol. If an anchor is missing or duplicated the build
aborts instead of falling back to textual guesswork, because a half-patched notebook that
still writes shared routing state is worse than no notebook at all.

Usage:
    python3 tools/build_v10.py            # write notebooks/clef-llama-colab-v10.ipynb
    python3 tools/build_v10.py --check    # build in memory, compare bytes, never write
"""

from __future__ import annotations

import ast
import json
import sys
import textwrap
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
V9_PATH = ROOT / "notebooks" / "source" / "clef-runtime-v9.ipynb"
LEASE_PATH = ROOT / "clef_lease.py"
OUT_PATH = ROOT / "notebooks" / "clef-llama-colab-v10.ipynb"


# --------------------------------------------------------------------------- primitives
def once(text: str, needle: str) -> None:
    """Fail unless `needle` occurs exactly once."""
    count = text.count(needle)
    if count != 1:
        raise RuntimeError(f"anchor must occur exactly once, found {count}: {needle[:90]!r}")


def sub(text: str, old: str, new: str) -> str:
    once(text, old)
    return text.replace(old, new)


def _class(tree: ast.Module, name: str) -> ast.ClassDef:
    found = [n for n in tree.body if isinstance(n, ast.ClassDef) and n.name == name]
    if len(found) != 1:
        raise RuntimeError(f"expected class {name} exactly once at module level, found {len(found)}")
    return found[0]


def _method(cls: ast.ClassDef, name: str):
    """The single function named `name` in `cls` (refuses zero or several)."""
    found = [n for n in cls.body if isinstance(n, (ast.FunctionDef, ast.AsyncFunctionDef)) and n.name == name]
    if len(found) != 1:
        raise RuntimeError(f"expected {cls.name}.{name} exactly once, found {len(found)}")
    return found[0]


def _span(node) -> tuple[int, int]:
    """0-based [start, end) line span of a node, including decorators."""
    start = min([node.lineno] + [d.lineno for d in getattr(node, "decorator_list", [])]) - 1
    return start, node.end_lineno


def _replace_span(source: str, start: int, end: int, replacement: str) -> str:
    lines = source.splitlines(keepends=True)
    block = textwrap.indent(textwrap.dedent(replacement).strip("\n"), "    ") + "\n"
    lines[start:end] = [block]
    out = "".join(lines)
    ast.parse(out)
    return out


def replace_method(source: str, class_name: str, method_name: str, replacement: str) -> str:
    start, end = _span(_method(_class(ast.parse(source), class_name), method_name))
    return _replace_span(source, start, end, replacement)


def delete_method(source: str, class_name: str, method_name: str) -> str:
    start, end = _span(_method(_class(ast.parse(source), class_name), method_name))
    return _replace_span(source, start, end, "")


def replace_class(source: str, class_name: str, replacement: str) -> str:
    start, end = _span(_class(ast.parse(source), class_name))
    lines = source.splitlines(keepends=True)
    lines[start:end] = [textwrap.dedent(replacement).strip("\n") + "\n"]
    out = "".join(lines)
    ast.parse(out)
    return out


def delete_class(source: str, class_name: str) -> str:
    start, end = _span(_class(ast.parse(source), class_name))
    lines = source.splitlines(keepends=True)
    del lines[start:end]
    out = "".join(lines)
    ast.parse(out)
    return out


# --------------------------------------------------------------------------- cell 0 / 1
CELL0 = """# Clef-Flash on Colab / Kaggle → a Plexus `decisions` upstream — **v10**

> **v10 runs beside other notebooks.** Each runtime gets its own UUID, provider slug, Tailscale
> hostname, private daemon state and bearer, and publishes itself through a tailnet-only
> **registrar** that owns a dedicated pool alias. Two v10 runtimes never write the same object.

**What changed from v9.** v9 rewrote the *shared* provider `clef-llama-remote` and toggled a rung
in `cloudflare/clef-flash`'s `LOCAL` group, so a second notebook overwrote the first, and startup
deleted same-named Tailscale nodes it had not proven were its own. v10 writes nothing shared: no
parent alias, no provider another run owns, and no device it cannot prove is its own.

Unchanged from v9, deliberately: the GGUF (`bartowski/Cloudflare_clef-flash-GGUF`, Q4_K_M), the
llama.cpp release (`b12292`), the CUDA runtime, `-b 16384 -ub 16384`, `CTX_T4` 32,768, the
GPU-residency proof, and the userspace outbound HTTP proxy that is the only way off this VM.

**What a run does**

1. serves `POST /v1/systemone` from llama.cpp `b12292` behind a round-robin, bearer-checking proxy;
2. joins the tailnet as an **ephemeral, preauthorised** node named `clef-nb-<uuid>` — a name no
   other runtime can have — on a private state file and socket under `runs/<uuid>/state`;
3. takes a **VM-wide lock** first: one runtime per VM, because a second kernel would contend for
   the GPUs, the loopback worker ports and tailscaled's userspace proxy port. **Separate Colab or
   Kaggle runtimes are independent and supported**; two kernels in one VM are refused;
4. creates a **lease** carrying its identity, worker count and per-run bearer;
5. asks the registrar for **admission**. The registrar owns this run's provider
   `clef-nb-<uuid>`, probes it *through Plexus* with a temporary provider-scoped key, and only
   then enables that single target in the dedicated `clef-flash-notebooks` pool. A notebook
   cannot admit itself, and there is no switch that skips the proof;
6. renews the lease every 20 s. The lease TTL is 180 s; expiry or withdrawal is terminal for that
   UUID, and the registrar keeps the provider as a disabled tombstone so the slug is never reused.

**Teardown** withdraws the lease and verifies it, waits for the routing reload and for local
in-flight requests to drain (600 s cap), then stops workers, proxy, and this run's own Tailscale
node. If withdrawal cannot be confirmed, everything keeps running and teardown reports INCOMPLETE
— a routed dead endpoint is worse than a live unused one.

**Hard death.** If the VM is deleted, no callback runs. The registrar expires the lease within
180 s and disables that one target; nobody has to edit the pool by hand.

**Standalone pool vs parent integration.** `clef-flash-notebooks` is a decisions pool that is
usable on its own — call the alias directly. Inserting a pointer to it into
`cloudflare/clef-flash` / `clef-flash-local` is a **one-time operator action** taken in a
quiescent control window. Notebook start, heartbeat, expiry and teardown never write a parent
alias.

## Hardware and session limits

| Requirement | Detail |
|---|---|
| GPU runtime | **required** — `Runtime → Change runtime type → T4 GPU` |
| GPUs | 2× T4 recommended (two workers); 1× T4 works (one worker); VRAM < 14 GiB is refused |
| One runtime per VM | a second kernel in the same VM is refused; separate VMs are independent |
| Download | ~8 GB (llama.cpp 198 MB + CUDA runtime 594 MB + GGUF 6.04 GB) into `/content` |
| Session cap | ~12 h on Colab Pro; free tier is shorter |
| Idle | ~90 min with no browser tab → disconnect; closing the tab ends the VM |

Everything mutable lives in the VM's `/content`, which is wiped with the VM. Model and binary
caches stay under `ROOT` and are re-used across runs on the same VM; per-run logs, the tailscaled
state and the runtime lock live under `ROOT/runs/<uuid>/`.

## Context per GPU (KV cache: 32 KiB/token)

| VRAM | `-c` | KV (f16) | Notes |
|---|---|---|---|
| ≥ 22 GiB | 262144 | 8.00 GiB | full trained context |
| 14–22 GiB (T4) | 32768 | 1.00 GiB | v9 T4 shape: 6.04 GB weights + KV + the 16,384-token physical batch |
| < 14 GiB | — | — | the notebook refuses to start |

Override with `CLEF_CTX`; hard cap 262144. The value actually in force is read back from
`GET /props` → `default_generation_settings.n_ctx` and printed per worker.
"""

CELL1 = """## Secrets (Colab key panel, Kaggle private dataset, or the environment outside a notebook)

Two secrets, and no Plexus credential at all:

| Name | Required | Used for |
|---|---|---|
| `TAILSCALE_API_KEY` | yes | Tailscale REST API: mint this run's single-use ephemeral auth key, resolve this run's device id, delete only that device on teardown |
| `CLEF_REGISTRY_KEY` | yes | bearer for the registrar: create/renew/admit/withdraw this run's lease |

`CLEF_REGISTRY_URL` is **not** a secret: it is the tailnet URL of the registrar, supplied as an
environment variable (or a `.env` file) and validated for shape before use. There is no default —
a published notebook must not carry a household endpoint.

What the notebook does *not* hold, and cannot use: `PLEXUS_ADMIN_KEY`, `PLEXUS_API_KEY`, or any
shared provider key. The per-run bearer that Plexus sends (`CLEF_REMOTE_KEY`) is generated in this
kernel with `secrets.token_hex(32)`, handed to the registrar once at lease creation, and never
rotated afterwards.

Cells read `google.colab.userdata` and fall back to `os.environ`, so the same code runs outside
Colab. **No secret value is ever printed** — only names and lengths.

The bearer lands in the `llama-server` command line, so it is visible to `ps` inside the VM.
Acceptable on a single-tenant ephemeral VM; not acceptable on a shared host.
"""

CELL15 = """## Wire contract (what Plexus sends, what llama.cpp must answer)

Plexus POSTs to `{provider.api_base_url.systemone}/systemone` with
`Authorization: Bearer <that provider's api_key>` and a `decisions` body:

```json
{"model": "clef-flash",
 "state": "<string | object | array>",
 "questions": {
   "<id>": {"type": "noul", "instructions": "<string | object | array>"},
   "<id>": {"type": "choice", "instructions": "...", "criteria": {"<option>": "<description | null>"}},
   "<id>": {"type": "score", "instructions": "...", "criteria": ["<level description>", "..."]}
 }}
```

It requires at least this back:

```json
{"model": "<string>",
 "answers": {
   "<id>": {"type": "choice", "choice": "<option>", "probabilities": {}, "confidence": 0.0},
   "<id>": {"type": "score", "score": 0.0, "legend": {}, "probabilities": {}},
   "<id>": {"type": "noul", "noul": 0.0}
 },
 "usage": {"input_tokens": 0, "output_tokens": 0}}
```

`llama-server` implements exactly this at `POST /v1/systemone`: it reads the GGUF's `systemone`
chat template and the decision head, and it does **not** validate the `model` field — so the routed
model string stays `clef-flash`. A real call through Plexus returned choice/score/noul answers
with `usage {"input_tokens": 335, "output_tokens": 0}`.

The proxy passes path and body through unchanged, so the contract is end-to-end: proxy auth
(`Bearer <this run's bearer>`) → worker auth (same value) → llama.cpp `/v1/systemone`.

**Score scales.** A `score` question returns `0 .. len(criteria)-1`, fractional; a `noul` returns
`0 .. 1`. `confidence` is provider-specific and is **not** a portable correctness guarantee — the
registrar's admission probe checks typed transport and capability, not model calibration, and it
never applies a confidence threshold.

**Timeouts.** The registrar creates each provider with `timeoutMs=600000` (10 min); the proxy's
upstream timeout is 540 s, so its own 502 reaches Plexus before Plexus abandons the request. Ten
minutes is still not a promise for large prompts: pre-fill on one T4 runs in the low hundreds of
tokens per second, so a ~100k-token `state` costs minutes of pre-fill before the first token.
Keep `state` inside the worker's context window and measure `usage.input_tokens` on a real call
before routing long prompts here.
"""

CELL16 = """## Verifying from another host

From a tailnet peer. `100.x.y.z` is this node's tailnet IPv4 and `clef-nb-<uuid>` is the provider
slug, both printed by the status block in the bring-up cell.

```bash
# 1. node liveness (unauthenticated by design: run id, per-worker health, in-flight count)
curl -s http://100.x.y.z:8080/health | jq

# 2. the proxy refuses unauthenticated inference
curl -s -o /dev/null -w '%{http_code}\\n' -X POST http://100.x.y.z:8080/v1/systemone -d '{}'
# -> 401

# 3. lease state straight from the registrar (bootstrap key, not a Plexus credential)
curl -s http://100.x.y.z:8769/v1/leases/<uuid> \\
  -H "authorization: Bearer $CLEF_REGISTRY_KEY" -H "x-lease-token: <lease token>" | jq

# 4. what the registrar stored for THIS run's provider
curl -s -H "x-admin-key: $PLEXUS_ADMIN_KEY" \\
  https://plexus.nebulosa-bass.ts.net/v0/management/providers/clef-nb-<uuid> | jq '.api_base_url, .maxConcurrency'

# 5. the dedicated pool: this run's target, enabled exactly once
curl -s -H "x-admin-key: $PLEXUS_ADMIN_KEY" \\
  https://plexus.nebulosa-bass.ts.net/v0/management/aliases/clef-flash-notebooks \\
  | jq '.target_groups[] | {name, selector, targets}'

# 6. end-to-end through Plexus, scoped to this one provider
curl -s https://plexus.nebulosa-bass.ts.net/v1/systemone \\
  -H "Authorization: Bearer <a key scoped to clef-nb-<uuid>>" -H 'content-type: application/json' \\
  -d '{"model":"direct/clef-nb-<uuid>/clef-flash","state":"ping","questions":{"n":{"type":"noul","instructions":"hi"}}}' | jq
```

Inside the VM the status block is authoritative: worker `n_ctx` is read back from `/props`, the
`/health` body carries this run's `run_id`, and admission is proven by the registrar rather than
by a local probe.
"""

CELL17 = """## Direct provider calls

Plexus resolves `direct/<provider>/<model>` before the alias selector, so a direct call proves one
provider rather than the whole fallback chain. The registrar uses exactly this shape for admission,
with a temporary inference key whose `allowedProviders` is `["clef-nb-<uuid>"]` — so no other
provider can answer in its place, and the key is deleted (and its absence verified) immediately
afterwards.

A notebook holds no inference key at all, so operator direct calls need a key you scope yourself:

```bash
# this runtime's provider, bypassing the pool alias
curl -s https://plexus.nebulosa-bass.ts.net/v1/systemone \\
  -H "Authorization: Bearer <key scoped to clef-nb-<uuid>>" -H 'content-type: application/json' \\
  -d '{"model":"direct/clef-nb-<uuid>/clef-flash","state":"ping","questions":{"n":{"type":"noul","instructions":"hi"}}}' | jq
```

Mint a throwaway key for the check and delete it afterwards — do not broaden a shared key. Build the
body with `jq` and keep the secret in a shell variable so neither ever appears in a document:

```bash
THROWAWAY="sk-$(openssl rand -hex 24)"     # local only; never paste it into a notebook
jq -n --arg s "$THROWAWAY" --arg p "clef-nb-<uuid>" \\
  '{secret:$s, comment:"temporary, delete after use", allowedProviders:[$p]}' > /tmp/probe-key.json
curl -s -X PUT "https://plexus.nebulosa-bass.ts.net/v0/management/keys/zz-verify-<uuid>" \\
  -H "x-admin-key: $PLEXUS_ADMIN_KEY" -H 'content-type: application/json' \\
  --data @/tmp/probe-key.json
# ... use it ...
curl -s -X DELETE "https://plexus.nebulosa-bass.ts.net/v0/management/keys/zz-verify-<uuid>" \\
  -H "x-admin-key: $PLEXUS_ADMIN_KEY"
shred -u /tmp/probe-key.json 2>/dev/null || rm -f /tmp/probe-key.json
```

The old `clef-llama-colab-v2` key allows only `clef-llama-remote` and `clef-flash-mac`; a uniquely
named provider is refused by it rather than silently rerouted. That key policy is deliberately
untouched by v10.
"""

CELL18 = """## Troubleshooting

**`another kernel on this VM holds .../clef-runtime.lock`.** One runtime per VM is by design: a
second kernel would contend for the GPUs, ports 8091/8092, the proxy port and tailscaled's
userspace proxy on 1055. Run the shutdown cell ([13/13]) in the other kernel first. Independent
Colab or Kaggle runtimes are unaffected — they are separate VMs.

**`a live Runtime already exists in this kernel`.** The configuration cell refuses to rebuild
globals under a running runtime, because the old workers, sockets and lease would be orphaned.
Stop it first.

**Worker dies during load.** `RuntimeError: worker N exited rc=...` carries the tail of
`runs/<uuid>/logs/llama-N.log`. CUDA OOM on a 15 GiB T4 means the context is too greedy: re-run
with `CLEF_CTX=16384` and bring up again.

**`worker(s) [0] hold less than 2048 MiB on a GPU`.** Bring-up proved the weights are not on the
card, so admission never happens. Either the wrong release was unpacked (no `libggml-cuda.so`), or
`LD_LIBRARY_PATH` is missing the `cudart-*` directory, or CUDA init failed and llama.cpp loaded on
the CPU. The worker log names the cause.

**`tcp/8080 does not answer over the tailnet`.** The probe left through tailscaled's userspace
SOCKS5 proxy and came back through `tailscale serve`; it failed, so the registrar was never asked
to admit this node. Check `tailscale serve status`, that `BackendState` is `Running`, and that the
ACL lets `tag:endpoints` traffic reach the port. `CLEF_ALLOW_UNREACHABLE=1` skips the gate and
leaves reachability unproven.

**`registrar unreachable over the tailnet`.** `CLEF_REGISTRY_URL` must be the tailnet URL of the
registrar (e.g. `http://100.x.y.z:8769`), and the VM only leaves via tailscaled's proxy — which is
armed by then. Prefer the literal tailnet IPv4: MagicDNS may be off on the peer.

**Admission refused (`ready` returns 4xx/5xx).** The registrar's probe is a real System One call
with a temporary key scoped to this provider. A 409 means a probe is already in flight, a 410 that
the lease ended (start a new runtime), a 503 that its Plexus write could not be confirmed. Read
the message; do not work around it by enabling anything by hand.

**`teardown : INCOMPLETE`.** Withdrawal was not confirmed, so workers, proxy and tailnet were left
running on purpose. Fix the registrar/Plexus error and re-run `stop()`, or accept the routed dead
endpoint only via `stop(force=True)`.

**Hard death (VM deleted / evicted).** No teardown runs. The registrar expires the lease within
180 s and disables that target; the provider and its tombstone stay, so the slug is never reused.
No manual pool edit is needed, and no other runtime is touched.

**Plexus gets 401 from this node.** The registrar wrote the provider's `api_key` from the lease
itself, so a mismatch means someone edited the provider. Do not fix it by editing the notebook —
withdraw the lease and start a new runtime.

**`tailscale up` prints a login URL instead of joining.** The minted key was rejected, or
`TAILSCALE_API_KEY` lacks device/key write authority. Check the token's scope.

**ACL blocks traffic.** The Plexus host must reach this node on the serve port (8080 or 8443), and
this node must reach the registrar on 8769. In userspace mode the daemon still terminates inbound
connections to served ports.

**Port ladder.** If 8080 is occupied inside the VM the proxy binds 8443, `tailscale serve` is
published on 8443, and the lease carries the 8443 base URL — no manual step.
"""


# --------------------------------------------------------------------------- cell 3
EMBED_HEADER = """# =====================================================================================
# Embedded lease client -- generated verbatim from clef_lease.py by tools/build_v10.py.
# Edit clef_lease.py and regenerate this notebook; do not edit this block here.
# ====================================================================================="""

IDENTITY_BLOCK = '''# --------------------------------------------------------------- this runtime's identity
import fcntl

_prior_runtime = globals().get("RUNTIME")
if _prior_runtime is not None and not getattr(_prior_runtime, "_stopped", False):
    raise RuntimeError(
        "a live Runtime already exists in this kernel — run its shutdown cell ([13/13]) before "
        "reconfiguring: rebuilding these globals would orphan its workers, proxy and lease"
    )

IDENT = identity()
RUN_ID = IDENT["run_id"]


def _registry_url_ok(value: str) -> bool:
    """A literal tailnet IPv4, with an explicit port, and nothing else.

    The lease token travels in a header over plain http inside the tailnet. A hostname, a public
    address, embedded credentials, a path or a query would let a typo — or a hostile peer that a
    name resolves to — capture this run's credentials, so the shape is checked before any request.
    """
    parsed = urllib.parse.urlsplit(value or "")
    try:
        return (
            parsed.scheme == "http"
            and parsed.path in ("", "/")
            and not parsed.query
            and not parsed.fragment
            and not parsed.username
            and not parsed.password
            and parsed.port is not None
            and ipaddress.ip_address(parsed.hostname) in ipaddress.ip_network("100.64.0.0/10")
        )
    except (TypeError, ValueError):
        return False


REGISTRY_URL = os.environ.get("CLEF_REGISTRY_URL", "").rstrip("/")
if not REGISTRY_URL:
    raise SystemExit(
        "set CLEF_REGISTRY_URL to the registrar's tailnet URL, for example "
        "http://100.x.y.z:8769"
    )
if not _registry_url_ok(REGISTRY_URL):
    raise SystemExit(
        f"CLEF_REGISTRY_URL={REGISTRY_URL!r} is not http://<tailnet IPv4>:<port>; no path, "
        "query, credentials or hostname is accepted"
    )

SECRETS = {
    "TAILSCALE_API_KEY": _secret("TAILSCALE_API_KEY"),
    "CLEF_REGISTRY_KEY": _secret("CLEF_REGISTRY_KEY"),
}
# The bearer Plexus will send is this run's alone. The registrar stores it as the provider's
# api_key at lease creation; it is never adopted from, nor written back to, any shared provider.
SECRETS["CLEF_REMOTE_KEY"] = secrets.token_hex(32)'''

CELL3_SECRETS_OLD = '''SECRETS = {
    "TAILSCALE_API_KEY": _secret("TAILSCALE_API_KEY"),
    "PLEXUS_ADMIN_KEY": _secret("PLEXUS_ADMIN_KEY"),
    "CLEF_REMOTE_KEY": _secret("CLEF_REMOTE_KEY", required=False),  # optional; provider api_key is authoritative
    "PLEXUS_API_KEY": _secret("PLEXUS_API_KEY"),
}'''

CELL3_KEYPROVIDER_OLD = '''# The Colab secret is optional: the provider's api_key is the single source of truth, and the
# notebook already holds an admin key. Adopting it here keeps the proxy bearer and Plexus in
# lockstep even when the notebook-scoped CLEF_REMOTE_KEY secret is stale or unbound.
def _provider_api_key() -> str:
    import json as _json
    import urllib.request as _url

    request = _url.Request(
        f"{PLEXUS_BASE}/v0/management/providers/{PROVIDER}",
        headers={"x-admin-key": SECRETS["PLEXUS_ADMIN_KEY"]},
    )
    with _url.urlopen(request, timeout=30) as response:
        return (_json.load(response).get("api_key") or "").strip()


if not SECRETS.get("CLEF_REMOTE_KEY"):
    SECRETS["CLEF_REMOTE_KEY"] = _provider_api_key()
    if not SECRETS["CLEF_REMOTE_KEY"]:
        raise SystemExit("missing CLEF_REMOTE_KEY and the Plexus provider has no api_key")
    print(f"clef_remote_key: adopted provider api_key len={len(SECRETS['CLEF_REMOTE_KEY'])}")'''

CELL3_KEYPROVIDER_NEW = '''# The bearer is generated in this kernel (see the identity block above) and handed to the
# registrar once. Nothing here reads or adopts another provider's api_key.
print(f"clef_remote_key: generated per run len={len(SECRETS['CLEF_REMOTE_KEY'])}")'''


def transform_cell3(src: str, lease_source: str) -> str:
    src = sub(
        src,
        "import os\nimport subprocess\nimport time\nfrom pathlib import Path\n",
        "import ipaddress\nimport os\nimport secrets\nimport subprocess\nimport time\n"
        "import urllib.parse\nfrom pathlib import Path\n",
    )
    # The Kaggle private-dataset loader must look for v10's two keys, not v9's four.
    src = sub(
        src,
        '        if {"TAILSCALE_API_KEY", "PLEXUS_ADMIN_KEY", "PLEXUS_API_KEY"} <= set(_d):',
        '        if {"TAILSCALE_API_KEY", "CLEF_REGISTRY_KEY"} <= set(_d):',
    )
    # Embed the lease client and derive this runtime's identity before anything reads it.
    src = sub(src, CELL3_SECRETS_OLD, EMBED_HEADER + "\n" + lease_source.strip("\n") + "\n\n\n" + IDENTITY_BLOCK)
    src = sub(
        src,
        '''ROOT = Path(os.environ.get("CLEF_ROOT") or _pick_root())
LOG_DIR = ROOT / "logs"
MODEL_DIR = ROOT / "models"
LLAMA_ROOT = ROOT / "llama"
STATE_DIR = ROOT / "state"''',
        '''ROOT = Path(os.environ.get("CLEF_ROOT") or _pick_root())
# Model and binary caches stay process-wide (a 6 GB GGUF and a 200 MB release are re-used), but
# every mutable runtime file lives under runs/<RUN_ID> so two runtimes can never collide.
RUN_ROOT = ROOT / "runs" / RUN_ID
LOG_DIR = RUN_ROOT / "logs"
MODEL_DIR = ROOT / "models"
LLAMA_ROOT = ROOT / "llama"
STATE_DIR = RUN_ROOT / "state"''',
    )
    src = sub(
        src,
        '''TS_DIR = _pick_ts_dir()
TS_SOCK = Path("/var/run/tailscale/tailscaled.sock")
if not os.access(TS_SOCK.parent, os.W_OK):
    TS_SOCK = STATE_DIR / "tailscaled.sock"
TS_STATE = Path("/var/lib/tailscale/tailscaled.state")
if not os.access(TS_STATE.parent, os.W_OK):
    TS_STATE = STATE_DIR / "tailscaled.state"''',
        '''TS_DIR = _pick_ts_dir()
# Never the system daemon's socket or state: this runtime owns a private userspace tailscaled.
TS_SOCK = STATE_DIR / "tailscaled.sock"
TS_STATE = STATE_DIR / "tailscaled.state"''',
    )
    src = sub(
        src,
        '''ALIAS = "cloudflare/clef-flash"
PROVIDER = "clef-llama-remote"
MODEL = "clef-flash"
SELF_TEST_MODEL = f"direct/{PROVIDER}/{MODEL}"
TS_API_BASE = "https://api.tailscale.com/api/v2"
TS_TAG = "tag:endpoints"
TS_AUTHKEY_EXPIRY_SECONDS = 3600
TS_HOSTNAME = "clef-llama-remote"
EXPECTED_DNSNAME = "clef-llama-remote.nebulosa-bass.ts.net"''',
        '''ALIAS = "cloudflare/clef-flash"   # parent alias: documented here, never written by a notebook
PROVIDER = IDENT["hostname"]      # unique per runtime, e.g. clef-nb-<32 hex>
MODEL = "clef-flash"
SELF_TEST_MODEL = f"direct/{PROVIDER}/{MODEL}"
TS_API_BASE = "https://api.tailscale.com/api/v2"
TS_TAG = "tag:endpoints"
TS_AUTHKEY_EXPIRY_SECONDS = 3600
TS_HOSTNAME = IDENT["hostname"]   # unique per runtime, so two notebooks never share a node name''',
    )
    src = sub(src, CELL3_KEYPROVIDER_OLD, CELL3_KEYPROVIDER_NEW)
    return src


# --------------------------------------------------------------------------- cell 8
PLEXUS_CLIENT_V10 = '''
class PlexusClient:
    """Read-only description of this runtime's routing state.

    v10 notebooks hold no Plexus credential and never write a provider or an alias: the
    registrar owns both, so the inherited management client is replaced rather than left as
    dead code that a later edit could re-arm.
    """

    def __init__(self, provider: str = PROVIDER) -> None:
        self.provider = provider

    def describe_rung(self) -> str:
        if globals().get("RUNTIME") is not None and getattr(RUNTIME, "lease", None) is not None:
            return f"notebook pool clef-flash-notebooks -> {PROVIDER} (owned by the registrar)"
        return "not published"
'''


def transform_cell8(src: str) -> str:
    src = delete_class(src, "PlexusError")
    src = replace_class(src, "PlexusClient", PLEXUS_CLIENT_V10)
    return src


# --------------------------------------------------------------------------- cell 9
def transform_cell9(src: str) -> str:
    src = sub(
        src,
        """        self.requests = 0
        self.served = 0
        self.failures = 0""",
        """        self.requests = 0
        self.served = 0
        self.failures = 0
        # In-flight authorized inference requests: teardown drains these before stopping workers.
        self.active = 0""",
    )
    src = sub(
        src,
        """                        {
                            "status": "ok",
                            "workers": [""",
        """                        {
                            "status": "ok",
                            # The registrar proves it is talking to THIS runtime, not a stale
                            # node that reused the name.
                            "run_id": RUN_ID,
                            "workers": [""",
    )
    src = sub(
        src,
        """                            "requests": proxy.requests,
                            "served": proxy.served,
                            "failures": proxy.failures,
                        },""",
        """                            "requests": proxy.requests,
                            "served": proxy.served,
                            "failures": proxy.failures,
                            "active": proxy.active,
                        },""",
    )
    src = sub(
        src,
        """                body = self._read_body()
                if body is None:
                    return
                with proxy._lock:
                    proxy.requests += 1
                self._forward("POST", "/v1/systemone", body)""",
        """                body = self._read_body()
                if body is None:
                    return
                with proxy._lock:
                    proxy.requests += 1
                    proxy.active += 1
                try:
                    self._forward("POST", "/v1/systemone", body)
                finally:
                    with proxy._lock:
                        proxy.active -= 1""",
    )
    return src


# --------------------------------------------------------------------------- cell 10
DELETE_DEVICE_V10 = '''
def delete_device(self, device_id):
    """Delete this run's own Tailscale device, and nothing else.

    Ownership is resolved from live status (Self.ID / tailnet addresses) at deletion time. A
    hostname match is not ownership: another runtime may legitimately hold a different device,
    and deleting it would take a peer's route away.
    """
    if not device_id:
        return False
    current = self.own_device_id()
    if current != device_id:
        raise RuntimeError("refusing to delete a Tailscale device not owned by this runtime")
    self._api("DELETE", f"/device/{device_id}", allowed=(200, 204, 404))
    if any(str(d.get("id")) == str(device_id) for d in self.devices()):
        raise RuntimeError("own Tailscale device deletion not confirmed")
    return True
'''


def transform_cell10(src: str) -> str:
    src = delete_method(src, "Tailscale", "_is_notebook_device")
    src = delete_method(src, "Tailscale", "cleanup_duplicate_nodes")
    src = replace_method(src, "Tailscale", "delete_device", DELETE_DEVICE_V10)
    return src


# --------------------------------------------------------------------------- cell 11
INIT_V10 = '''
def __init__(self, client: PlexusClient | None = None, ts: Tailscale | None = None) -> None:
    self.client = client or PlexusClient()
    self.ts = ts or Tailscale()
    self.workers: list = []
    self.proxy = None
    self.serve_port = None
    self.ip = None
    self.dnsname = None
    self.device_id = None
    self.base_url = None
    self.lease = None
    self._vm_lock = None
    self._stop = threading.Event()
    self._done = threading.Event()
    self._guard = threading.Lock()
    self._stopped = False
    self._rung_on = False
    self._armed = False
    self._heartbeat = None
    self.gpu_mib = {}
    self.concurrency = None
    self.provider_timeout_ms = None
'''

START_V10 = '''
def start(self) -> "Runtime":
    with self._guard:
        if self._stopped:
            raise RuntimeError("this Runtime was already stopped — construct a new one")
        if self.workers or self.proxy is not None:
            raise RuntimeError("Runtime.start() was already called in this kernel")
        if self._vm_lock is not None:
            raise RuntimeError("bring-up already started in this kernel")
    # One runtime per VM. A second kernel would contend for the GPUs, for the loopback worker
    # ports and for tailscaled's single userspace proxy port, so it is refused before anything
    # is spawned. Separate VMs remain fully independent.
    lock_path = ROOT / "clef-runtime.lock"
    lock_path.parent.mkdir(parents=True, exist_ok=True)
    lock_handle = open(lock_path, "a+")
    try:
        fcntl.flock(lock_handle.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
    except OSError:
        lock_handle.close()
        raise RuntimeError(
            f"another kernel on this VM holds {lock_path}: refusing to share the GPUs, the "
            "worker ports or the userspace tailscale proxy. Stop that runtime first; separate "
            "Colab/Kaggle runtimes are independent and supported."
        ) from None
    with self._guard:
        if self._stopped:
            fcntl.flock(lock_handle.fileno(), fcntl.LOCK_UN)
            lock_handle.close()
            raise RuntimeError("teardown requested before bring-up started")
        self._vm_lock = lock_handle
    self._arm_callbacks()
    log(
        "bring-up     : workers -> gpu proof -> tailscale -> proxy -> reachability "
        "-> registrar -> admission"
    )
    try:
        for stage in (
            self._start_workers,
            self._prove_gpu,
            self._start_tailnet,
            self._start_proxy,
            self._prove_reachable,
            self._prove_plexus,
            self._register_provider,
        ):
            self._mark(stage.__name__)
            if stage in (self._start_tailnet, self._start_proxy, self._register_provider):
                # These phases create external state (tailnet, Serve, lease). Serialize them
                # with stop() so teardown cannot miss a late side effect.
                with self._guard:
                    self._ensure_running()
                    stage()
            else:
                self._ensure_running()
                stage()
        # Admission is the ONLY enable operation: the registrar creates this run's provider,
        # proves it through Plexus and enables that single pool target. It is serialized with
        # stop() so a teardown that won the guard can never be reversed by a late admission.
        self._ensure_running()
        self._mark("admission")
        with self._guard:
            self._ensure_running()
            self._self_test()
        self._ensure_running()
    except BaseException:
        log("bring-up     : failed — tearing down (lease withdrawn first)")
        self.stop(reason="bring-up failed")
        raise
    self._heartbeat = threading.Thread(target=self._watch, name="heartbeat", daemon=True)
    self._heartbeat.start()
    self.status()
    self._mark("ready")
    log("bring-up     : ready")
    return self
'''

START_TAILNET_V10 = '''
def _start_tailnet(self) -> None:
    # No cleanup pass and no shared hostname: this run's node name is its own UUID, so there is
    # nothing to delete and no peer to disturb.
    self.ts.start_daemon()
    authkey = self.ts.mint_auth_key()
    try:
        self.ts.up(authkey)
    finally:
        self.ts.revoke_auth_key()
    self.ip = self.ts.ipv4()
    self.dnsname = self.ts.dnsname()
    self.device_id = self.ts.own_device_id()
    # Userspace networking => every tailnet destination needs tailscaled's local proxy.
    # Without this the registrar call has no route and stalls or errors.
    os.environ["CLEF_TAILNET_PROXY"] = "http://127.0.0.1:1055"
    log(f"tailnet      : {self.ip} instance={RUN_ID} name={self.dnsname or '(no DNSName)'}")
    log("tailnet      : outbound proxy 127.0.0.1:1055 armed for tailnet destinations")
    if self.device_id:
        log(f"tailnet      : device id {self.device_id} is the only device teardown may delete")
    else:
        log(
            "WARNING: could not resolve this node's Tailscale device id — teardown cannot delete "
            "it; the ephemeral node expires on its own and no peer is affected"
        )
'''

PROVE_PLEXUS_V10 = '''
def _prove_plexus(self) -> None:
    """Prove this VM can reach the registrar over the tailnet before asking it for a lease.

    The `/health` probe above only proves the local tailnet stack + serve path
    (container -> itself). This is the first genuinely cross-node call, so failing here names
    the problem instead of surfacing it later as a mystery lease error.
    """
    started = time.time()
    request = urllib.request.Request(REGISTRY_URL + "/health")
    try:
        with _opener().open(request, timeout=PROBE_TIMEOUT) as response:
            body = json.load(response)
    except Exception as exc:
        raise RuntimeError(
            f"registrar unreachable over the tailnet after {time.time() - started:.1f}s "
            f"({type(exc).__name__}: {exc}); CLEF_REGISTRY_URL={REGISTRY_URL!r} "
            f"CLEF_TAILNET_PROXY={os.environ.get('CLEF_TAILNET_PROXY')!r}"
        ) from exc
    if not isinstance(body, dict) or body.get("status") != "ok":
        raise RuntimeError(f"registrar health was not ok: {body!r:.200}")
    log(f"registrar    : reachable over tailnet in {time.time() - started:.1f}s")
'''

REGISTER_PROVIDER_V10 = '''
def _register_provider(self) -> None:
    """Create this run's lease. The registrar, not this notebook, writes the provider."""
    self.base_url = f"http://{self.ip}:{self.serve_port}/v1"
    self.concurrency = len(self.workers)
    self.provider_timeout_ms = PROVIDER_TIMEOUT_MS
    self.lease = Lease(
        REGISTRY_URL,
        SECRETS["CLEF_REGISTRY_KEY"],
        IDENT,
        _opener,
        lambda: sum(1 for worker in self.workers if worker.alive() and worker.healthy()),
    )
    created = self.lease.create(self.base_url, self.concurrency, SECRETS["CLEF_REMOTE_KEY"])
    log(
        f"lease        : created {RUN_ID} workers={self.concurrency} "
        f"provider={created.get('provider')} phase={created.get('phase')}"
    )
'''

SELF_TEST_V10 = '''
def _self_test(self) -> None:
    """Admission: the registrar proves this provider through Plexus before publishing it.

    There is no local shortcut and no environment switch that admits this node unverified —
    an unproven target in the pool is exactly the failure v10 exists to prevent.
    """
    result = self.lease.ready()
    if result.get("provider") != PROVIDER or result.get("run_id") != RUN_ID:
        raise SelfTestFailure(
            f"admission response belongs to another runtime: {result!r:.300}"
        )
    self._rung_on = True
    log(f"admission    : {PROVIDER} admitted to clef-flash-notebooks ({result.get('phase')})")
'''

ENABLE_RUNG_V10 = '''
def _enable_rung(self) -> None:
    # There is no notebook-side enable step. The registrar owns clef-flash-notebooks and enables
    # exactly one target as part of admission; this method exists only for lifecycle compatibility.
    if self.lease is None:
        raise RuntimeError("lease not created")
'''

WATCH_V10 = '''
def _watch(self) -> None:
    ts_misses = 0
    worker_misses = 0
    while not self._stop.wait(WATCH_INTERVAL):
        try:
            state = self.ts.backend_state()
        except Exception as exc:
            state = f"error:{exc!r}"
        if state == "Running":
            ts_misses = 0
        else:
            ts_misses += 1
            log(f"heartbeat    : BackendState={state!r} ({ts_misses}/2)")
            if ts_misses >= 2:
                log("heartbeat    : tailnet is down — withdrawing the lease so the pool stops routing here")
                self.stop(reason=f"tailscale BackendState={state}")
                return
        # One healthy worker still serves; every worker dead or unhealthy twice running does not.
        # The lease's own heartbeat reports the healthy count, and the registrar shrinks this
        # provider's cap to match (or withdraws it at zero).
        states = [(worker.slot, worker.alive(), worker.healthy()) for worker in self.workers]
        if states and not any(healthy for _, _, healthy in states):
            worker_misses += 1
            log(f"heartbeat    : no healthy worker {states} ({worker_misses}/2)")
            if worker_misses >= 2:
                log("heartbeat    : withdrawing the lease before stopping the node")
                self.stop(reason="every worker is down or unhealthy")
                return
        else:
            worker_misses = 0
        if self.lease is None:
            continue
        if self.lease.withdrawn:
            log("heartbeat    : lease is withdrawn — this runtime no longer serves; stopping")
            self.stop(reason="lease withdrawn")
            return
        if self.lease.lost:
            # Renewal has not been confirmed for a while. Do NOT re-advertise by hand and do not
            # assume the pool still contains this target: ask the registrar what it thinks.
            try:
                phase = str(self.lease.status().get("phase"))
            except Exception as exc:
                log(f"heartbeat    : lease unconfirmed and status unreadable ({exc}); retrying")
                continue
            if phase in ("withdrawn", "expired"):
                log(f"heartbeat    : lease {phase} — pool exclusion confirmed; stopping this runtime")
                self.stop(reason=f"lease {phase}")
                return
            log(f"heartbeat    : lease renewal unconfirmed (phase={phase}); not re-advertising")
'''

STOP_V10 = '''
def stop(self, reason: str = "", force: bool = False) -> None:
    with self._guard:
        if self._stopped:
            self._await_teardown()
            return
        self._stopped = True
        self._done.clear()
    log(f"teardown     : start{' — ' + reason if reason else ''}")
    leasing = self.lease is not None
    withdrawn = not leasing
    if leasing:
        # 1. Lease first: the registrar must stop selecting this node before its sockets die.
        # If that cannot be verified, do not create a routed dead endpoint.
        for attempt in range(3):
            try:
                result = self.lease.withdraw()
                if result.get("phase") == "withdrawn":
                    withdrawn = True
                    log(f"lease        : {RUN_ID} withdrawn (re-GET verified)")
                    break
                log(f"lease        : withdraw attempt {attempt + 1}/3 returned {result!r:.200}")
            except Exception as exc:
                log(f"lease        : withdraw attempt {attempt + 1}/3 failed ({exc})")
            time.sleep(2.0)
        if not withdrawn and not force:
            log(
                "teardown     : INCOMPLETE — lease withdrawal is unconfirmed, so workers, proxy "
                "and tailnet are being LEFT RUNNING to avoid a routed dead endpoint. Fix the "
                "registrar/Plexus error and re-run stop(), or call stop(force=True) to destroy "
                "the node anyway."
            )
            with self._guard:
                self._stopped = False
            self._done.set()
            return
        if not withdrawn:
            log(
                "WARNING: stop(force=True) is destroying the node while its pool target may still "
                f"be enabled: clef-flash-notebooks -> clef-nb-{RUN_ID}"
            )
    self._stop.set()
    try:
        if withdrawn and leasing:
            # 2. Let Plexus reload routing, then let local in-flight requests finish. A request
            # already inside the proxy is still someone's answer; killing it mid-flight turns a
            # clean stop into an error on the caller's side.
            time.sleep(10.0)
            deadline = time.time() + 600.0
            while (
                self.proxy is not None
                and getattr(self.proxy, "active", 0) > 0
                and time.time() < deadline
            ):
                log(f"teardown     : draining, active={self.proxy.active}")
                time.sleep(2.0)
            if self.proxy is not None and getattr(self.proxy, "active", 0) > 0:
                log(
                    f"teardown     : INCOMPLETE — {self.proxy.active} request(s) still in flight "
                    "after the 600 s drain deadline; workers are being left running"
                )
                if not force:
                    with self._guard:
                        self._stopped = False
                    self._done.set()
                    return
        # 3. workers
        for worker in self.workers:
            log(f"worker {worker.slot}     : stopping")
            try:
                worker.stop()
            except Exception as exc:
                log(f"worker {worker.slot}     : stop failed ({exc})")
        # 4. proxy
        if self.proxy is not None:
            try:
                self.proxy.stop()
                log("proxy        : closed")
            except Exception as exc:
                log(f"proxy        : stop failed ({exc})")
        # 5. tailnet: resolve and delete ONLY this run's device, while status still has Self.ID.
        if self.ts is not None:
            device_id = None
            try:
                device_id = self.ts.own_device_id()
            except Exception as exc:
                log(f"tailscale    : own device id unreadable ({exc})")
            if device_id:
                try:
                    if self.ts.delete_device(device_id):
                        log(f"tailscale    : own device {device_id} deleted (ownership verified)")
                except Exception as exc:
                    log(
                        f"WARNING: own device deletion not confirmed ({exc}); the ephemeral node "
                        "expires on its own and no other device is touched"
                    )
            else:
                log("WARNING: own device id unresolved — leaving the ephemeral node to expiry")
            try:
                self.ts.serve_reset()
            except Exception as exc:
                log(f"tailscale    : serve reset failed ({exc})")
            try:
                self.ts.down()
                log("tailscale    : node left the tailnet")
            except Exception as exc:
                log(f"tailscale    : down failed ({exc})")
            try:
                self.ts.stop_daemon()
            except Exception as exc:
                log(f"tailscale    : daemon stop failed ({exc})")
    finally:
        if self._vm_lock is not None:
            try:
                fcntl.flock(self._vm_lock.fileno(), fcntl.LOCK_UN)
                self._vm_lock.close()
            except Exception:
                pass
            self._vm_lock = None
        self._done.set()
        log("teardown     : done")
'''


def transform_cell11(src: str) -> str:
    src = replace_method(src, "Runtime", "__init__", INIT_V10)
    src = replace_method(src, "Runtime", "start", START_V10)
    src = replace_method(src, "Runtime", "_start_tailnet", START_TAILNET_V10)
    src = replace_method(src, "Runtime", "_prove_plexus", PROVE_PLEXUS_V10)
    src = replace_method(src, "Runtime", "_register_provider", REGISTER_PROVIDER_V10)
    src = replace_method(src, "Runtime", "_enable_rung", ENABLE_RUNG_V10)
    src = replace_method(src, "Runtime", "_self_test", SELF_TEST_V10)
    src = replace_method(src, "Runtime", "_watch", WATCH_V10)
    src = delete_method(src, "Runtime", "_restore_concurrency")
    src = replace_method(src, "Runtime", "stop", STOP_V10)
    return src


# --------------------------------------------------------------------------- cell 12
CELL12_TRY_OLD = '''try:
    RUNTIME = Runtime()
    os.environ.setdefault("CLEF_SKIP_SELF_TEST", "0")
    RUNTIME.start()
except Exception:
    _tb_status.print_exc()
    _write_status({"phase": "bringup", "ok": False, "error": _tb_status.format_exc()})
    raise
else:
    _write_status({"phase": "serving", "ok": True})'''

CELL12_TRY_NEW = '''try:
    _prior = globals().get("RUNTIME")
    if _prior is not None and not getattr(_prior, "_stopped", False):
        raise RuntimeError(
            "a live Runtime already exists in this kernel — run the shutdown cell ([13/13]) "
            "first. Starting another would orphan its workers, proxy and lease."
        )
    RUNTIME = Runtime()
    RUNTIME.start()
except Exception:
    _tb_status.print_exc()
    _write_status(
        {
            "phase": "bringup",
            "ok": False,
            "run_id": RUN_ID,
            "provider": PROVIDER,
            "pool": "clef-flash-notebooks",
            "error": _tb_status.format_exc(),
        }
    )
    raise
else:
    _write_status(
        {
            "phase": "serving",
            "ok": True,
            "run_id": RUN_ID,
            "provider": PROVIDER,
            "pool": "clef-flash-notebooks",
        }
    )'''


def transform_cell12(src: str) -> str:
    return sub(src, CELL12_TRY_OLD, CELL12_TRY_NEW)


# --------------------------------------------------------------------------- cell 14
CELL14 = '''# [13/13] explicit graceful shutdown — idempotent, safe to run twice.
# Order: lease withdrawn (verified) -> routing reload + local drain -> workers -> proxy
#        -> this run's own tailnet node.
RUNTIME.stop(reason="manual shutdown cell")
try:
    print("lease now:", RUNTIME.lease.status() if RUNTIME.lease is not None else "no lease")
except Exception as exc:
    print("lease state unreadable:", exc)
'''


# --------------------------------------------------------------------------- build
def build() -> str:
    nb = json.loads(V9_PATH.read_text())
    lease_source = LEASE_PATH.read_text()

    sources = {"".join(c["source"]) for c in nb["cells"]}
    if len(sources) != len(nb["cells"]):
        pass  # duplicate cell text is allowed; per-index edits are anchored anyway

    cells = nb["cells"]
    edits = {
        0: CELL0,
        1: CELL1,
        3: transform_cell3("".join(cells[3]["source"]), lease_source),
        8: transform_cell8("".join(cells[8]["source"])),
        9: transform_cell9("".join(cells[9]["source"])),
        10: transform_cell10("".join(cells[10]["source"])),
        11: transform_cell11("".join(cells[11]["source"])),
        12: transform_cell12("".join(cells[12]["source"])),
        14: CELL14,
        15: CELL15,
        16: CELL16,
        17: CELL17,
        18: CELL18,
    }
    for index, text in edits.items():
        if cells[index]["cell_type"] != "markdown" and not text.lstrip().startswith("#"):
            raise RuntimeError(f"cell {index} replacement does not look like code")
        new = textwrap.dedent(text).strip("\n") + "\n"
        cells[index]["source"] = new.splitlines(keepends=True)

    for cell in cells:
        cell["metadata"] = {}
        if cell["cell_type"] == "code":
            cell["outputs"] = []
            cell["execution_count"] = None

    return json.dumps(nb, indent=1, sort_keys=True) + "\n"


def main(argv: list[str]) -> int:
    check = "--check" in argv[1:]
    rendered = build()
    if check:
        if not OUT_PATH.exists():
            print(f"V10_MISSING: {OUT_PATH}", file=sys.stderr)
            return 1
        current = OUT_PATH.read_text()
        if current != rendered:
            print("V10_NOT_REPRODUCIBLE: regenerate with tools/build_v10.py", file=sys.stderr)
            return 1
        print("V10_REPRODUCIBLE")
        return 0
    OUT_PATH.write_text(rendered)
    print("BUILT_V10", OUT_PATH)
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv))
