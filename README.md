# clef-llama-remote — Clef-Flash on Colab as a Plexus `decisions` upstream

Turns a **Colab GPU runtime** into an ephemeral Plexus upstream for the `cloudflare/clef-flash`
alias, and self-registers it. It joins the tailnet as an ephemeral node, serves the GGUF with
llama.cpp's native System One surface, and enables its rung in Plexus only once the workers answer.

Intended use: an on-demand `decisions`/`talkie-judge` rung that costs nothing when idle, sits behind
the always-on `neuralwatt` and `openrouter` rungs, and disappears when the VM dies.

```
Colab VM (ephemeral)                                    constellation
┌──────────────────────────────────────────────┐        ┌───────────────────────────┐
│ llama-server  :8091  CUDA_VISIBLE_DEVICES=0  │        │ Plexus gateway             │
│ llama-server  :8092  CUDA_VISIBLE_DEVICES=1  │◀──┐    │  alias cloudflare/clef-   │
│        ▲            ▲                        │   │    │        flash              │
│        └─ round-robin proxy 127.0.0.1:8080 ──┼───┘    │   LOCAL  ──▶ clef-llama-  │
│              (bearer + retry)                │        │              remote       │
│                     ▲                        │        │   SUB    ──▶ neuralwatt   │
│        tailscale serve tcp/8080              │        │   FALLBACK ▶ openrouter   │
│                     ▲                        │        └───────────────────────────┘
│        tailscaled --tun=userspace-networking │                 ▲
└─────────────────────┼────────────────────────┘                 │ POST /v1/systemone
                      └──────── tailnet 100.x ──────────────────┘  Bearer <provider api_key>
```

## Repo layout

| Path | Purpose |
|---|---|
| `notebooks/clef-llama-colab-v5.ipynb` | the runtime: 19 cells, 13 code cells, 3 labelled shell cells |
| `plexus_setup.py` | one-time: creates the provider + the **disabled** rung, pins the alias context |
| `README.md` | this file |
| `EVIDENCE.md` | verified-facts log: management calls, rung context sources, and the end-to-end routing proof |
| `tools/rung.py` | `status` / `on --base …` / `off` / `base --base …` — flip the rung by hand, each write re-`GET`-ed |
| `tools/standin_systemone.py` | stand-in `/v1/systemone` upstream (marked answers, no model) used to prove routing end to end |
| `clef_remote_key.txt` | local-only bearer for the provider; gitignored, never committed |

## What the run does, in order

1. **Deps** — `huggingface_hub` only (shell cell 1/3).
2. **llama.cpp `b12292`** — prebuilt `llama-b12292-bin-ubuntu-cuda-12.8-x64` (198 MB) **plus**
   `cudart-llama-b12292-bin-ubuntu-cuda-12.8-x64` (594 MB, ships the CUDA runtime libs so the
   Colab driver mismatch never surfaces). No source build (shell cell 2/3).
3. **Capability gate** — greps `libllama-server-impl.so` for `/v1/systemone` and aborts with a
   clear message if the release cannot serve `decisions`. Then downloads
   `bartowski/Cloudflare_clef-flash-GGUF` → `Cloudflare_clef-flash-Q4_K_M.gguf` (6.04 GB, no token).
4. **GPU policy** — `nvidia-smi --query-gpu=index,name,memory.total`. 0 GPUs → fail loudly.
   Exactly 1 → one worker pinned to `CUDA_VISIBLE_DEVICES=0`. 2+ → two workers (`=0`, `=1`),
   extras logged and ignored. Never one worker across both GPUs, never a silent 2→1 degrade.
   After load, each worker must also be **provably resident on a GPU**: its pid has to appear in
   `nvidia-smi --query-compute-apps` holding ≥ 2 GiB, or (only if the driver will not enumerate
   compute processes) its log must report `offloaded N/M layers`. No proof ⇒ bring-up fails with
   the rung still disabled, because a CPU-only load would burn the provider timeout per call.
5. **Context per GPU** — from that GPU's total VRAM, capped at 262144, overridable with `CLEF_CTX`.
6. **tailscale 1.104.6** — static tarball (shell cell 3/3); `install.sh` is not usable on Colab.
7. **Plexus client** — alias/provider read-modify-write, every write re-`GET`-ed and asserted.
8. **Workers + proxy** — `llama-server` per GPU, loopback only, behind a bearer-checking
   round-robin proxy with upstream retry.
9. **tailnet** — userspace daemon, ephemeral join as `clef-llama-remote`, `tailscale serve
   --bg --tcp=8080` (fallback 8443). The published port is then **probed from inside the VM**
   through `tailscaled`'s userspace SOCKS5 proxy (`127.0.0.1:1055`), i.e. out onto the tailnet and
   back in through `serve`; a failure stops bring-up before Plexus is told about the node, unless
   `CLEF_ALLOW_UNREACHABLE=1` explicitly skips the gate.
10. **Registration** — provider `api_base_url.systemone` → `http://<tailnet IPv4>:<port>/v1`, then
    `maxConcurrency` → the live worker count (1 or 2), then `timeoutMs` **read live** and raised to
    600000 if the provider still carries less than 300000 (and re-`GET`-ed; a write that does not
    stick aborts bring-up), then the rung's `enabled` flag flipped in the alias's `LOCAL` group.
11. **Self-test** (requires `PLEXUS_API_KEY`) — one choice + one noul question through
    `https://plexus.example.ts.net/v1/systemone`; the proxy's served counter proves the call
    landed here. The checker requires a `choice` answer with a real criterion and a numeric
    `noul` in `[0,1]`. Any request/answer/routing failure triggers teardown, which disables the
    rung first.

Teardown reverses the dependency order: **rung disabled first** (verified), then the provider's
`maxConcurrency` restored to its configured baseline (2), then workers (SIGTERM → SIGKILL after
15 s), then proxy, then `tailscale serve reset` + `tailscale down`. If the rung cannot be verified
disabled after three attempts, v5 stops before killing the workers and reports
`teardown : INCOMPLETE`; the explicit `stop(force=True)` path is required to destroy a still-routed
node.
`atexit` plus SIGINT/SIGTERM/SIGHUP handlers, so a stopped cell or a dead kernel still cleans up
— but only while the kernel exists. A second caller (atexit, a signal, or the shutdown cell) waits
for the teardown already in flight instead of returning while sockets are still closing, and worker
spawn is serialised against teardown so a signal during bring-up cannot leave a `llama-server`
holding VRAM on a node whose rung is already disabled. If Colab deletes or evicts the VM (or the host dies), **no
teardown runs at all**: the rung stays enabled and Plexus keeps selecting a node that no longer
answers until the provider cools down. Nothing in-process can fix that; `tools/rung.py off` is the
manual remedy.
A 30 s heartbeat watches `tailscale status --json` **and** every worker's `/health`; two
consecutive rounds of a dead tailnet, or of all workers dead/unhealthy (one healthy worker is still
a serving node), trigger the same teardown — rung disabled before any socket closes, so Plexus
stops selecting the node first.

## Quick start

1. Create the notebook's secrets (Colab 🔑 panel) — see the table below.
2. Open `notebooks/clef-llama-colab-v5.ipynb` in Colab, `Runtime → Change runtime type → T4 GPU`.
   The concise credential and shutdown checklist is in [`COLAB_HANDOFF.md`](COLAB_HANDOFF.md).
3. `Runtime → Run all`. Expect ~8 GB of downloads on the first run, then a few minutes to the first
   answer while 6 GB of weights load.
4. The status block prints the tailnet IP, serve port, worker contexts and the verified rung state.
5. Leave the "hold open" cell running. When done, run the shutdown cell (or Ctrl-C).

One-time Plexus setup, if it has not been done on this host:

```bash
export PLEXUS_BASE_URL=https://plexus.example.ts.net
export PLEXUS_ADMIN_KEY=...
python3 plexus_setup.py          # creates provider clef-llama-remote + a DISABLED LOCAL rung
```

`plexus_setup.py` is idempotent and snapshots the alias before writing. It deliberately leaves the
rung disabled: only a running notebook may enable it.

## Secrets

| Name | Required | Used for |
|---|---|---|
| `TAILSCALE_API_KEY` | yes | device list/create/delete for the ephemeral tailnet node |
| `PLEXUS_ADMIN_KEY` | yes | `x-admin-key` on the management API |
| `PLEXUS_API_KEY` | yes | scoped inference key used by the bring-up self-test |
| `CLEF_REMOTE_KEY` | no | optional upstream bearer; v5 otherwise adopts the provider's stored `api_key` |

Cells read `google.colab.userdata` and fall back to `os.environ`, so they also run outside Colab.
Only names and lengths are ever printed. In v5, `CLEF_REMOTE_KEY` is optional: the config cell
reads the live provider's `api_key` with `PLEXUS_ADMIN_KEY` and uses that for the proxy and
workers. If you supply `CLEF_REMOTE_KEY`, it must match the provider's key or every call 401s.

Both secrets end up in a process command line (`--api-key`, `--authkey`), so they are visible to
`ps` inside the VM. That is acceptable on a single-tenant ephemeral VM and is not acceptable on a
shared host. `PLEXUS_ADMIN_KEY` is the existing broad Plexus administration credential, not a
notebook-scoped key: provider/alias writes require admin authority, so an ordinary inference key
cannot replace it. Do not share the Colab runtime; rotate the admin credential if it is exposed.
Every error path redacts them: `Tailscale._run` converts `subprocess.TimeoutExpired`
(whose text embeds the whole argv) into a message with `--authkey=***`, and `Worker.start` prints
`***` in place of `--api-key`.

## Context per GPU (KV = 32 KiB/token at f16)

| VRAM | `-c` | KV | Notes |
|---|---|---|---|
| ≥ 22 GiB | 262144 | 8.00 GiB | full trained context |
| 14–22 GiB (T4) | 131072 | 4.00 GiB | 6.04 GB weights + 4 GiB KV ⇒ ~4 GiB headroom |
| < 14 GiB | — | — | refused with an explanatory error |

`CLEF_CTX` overrides; the hard cap is 262144. The value in force is read back per worker from
`GET /props` → `default_generation_settings.n_ctx` and printed; a clamp is reported as a warning.

## Timeouts and long prompts

| Layer | Value | Why |
|---|---|---|
| provider `timeoutMs` | 600000 (10 min) | written by `plexus_setup.py` (was 60000) **and verified live at bring-up**: anything under 300000 is raised and re-`GET`-ed |
| proxy upstream timeout | 540 s | deliberately under the provider's, so the proxy's own 502 is what Plexus receives |
| self-test client timeout | 600 s | never shorter than the gateway's, so a slow answer is reported as slow, not unreachable |
| worker readiness | 900 s | 6.04 GB of weights off `/content`, plus CUDA context setup |

Practical constraint: these bounds cover decode, not unbounded pre-fill. On a single T4, prompt
pre-fill runs in the low hundreds of tokens per second, so a ~100k-token `state` costs several
minutes before the first token and a large `questions` block adds more. A request that exceeds
540 s still fails — but with the proxy's 502 rather than a silent Plexus timeout — and a request
that exceeds the provider's 600 s cools the provider down. Check `usage.input_tokens` on a real
call and keep `state` well inside both the worker's context and this budget. (Pre-fill throughput
is an estimate from T4-class hardware; measure it on the runtime you actually get.)

## Wire contract

Plexus POSTs to `{provider.api_base_url.systemone}/systemone` with
`Authorization: Bearer <provider api_key>`:

```json
{"model": "clef-flash",
 "state": "<string | object | array>",
 "questions": {
   "<id>": {"type": "noul",   "instructions": "<string | object | array>"},
   "<id>": {"type": "choice", "instructions": "...", "criteria": {"<option>": "<desc | null>"}},
   "<id>": {"type": "score",  "instructions": "...", "criteria": ["<level desc>", "..."]}
 }}
```

Minimum acceptable response:

```json
{"model": "<string>",
 "answers": {
   "<id>": {"type": "choice", "choice": "<option>", "probabilities": {}, "confidence": 0.0},
   "<id>": {"type": "score",  "score": 0.0, "legend": {}, "probabilities": {}},
   "<id>": {"type": "noul",   "noul": 0.0}
 },
 "usage": {"input_tokens": 0, "output_tokens": 0}}
```

`llama-server` implements this natively at `/v1/systemone` (named `systemone` chat template + the
decision head) and does **not** validate the `model` field, so the routed model string stays
`clef-flash`. A real call through Plexus today returned choice/score/noul answers with
`usage {"input_tokens": 335, "output_tokens": 0}`.

The proxy forwards `/v1/systemone` (and `GET /v1/models`) verbatim, adding only its own bearer.
`GET /health` is deliberately unauthenticated — liveness only, and it is what an operator polls.

## Verifying from the host

```bash
# node liveness: worker health + counters
curl -s http://100.x.y.z:8080/health | jq

# unauthenticated inference is refused
curl -s -o /dev/null -w '%{http_code}\n' -X POST http://100.x.y.z:8080/v1/systemone -d '{}'   # 401

# direct decisions call at the worker
curl -s http://100.x.y.z:8080/v1/systemone \
  -H "Authorization: Bearer $CLEF_REMOTE_KEY" -H 'content-type: application/json' \
  -d '{"model":"clef-flash","state":"ping","questions":{"n":{"type":"noul","instructions":"hi"}}}' | jq

# what Plexus stored
curl -s -H "x-admin-key: $PLEXUS_ADMIN_KEY" \
  https://plexus.example.ts.net/v0/management/providers/clef-llama-remote | jq '.api_base_url'

# rung state
curl -s -H "x-admin-key: $PLEXUS_ADMIN_KEY" \
  https://plexus.example.ts.net/v0/management/aliases/cloudflare/clef-flash \
  | jq '.target_groups[] | {name, targets}'

# end to end through Plexus
curl -s https://plexus.example.ts.net/v1/systemone \
  -H "Authorization: Bearer $PLEXUS_API_KEY" -H 'content-type: application/json' \
  -d '{"model":"clef-flash","state":"ping","questions":{"n":{"type":"noul","instructions":"hi"}}}' | jq
```

## Operational notes

- **Provider base URL is rewritten every run.** The Plexus host has CorpDNS/MagicDNS off and cannot resolve
  `*.ts.net`, so the notebook writes its own `100.x` IPv4 into `api_base_url.systemone`. Never pin a
  `*.ts.net` name there.
- **Hostname drift is survivable.** If the join lands on `clef-llama-remote-1...` (stale ephemeral
  node), the notebook warns loudly and continues, because routing uses the IP. Remedy: delete the
  stale device in the admin console.
- **Port ladder.** Proxy binds 8080 and publishes it with `tailscale serve`; if 8080 is taken it
  binds 8443 and rewrites the provider accordingly.
- **Session lifetime.** ~12 h cap on Colab Pro, ~90 min idle disconnect without a browser tab. Every
  restart repeats the whole bring-up; nothing outside `/content` persists.
- **Hard death leaves the rung enabled.** Nothing in the notebook can prevent it: `atexit` and the
  signal handlers live inside the kernel, so a deleted/evicted VM (or a lost host) runs no teardown
  and Plexus keeps selecting a node whose IP is gone. Plexus will cool the provider down
  (`disable_cooldown: false`, `stall_cooldown: false`). Either wait it out or clear the LOCAL
  rung's `enabled` flag through the management API (`tools/rung.py off`).
- **Alias context is the max over rungs.** `metadata.overrides.context_length` is 262128 (set by
  `plexus_setup.py`) while a T4 rung serves 131072 — an oversized prompt routed here will fail on
  this rung and fall through. Lower the alias override if that matters more than advertising the
  cloud rungs' window.
- **One or two workers, one provider.** `plexus_setup.py` writes a baseline `maxConcurrency: 2`;
  bring-up rewrites it to `len(WORKER_PLAN)` (1 on a single GPU, 2 otherwise) and teardown restores
  the baseline. Without that, a one-worker run advertises two slots and the second call queues
  behind a 131k-context decode. The proxy is the only thing balancing between workers.
- **Not implemented on purpose:** no `/v1/chat/completions` bridging (this rung is `decisions`
  only), no TLS or auth on `/health` (the tailnet is the transport boundary), no model persistence,
  no autoscaling past two GPUs.

## Troubleshooting

| Symptom | Cause / fix |
|---|---|
| `no GPUs visible` | runtime type is not GPU; `Change runtime type → T4 GPU` and re-run |
| worker exits during load, CUDA OOM in `/content/logs/llama-N.log` | context too greedy for the free card; re-run with `CLEF_CTX=65536` |
| `worker(s) [0] hold less than 2048 MiB on a GPU` | weights never reached the card: wrong release (no `libggml-cuda.so`), `LD_LIBRARY_PATH` missing the `cudart-*` dir, or CUDA init fell back to CPU. Rung stays disabled |
| `tcp/8080 does not answer over the tailnet` | reachability probe failed: check `tailscale serve status`, `BackendState`, and the `tag:endpoints` ACL. `CLEF_ALLOW_UNREACHABLE=1` skips the gate |
| `timeoutMs=600000 did not stick` | the provider kept a sub-floor timeout (under 300000) and the PUT was not honoured; fix it with `plexus_setup.py` or the management API, since bring-up refuses to register with it |
| `n_ctx` smaller than requested | server clamped it (VRAM or `--parallel`); the warning prints both values |
| `libcudart.so.12: cannot open shared object` | cudart tarball missing or not on `LD_LIBRARY_PATH`; re-run shell cell 2/3 and the locate cell |
| `ABORT: this llama.cpp build has no /v1/systemone` | wrong release; the notebook needs `b12292` |
| Plexus 401 | `CLEF_REMOTE_KEY` ≠ the provider's `api_key`; make them match |
| `tailscale up` prints a login URL | authkey consumed/expired; mint a new preauthorised+ephemeral+reusable key |
| node unreachable from the Plexus host | tailnet ACL must allow the caller to reach this node on 8080/8443 |
| self-test passes but says "routed elsewhere" | another LOCAL target is enabled or the selector changed; the proxy counter is the tell |
| everything 502 for a minute after a restart | workers are reloading 6 GB from `/content/models`; that is the 900 s readiness window |
