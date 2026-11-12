# clef-llama-remote — verified facts and evidence log

Everything below was executed on 2026-11-11 from the operator workstation against the live
constellation. No secrets are recorded here.

## Plexus (`https://plexus.example.ts.net`)

| Check | Result |
|---|---|
| `GET /v0/management/auth/verify` | `{"ok":true,"role":"admin"}` |
| `/v1/systemone` (before changes) | routed to neuralwatt, `usage.input_tokens=335`, choice/score/noul answers all valid |
| provider `clef-llama-remote` created | `api_base_url.systemone`, model `clef-flash` `type=decisions` `access_via=["systemone"]` |
| rung added to alias `cloudflare/clef-flash` LOCAL group | `enabled=false` (verified by re-GET) |
| alias `metadata.overrides.context_length` | `262128` (max of the rung set) |

Rung context lengths, with sources:

| rung | ctx | source |
|---|---|---|
| `clef-flash-mac:clef-flash` | 16384 | `CLEF_CTX` in `~/.clef/supervisor.py` |
| `clef-llama-remote:clef-flash` | 131072 | T4 KV budget (below) |
| `neuralwatt:clef-flash` | 262128 | `GET https://api.neuralwatt.com/v1/models` → `max_model_len` |
| `openrouter:cloudflare/clef-flash` | 24576 | `GET https://openrouter.ai/api/v1/models/cloudflare/clef-flash/endpoints` |

T4 KV budget for `t4-compatible` context: GGUF `clef.context_length=262144`,
32 layers with every 4th full attention (8 KV layers), `head_count_kv=4`,
key/value length 256 → f16 KV = 8 × 4 × 512 × 2 = 32 KiB/token.
6.04 GB model + 4 GiB KV at 131072 tokens = ~10.1 GB of a T4's ~14.7 GB usable,
leaving ~4 GB for compute buffers. At 262144 the same math reaches ~14.6 GB and
is expected to OOM, so the notebook uses 131072 per GPU (262144 only on
>= 22 GiB cards).

## End-to-end routing proof (stand-in upstream, local workstation)

1. `tools/standin_systemone.py` on `127.0.0.1:8080` returning marked answers.
2. `tailscale serve --bg --tcp=8080 tcp://127.0.0.1:8080` → serve accepts an
   arbitrary TCP port; status shows `tcp://100.64.0.10:8080 (tailnet only)`.
3. **From the Plexus host** (no MagicDNS): `curl http://100.64.0.10:8080/health` →
   `{"status": "ok", "standin": true}`. Plain HTTP over the tailnet IP works;
   this is why the provider base URL is an IP, not a `*.ts.net` name.
4. `tools/rung.py on --base http://100.64.0.10:8080/v1` → Plexus
   `/v1/systemone` returned `provider: standin-t4` with valid answers.
5. `tools/rung.py off` → next call fell back to neuralwatt (real probabilities
   0.5176/0.4824), i.e. the rung is fully removed from routing when disabled.
6. Restored: rung disabled, provider base back to the placeholder, serve rule
   removed.

## Colab-side facts checked

| Fact | Evidence |
|---|---|
| `tailscale_latest_amd64.tgz` resolves | 302 → `tailscale_1.104.6_amd64.tgz`, 41 MB, contains `tailscaled` + `tailscale` |
| serve works in userspace-networking | Tailscale docs: "Serve and Funnel work the same in this mode" |
| llama.cpp release `b12292` has `/v1/systemone` | `strings libllama-server-impl.so` → `/v1/systemone`, `This model is not a decision model`, `decision model has no labels` |
| same release has the `clef` arch | `strings libllama.so` → `llama_model_clef`, `llm_graph_input_attn_clef` |
| assets exist | `llama-b12292-bin-ubuntu-cuda-12.8-x64.tar.gz` 198 MB, `cudart-llama-b12292-bin-ubuntu-cuda-12.8-x64.tar.gz` 594 MB |
| GGUF source | `bartowski/Cloudflare_clef-flash-GGUF` / `Cloudflare_clef-flash-Q4_K_M.gguf`, 6 040 541 344 B |
| tailnet ACL | single `{"action":"accept","src":["*"],"dst":["*:*"]}` rule — the Plexus host (tagged) may reach an untagged/tagged Colab node and vice versa |
| MagicDNS | tailnet-level `magicDNS: true`, but the Plexus host `CorpDNS: false` → **IP required** |

## Tailscale credentials minted

- API key from clipboard → `TAILSCALE_API_KEY` in `~/.hermes/.env` on the operator workstation
  and `/home/hermes/.hermes/.env` on the agent host (`agent`).
- Ephemeral auth key (id `[REDACTED]`, reusable, ephemeral,
  preauthorized, `tag:endpoints`, expires 2027-02-09) → `TAILSCALE_CLEF_AUTHKEY`
  in the operator workstation `~/.hermes/.env`.

## Credential handoff and final live state — 2026-11-11

- Tailscale key `[REDACTED]` was read back from the Tailscale API: unrevoked, expires
  `2027-02-09T22:33:02Z`, capabilities `reusable: true`, `ephemeral: true`,
  `preauthorized: true`, tags `["tag:endpoints"]`. The secret value was not printed.
- `TAILSCALE_CLEF_AUTHKEY` is now present in the operator workstation's `~/.hermes/.env` and the agent host's
  `/home/hermes/.hermes/.env`. The remote write was followed by a value comparison and permission
  check: both files are `0600`; values match. `TAILSCALE_API_KEY` is present separately on both.
- `~/Downloads/clef-llama-colab-secrets.json` was created with mode `0600` for manual
  entry into Colab's Secrets panel. It contains `TS_AUTHKEY`, `PLEXUS_ADMIN_KEY`, and
  `PLEXUS_API_KEY`. It must be deleted after import and is not attached to the notebook or chat.
  The admin key is the existing broad administration credential; provider/alias writes require
  admin authority, so an ordinary inference key cannot perform the notebook's registration.
- Live read-back: provider `clef-llama-remote` still has `timeoutMs: 60000` and
  `maxConcurrency: 2`; the remote target is disabled and both cloud fallbacks are enabled.
  `plexus_setup.py` now specifies 600000 ms, and notebook bring-up raises the live value to
  600000 ms with read-back verification before enabling. No live timeout mutation was performed
  during credential handoff.
- `COLAB_HANDOFF.md` contains the concise upload/Secrets/run/shutdown instructions. A real Colab
  T4 run remains the operator's acceptance test; it was not simulated or claimed as executed.

## v5 live Colab verification — 2026-11-12

- Notebook `notebooks/clef-llama-colab-v5.ipynb` ran on a Colab T4 with `TAILSCALE_API_KEY`,
  `PLEXUS_ADMIN_KEY`, and `PLEXUS_API_KEY` enabled. It downloaded the 6.04 GB GGUF, loaded one
  worker on `cuda:0` at `ctx=131072` (`kv=4.00 GiB`), joined the tailnet as
  `clef-llama-remote`, rewrote the provider base URL to its tailnet IPv4, and enabled the LOCAL
  rung only after bring-up gates passed.
- `GET /health` from the tailnet reported `workers healthy`, `served=2`, `failures=0` after the
  built-in self-test and one external verification call.
- `POST https://plexus.example.ts.net/v1/systemone` with
  `model=direct/clef-llama-remote/clef-flash` returned **HTTP 200**, the real GGUF path,
  `fruit={"type":"choice","choice":"apple",...}`, `sure={"type":"noul","noul":0.9508...}`,
  and `usage.input_tokens=216`.
- v5 adds three lifecycle guards: teardown leaves workers/proxy/tailnet up if the rung cannot be
  verified disabled; duplicate cleanup refuses nodes seen within 300 s; the self-test validates
  choice/noul answer shape. The offline harness now reports **43/43** checks.


