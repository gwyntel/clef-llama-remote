# Colab handoff — Clef-Flash through Plexus

## Open and configure

1. Open `notebooks/clef-llama-colab-v5.ipynb` in Colab.
2. Select **Runtime → Change runtime type → T4 GPU**.
3. In Colab's **🔑 Secrets** panel, enable:

   - `TAILSCALE_API_KEY` — device list/create/delete for the ephemeral node
   - `PLEXUS_ADMIN_KEY` — provider base URL, concurrency, and rung control
   - `PLEXUS_API_KEY` — scoped inference key for the bring-up self-test

   `CLEF_REMOTE_KEY` is **optional**. v5 adopts the provider's stored `api_key` from
   `GET /v0/management/providers/clef-llama-remote` using `PLEXUS_ADMIN_KEY`, so the notebook
   does not need a second copy of the upstream bearer. If present, it is used as-is.

   Values are staged locally in `~/Downloads/clef-llama-colab-secrets.json`
   (mode `0600`). Copy them into Colab manually; do not upload that file to Drive, paste it
   into a notebook cell, or attach it in chat.

4. Set the two environment-specific constants in the configuration cell:

   - `PLEXUS_BASE` — your Plexus base URL (for example `https://plexus.example.ts.net`)
   - `EXPECTED_DNSNAME` — optional; set it to the node's expected `*.ts.net` name if you want
     the notebook to warn on a hostname collision. Leave it empty to skip that check.

5. Set **Runtime → Run all**.
6. Keep the **hold open** cell running. The first run downloads roughly 8 GB, starts one worker
   per physical GPU, joins Tailscale as the ephemeral `clef-llama-remote` node, proves GPU
   residency and Serve reachability, rewrites the provider base URL, and enables the LOCAL rung
   only after the self-test succeeds.
7. When finished, run the **shutdown** cell. It disables the rung first, restores
   `maxConcurrency`, stops workers/proxy, leaves the tailnet, and deletes the device.

## v5 lifecycle guarantees

- **Teardown will not create a routed dead endpoint.** If the rung cannot be verified disabled
  after three attempts, `stop()` leaves workers, proxy, and tailnet running and reports
  `teardown : INCOMPLETE`. Re-run `stop()` after fixing the control-plane error, or call
  `stop(force=True)` to destroy the node deliberately.
- **Duplicate cleanup refuses fresh nodes.** Matching hostnames seen within the last 300 seconds
  are treated as a live second notebook and raise instead of being deleted. Stop the other
  notebook or delete the device manually.
- **Self-test validates answer shape.** A choice answer must carry `type: "choice"` with a
  criterion key; a noul answer must carry `type: "noul"` with a number in `[0,1]`. Empty or
  null answer objects fail bring-up.
- **Credential scope is unchanged.** `PLEXUS_ADMIN_KEY` is the broad Plexus administration
  credential and `TAILSCALE_API_KEY` can delete devices. Treat the Colab runtime as
  single-tenant; rotate either credential if the runtime or handoff file is exposed.

## Verified live state (v5, 2026-11-12)

- Provider `clef-llama-remote`, model `clef-flash`, `decisions`, `access_via: ["systemone"]`.
- Alias `cloudflare/clef-flash`; LOCAL target enabled by the running notebook.
- Tailscale node `clef-llama-remote` joined on a Colab T4.
- Worker: `cuda:0`, `ctx=131072`, `kv=4.00 GiB`.
- Proxy health: `served=2`, `failures=0` after the self-test and an external verification call.
- End-to-end `POST /v1/systemone` through Plexus returned HTTP 200 with a real GGUF path,
  `fruit=apple` (choice), and a numeric noul answer.

## Limits

- Deleting or evicting the Colab VM cannot execute notebook cleanup. Disable the target manually
  with `python3 tools/rung.py off` from `~/projects/clef-llama-remote`.
- Per-target context-length fields do not exist in Plexus's model-target schema. The alias-level
  context override is `262128`; worker context is selected from available VRAM
  (131072 on T4-class memory, 262144 with at least 22 GiB) and read back from `/props`.
- The local secret handoff file remains on disk until deleted. It is mode `0600`; delete it after
  the credentials have been entered into Colab.
