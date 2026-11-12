#!/usr/bin/env python3
"""One-time Plexus setup for the Colab clef-llama-remote rung.

Creates the `clef-llama-remote` provider (System One / `decisions` surface) and
registers it as a DISABLED target on the `cloudflare/clef-flash` alias, so the
notebook can enable it once its GPU workers are serving. Also pins the alias's
advertised context length to the largest value any rung advertises.

Idempotent: re-running produces the same config. Always snapshots the alias
dict before writing.

Two provider fields are deliberately not per-run:
  timeoutMs       600000  — the notebook's proxy gives up at 540 s, so its own 502
                            reaches Plexus before Plexus abandons the request.
  maxConcurrency  2       — a baseline, since a one-time setup cannot know how many
                            GPUs a run gets. The notebook rewrites it to its worker
                            count at bring-up and restores this baseline on teardown.

Env:
  PLEXUS_BASE_URL    e.g. https://plexus.example.ts.net
  PLEXUS_ADMIN_KEY   x-admin-key
  CLEF_REMOTE_KEY    bearer the notebook will require (default: read from
                     ~/projects/clef-llama-remote/clef_remote_key.txt)
"""

from __future__ import annotations

import json
import os
import pathlib
import sys
import urllib.error
import urllib.request

BASE = os.environ.get("PLEXUS_BASE_URL", "").rstrip("/")
ADMIN = os.environ.get("PLEXUS_ADMIN_KEY", "")
ALIAS = "cloudflare/clef-flash"
PROVIDER = "clef-llama-remote"
MODEL = "clef-flash"
HERE = pathlib.Path(__file__).resolve().parent

# Advertised context length per (provider, model) rung. Sources in README.md.
RUNG_CONTEXT = {
    ("clef-flash-mac", "clef-flash"): 16384,      # supervisor CLEF_CTX
    ("clef-llama-remote", "clef-flash"): 131072,  # T4 KV budget, see notebook
    ("neuralwatt", "clef-flash"): 262128,         # /v1/models max_model_len
    ("openrouter", "cloudflare/clef-flash"): 24576,  # /api/v1/models/.../endpoints
}


def req(method: str, path: str, payload: dict | None = None) -> dict:
    url = f"{BASE}{path}"
    data = json.dumps(payload).encode() if payload is not None else None
    r = urllib.request.Request(url, data=data, method=method)
    r.add_header("x-admin-key", ADMIN)
    if data:
        r.add_header("content-type", "application/json")
    try:
        with urllib.request.urlopen(r, timeout=30) as resp:
            body = resp.read()
            return json.loads(body) if body else {}
    except urllib.error.HTTPError as e:
        raise SystemExit(f"{method} {path} -> {e.code}: {e.read()[:500]!r}") from None


def provider_config(api_key: str, base_url: str) -> dict:
    return {
        "api_base_url": {"systemone": base_url},
        "display_name": "clef-flash colab (ephemeral T4, on-demand)",
        "api_key": api_key,
        "enabled": True,
        "allow_100_percent_utilization": True,
        "models": {
            MODEL: {
                "pricing": {"source": "simple", "input": 0, "output": 0},
                "type": "decisions",
                "access_via": ["systemone"],
            }
        },
        "headers": {},
        "extraBody": {},
        "model_autosync": {"enabled": False, "intervalMinutes": 60},
        # baseline; the notebook sets this to its worker count at bring-up, restores it on stop
        "maxConcurrency": 2,
        # must exceed the proxy's 540 s upstream timeout, or Plexus gives up mid-decode
        "timeoutMs": 600000,
    }


def main() -> int:
    if not BASE or not ADMIN:
        print("PLEXUS_BASE_URL and PLEXUS_ADMIN_KEY must be set", file=sys.stderr)
        return 2

    key_file = pathlib.Path(os.environ.get("CLEF_REMOTE_KEY_FILE", HERE / "clef_remote_key.txt"))
    if not key_file.exists():
        print(f"missing {key_file} (the bearer the notebook will require)", file=sys.stderr)
        return 2
    api_key = key_file.read_text().strip()
    if len(api_key) < 24:
        print("remote api key too short", file=sys.stderr)
        return 2

    # 0. snapshot
    aliases = req("GET", "/v0/management/aliases")
    (HERE / "aliases.snapshot.json").write_text(json.dumps(aliases, indent=1, sort_keys=True))
    print(f"snapshot: {len(aliases)} aliases -> aliases.snapshot.json")

    # 1. provider (base URL is a placeholder; the notebook rewrites it to its
    #    current tailnet IP on every run, because the Plexus host runs with CorpDNS off)
    existing = None
    try:
        existing = req("GET", f"/v0/management/providers/{PROVIDER}")
    except SystemExit:
        pass
    cfg = provider_config(api_key, "http://clef-llama-remote.example.ts.net:8080/v1")
    if existing and existing.get("api_base_url", {}).get("systemone"):
        cfg["api_base_url"] = existing["api_base_url"]
    req("PUT", f"/v0/management/providers/{PROVIDER}", cfg)
    got = req("GET", f"/v0/management/providers/{PROVIDER}")
    assert got["models"][MODEL]["type"] == "decisions", got["models"]
    assert got["models"][MODEL]["access_via"] == ["systemone"], got["models"]
    print(f"provider {PROVIDER}: ok (base {got['api_base_url']['systemone']})")

    # 2. alias: register the rung, disabled
    alias = req("GET", f"/v0/management/aliases/{ALIAS}")
    alias.pop("slug", None)  # read-only echo from the single-alias GET
    groups = alias.get("target_groups") or []
    local = next((g for g in groups if g.get("name") == "LOCAL"), None)
    if local is None:
        raise SystemExit("alias has no LOCAL group; refusing to guess placement")
    targets = local.setdefault("targets", [])
    rung = next((t for t in targets if t.get("provider") == PROVIDER and t.get("model") == MODEL), None)
    if rung is None:
        targets.append({"provider": PROVIDER, "model": MODEL, "enabled": False})
        print(f"added rung {PROVIDER}:{MODEL} to LOCAL group (enabled=false)")
    else:
        rung["enabled"] = False
        print("rung already present; forced enabled=false")

    # 3. advertised context = max over rungs
    pairs = [
        (t.get("provider"), t.get("model"))
        for g in groups
        for t in (g.get("targets") or [])
        if t.get("provider")
    ]
    known = {p: RUNG_CONTEXT[p] for p in pairs if p in RUNG_CONTEXT}
    missing = [p for p in pairs if p not in RUNG_CONTEXT]
    if missing:
        raise SystemExit(f"no context length recorded for rungs {missing}; extend RUNG_CONTEXT")
    ctx = max(known.values())
    meta = alias.get("metadata") or {"source": "custom"}
    overrides = meta.setdefault("overrides", {})
    overrides["name"] = overrides.get("name") or ALIAS
    overrides["context_length"] = ctx
    alias["metadata"] = meta
    req("PUT", f"/v0/management/aliases/{ALIAS}", alias)
    print("rung context lengths: " + ", ".join(f"{p[0]}:{p[1]}={v}" for p, v in known.items()))
    print(f"alias context_length -> {ctx}")

    # 4. verify
    after = req("GET", f"/v0/management/aliases/{ALIAS}")
    got_ctx = (after.get("metadata") or {}).get("overrides", {}).get("context_length")
    got_targets = [
        (t.get("provider"), t.get("model"), t.get("enabled"))
        for g in after.get("target_groups", [])
        for t in g.get("targets", [])
    ]
    assert got_ctx == ctx, (got_ctx, ctx)
    assert (PROVIDER, MODEL, False) in got_targets, got_targets
    print("verified alias:", got_targets, "ctx=", got_ctx)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
