#!/usr/bin/env python3
"""Toggle the clef-llama-remote rung on cloudflare/clef-flash and/or repoint it.

  rung.py status
  rung.py on  --base http://100.x.y.z:8080/v1     # set provider base, enable rung
  rung.py off                                     # disable rung
  rung.py base --base http://...                  # repoint provider only

Every mutation is followed by a re-GET assertion: a PUT that returns
{"success": true} is not evidence the config changed.
"""
from __future__ import annotations

import argparse
import json
import os
import sys
import urllib.error
import urllib.request

BASE = os.environ.get("PLEXUS_BASE_URL", "").rstrip("/")
ADMIN = os.environ.get("PLEXUS_ADMIN_KEY", "")
ALIAS = "cloudflare/clef-flash"
PROVIDER = "clef-llama-remote"
MODEL = "clef-flash"


def req(method, path, payload=None):
    data = json.dumps(payload).encode() if payload is not None else None
    r = urllib.request.Request(BASE + path, data=data, method=method)
    r.add_header("x-admin-key", ADMIN)
    if data:
        r.add_header("content-type", "application/json")
    try:
        with urllib.request.urlopen(r, timeout=30) as resp:
            b = resp.read()
            return json.loads(b) if b else {}
    except urllib.error.HTTPError as e:
        raise SystemExit(f"{method} {path} -> {e.code}: {e.read()[:400]!r}") from None


def alias():
    a = req("GET", f"/v0/management/aliases/{ALIAS}")
    a.pop("slug", None)
    return a


def find_target(a):
    for g in a.get("target_groups") or []:
        for t in g.get("targets") or []:
            if t.get("provider") == PROVIDER and t.get("model") == MODEL:
                return g, t
    raise SystemExit(f"rung {PROVIDER}:{MODEL} not present on alias {ALIAS}")


def set_enabled(on: bool):
    a = alias()
    _, t = find_target(a)
    t["enabled"] = on
    req("PUT", f"/v0/management/aliases/{ALIAS}", a)
    t2 = find_target(alias())[1]
    if t2.get("enabled") is not on:
        raise SystemExit(f"verify failed: enabled={t2.get('enabled')} wanted {on}")
    return t2


def set_base(url: str):
    p = req("GET", f"/v0/management/providers/{PROVIDER}")
    p["api_base_url"] = {"systemone": url}
    req("PUT", f"/v0/management/providers/{PROVIDER}", p)
    got = req("GET", f"/v0/management/providers/{PROVIDER}")["api_base_url"]["systemone"]
    if got != url:
        raise SystemExit(f"verify failed: base is {got}, wanted {url}")
    return got


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("action", choices=["status", "on", "off", "base"])
    ap.add_argument("--base")
    a = ap.parse_args()
    if not BASE or not ADMIN:
        raise SystemExit("PLEXUS_BASE_URL and PLEXUS_ADMIN_KEY must be set")
    if a.action in ("on", "base"):
        if not a.base:
            raise SystemExit("--base required")
        print("provider base ->", set_base(a.base))
    if a.action == "on":
        t = set_enabled(True)
        print("rung enabled:", json.dumps(t))
    elif a.action == "off":
        t = set_enabled(False)
        print("rung disabled:", json.dumps(t))
    if a.action in ("status", "on", "off"):
        al = alias()
        p = req("GET", f"/v0/management/providers/{PROVIDER}")
        print(json.dumps({
            "provider_base": p["api_base_url"],
            "targets": [[t.get("provider"), t.get("model"), t.get("enabled")]
                        for g in al.get("target_groups", []) for t in g.get("targets", [])],
            "ctx": (al.get("metadata") or {}).get("overrides", {}).get("context_length"),
        }, indent=1))


if __name__ == "__main__":
    main()
