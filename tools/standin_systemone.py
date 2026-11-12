#!/usr/bin/env python3
"""Stand-in System One upstream used to verify the clef-llama-remote rung end to end.

Serves POST /v1/systemone with a syntactically valid Decisions response whose
answers are marked with the caller-supplied state, so a Plexus caller can prove
*which* rung answered. Not a model — plumbing test only.
"""
import json, os, sys, threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

KEY = os.environ.get("CLEF_REMOTE_KEY", "")
PORT = int(os.environ.get("STANDIN_PORT", "8080"))


class H(BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"

    def log_message(self, fmt, *a):
        sys.stderr.write("standin " + (fmt % a) + "\n")

    def _json(self, code, obj):
        body = json.dumps(obj).encode()
        self.send_response(code)
        self.send_header("content-type", "application/json")
        self.send_header("content-length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def _auth(self):
        h = self.headers.get("Authorization", "")
        return h.startswith("Bearer ") and h[7:].strip() == KEY

    def do_GET(self):
        if self.path in ("/health", "/v1/health"):
            return self._json(200, {"status": "ok", "standin": True})
        if self.path == "/v1/models":
            return self._json(200, {"object": "list", "data": [
                {"id": "clef-flash", "object": "model", "owned_by": "standin",
                 "max_model_len": 131072, "meta": {"n_ctx": 131072, "n_ctx_train": 262144}}]})
        self._json(404, {"error": "not found"})

    def do_POST(self):
        if not self._auth():
            return self._json(401, {"error": {"message": "invalid api key", "type": "auth_error"}})
        n = int(self.headers.get("content-length") or 0)
        req = json.loads(self.rfile.read(n) or b"{}")
        answers = {}
        for qid, q in (req.get("questions") or {}).items():
            t = q.get("type")
            if t == "choice":
                opts = list((q.get("criteria") or {}).keys()) or ["a"]
                answers[qid] = {"type": "choice", "choice": opts[0],
                                "probabilities": {o: (1.0 if o == opts[0] else 0.0) for o in opts},
                                "confidence": 1.0}
            elif t == "score":
                levels = q.get("criteria") or ["low", "high"]
                answers[qid] = {"type": "score", "score": len(levels) - 1,
                                "legend": {str(i): str(d) for i, d in enumerate(levels)},
                                "probabilities": {str(i): (1.0 if i == len(levels) - 1 else 0.0)
                                                  for i in range(len(levels))},
                                "confidence": 1.0}
            else:
                answers[qid] = {"type": "noul", "noul": 1.0}
        self._json(200, {"model": "clef-flash", "provider": "standin-t4", "answers": answers,
                         "usage": {"input_tokens": 42, "output_tokens": 0}})


if __name__ == "__main__":
    if not KEY:
        sys.exit("CLEF_REMOTE_KEY must be set")
    srv = ThreadingHTTPServer(("127.0.0.1", PORT), H)
    srv.daemon_threads = True
    print(f"standin listening 127.0.0.1:{PORT}", flush=True)
    srv.serve_forever()
