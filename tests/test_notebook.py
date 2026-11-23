"""Contract tests for the generated v10 notebook.

These never execute a notebook cell: they inspect the generated JSON and its ASTs.
"""

import ast
import json
import re
import subprocess
import tempfile
import unittest
from pathlib import Path

V9 = Path("notebooks/source/clef-runtime-v9.ipynb")
V10 = Path("notebooks/clef-llama-colab-v10.ipynb")
BUILD = Path("tools/build_v10.py")

# Cells whose source must survive the transform byte-identically: shell cells plus the
# GPU/model-download/worker-plan cells that define the actual serving behaviour.
PRESERVED_CELLS = (2, 4, 5, 6, 7)

# Symbols that only exist to write shared routing state, or that let a notebook skip its
# own admission proof. None may survive anywhere in the generated notebook.
FORBIDDEN = (
    "cleanup_duplicate_nodes",
    "_is_notebook_device",
    "set_rung_enabled(",
    "_restore_concurrency(",
    "_provider_api_key(",
    "CLEF_SKIP_SELF_TEST",
    "_secret(\"PLEXUS_ADMIN_KEY\")",
    "_secret(\"PLEXUS_API_KEY\")",
    "put_alias(",
    "set_provider_base(",
    "set_provider_concurrency(",
    "set_provider_timeout_ms(",
)

REQUIRED = (
    'TS_SOCK = STATE_DIR / "tailscaled.sock"',
    "CLEF_REGISTRY_KEY",
    "clef-flash-notebooks",
    "def identity()",
    "lease.create(",
    "lease.ready()",
    "lease.withdraw()",
    "clef-runtime.lock",
)


def load(path):
    return json.loads(Path(path).read_text())


def code_cells(nb):
    return [(i, "".join(c["source"])) for i, c in enumerate(nb["cells"]) if c["cell_type"] == "code"]


def joined_code(nb):
    return "\n".join(src for _, src in code_cells(nb))


def parseable(src):
    """A cell is parseable when it is Python; %%bash cells are not."""
    if src.lstrip().startswith("%%"):
        return None
    return ast.parse(src)


def classes(tree):
    return {n.name: n for n in ast.walk(tree) if isinstance(n, ast.ClassDef)}


def methods(cls):
    return {n.name: n for n in cls.body if isinstance(n, (ast.FunctionDef, ast.AsyncFunctionDef))}


class Generated(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.nb = load(V10)
        cls.old = load(V9)

    def test_preserved_cells_are_byte_identical(self):
        for i in PRESERVED_CELLS:
            self.assertEqual(
                self.nb["cells"][i]["source"],
                self.old["cells"][i]["source"],
                f"cell {i} must be byte-identical to v9",
            )

    def test_notebook_shape(self):
        self.assertEqual(len(self.nb["cells"]), len(self.old["cells"]))
        self.assertEqual(self.nb["nbformat"], self.old["nbformat"])
        self.assertEqual(self.nb["nbformat_minor"], self.old["nbformat_minor"])
        for i, cell in enumerate(self.nb["cells"]):
            self.assertEqual(cell["cell_type"], self.old["cells"][i]["cell_type"], f"cell {i} type")
            self.assertFalse(cell.get("outputs"), f"cell {i} carries outputs")
            self.assertEqual(cell.get("execution_count"), None, f"cell {i} carries an execution count")
            self.assertEqual(cell.get("metadata"), {}, f"cell {i} carries metadata")

    def test_every_python_cell_compiles(self):
        for i, src in code_cells(self.nb):
            tree = parseable(src)
            if tree is None:
                continue
            self.assertIsInstance(tree, ast.Module, f"cell {i}")

    def test_no_shared_writer_survives(self):
        src = joined_code(self.nb)
        for needle in FORBIDDEN:
            self.assertNotIn(needle, src, f"forbidden symbol survived: {needle}")

    def test_required_runtime_pieces_present(self):
        src = joined_code(self.nb)
        for needle in REQUIRED:
            self.assertIn(needle, src, f"missing required symbol: {needle}")

    def test_heading_says_v10(self):
        head = "".join(self.nb["cells"][0]["source"])
        self.assertIn("v10", head)
        self.assertNotIn("— **v6**", head)

    def test_no_admin_credential_is_read(self):
        src = joined_code(self.nb)
        self.assertNotIn("PLEXUS_ADMIN_KEY", src)
        self.assertNotIn("x-admin-key", src)

    def test_plexus_client_is_read_only(self):
        """The inherited PlexusClient must be replaced, not left as dead dangerous code."""
        trees = [parseable(src) for _, src in code_cells(self.nb)]
        found = []
        for tree in trees:
            if tree is None:
                continue
            for name, cls in classes(tree).items():
                if name == "PlexusClient":
                    found.append(cls)
        self.assertEqual(len(found), 1, "expected exactly one PlexusClient definition")
        cls = found[0]
        names = methods(cls)
        for banned in ("_req", "put_provider", "get_provider", "put_alias", "get_alias",
                       "set_rung_enabled", "rung_enabled", "set_provider_base",
                       "set_provider_concurrency", "set_provider_timeout_ms"):
            self.assertNotIn(banned, names, f"PlexusClient.{banned} must not survive")
        unparsed = ast.unparse(cls)
        self.assertNotIn("urllib", unparsed)
        self.assertNotIn("admin", unparsed.lower())

    def test_tailscale_client_has_no_sweep_and_checks_ownership(self):
        trees = [parseable(src) for _, src in code_cells(self.nb)]
        ts = None
        for tree in trees:
            if tree is None:
                continue
            if "Tailscale" in classes(tree):
                ts = classes(tree)["Tailscale"]
        self.assertIsNotNone(ts, "Tailscale class not found")
        names = methods(ts)
        self.assertNotIn("cleanup_duplicate_nodes", names)
        self.assertNotIn("_is_notebook_device", names)
        self.assertIn("delete_device", names)
        src = ast.unparse(names["delete_device"])
        self.assertIn("own_device_id", src, "delete_device must resolve its own device first")
        self.assertIn("refusing to delete", src)
        self.assertIn("deletion not confirmed", src)

    def test_runtime_owns_no_routing_writes(self):
        trees = [parseable(src) for _, src in code_cells(self.nb)]
        rt = None
        for tree in trees:
            if tree is None:
                continue
            if "Runtime" in classes(tree):
                rt = classes(tree)["Runtime"]
        self.assertIsNotNone(rt, "Runtime class not found")
        names = methods(rt)
        for banned in ("_restore_concurrency",):
            self.assertNotIn(banned, names)
        # stop() must withdraw the lease, verify it, drain, and only then free resources.
        stop = ast.unparse(names["stop"])
        self.assertIn("withdraw()", stop)
        self.assertIn("active", stop, "stop must drain local in-flight requests")
        self.assertIn("own_device_id", stop, "stop must resolve its own device before leaving")
        # _self_test must be admission, not an optional local probe.
        selftest = ast.unparse(names["_self_test"])
        self.assertIn("lease.ready()", selftest)
        self.assertNotIn("PLEXUS_API_KEY", selftest)
        # _watch must consult the lease instead of an alias flag.
        watch = ast.unparse(names["_watch"])
        self.assertNotIn("rung_enabled", watch)
        self.assertIn("lease", watch)
        # start() must take the VM-wide lock before spawning workers, and the lock helper
        # must be the thing that actually flocks (a no-op helper would satisfy a substring).
        start = names["start"]
        lock_call = [n.lineno for n in ast.walk(start)
                     if isinstance(n, ast.Call) and isinstance(n.func, ast.Name)
                     and n.func.id == "_acquire_vm_lock"]
        worker_ref = [n.lineno for n in ast.walk(start)
                      if isinstance(n, ast.Attribute) and n.attr == "_start_workers"]
        self.assertTrue(lock_call, "start() must call _acquire_vm_lock")
        self.assertTrue(worker_ref, "start() must reference _start_workers")
        self.assertLess(min(lock_call), min(worker_ref),
                        "the VM-wide lock must be taken before any worker is started")

        # Teardown ordering, by AST position: withdraw -> drain -> workers -> own device -> down.
        stop = names["stop"]

        def at(predicate):
            lines = [n.lineno for n in ast.walk(stop) if predicate(n)]
            return min(lines) if lines else None

        def call(attr, name=None):
            def predicate(node):
                if not isinstance(node, ast.Call) or not isinstance(node.func, ast.Attribute):
                    return False
                if node.func.attr != attr:
                    return False
                if name is not None:
                    return isinstance(node.func.value, ast.Name) and node.func.value.id == name
                return True
            return predicate

        order = [
            ("withdraw", at(call("withdraw"))),
            ("drain", at(lambda n: isinstance(n, ast.Attribute) and n.attr == "active")),
            ("workers", at(call("stop", "worker"))),
            ("own device", at(call("own_device_id"))),
            ("tailnet down", at(call("down"))),
        ]
        for label, line in order:
            self.assertIsNotNone(line, f"stop() never {label}")
        self.assertEqual([line for _, line in order],
                         sorted(line for _, line in order),
                         "stop() must withdraw, drain, stop workers, delete its own device, "
                         "then leave the tailnet")

    def test_proxy_tracks_active_and_reports_run_id(self):
        trees = [parseable(src) for _, src in code_cells(self.nb)]
        proxy = None
        for tree in trees:
            if tree is None:
                continue
            if "RoundRobinProxy" in classes(tree):
                proxy = classes(tree)["RoundRobinProxy"]
        self.assertIsNotNone(proxy)
        src = ast.unparse(proxy)
        self.assertIn("self.active = 0", src)
        self.assertIn("proxy.active += 1", src)
        self.assertIn("proxy.active -= 1", src)
        self.assertIn("run_id", src)

    def test_generation_is_deterministic(self):
        subprocess.run(["python3", str(BUILD), "--check"], check=True)

    def test_no_secret_literal_in_notebook(self):
        import re

        src = "\n".join("".join(c["source"]) for c in self.nb["cells"])
        patterns = (r"tskey-[A-Za-z0-9-]{10,}", r"sk-[A-Za-z0-9_-]{20,}",
                    r"ghp_[A-Za-z0-9]{20,}", r"\b[0-9a-f]{64}\b")
        for pattern in patterns:
            self.assertIsNone(re.search(pattern, src), f"literal secret shape {pattern} present")




class RegistryUrl(unittest.TestCase):
    """The config cell validates CLEF_REGISTRY_URL before any credential is sent.

    The validator is extracted from the generated notebook and driven directly: the cell's
    other side effects (secrets, filesystem, GPU probes) must not run in a test.
    """

    @classmethod
    def setUpClass(cls):
        import ipaddress
        import urllib.parse

        src = "".join(load(V10)["cells"][3]["source"])
        tree = ast.parse(src)
        found = [n for n in tree.body
                 if isinstance(n, ast.FunctionDef) and n.name == "_registry_url_ok"]
        if len(found) != 1:
            raise AssertionError("expected one _registry_url_ok in the config cell")
        namespace = {"urllib": urllib, "ipaddress": ipaddress}
        exec(compile(ast.Module(body=[found[0]], type_ignores=[]), "<validator>", "exec"),
             namespace)
        cls.ok = staticmethod(namespace["_registry_url_ok"])

    def test_accepts_literal_tailnet_ipv4_with_port(self):
        self.assertTrue(self.ok("http://100.64.7.7:8769"))
        self.assertTrue(self.ok("http://100.64.0.1:8769/"))

    def test_rejects_shapes_that_could_capture_the_lease_token(self):
        for bad in ("", "http://100.64.7.7", "https://100.64.7.7:8769",
                    "http://plexus.example.invalid:8769", "http://8.8.8.8:8769",
                    "http://192.0.2.5:8769", "http://user:pw@100.64.7.7:8769",
                    "http://100.64.7.7:8769/v1", "http://100.64.7.7:8769?x=1",
                    "http://100.64.7.7:8769#frag"):
            self.assertFalse(self.ok(bad), bad)

    def test_registry_url_can_come_from_the_colab_secret_panel(self):
        """In Colab the natural place for a value is the secret panel, not the shell.

        _secret() checks userdata first and falls back to the environment, so using it here is
        what makes CLEF_REGISTRY_URL settable the same way as the two credentials.
        """
        src = "".join(load(V10)["cells"][3]["source"])
        self.assertIn('_secret("CLEF_REGISTRY_URL", required=False)', src)
        self.assertNotIn('REGISTRY_URL = os.environ.get("CLEF_REGISTRY_URL", "")', src)

    def test_config_cell_refuses_to_start_with_a_bad_url(self):
        src = "".join(load(V10)["cells"][3]["source"])
        self.assertIn("if not _registry_url_ok(REGISTRY_URL):", src)


class NoHouseholdDetails(unittest.TestCase):
    """The repo is public: no private tailnet hostname or address may be embedded.

    The notebook reaches exactly one private service (the registrar), and its address comes
    from CLEF_REGISTRY_URL at run time. A baked-in default would be a household detail in a
    published artefact.
    """

    def test_notebook_embeds_no_household_endpoint(self):
        nb = load(V10)
        src = "\n".join("".join(c["source"]) for c in nb["cells"])
        # Structural, so the check itself names nothing: no *.ts.net host and no concrete
        # address in the tailnet range. 100.64.0.0/10 is the range, not a host.
        self.assertIsNone(re.search(r"\.ts\.net\b", src), "tailnet DNS name embedded")
        for match in re.finditer(
                r"\b100\.(?:6[4-9]|[7-9][0-9]|1[01][0-9]|12[0-7])\.\d{1,3}\.\d{1,3}\b", src):
            if src[match.end():match.end() + 1] != "/":
                self.fail(f"concrete tailnet address embedded: {match.group(0)}")

    def test_no_gateway_default_is_baked_in(self):
        nb = load(V10)
        src = "\n".join("".join(c["source"]) for c in nb["cells"])
        self.assertTrue("PLEXUS_BASE = (os.environ.get" not in src,
                        "no gateway endpoint default may be baked in")


def _method_tree(nb, class_name, method_name):
    """The AST node for one method of one class in the generated notebook."""
    for _, src in code_cells(nb):
        tree = parseable(src)
        if tree is None:
            continue
        for node in tree.body:
            if isinstance(node, ast.ClassDef) and node.name == class_name:
                for item in node.body:
                    if isinstance(item, ast.FunctionDef) and item.name == method_name:
                        return item
    raise AssertionError(f"{class_name}.{method_name} not found")


def _exec_function(nb, cell_index, name, extra=None):
    """Compile one module-level function out of a cell and return it.

    Only the requested definition runs, so a cell's secrets, filesystem probes and GPU calls
    never execute in a test.
    """
    tree = ast.parse("".join(nb["cells"][cell_index]["source"]))
    found = [n for n in tree.body if isinstance(n, ast.FunctionDef) and n.name == name]
    if len(found) != 1:
        raise AssertionError(f"expected exactly one {name} in cell {cell_index}")
    namespace = dict(extra or {})
    exec(compile(ast.Module(body=[found[0]], type_ignores=[]), f"<{name}>", "exec"), namespace)
    return namespace[name]


class Behaviour(unittest.TestCase):
    """Drive the generated notebook's own code, not a description of it.

    Everything here is offline: no Tailscale call, no socket, no secret.
    """

    @classmethod
    def setUpClass(cls):
        cls.nb = load(V10)

    # ---------------------------------------------------------------- VM-wide lock
    def test_a_second_kernel_on_the_same_vm_is_refused(self):
        import fcntl as _fcntl
        import pathlib

        acquire = _exec_function(self.nb, 11, "_acquire_vm_lock",
                                 {"fcntl": _fcntl, "Path": pathlib.Path})
        with tempfile.TemporaryDirectory() as tmp:
            first = acquire(tmp)
            try:
                with self.assertRaises(RuntimeError) as caught:
                    acquire(tmp)
                self.assertIn("another kernel on this VM", str(caught.exception))
            finally:
                first.close()
            # Releasing must make it available again, or a restart could never take the lock.
            second = acquire(tmp)
            second.close()

    # ------------------------------------------------------- exact-owner cleanup
    def _tailscale(self):
        import datetime
        import json as _json
        import os as _os
        import pathlib
        import re as _re
        import signal as _signal
        import socket as _socket
        import subprocess as _subprocess
        import time as _time
        import urllib.error as _uerr
        import urllib.request as _ureq

        namespace = {
            "TS_DIR": pathlib.Path("/nonexistent-ts"), "TS_SOCK": pathlib.Path("/nonexistent.sock"),
            "TS_STATE": pathlib.Path("/nonexistent.state"), "TS_HOSTNAME": "clef-nb-" + "a" * 32,
            "SECRETS": {"TAILSCALE_API_KEY": "test-key"}, "PROBE_TIMEOUT": 1.0,
            "TS_API_BASE": "https://api.tailscale.com/api/v2", "TS_TAG": "tag:endpoints",
            "TS_AUTHKEY_EXPIRY_SECONDS": 3600, "Path": pathlib.Path, "re": _re, "json": _json,
            "os": _os, "time": _time, "socket": _socket, "signal": _signal,
            "subprocess": _subprocess, "urllib": _ureq, "datetime": datetime,
            "log": lambda message: None,
        }
        tree = ast.parse("".join(self.nb["cells"][10]["source"]))
        klass = [n for n in tree.body if isinstance(n, ast.ClassDef) and n.name == "Tailscale"]
        self.assertEqual(len(klass), 1)
        exec(compile(ast.Module(body=[klass[0]], type_ignores=[]), "<Tailscale>", "exec"), namespace)
        return namespace["Tailscale"]()

    def test_only_the_recorded_own_device_may_be_deleted(self):
        ts = self._tailscale()
        ours = {"id": "d-own", "nodeId": "node-1", "addresses": ["100.64.7.7"],
                "hostname": "clef-nb-" + "a" * 32}
        # Same hostname prefix, different node: on a shared hostname this is exactly the
        # device a prefix sweep would have deleted.
        other = {"id": "d-other", "nodeId": "node-2", "addresses": ["100.64.8.8"],
                 "hostname": "clef-nb-" + "b" * 32}
        devices = [ours, other]
        deleted = []
        ts.status = lambda: {"Self": {"ID": "node-1", "TailscaleIPs": ["100.64.7.7"]}}
        ts.devices = lambda: list(devices)

        def api(method, path, payload=None, timeout=45.0, allowed=(200,)):
            self.assertEqual(method, "DELETE")
            deleted.append(path)
            devices.remove(ours)
            return 200, {}

        ts._api = api
        with self.assertRaises(RuntimeError) as caught:
            ts.delete_device("d-other")
        self.assertIn("refusing to delete", str(caught.exception))
        self.assertEqual(deleted, [], "no foreign device may be deleted")

        self.assertTrue(ts.delete_device("d-own"))
        self.assertEqual(deleted, ["/device/d-own"])

    def test_deletion_must_be_confirmed(self):
        ts = self._tailscale()
        ours = {"id": "d-own", "nodeId": "node-1", "addresses": ["100.64.7.7"], "hostname": "x"}
        ts.status = lambda: {"Self": {"ID": "node-1", "TailscaleIPs": ["100.64.7.7"]}}
        ts.devices = lambda: [ours]          # the API keeps listing it after the DELETE
        ts._api = lambda *a, **k: (200, {})
        with self.assertRaises(RuntimeError) as caught:
            ts.delete_device("d-own")
        self.assertIn("deletion not confirmed", str(caught.exception))

    def test_no_device_id_is_a_no_op(self):
        self.assertFalse(self._tailscale().delete_device(None))

    # --------------------------------------------------- rerun refuses a live runtime
    def _guard(self):
        """Compile the config cell's live-runtime guard on its own."""
        tree = ast.parse("".join(self.nb["cells"][3]["source"]))
        nodes = []
        for index, node in enumerate(tree.body):
            if isinstance(node, ast.Assign) and any(
                    isinstance(t, ast.Name) and t.id == "_prior_runtime" for t in node.targets):
                nodes = [node, tree.body[index + 1]]
                break
        if len(nodes) != 2:
            raise AssertionError("live-runtime guard not found in the config cell")
        return compile(ast.Module(body=nodes, type_ignores=[]), "<guard>", "exec")

    def test_reconfiguring_under_a_live_runtime_is_refused(self):
        guard = self._guard()

        class Live:
            _stopped = False

        with self.assertRaises(RuntimeError) as caught:
            exec(guard, {"RUNTIME": Live()})
        self.assertIn("live Runtime already exists", str(caught.exception))

        class Stopped:
            _stopped = True

        exec(guard, {"RUNTIME": Stopped()})     # a stopped runtime may be replaced
        exec(guard, {})                          # first run in a fresh kernel


if __name__ == "__main__":
    unittest.main()


class Teardown(unittest.TestCase):
    """Drive the generated Runtime.stop() itself against fakes.

    This is the part Appendix F warns cannot be proven by ast.parse: ordering, the drain
    deadline, and what teardown is allowed to release while resources are still running.
    """

    @classmethod
    def setUpClass(cls):
        cls.nb = load(V10)

    def _build(self):
        import fcntl as _fcntl
        import threading
        import types

        node = _method_tree(self.nb, "Runtime", "stop")
        module = ast.Module(
            body=[ast.ClassDef(name="Fake", bases=[], keywords=[], body=[node],
                               decorator_list=[], type_params=[])],
            type_ignores=[])
        ast.fix_missing_locations(module)
        # A clock that advances only when slept on: the drain loop then reaches its 600 s
        # deadline in a few hundred iterations instead of spinning or waiting for real.
        now = [0.0]

        def sleep(seconds):
            now[0] += seconds

        clock = types.SimpleNamespace(sleep=sleep, time=lambda: now[0])
        namespace = {"fcntl": _fcntl, "time": clock, "log": lambda message: None,
                     "RUN_ID": "a" * 32}
        exec(compile(module, "<stop>", "exec"), namespace)
        return namespace["Fake"](), threading

    class _Lease:
        def __init__(self, phase="withdrawn"):
            self.phase = phase

        def withdraw(self):
            if self.phase != "withdrawn":
                raise RuntimeError("withdrawal unconfirmed")
            return {"phase": "withdrawn"}

    class _Proxy:
        def __init__(self, active=0):
            self.active = active
            self.stopped = False

        def stop(self):
            self.stopped = True

    class _Worker:
        slot = 0

        def __init__(self):
            self.stopped = False

        def stop(self):
            self.stopped = True

    class _Ts:
        def __init__(self):
            self.deleted = []
            self.down_called = False

        def own_device_id(self):
            return "d-own"

        def delete_device(self, device_id):
            self.deleted.append(device_id)
            return True

        def serve_reset(self):
            pass

        def down(self):
            self.down_called = True

        def stop_daemon(self):
            pass

    def test_drain_deadline_leaves_resources_and_the_vm_lock_alone(self):
        runtime, threading = self._build()
        runtime._guard = threading.Lock()
        runtime._stopped = False
        runtime._done = threading.Event()
        runtime._stop = threading.Event()
        runtime.lease = self._Lease()
        runtime.proxy = self._Proxy(active=1)     # never drains
        worker = self._Worker()
        runtime.workers = [worker]
        runtime.ts = self._Ts()
        with tempfile.NamedTemporaryFile() as lock_handle:
            runtime._vm_lock = open(lock_handle.name, "a+")
            runtime.stop(reason="test")
            self.assertFalse(worker.stopped, "workers must survive an incomplete drain")
            self.assertFalse(runtime.proxy.stopped, "the proxy must survive too")
            self.assertFalse(runtime._vm_lock.closed,
                             "a kernel still holding GPUs must keep the VM lock")
            self.assertFalse(runtime._stopped, "a retryable stop must not look finished")

    def test_confirmed_withdrawal_and_empty_drain_stops_everything_in_order(self):
        runtime, threading = self._build()
        runtime._guard = threading.Lock()
        runtime._stopped = False
        runtime._done = threading.Event()
        runtime._stop = threading.Event()
        runtime.lease = self._Lease()
        runtime.proxy = self._Proxy(active=0)
        worker = self._Worker()
        runtime.workers = [worker]
        ts = self._Ts()
        runtime.ts = ts
        with tempfile.NamedTemporaryFile() as lock_handle:
            runtime._vm_lock = open(lock_handle.name, "a+")
            handle = runtime._vm_lock
            runtime.stop(reason="test")
            self.assertTrue(worker.stopped)
            self.assertTrue(runtime.proxy.stopped)
            self.assertEqual(ts.deleted, ["d-own"], "only its own device may be deleted")
            self.assertTrue(ts.down_called)
            self.assertTrue(handle.closed, "the VM lock is released once resources are gone")

    def test_unconfirmed_withdrawal_keeps_everything_running(self):
        runtime, threading = self._build()
        runtime._guard = threading.Lock()
        runtime._stopped = False
        runtime._done = threading.Event()
        runtime._stop = threading.Event()
        runtime.lease = self._Lease(phase="active")   # withdrawal will keep failing
        runtime.proxy = self._Proxy(active=0)
        worker = self._Worker()
        runtime.workers = [worker]
        runtime.ts = self._Ts()
        with tempfile.NamedTemporaryFile() as lock_handle:
            runtime._vm_lock = open(lock_handle.name, "a+")
            runtime.stop(reason="test")
            self.assertFalse(worker.stopped, "an unconfirmed withdrawal must not stop serving")
            self.assertFalse(runtime._stopped)
            self.assertFalse(runtime._vm_lock.closed)
