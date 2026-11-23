"""Contract tests for the generated v10 notebook.

These never execute a notebook cell: they inspect the generated JSON and its ASTs.
"""

import ast
import json
import re
import subprocess
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
        # start() must take the VM-wide lock before spawning workers.
        start = ast.unparse(names["start"])
        self.assertIn("flock", start)
        self.assertIn("LOCK_NB", start)

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


if __name__ == "__main__":
    unittest.main()


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
        self.assertTrue(self.ok("http://100.121.214.107:8769"))
        self.assertTrue(self.ok("http://100.64.0.1:8769/"))

    def test_rejects_shapes_that_could_capture_the_lease_token(self):
        for bad in ("", "http://100.121.214.107", "https://100.121.214.107:8769",
                    "http://plexus.example.invalid:8769", "http://8.8.8.8:8769",
                    "http://192.168.86.5:8769", "http://user:pw@100.121.214.107:8769",
                    "http://100.121.214.107:8769/v1", "http://100.121.214.107:8769?x=1",
                    "http://100.121.214.107:8769#frag"):
            self.assertFalse(self.ok(bad), bad)

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
