from __future__ import annotations

import json
import os
import subprocess
import unittest
from unittest.mock import patch
from pathlib import Path
from tempfile import TemporaryDirectory

from tenet.tools import ToolRegistry


class ToolRegistryTests(unittest.TestCase):
    def test_definitions_load_shared_manifest(self) -> None:
        registry = ToolRegistry()
        definitions = registry.definitions()
        names = {item["name"] for item in definitions}
        for name in {"apply_patch", "git_log", "git_show", "git_branch"}:
            self.assertIn(name, names)
        for item in definitions:
            self.assertTrue(item["parameters_schema"])

    def test_definitions_gate_web_search(self) -> None:
        with patch.dict(os.environ, {"TENET_WEB_SEARCH_ENABLED": "", "BRAVE_API_KEY": "", "TAVILY_API_KEY": "", "BING_API_KEY": ""}, clear=False):
            names = {item["name"] for item in ToolRegistry().definitions()}
            self.assertNotIn("web_search", names)
        with patch.dict(os.environ, {"BRAVE_API_KEY": "test-key"}, clear=False):
            names = {item["name"] for item in ToolRegistry().definitions()}
            self.assertIn("web_search", names)

    def test_read_and_write_file(self) -> None:
        with TemporaryDirectory() as tmp:
            workspace = Path(tmp)
            registry = ToolRegistry()
            result = registry.execute("write_file", {"path": "notes/a.txt", "content": "one\ntwo\nthree"}, workspace)
            self.assertFalse(result.is_error)

            result = registry.execute("read_file", {"path": "notes/a.txt", "offset": 2, "limit": 1}, workspace)
            self.assertEqual(result.stdout, "two")

    def test_path_escape_is_blocked(self) -> None:
        with TemporaryDirectory() as tmp:
            registry = ToolRegistry()
            result = registry.execute("read_file", {"path": "../outside.txt"}, Path(tmp))
            self.assertTrue(result.is_error)
            self.assertIn("escapes workspace", result.stderr)

    def test_shell_blocks_dangerous_command(self) -> None:
        with TemporaryDirectory() as tmp:
            registry = ToolRegistry()
            result = registry.execute("shell", {"command": "rm -rf /"}, Path(tmp))
            self.assertTrue(result.is_error)
            self.assertEqual(result.exit_code, 126)

    def test_shell_runs_inside_workspace(self) -> None:
        with TemporaryDirectory() as tmp:
            workspace = Path(tmp)
            registry = ToolRegistry()
            result = registry.execute("shell", {"command": "pwd", "timeout_seconds": 2}, workspace)
            self.assertFalse(result.is_error)
            self.assertIn(str(workspace), result.stdout)

    def test_tool_arguments_are_validated_against_manifest_schema(self) -> None:
        with TemporaryDirectory() as tmp:
            registry = ToolRegistry()
            workspace = Path(tmp)

            result = registry.execute("write_file", {"path": "notes.txt"}, workspace)
            self.assertTrue(result.is_error)
            self.assertEqual(result.exit_code, 2)
            self.assertIn("content is required", result.stderr)

            result = registry.execute("read_file", {"path": 123}, workspace)
            self.assertTrue(result.is_error)
            self.assertIn("path must be a string", result.stderr)

            result = registry.execute("web_search", {"query": "tenet", "limit": 99}, workspace)
            self.assertTrue(result.is_error)
            self.assertIn("limit must be <= 10", result.stderr)

    def test_structured_file_tools(self) -> None:
        with TemporaryDirectory() as tmp:
            workspace = Path(tmp)
            (workspace / "src").mkdir()
            (workspace / "src" / "main.py").write_text("print('hello')\n", encoding="utf-8")
            (workspace / "README.md").write_text("hello Tenet\n", encoding="utf-8")
            registry = ToolRegistry()

            result = registry.execute("append_file", {"path": "README.md", "content": "more\n"}, workspace)
            self.assertFalse(result.is_error)
            result = registry.execute("replace_in_file", {"path": "README.md", "old": "Tenet", "new": "Agent"}, workspace)
            self.assertFalse(result.is_error)
            self.assertIn("hello Agent", (workspace / "README.md").read_text(encoding="utf-8"))

            result = registry.execute("list_dir", {"recursive": True}, workspace)
            entries = json.loads(result.stdout)["entries"]
            self.assertTrue(any(entry["path"] == "src/main.py" for entry in entries))

            result = registry.execute("search_files", {"query": "main"}, workspace)
            self.assertIn("src/main.py", result.stdout)

            result = registry.execute("grep", {"query": "print"}, workspace)
            self.assertIn("print", result.stdout)

            result = registry.execute("file_info", {"path": "README.md"}, workspace)
            self.assertIn("README.md", result.stdout)

    def test_apply_patch(self) -> None:
        with TemporaryDirectory() as tmp:
            workspace = Path(tmp)
            (workspace / "README.md").write_text("hello Tenet\n", encoding="utf-8")
            registry = ToolRegistry()
            result = registry.execute(
                "apply_patch",
                {"patch": "--- README.md\n+++ README.md\n@@ -1 +1 @@\n-hello Tenet\n+hello Agent\n"},
                workspace,
            )
            self.assertFalse(result.is_error, result.stderr)
            self.assertEqual((workspace / "README.md").read_text(encoding="utf-8"), "hello Agent\n")

    def test_git_tools(self) -> None:
        with TemporaryDirectory() as tmp:
            workspace = Path(tmp)
            _run_git(workspace, "init")
            _run_git(workspace, "config", "user.email", "test@example.com")
            _run_git(workspace, "config", "user.name", "Test User")
            (workspace / "README.md").write_text("hello\n", encoding="utf-8")
            _run_git(workspace, "add", "README.md")
            _run_git(workspace, "commit", "-m", "initial commit")
            registry = ToolRegistry()

            result = registry.execute("git_log", {"limit": 1}, workspace)
            self.assertFalse(result.is_error, result.stderr)
            self.assertIn("initial commit", result.stdout)
            result = registry.execute("git_show", {"rev": "HEAD", "max_bytes": 20000}, workspace)
            self.assertFalse(result.is_error, result.stderr)
            self.assertIn("initial commit", result.stdout)
            result = registry.execute("git_branch", {}, workspace)
            self.assertFalse(result.is_error, result.stderr)
            self.assertTrue(result.stdout.strip())

    def test_http_fetch_blocks_localhost(self) -> None:
        with TemporaryDirectory() as tmp:
            registry = ToolRegistry()
            result = registry.execute("http_fetch", {"url": "http://127.0.0.1:12345"}, Path(tmp))
            self.assertTrue(result.is_error)
            self.assertTrue("private or local" in result.stderr or "localhost" in result.stderr)


def _run_git(workspace: Path, *args: str) -> None:
    subprocess.run(["git", *args], cwd=workspace, text=True, capture_output=True, check=True)


if __name__ == "__main__":
    unittest.main()
