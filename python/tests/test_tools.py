from __future__ import annotations

import unittest
from pathlib import Path
from tempfile import TemporaryDirectory

from tenet.tools import ToolRegistry


class ToolRegistryTests(unittest.TestCase):
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
            result = registry.execute("shell", {"command": "pwd", "timeout": 2}, workspace)
            self.assertFalse(result.is_error)
            self.assertIn(str(workspace), result.stdout)


if __name__ == "__main__":
    unittest.main()
