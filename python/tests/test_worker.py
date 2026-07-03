from __future__ import annotations

import asyncio
import unittest

from tenet.worker import StatelessWorker, validate_fencing_token


class StatelessWorkerTests(unittest.TestCase):
    def test_generate_thought_is_stateless(self) -> None:
        worker = StatelessWorker()
        first = asyncio.run(worker.generate_thought({"task_id": "task:1", "messages": [{"content": "hello"}]}))
        second = asyncio.run(worker.generate_thought({"task_id": "task:1", "messages": [{"content": "hello"}]}))
        self.assertEqual(first, second)
        self.assertTrue(first["is_final"])

    def test_health_check(self) -> None:
        worker = StatelessWorker()
        response = asyncio.run(worker.health_check())
        self.assertEqual(response["status"], "SERVING")

    def test_validate_fencing_token(self) -> None:
        request = {"session_id": "task:1", "fencing_token": 3}
        self.assertEqual(validate_fencing_token(request, lambda session_id: 3), "")
        self.assertIn("mismatch", validate_fencing_token(request, lambda session_id: 4))
        self.assertIn("missing", validate_fencing_token(request, lambda session_id: None))


if __name__ == "__main__":
    unittest.main()
