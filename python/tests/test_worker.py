from __future__ import annotations

import asyncio
import unittest

from tenet.worker import StatelessWorker


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


if __name__ == "__main__":
    unittest.main()
