from __future__ import annotations

import unittest

try:
    import grpc  # type: ignore[import-not-found]
except ImportError:  # pragma: no cover - local bare-python test path.
    grpc = None


@unittest.skipIf(grpc is None, "grpcio is not installed")
class GrpcWorkerTests(unittest.TestCase):
    def test_health_check_round_trip(self) -> None:
        from tenet.grpc_worker import create_server
        from tenet.v1 import tenet_pb2, tenet_pb2_grpc
        from tenet.worker import StatelessWorker

        server = create_server(StatelessWorker(), max_workers=1)
        port = server.add_insecure_port("127.0.0.1:0")
        self.assertGreater(port, 0)
        server.start()
        self.addCleanup(lambda: server.stop(grace=0))

        with grpc.insecure_channel(f"127.0.0.1:{port}") as channel:
            response = tenet_pb2_grpc.TenetWorkerStub(channel).HealthCheck(tenet_pb2.HealthCheckRequest(), timeout=5)

        self.assertEqual(response.status, "SERVING")
        self.assertEqual(response.worker_count, 1)


if __name__ == "__main__":
    unittest.main()
