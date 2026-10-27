"""Smoke tests on synthetic streams: scheduler invariants offline, and one end-to-end run through the TLS gateway.
run: python -m unittest discover -s tests -v"""
import random
import shutil
import socket
import ssl
import subprocess
import sys
import tempfile
import threading
import time
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))
sys.path.insert(0, str(ROOT / "examples"))

import demo_upstream  # noqa: E402
from promptveil_gateway.scheduler import COMMENT_OVERHEAD, load_config, load_cover_kernel, replay  # noqa: E402
from promptveil_gateway.sse import ChunkedSSEParser  # noqa: E402


def synthetic_trace(seed, n_events):
    r = random.Random(seed)
    times, t = [], 0.
    for _ in range(n_events + 1):
        t += r.choice([r.uniform(0., 2.), r.uniform(2., 30.), r.uniform(30., 300.)])
        times.append(t)
    sizes = [r.randint(20, 400) for _ in range(n_events)] + [14]
    return times, sizes


class SchedulerTest(unittest.TestCase):
    def setUp(self):
        self.config = load_config()
        self.kernel = load_cover_kernel()

    def test_invariants(self):
        for seed in range(300):
            times, sizes = synthetic_trace(seed, 1 + seed % 120)
            s = replay(self.config, self.kernel, f"r{seed}", seed, times, sizes)
            real = [o for o in s.outputs if not o["cover"]]
            covers = [o for o in s.outputs if o["cover"]]
            released = [e for o in real for e in o["events"]]
            # every event is released exactly once and in arrival order
            self.assertEqual([(e.t, e.n) for e in released], list(zip(times, sizes)))
            self.assertTrue(all(o["size"] == o["raw_size"] + COMMENT_OVERHEAD + o["payload"] for o in real))
            self.assertTrue(all(o["t"] >= o["ready"] - 1e-9 for o in real))
            self.assertTrue(all(b["t"] >= a["t"] for a, b in zip(s.outputs, s.outputs[1:])))
            self.assertLessEqual(len(covers), s.cover_cap)
            self.assertTrue(all(o["t"] <= real[-1]["t"] for o in covers))
            # clocked steps serve exactly the padded workload since the previous release (Eq. 5)
            for prev, unit in zip(real, real[1:]):
                served = s.workload(unit["t"] - s.t1) - s.workload(prev["t"] - s.t1)
                self.assertGreaterEqual(served, unit["size"] - 1e-6)
            self.assertEqual(s.t1, real[0]["t"])
            self.assertEqual(s.finished_at, real[-1]["t"])

    def test_same_key_same_schedule(self):
        times, sizes = synthetic_trace(7, 60)
        a = replay(self.config, self.kernel, "same", 1234, times, sizes)
        b = replay(self.config, self.kernel, "same", 1234, times, sizes)
        self.assertEqual([(o["t"], o["size"]) for o in a.outputs], [(o["t"], o["size"]) for o in b.outputs])


def free_port():
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


@unittest.skipIf(shutil.which("openssl") is None, "openssl is needed to create a temporary test certificate")
class EndToEndTest(unittest.TestCase):
    def test_stream_is_preserved(self):
        tmp = Path(tempfile.mkdtemp())
        subprocess.run(["openssl", "req", "-x509", "-newkey", "rsa:2048", "-nodes", "-days", "1",
                        "-config", str(ROOT / "examples" / "localhost.cnf"),
                        "-keyout", str(tmp / "key.pem"), "-out", str(tmp / "cert.pem")], check=True, capture_output=True)
        up_port, gw_port = free_port(), free_port()
        upstream = socket.socket()
        upstream.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        upstream.bind(("127.0.0.1", up_port))
        upstream.listen(4)

        def accept_loop():
            while True:
                conn, _ = upstream.accept()
                threading.Thread(target=demo_upstream.serve, args=(conn, 25), daemon=True).start()

        threading.Thread(target=accept_loop, daemon=True).start()
        gateway = subprocess.Popen([sys.executable, "-m", "promptveil_gateway", "--port", str(gw_port),
                                    "--upstream-port", str(up_port), "--tls-cert", str(tmp / "cert.pem"),
                                    "--tls-key", str(tmp / "key.pem"), "--log-dir", str(tmp / "logs")],
                                   cwd=ROOT, stdout=subprocess.PIPE, text=True)
        try:
            self.assertIn("ready", gateway.stdout.readline())
            ctx = ssl.create_default_context(cafile=str(tmp / "cert.pem"))
            ctx.minimum_version = ssl.TLSVersion.TLSv1_3
            for response_id in ("alpha", "beta"):
                expected = [event for _, event in demo_upstream.synthetic_response(response_id, 25)]
                with ctx.wrap_socket(socket.create_connection(("127.0.0.1", gw_port)), server_hostname="localhost") as tls:
                    tls.sendall(b"GET /stream?sid=%s HTTP/1.1\r\nHost: localhost\r\n\r\n" % response_id.encode())
                    parser, events, comments = ChunkedSSEParser(), [], 0
                    while True:
                        try:
                            data = tls.recv(65536)
                        except (ssl.SSLEOFError, ConnectionResetError):
                            data = b""
                        if not data:
                            break
                        for block in parser.feed(data):
                            if block.startswith(b":"):
                                comments += 1
                            else:
                                events.append(block)
                self.assertTrue(parser.head.startswith(b"HTTP/1.1 200"))
                self.assertEqual(events, expected)      # exact bytes, order and [DONE]
                self.assertTrue(parser.done)            # terminal chunk
                self.assertGreater(comments, 0)         # padding comments are present and skipped
                self.assertTrue((tmp / "logs" / f"gw_{response_id}.json").exists())
        finally:
            gateway.kill()
            gateway.wait()
            gateway.stdout.close()
            time.sleep(.2)
            shutil.rmtree(tmp, ignore_errors=True)


if __name__ == "__main__":
    unittest.main()
