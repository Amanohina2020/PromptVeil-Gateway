"""Synthetic upstream SSE service for trying the gateway locally (plain HTTP/1.1 on 127.0.0.1).

GET /stream?sid=ID streams a synthetic response: `--events` SSE events at random gaps, then "data: [DONE]", each event
in its own HTTP chunk. The content depends only on the id, so a client can check what it receives.
usage: python examples/demo_upstream.py --port 8080 [--events 40]"""
import argparse
import random
import socket
import threading
import time

HEAD = b"HTTP/1.1 200 OK\r\nContent-Type: text/event-stream\r\nTransfer-Encoding: chunked\r\nConnection: close\r\n\r\n"


def synthetic_response(response_id, n_events):
    """Return (gap_ms, event bytes) pairs; the last event is [DONE]."""
    r = random.Random(response_id)
    events = []
    for i in range(n_events):
        text = "x" * r.randint(1, 40)
        events.append((r.uniform(1., 60.), b'data: {"sid":"%s","i":%d,"text":"%s"}\n\n' % (response_id.encode(), i, text.encode())))
    events.append((r.uniform(1., 60.), b"data: [DONE]\n\n"))
    return events


def chunk(data):
    return b"%x\r\n" % len(data) + data + b"\r\n"


def serve(conn, n_events):
    conn.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
    request = b""
    while b"\r\n\r\n" not in request:
        data = conn.recv(4096)
        if not data:
            conn.close()
            return
        request += data
    response_id = request.split(b" ", 2)[1].split(b"sid=", 1)[1].decode()
    conn.sendall(HEAD)
    for gap_ms, event in synthetic_response(response_id, n_events):
        time.sleep(gap_ms / 1000.)
        conn.sendall(chunk(event))
    conn.sendall(b"0\r\n\r\n")
    conn.close()


def main():
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    ap.add_argument("--port", type=int, default=8080)
    ap.add_argument("--events", type=int, default=40)
    args = ap.parse_args()
    server = socket.socket()
    server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    server.bind(("127.0.0.1", args.port))
    server.listen(16)
    print("upstream ready", args.port, flush=True)
    while True:
        conn, _ = server.accept()
        threading.Thread(target=serve, args=(conn, args.events), daemon=True).start()


if __name__ == "__main__":
    main()
