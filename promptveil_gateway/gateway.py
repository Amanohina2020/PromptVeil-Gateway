"""PromptVeil TLS gateway (HTTPS with TLS 1.3, HTTP/1.1 chunked SSE).

For each client request the gateway opens the upstream SSE stream, delivers every SSE event to a PromptVeilScheduler
at its measured arrival time, advances the scheduler's timers when they fall due, and writes each released unit at
once as one HTTP chunk, i.e. three TLS records (chunk header, data, CRLF). A real unit carries its events followed by
an SSE comment that pads the chunk to the scheduler's padded size; a cover record is a comment-only chunk.
"""
import argparse
import json
import socket
import ssl
import sys
import threading
from pathlib import Path

from .scheduler import DEFAULT_CONFIG, Event, PromptVeilScheduler, load_config, load_cover_kernel
from .sse import RESPONSE_HEAD, TERMINAL_CHUNK, ChunkedSSEParser, chunk_parts
from .timing import enable_high_resolution_timers, now_ns, sleep_until, wait_readable_until

DONE_PREFIX = b"data: [DONE]"
IDLE_WAIT_NS = 500_000_000


def read_request_head(conn):
    request = b""
    while b"\r\n\r\n" not in request:
        data = conn.recv(4096)
        if not data:
            return None
        request += data
    return request


def response_id_of(request):
    """The response id is the value after 'sid=' in the request target, e.g. GET /stream?sid=abc."""
    target = request.split(b" ", 2)[1]
    if b"sid=" not in target:
        return None
    return target.split(b"sid=", 1)[1].decode()


def unit_bytes(output):
    """Chunk data of one released output: member events, then an SSE comment filling up to the padded size."""
    if output["cover"]:
        return b":" + b"c" * (output["size"] - 3) + b"\n\n"
    events = b"".join(e.payload for e in output["events"])
    return events + b":" + b"p" * (output["size"] - len(events) - 3) + b"\n\n"


def serve_stream(conn, args, config, kernel):
    conn.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, args.nodelay)
    request = read_request_head(conn)
    if request is None:
        return
    response_id = response_id_of(request)
    if response_id is None:
        conn.sendall(b"HTTP/1.1 400 Bad Request\r\nContent-Length: 0\r\nConnection: close\r\n\r\n")
        return

    upstream = socket.create_connection((args.upstream_host, args.upstream_port))
    try:
        upstream.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
        t0 = now_ns()
        upstream.sendall(b"GET /stream?sid=%s HTTP/1.1\r\nHost: %s\r\n\r\n"
                         % (response_id.encode(), args.upstream_host.encode()))

        def ms(ns):
            return (ns - t0) / 1e6

        h0 = now_ns()
        conn.sendall(RESPONSE_HEAD)
        h1 = now_ns()
        scheduler = PromptVeilScheduler(config, kernel, response_id, args.key)
        parser = ChunkedSSEParser()
        upstream.setblocking(False)
        arrivals, units = [], []
        late_timers = []
        state = dict(written=0, eos=False, upstream_closed=False, clock_clamps=0)

        def write_released():
            while state["written"] < len(scheduler.outputs):
                output = scheduler.outputs[state["written"]]
                state["written"] += 1
                data = unit_bytes(output)
                assert len(data) == output["size"]
                writes = []
                for part in chunk_parts(data):
                    t = now_ns()
                    conn.sendall(part)
                    writes.append([ms(t), ms(now_ns())])
                units.append(dict(planned=output["t"], ready=output["ready"], size=output["size"], raw=output["raw_size"],
                                  payload=output["payload"], cover=output["cover"],
                                  members=[e.index for e in output["events"]], writes=writes))

        def on_bytes(data, t_ms):
            for block in parser.feed(data):
                last = block.startswith(DONE_PREFIX)
                t = t_ms
                if t < scheduler.clock:       # bytes read late; never move the scheduler clock backwards
                    state["clock_clamps"] += 1
                    t = scheduler.clock
                event = Event(float(t), len(block), not last, payload=block, index=len(arrivals))
                arrivals.append([len(arrivals), t, len(block)])
                scheduler.advance(event.t, inclusive=False)
                scheduler.receive(event)
                scheduler.advance(event.t)
                write_released()
                if last:
                    scheduler.end()
                    write_released()
                    state["eos"] = True

        while True:
            due = scheduler.next_timer()
            if state["eos"] and due is None:
                break
            if due is not None and ms(now_ns()) >= due:
                late_timers.append(ms(now_ns()) - due)
                scheduler.advance(due)
                write_released()
                continue
            target = t0 + int(due * 1e6) if due is not None else now_ns() + IDLE_WAIT_NS
            if not state["eos"] and not state["upstream_closed"]:
                if wait_readable_until(upstream, target):
                    t_arrival = now_ns()
                    try:
                        data = upstream.recv(65536)
                    except BlockingIOError:
                        continue
                    if not data:
                        state["upstream_closed"] = True
                        continue
                    on_bytes(data, ms(t_arrival))
                    continue
            else:
                if due is None:
                    raise RuntimeError(f"upstream closed before the end of stream for {response_id}")
                sleep_until(target)
            if due is not None:
                scheduler.advance(due)
                write_released()

        t = now_ns()
        conn.sendall(TERMINAL_CHUNK)
        t_end = ms(now_ns())
    finally:
        upstream.close()

    if args.log_dir:
        log = dict(sid=response_id, nodelay=args.nodelay, T0_ns=t0, head_write=[ms(h0), ms(h1)],
                   terminal_write=[ms(t), t_end], arrivals=arrivals, units=units, finished_at=scheduler.finished_at,
                   scheduler_outputs=[[o["t"], o["size"], o["cover"]] for o in scheduler.outputs],
                   notes=dict(clock_clamps=state["clock_clamps"], late_timer_events=len(late_timers),
                              late_timer_max_ms=max(late_timers) if late_timers else 0.))
        (Path(args.log_dir) / f"gw_{response_id}.json").write_text(json.dumps(log))


def handle(conn, args, config, kernel):
    try:
        serve_stream(conn, args, config, kernel)
    except Exception as exc:
        print(f"stream failed: {exc!r}", file=sys.stderr, flush=True)
    finally:
        conn.close()


def parse_args(argv=None):
    ap = argparse.ArgumentParser(prog="python -m promptveil_gateway", description="PromptVeil TLS gateway")
    ap.add_argument("--host", default="127.0.0.1", help="listening address (default 127.0.0.1)")
    ap.add_argument("--port", type=int, default=8443, help="listening port (default 8443)")
    ap.add_argument("--upstream-host", default="127.0.0.1", help="upstream SSE service address (default 127.0.0.1)")
    ap.add_argument("--upstream-port", type=int, required=True, help="upstream SSE service port (plain HTTP/1.1)")
    ap.add_argument("--tls-cert", required=True, help="server certificate chain (PEM)")
    ap.add_argument("--tls-key", required=True, help="server private key (PEM)")
    ap.add_argument("--config", default=str(DEFAULT_CONFIG), help="shaping parameters (JSON)")
    ap.add_argument("--key", type=int, default=None,
                    help="integer defense key; omit for a fresh random key per response (use only for reproducible tests)")
    ap.add_argument("--nodelay", type=int, default=1, choices=(0, 1), help="TCP_NODELAY on the client socket (default 1)")
    ap.add_argument("--log-dir", default=None, help="write one timing log per response to this directory")
    ap.add_argument("--tls-keylog", default=None, help="write TLS session secrets to this file (debugging only)")
    return ap.parse_args(argv)


def main(argv=None):
    args = parse_args(argv)
    enable_high_resolution_timers()
    config = load_config(args.config)
    kernel = load_cover_kernel()
    if args.log_dir:
        Path(args.log_dir).mkdir(parents=True, exist_ok=True)
    ctx = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
    ctx.minimum_version = ssl.TLSVersion.TLSv1_3
    ctx.load_cert_chain(args.tls_cert, args.tls_key)
    if args.tls_keylog:
        ctx.keylog_filename = args.tls_keylog
    server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    server.bind((args.host, args.port))
    server.listen(16)
    print("gateway ready", args.host, args.port, flush=True)
    while True:
        client, _ = server.accept()
        try:
            tls = ctx.wrap_socket(client, server_side=True)
        except (ssl.SSLError, OSError):
            client.close()
            continue
        threading.Thread(target=handle, args=(tls, args, config, kernel), daemon=True).start()


if __name__ == "__main__":
    main()
