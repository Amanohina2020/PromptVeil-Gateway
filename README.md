# PromptVeil

PromptVeil is a provider-side traffic shaper for streamed LLM responses. A passive observer of an encrypted stream sees the number, lengths and send times of the TLS records, and these follow token generation closely enough to reveal properties of the prompt. PromptVeil reshapes all three before encryption. This repository contains the online TLS gateway used in our experiments.

## Overview

The gateway sits between an upstream service that streams server-sent events (SSE) and the client. It processes each response in three coupled stages:

- **Event grouping.** Incoming SSE events are merged into fewer units. Each group draws one of four modes: silence (`raw_tau`), flush, and mild or strong batching. The modes differ in their arrival-count limits and deadlines.
- **Randomized padding.** Each unit receives an SSE comment of fixed overhead plus a random filler bounded by a random cap. Clients ignore SSE comments, so the event stream itself is unchanged.
- **Ramped workload clock.** A unit leaves once the clock has served its padded size after the previous release, or when it is ready, whichever is later. The clock's service rate ramps up after the first release. Each response draws its own rate and ramp duration.

A unit released with the `gap_fill` action can also arm a few cover records, which are comment-only chunks. Each released unit is written as one HTTP chunk, which produces three TLS records: chunk header, data and CRLF.

## Requirements

- Python 3.8 or later (tested with Python 3.10)
- NumPy
- OpenSSL, only for creating a local test certificate

## Installation

```
python -m venv .venv
source .venv/bin/activate        # Windows: .venv\Scripts\activate
pip install -r requirements.txt
```

## Usage

Create a self-signed certificate for local testing:

```
openssl req -x509 -newkey rsa:2048 -nodes -days 30 -config examples/localhost.cnf -keyout key.pem -out cert.pem
```

Start the synthetic upstream service, then the gateway, each in its own terminal:

```
python examples/demo_upstream.py --port 8080
python -m promptveil_gateway --port 8443 --upstream-port 8080 --tls-cert cert.pem --tls-key key.pem
```

Request a stream through the gateway:

```
curl --cacert cert.pem -N "https://localhost:8443/stream?sid=demo1"
```

Run the smoke tests (the end-to-end test needs `openssl` on the PATH):

```
python -m unittest discover -s tests -v
```

## Configuration

Command-line options (`python -m promptveil_gateway --help`):

| Option | Meaning |
|---|---|
| `--host`, `--port` | Listening address and port (default `127.0.0.1:8443`) |
| `--upstream-host`, `--upstream-port` | Upstream SSE service (plain HTTP/1.1) |
| `--tls-cert`, `--tls-key` | Server certificate chain and private key (PEM). The gateway accepts TLS 1.3 only. |
| `--config` | Shaping parameters (default `config/promptveil.json`) |
| `--key` | Integer defense key. If you omit it, each response draws a fresh random key. A fixed key makes schedules reproducible and should only be used for testing. |
| `--nodelay` | TCP_NODELAY on the client connection (default 1) |
| `--log-dir` | Write one JSON timing log per response |
| `--tls-keylog` | Write TLS session secrets to a file (debugging only) |

`config/promptveil.json` holds the shaping parameters used in our experiments:

| Key | Meaning |
|---|---|
| `strong.egc` | Probabilities of the grouping modes `raw_tau` (silence), `random_flush`, `random_batch_mild`, `random_batch_strong` |
| `strong.plc` | Probabilities of the padding caps 8 to 128 bytes (`pad8` … `pad128`) |
| `plc.scale` | Multiplier of the padding cap |
| `strong.tsc` | Probabilities of release jitter of 0, up to 20 or up to 50 ms (`no_jitter`, `jitter20`, `jitter50`), and of `gap_fill` (no jitter, may arm cover records) |
| `tsc.pace` | Range of the per-response clock rate in bytes/ms, drawn log-uniformly |
| `mech.r0`, `mech.T` | Initial fraction of the clock rate, and range of the ramp duration in ms |
| `tsc.cover_cap` | Maximum cover records per response |
| `cover_tail_quantile` | Cover delays are drawn above this quantile of the gap distribution |

Probabilities must be listed in the order shown in the file. The grouping limits and deadlines (silence gap 1.04 ms; flush: 3 events, 5–25 ms; mild batching: 2–3 events, 20–50 ms; strong batching: 3–5 events, 40–80 ms) and the 11-byte comment overhead are constants at the top of `promptveil_gateway/scheduler.py`. The cover-record timing and size distributions are in `promptveil_gateway/cover_kernel.json`.

## Streaming interface

- **Client.** A client connects over TLS 1.3 and sends `GET /stream?sid=<id> HTTP/1.1`. The id names the response and enters its random streams.
- **Upstream request.** The gateway opens `GET /stream?sid=<id> HTTP/1.1` on the upstream service.
- **Upstream response.** The upstream answers with a chunked `text/event-stream` body. Every SSE block must end with a blank line, and the last event must be `data: [DONE]`. The upstream then closes the body with the terminal chunk.
- **Delivery.** The gateway forwards every upstream SSE block byte for byte and in order, interleaved with padding comments. It then sends the terminal chunk.

Release times need sub-millisecond timers, so the gateway busy-waits for the last 20 ms before each deadline. Each open stream therefore keeps one CPU core busy part of the time.

## Reference

PromptVeil: Resource-Efficient Multidimensional Traffic Shaping Against Prompt Inference in Encrypted LLM Streams.
