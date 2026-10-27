"""PromptVeil scheduler: event grouping, randomized padding, the ramped workload clock and cover records.

The caller drives the scheduler. Each arrived SSE event is delivered with receive(); advance() moves the clock
forward and fires the timers that fall due. Times are milliseconds since the upstream request. Every random draw
comes from a keyed stream of the response (key, response id, stream name, index), never from the content.
Released units and cover records are appended to `outputs` in release order.
"""
import heapq
import json
import math
import secrets
from dataclasses import dataclass
from pathlib import Path

import numpy as np

from .rng import stable_rng, weighted_choice

DEFAULT_CONFIG = Path(__file__).resolve().parent.parent / "config" / "promptveil.json"
COVER_KERNEL = Path(__file__).resolve().parent / "cover_kernel.json"

# Grouping modes. raw_tau is the silence mode: the group closes after a gap without arrivals.
GROUPING_MODES = ["raw_tau", "random_flush", "random_batch_mild", "random_batch_strong"]
SILENCE_GAP_MS = 1.04
FLUSH_COUNT = 3
FLUSH_DEADLINE_MS = (5., 25.)
FLUSH_CLOSE_PROBABILITY = .5
MILD_BATCH_COUNTS = (2, 3)
MILD_BATCH_DEADLINE_MS = (20., 50.)
STRONG_BATCH_COUNTS = (3, 4, 5)
STRONG_BATCH_DEADLINE_MS = (40., 80.)

# Padding: every real unit carries an SSE comment of COMMENT_OVERHEAD bytes plus a random filler.
PADDING_CAPS = ["pad8", "pad16", "pad32", "pad64", "pad128"]
COMMENT_OVERHEAD = 11

# Release jitter; gap_fill releases without jitter and lets the unit arm cover records.
JITTER_ACTIONS = ["no_jitter", "jitter20", "jitter50", "gap_fill"]
JITTER_MAX_MS = {"jitter20": 20., "jitter50": 50.}

CANONICAL_ORDER = {"egc": GROUPING_MODES, "plc": PADDING_CAPS, "tsc": JITTER_ACTIONS}


def load_config(path=DEFAULT_CONFIG):
    config = json.loads(Path(path).read_text(encoding="utf-8"))
    for head, order in CANONICAL_ORDER.items():
        if list(config["strong"][head]) != order:
            raise ValueError(f"strong.{head} must list {order} in this order")
    mech = config["mech"]
    if (mech.get("type"), mech.get("origin"), mech.get("rule")) != ("ramp", "first_release", "integral"):
        raise ValueError("only the integral ramp clock anchored at the first release is implemented")
    return config


def load_cover_kernel(path=COVER_KERNEL):
    return json.loads(Path(path).read_text(encoding="utf-8"))


def normalized(distribution):
    total = sum(distribution.values())
    return {action: p / total for action, p in distribution.items()}


@dataclass(frozen=True)
class Event:
    t: float      # arrival time (ms)
    n: int        # serialized size of the SSE event (bytes)
    real: bool = True   # False for the terminal "data: [DONE]" event
    payload: bytes = b""   # forwarded bytes and arrival index; the scheduler reads only t, n and real
    index: int = -1


class PromptVeilScheduler:
    def __init__(self, config, kernel, response_id, key=None):
        self.kernel = kernel
        self.response_id = response_id
        # Without a key every response gets a fresh random key.
        self.seed = secrets.randbits(128) if key is None else int(key)
        self.laws = {head: normalized(config["strong"][head]) for head in CANONICAL_ORDER}
        self.pad_scale = float(config["plc"]["scale"])
        self.cover_cap = int(config["tsc"]["cover_cap"])
        self.cover_tail_quantile = float(config["cover_tail_quantile"])

        # Per-response clock draws: base rate r (bytes/ms, log-uniform) and ramp duration T (ms).
        # Stream names are kept from the verified implementation so that a key reproduces its schedules.
        lo, hi = config["tsc"]["pace"]
        self.rate = math.exp(self.rng("v2.tsc.pace", 0).uniform(math.log(lo), math.log(hi)))
        self.f0 = float(config["mech"]["r0"])
        lo, hi = config["mech"]["T"]
        self.ramp_T = self.rng("v3.ramp.T", 0).uniform(float(lo), float(hi))

        self.clock = 0.
        self.pending = []
        self.mode = None
        self.limit = None
        self.flush_rng = None
        self.group_timer = None
        self.ordinal = 0
        self.prev_arrival = None
        self.ewma = None
        self.ready_heap = []
        self.last_release = 0.
        self.t1 = None
        self.cover_timer = None
        self.cover_origin = None
        self.cover_count = 0
        self.prev_size = None
        self.outputs = []
        self.eos = False
        self.finished_at = None

    def rng(self, namespace, index=None):
        return stable_rng(self.seed, self.response_id, namespace, self.ordinal if index is None else index)

    def rate_bucket(self):
        if self.ewma is None:
            return "unknown"
        fast, medium = self.kernel["rate_cutoffs_ms"]
        return "fast" if self.ewma <= fast else "medium" if self.ewma <= medium else "slow"

    def size_bucket(self, n):
        return int(np.searchsorted(self.kernel["size_cutoffs_bytes"], n, side="right"))

    # ---------- ramped workload clock ----------
    def workload(self, u):
        """Work the clock can serve during [t1, t1 + u]: Psi(u) = r * integral of phi over [0, u]."""
        r, f0, T = self.rate, self.f0, self.ramp_T
        if u <= 0:
            return r * f0 * u
        if u <= T:
            return r * (f0 * u + (1 - f0) * u * u / (2 * T))
        return r * (u - (1 - f0) * T / 2)

    def workload_inverse(self, w):
        r, f0, T = self.rate, self.f0, self.ramp_T
        if w <= 0:
            return w / (r * f0)
        if w <= r * T * (1 + f0) / 2:
            a = (1 - f0) / (2 * T)
            return (-f0 + (f0 * f0 + 4 * a * w / r) ** .5) / (2 * a)
        return w / r + (1 - f0) * T / 2

    # ---------- event grouping ----------
    def receive(self, event):
        """Deliver one arrived event. The caller must first advance the clock to event.t."""
        if self.eos:
            raise ValueError("event after EOS")
        if abs(event.t - self.clock) > 1e-7:
            raise ValueError("advance the clock to the event time before delivering the event")
        if event.real:
            if self.prev_arrival is not None:
                gap = max(0., event.t - self.prev_arrival)
                self.ewma = gap if self.ewma is None else .25 * gap + .75 * self.ewma
            self.prev_arrival = event.t
        if not self.pending:
            self.open_group(event.t)
        self.pending.append(event)
        if self.mode == "raw_tau":
            self.group_timer = event.t + SILENCE_GAP_MS
        elif len(self.pending) >= self.limit:
            self.close_group()
        elif self.mode == "random_flush" and self.flush_rng.random() < FLUSH_CLOSE_PROBABILITY:
            self.close_group()

    def open_group(self, t):
        self.mode = weighted_choice(self.laws["egc"], self.rng("policy.egc"))
        if self.mode == "raw_tau":
            self.limit = None
            self.group_timer = t + SILENCE_GAP_MS
        elif self.mode == "random_flush":
            # One stream draws the deadline and then one Bernoulli trial per arrival.
            self.flush_rng = self.rng("scheduler.random_flush")
            self.limit = FLUSH_COUNT
            self.group_timer = t + self.flush_rng.uniform(*FLUSH_DEADLINE_MS)
        else:
            r = self.rng("scheduler.parameters")
            if self.mode == "random_batch_mild":
                self.limit = r.choice(MILD_BATCH_COUNTS)
                self.group_timer = t + r.uniform(*MILD_BATCH_DEADLINE_MS)
            else:
                self.limit = r.choice(STRONG_BATCH_COUNTS)
                self.group_timer = t + r.uniform(*STRONG_BATCH_DEADLINE_MS)

    # ---------- padding and release time ----------
    def close_group(self):
        self.group_timer = None
        self.cover_timer = None    # any closing cancels armed cover records
        self.cover_origin = None
        n = sum(e.n for e in self.pending)
        cap = int(weighted_choice(self.laws["plc"], self.rng("policy.plc"))[3:])
        filler = self.rng("plc.payload").randint(0, int(math.floor(self.pad_scale * cap)))
        padded = n + COMMENT_OVERHEAD + filler
        action = weighted_choice(self.laws["tsc"], self.rng("policy.tsc"))
        jitter = self.rng("tsc.jitter").uniform(0., JITTER_MAX_MS[action]) if action in JITTER_MAX_MS else 0.
        ready = self.clock + jitter
        if self.t1 is None:
            release = max(self.last_release, ready)     # t_1 = c_1 + j_1
            self.t1 = release
        else:
            # Slot: the clock serves exactly the padded workload after the previous release (Eq. 5).
            slot = self.t1 + self.workload_inverse(self.workload(self.last_release - self.t1) + padded)
            release = max(ready, slot)
        self.last_release = release
        unit = dict(at=release, ready=self.clock, n=n, padded=padded, filler=filler, events=self.pending,
                    ordinal=self.ordinal, can_cover=(action == "gap_fill"))
        heapq.heappush(self.ready_heap, (release, self.ordinal, unit))
        self.pending = []
        self.ordinal += 1

    def release_group(self, unit):
        real_count = sum(e.real for e in unit["events"])
        self.outputs.append(dict(t=self.clock, size=unit["padded"], raw_size=unit["n"], payload=unit["filler"],
                                 cover=False, real_count=real_count, ordinal=unit["ordinal"], ready=unit["ready"],
                                 events=unit["events"]))
        self.cover_timer = None
        self.cover_origin = None
        if real_count:
            self.prev_size = unit["padded"]
            # A later unit may already be queued: never arm cover behind it.
            if unit["can_cover"] and not self.ready_heap and not self.eos:
                self.cover_origin = self.clock
                self.arm_cover()
        self.maybe_finished()

    # ---------- cover records ----------
    def arm_cover(self):
        """Arm the next cover record at a delay from the upper tail of the empirical inter-record gap distribution."""
        if self.eos or self.cover_count >= self.cover_cap or self.prev_size is None:
            return
        quantiles = self.kernel["gap_quantiles"]
        q = np.asarray(quantiles.get(f"{self.rate_bucket()}:{self.size_bucket(self.prev_size)}", quantiles["global"]), float)
        elapsed = self.clock - self.cover_origin
        if elapsed >= q[-1]:
            return
        grid = np.linspace(0, 1, len(q))
        lower = max(float(np.interp(elapsed, q, grid)), self.cover_tail_quantile)
        if lower >= 1:
            return
        u = self.rng("vnext.cover.timer", self.cover_count + 100 * self.ordinal).uniform(lower, 1.)
        target = float(np.interp(u, grid, q))
        if target <= elapsed:
            return
        self.cover_timer = self.cover_origin + target

    def emit_cover(self):
        self.cover_timer = None
        if self.eos or self.cover_count >= self.cover_cap:
            return
        r = self.rng("vnext.cover.size", self.cover_count + 100 * self.ordinal)
        row = self.kernel["size_kernel"][str(self.size_bucket(self.prev_size))]
        point = r.random()
        b = min(int(np.searchsorted(np.cumsum(row["probabilities"]), point, side="right")), len(row["values"]) - 1)
        size = int(r.choice(row["values"][b]))
        assert size >= COMMENT_OVERHEAD
        self.outputs.append(dict(t=self.clock, size=size, raw_size=0, payload=0, cover=True, real_count=0,
                                 ordinal=-1, ready=self.clock, events=[]))
        self.cover_count += 1
        self.arm_cover()

    # ---------- clock and stream lifecycle ----------
    def next_timer(self):
        due = [t for t in (self.group_timer, self.cover_timer) if t is not None]
        if self.ready_heap:
            due.append(self.ready_heap[0][0])
        return min(due) if due else None

    def advance(self, to, inclusive=True):
        """Move the clock to `to`, firing due timers in time order (on ties: closing, release, cover).
        With inclusive=False, timers due exactly at `to` wait until the event arriving at `to` is delivered."""
        if to < self.clock - 1e-8:
            raise ValueError("clock reversal")
        while True:
            due = []
            if self.group_timer is not None:
                due.append((self.group_timer, 0))
            if self.ready_heap:
                due.append((self.ready_heap[0][0], 1))
            if self.cover_timer is not None:
                due.append((self.cover_timer, 2))
            if not due:
                break
            t, kind = min(due)
            if t > to or (not inclusive and t >= to):
                break
            self.clock = t
            if kind == 0:
                self.close_group()
            elif kind == 1:
                self.release_group(heapq.heappop(self.ready_heap)[2])
            else:
                self.emit_cover()
        self.clock = float(to)
        self.maybe_finished()

    def end(self):
        """The end of the stream has arrived. Silence and flush groups close now; batching keeps its deadline."""
        self.eos = True
        self.cover_timer = None
        self.cover_origin = None
        if self.pending and self.mode in ("raw_tau", "random_flush"):
            self.close_group()
        self.maybe_finished()

    def maybe_finished(self):
        if self.eos and not self.pending and not self.ready_heap and self.finished_at is None:
            self.finished_at = self.clock

    def drain(self):
        """Offline use: fire all remaining timers after end()."""
        while self.next_timer() is not None:
            self.advance(self.next_timer())


def replay(config, kernel, response_id, key, times, sizes):
    """Run the scheduler offline on recorded arrival times and sizes; the last event is the [DONE] event."""
    s = PromptVeilScheduler(config, kernel, response_id, key)
    for i, (t, n) in enumerate(zip(times, sizes)):
        event = Event(float(t), int(n), i < len(times) - 1)
        s.advance(event.t, inclusive=False)
        s.receive(event)
        s.advance(event.t)
    s.end()
    s.drain()
    return s
