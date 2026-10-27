"""Keyed, namespace-separated random streams."""
import hashlib
import random


def stable_rng(seed, response_id, namespace, index=0):
    """Return the random stream of one independent decision of one response."""
    material = f"{int(seed)}\0{response_id}\0{namespace}\0{int(index)}".encode("utf-8")
    digest = hashlib.sha256(material).digest()
    return random.Random(int.from_bytes(digest[:8], "big", signed=False))


def weighted_choice(distribution, rng):
    total = sum(float(p) for p in distribution.values())
    if abs(total - 1.0) > 1e-8:
        raise ValueError(f"probabilities must sum to 1, got {total}")
    point = rng.random()
    cumulative = 0.0
    last = None
    for value, probability in distribution.items():
        last = value
        cumulative += float(probability)
        if point < cumulative:
            return value
    return last
