"""Configuration boundary for opt-in research policies."""

import math


def finite_number(value: object) -> bool:
    try:
        return type(value) in (int, float) and math.isfinite(value)
    except OverflowError:
        return False


def scheduler_config(value: object) -> dict:
    if not isinstance(value, dict):
        raise ValueError("serving.scheduler must be an object")
    name = value.get("name")
    if name not in ("fcfs", "sjf", "mlfq", "atlas"):
        raise ValueError("serving.scheduler.name must be fcfs, sjf, mlfq, or atlas")
    feedback = name in ("mlfq", "atlas")
    allowed = {"name", "quanta_ms", "starvation_ratio"} if feedback else {"name"}
    if value.keys() - allowed:
        raise ValueError("serving.scheduler has unsupported fields")
    if not feedback:
        return {"name": name}
    quanta = value.get("quanta_ms", [100 * 2**i for i in range(8)])
    if not isinstance(quanta, list) or not 1 <= len(quanta) <= 64:
        raise ValueError("serving.scheduler.quanta_ms must have 1..64 entries")
    for i, quantum in enumerate(quanta):
        if (not finite_number(quantum)
                or quantum < .001 or quantum * 1000 >= 2**63
                or (i and quantum <= quanta[i - 1])):
            raise ValueError("serving.scheduler.quanta_ms must be positive, increasing microsecond-representable durations")
    if sum(quanta) * 1000 >= 2**63:
        raise ValueError("serving.scheduler cumulative quanta exceed the simulation clock")
    ratio = value.get("starvation_ratio", 10)
    if not finite_number(ratio) or ratio <= 0:
        raise ValueError("serving.scheduler.starvation_ratio must be finite and positive")
    return {"name": name, "quanta_ms": quanta, "starvation_ratio": ratio}


def router_parameters(name: object, value: object) -> dict:
    if name not in ("jsq", "least-token", "random", "agentix", "preble-e2"):
        raise ValueError("serving.router.research_policy must be jsq, least-token, random, agentix, or preble-e2")
    positive = {"history_window_s", "prefill_us_per_token", "decode_us_per_token"}
    nonnegative = {"latency_us", "cache_event_delay_us", "initial_output_tokens"}
    if not isinstance(value, dict) or value.keys() - positive - nonnegative:
        raise ValueError("serving.router.parameters must contain only research router parameters")
    for key, item in value.items():
        if not finite_number(item) or item < 0 or (key in positive and item == 0):
            raise ValueError(f"serving.router.parameters.{key} has an invalid value")
        if key in {"latency_us", "cache_event_delay_us"} and (type(item) is not int or item >= 2**63):
            raise ValueError(f"serving.router.parameters.{key} must fit a nonnegative signed 64-bit integer")
    required = {"prefill_us_per_token", "decode_us_per_token", "initial_output_tokens"}
    if name == "preble-e2" and required - value.keys():
        raise ValueError("serving.router.parameters requires explicit prefill/decode costs and initial_output_tokens for preble-e2")
    return dict(value)
