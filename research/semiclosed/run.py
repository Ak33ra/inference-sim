"""Run a semi-closed research experiment from a strict JSON configuration."""

from __future__ import annotations

import argparse
from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
import hashlib
import importlib.util
import json
import math
import os
from pathlib import Path
import shlex
import shutil
import subprocess
import sys

if __package__:
    from .mix import parse_sources
else:
    from mix import parse_sources

REPOSITORY = Path(__file__).resolve().parents[2]
COEFFICIENTS = [f"cost-model-{name}" for name in (
    "primitives", "collectives", "host-overheads", "attention", "recurrent", "memory")]
WEEKS = tuple(
    f"{date(2026, 5, 17) + timedelta(days=7 * i)}_"
    f"{date(2026, 5, 23) + timedelta(days=7 * i)}" for i in range(16)
)
WEEK_FILES = ("manifest.json", "processed/tables.json", "processed/sessions.parquet",
              "processed/requests.parquet", "processed/tool_calls.parquet")
ENGINE_FIELDS = {
    "quantization": str, "cache_dtype": str, "block_size": int,
    "max_num_batched_tokens": int, "max_num_seqs": int, "max_model_len": int,
    "cudagraph_mode": str, "max_cudagraph_capture_size": int,
    "cudagraph_capture_sizes": list, "gpu_memory_utilization": float,
    "enable_prefix_caching": bool,
    "engram_config": dict, "speculative": dict,
}


def object_fields(value: object, allowed: set[str], required: set[str], path: str) -> dict:
    if not isinstance(value, dict):
        raise ValueError(f"{path} must be an object")
    unknown, missing = value.keys() - allowed, required - value.keys()
    if unknown:
        raise ValueError(f"{path}: unsupported field(s): {', '.join(sorted(unknown))}")
    if missing:
        raise ValueError(f"{path}: missing field(s): {', '.join(sorted(missing))}")
    return value


def typed(value: object, kind: type, path: str):
    valid = type(value) is kind
    if kind is float:
        try:
            valid = type(value) in (int, float) and math.isfinite(value)
        except OverflowError:
            valid = False
    if not valid or (kind is str and not value.strip()):
        raise ValueError(f"{path} must be a nonempty string" if kind is str
                         else f"{path} must be {kind.__name__}")
    return value


def positive(value: object, path: str) -> int:
    value = typed(value, int, path)
    if value < 1:
        raise ValueError(f"{path} must be positive")
    return value


def unique_object(pairs: list[tuple[str, object]]) -> dict:
    value = {}
    for key, item in pairs:
        if key in value:
            raise ValueError(f"duplicate JSON key: {key}")
        value[key] = item
    return value


def reject_constant(value: str):
    raise ValueError(f"nonfinite JSON number: {value}")


def path_from(value: object, base: Path, field: str) -> Path:
    path = Path(typed(value, str, field)).expanduser()
    return Path(os.path.abspath(base / path))


@dataclass(frozen=True)
class Layout:
    hardware: str
    fabric: str | None
    replicas: int
    gpus_per_node: int
    tp: int
    enable_expert_parallel: bool

    @property
    def nodes(self) -> int:
        if self.tp <= self.gpus_per_node:
            per_node = self.gpus_per_node // self.tp
            return (self.replicas + per_node - 1) // per_node
        return self.replicas * (self.tp // self.gpus_per_node)

    @classmethod
    def parse(cls, value: object) -> Layout:
        field = "serving.cluster"
        value = object_fields(value, {"hardware", "fabric", "replicas", "gpus_per_node",
                                      "tp", "pp", "dp", "enable_expert_parallel"},
                              {"hardware", "replicas", "gpus_per_node", "tp"}, field)
        for name in ("pp", "dp"):
            if positive(value.get(name, 1), f"{field}.{name}") != 1:
                raise ValueError(f"{field}.{name}: config runs support only colocated PP1/DP1")
        layout = cls(
            typed(value["hardware"], str, f"{field}.hardware"),
            typed(value["fabric"], str, f"{field}.fabric") if "fabric" in value else None,
            positive(value["replicas"], f"{field}.replicas"),
            positive(value["gpus_per_node"], f"{field}.gpus_per_node"),
            positive(value["tp"], f"{field}.tp"),
            typed(value.get("enable_expert_parallel", False), bool,
                  f"{field}.enable_expert_parallel"))
        if max(layout.tp, layout.gpus_per_node) % min(layout.tp, layout.gpus_per_node):
            raise ValueError(f"{field}: TP groups must pack evenly within or across nodes")
        if layout.nodes > 1 and layout.fabric is None:
            raise ValueError(f"{field}.fabric is required for a multi-node layout")
        return layout


def resolve_weeks(value: object, source: Path) -> tuple[list[int], list[int]]:
    if value == "all":
        selected = [i for i, name in enumerate(WEEKS, 1)
                    if all((source / name / part).is_file() for part in WEEK_FILES)]
        if not selected:
            raise ValueError(f"trace.weeks: no complete release weeks in {source}")
        return selected, [i for i in range(1, 17) if i not in selected]
    if type(value) is int:
        value = [value]
    if not isinstance(value, list) or not value:
        raise ValueError('trace.weeks must be an integer, a nonempty integer list, or "all"')
    for ordinal in value:
        if type(ordinal) is not int or not 1 <= ordinal <= len(WEEKS):
            raise ValueError("trace.weeks: each ordinal must be an integer from 1 through 16")
    if len(set(value)) != len(value):
        raise ValueError("trace.weeks: duplicate week ordinal")
    for ordinal in value:
        missing = [part for part in WEEK_FILES if not (source / WEEKS[ordinal - 1] / part).is_file()]
        if missing:
            raise ValueError(f"trace.weeks: week {ordinal} ({WEEKS[ordinal - 1]}) is incomplete: "
                             + ", ".join(missing))
    return sorted(value), []


def parse_engine(value: object, acceptance: object) -> tuple[dict, dict | None]:
    field = "serving.engine"
    engine = object_fields(value, set(ENGINE_FIELDS), set(), field)
    for name, item in engine.items():
        typed(item, ENGINE_FIELDS[name], f"{field}.{name}")
        if ENGINE_FIELDS[name] is int:
            positive(item, f"{field}.{name}")
    if "gpu_memory_utilization" in engine and not 0 < engine["gpu_memory_utilization"] <= 1:
        raise ValueError(f"{field}.gpu_memory_utilization must be in (0, 1]")
    if "cudagraph_capture_sizes" in engine:
        sizes = engine["cudagraph_capture_sizes"]
        if not sizes:
            raise ValueError(f"{field}.cudagraph_capture_sizes must not be empty")
        for size in sizes:
            positive(size, f"{field}.cudagraph_capture_sizes")
        if len(set(sizes)) != len(sizes):
            raise ValueError(f"{field}.cudagraph_capture_sizes must be distinct")
    if "engram_config" in engine:
        engram = object_fields(engine["engram_config"], {"cpu_offload"}, {"cpu_offload"},
                               f"{field}.engram_config")
        typed(engram["cpu_offload"], bool, f"{field}.engram_config.cpu_offload")
    if "speculative" in engine:
        spec = object_fields(engine["speculative"], {"method", "num_spec_tokens"},
                             {"method", "num_spec_tokens"}, f"{field}.speculative")
        if typed(spec["method"], str, f"{field}.speculative.method") not in ("dspark", "mtp"):
            raise ValueError(f"{field}.speculative.method must be dspark or mtp")
        positive(spec["num_spec_tokens"], f"{field}.speculative.num_spec_tokens")
        acceptance = object_fields(acceptance, {"category", "thinking"},
                                   {"category", "thinking"}, "serving.acceptance")
        typed(acceptance["category"], str, "serving.acceptance.category")
        if typed(acceptance["thinking"], str, "serving.acceptance.thinking") not in ("on", "off"):
            raise ValueError("serving.acceptance.thinking must be on or off")
    elif acceptance is not None:
        raise ValueError("serving.acceptance requires serving.engine.speculative")
    return engine, acceptance


def python_command(explicit: str | None, *, needs_arrow: bool = True) -> tuple[str, ...]:
    if explicit:
        executable = shutil.which(explicit)
        if executable is None:
            raise ValueError(f"Python executable not found: {explicit}")
        return (executable, "-B")
    if needs_arrow and importlib.util.find_spec("pyarrow") is None and (uv := shutil.which("uv")):
        return (uv, "run", "--no-project", "--python", ">=3.11", "--with-requirements",
                str(REPOSITORY / "research/freeinference_trace/requirements.txt"), "python", "-B")
    return (sys.executable, "-B")


def parse_arrivals(value: object, seed: int) -> dict:
    value = object_fields(value, {"mode", "interval_s", "rate_per_s", "seed"},
                          {"mode"}, "run.arrivals")
    mode = typed(value["mode"], str, "run.arrivals.mode")
    if mode not in ("trace", "fixed", "poisson"):
        raise ValueError("run.arrivals.mode must be trace, fixed, or poisson")
    allowed = {"mode", "seed"} | ({"interval_s"} if mode == "fixed" else
                                  {"rate_per_s"} if mode == "poisson" else set())
    object_fields(value, allowed, {"rate_per_s"} if mode == "poisson" else set(),
                  "run.arrivals")
    result = {"mode": mode, "seed": typed(value.get("seed", seed), int, "run.arrivals.seed")}
    if not -(2**63) <= result["seed"] < 2**63:
        raise ValueError("run.arrivals.seed must fit a signed 64-bit integer")
    if mode in ("fixed", "poisson"):
        field = "interval_s" if mode == "fixed" else "rate_per_s"
        number = typed(value.get(field, 0), float, f"run.arrivals.{field}")
        if number < 0 or (mode == "poisson" and number == 0):
            raise ValueError(f"run.arrivals.{field} must be {'positive' if mode == 'poisson' else 'nonnegative'}")
        result[field] = number
    return result


def json_text(value: object) -> str:
    return json.dumps(value, indent=2, sort_keys=True, allow_nan=False) + "\n"


@dataclass(frozen=True)
class RunPlan:
    output: Path
    resolved: dict
    original: bytes
    scenario: str
    commands: tuple[tuple[str, ...], ...]
    router_config: str | None = None


def prepare(config: Path, *, output: Path | None = None, python: str | None = None,
            caller: Path | None = None) -> RunPlan:
    caller = caller or Path.cwd()
    config = Path(os.path.abspath(caller / config))
    original = config.read_bytes()
    raw = object_fields(json.loads(original, object_pairs_hook=unique_object,
                                   parse_constant=reject_constant),
                        {"version", "trace", "serving", "output", "run", "catalog",
                         "registry", "defaults", "python"}, {"version", "trace", "serving"}, "config")
    if typed(raw["version"], int, "version") != 1:
        raise ValueError("version must be 1")
    trace = object_fields(raw["trace"], {"format", "path", "sources", "source", "model", "weeks", "start_ms", "end_ms"}, set(), "trace")
    trace_format = typed(trace.get("format", "freeinference"), str, "trace.format")
    selected, unavailable = [], []
    sources = ()
    if trace_format == "weka-dag":
        object_fields(trace, {"format", "path", "sources"}, set(), "trace")
        if ("path" in trace) == ("sources" in trace):
            raise ValueError("weka-dag trace requires exactly one of path or sources")
        if "sources" in trace:
            sources = parse_sources(trace["sources"], config.parent)
        else:
            source = path_from(trace["path"], config.parent, "trace.path")
            if not source.is_file():
                raise ValueError(f"trace.path is not a file: {source}")
    elif trace_format == "freeinference":
        object_fields(trace, {"format", "source", "model", "weeks", "start_ms", "end_ms"}, {"model", "weeks"}, "trace")
        source = path_from(trace.get("source", str(REPOSITORY / "traces/freeinference_agentic_trace/release")),
                           config.parent, "trace.source")
        model = typed(trace["model"], str, "trace.model")
        selected, unavailable = resolve_weeks(trace["weeks"], source)
        for name in ("start_ms", "end_ms"):
            if name in trace:
                typed(trace[name], int, f"trace.{name}")
        if "start_ms" in trace and "end_ms" in trace and trace["start_ms"] >= trace["end_ms"]:
            raise ValueError("trace.start_ms must precede trace.end_ms")
    else:
        raise ValueError("trace.format must be freeinference or weka-dag")
    serving = object_fields(raw["serving"], {"model", "cluster", "engine", "acceptance", "router",
                                             "engine_version", "coefficients"},
                            {"model", "cluster", "engine"}, "serving")
    serving_model = typed(serving["model"], str, "serving.model")
    version = typed(serving.get("engine_version", "0.31.0"), str, "serving.engine_version")
    coefficients = typed(serving.get("coefficients", COEFFICIENTS), list, "serving.coefficients")
    if not coefficients:
        raise ValueError("serving.coefficients must not be empty")
    for item in coefficients:
        typed(item, str, "serving.coefficients")
    if len(set(coefficients)) != len(coefficients):
        raise ValueError("serving.coefficients must be distinct")
    layout = Layout.parse(serving["cluster"])
    engine, acceptance = parse_engine(serving["engine"], serving.get("acceptance"))
    router_config, router_source = None, None
    if "router" in serving:
        router = object_fields(serving["router"], {"endpoint_picker_config"},
                               {"endpoint_picker_config"}, "serving.router")
        router_source = path_from(router["endpoint_picker_config"], config.parent,
                                  "serving.router.endpoint_picker_config")
        if not router_source.is_file():
            raise ValueError(f"serving.router.endpoint_picker_config is not a file: {router_source}")
        router_config = router_source.read_text()
        if not router_config.strip():
            raise ValueError("serving.router.endpoint_picker_config must not be empty")
    run = object_fields(raw.get("run", {}), {"seed", "join_policy", "series_interval_s", "arrivals"}, set(), "run")
    seed = typed(run.get("seed", 42), int, "run.seed")
    if not -(2**63) <= seed < 2**63:
        raise ValueError("run.seed must fit a signed 64-bit integer")
    join = typed(run.get("join_policy", "explicit" if trace_format == "weka-dag" else "timing-inferred"), str, "run.join_policy")
    if join not in (("explicit",) if trace_format == "weka-dag" else ("timing-inferred", "none")):
        raise ValueError("run.join_policy must be explicit for weka-dag, or timing-inferred/none for freeinference")
    arrivals = parse_arrivals(run.get("arrivals", {"mode": "trace"}), seed)
    if trace_format == "freeinference" and arrivals["mode"] != "trace":
        raise ValueError("config-generated arrivals currently require trace.format=weka-dag; FreeInference keeps recorded arrivals")
    series = typed(run.get("series_interval_s", 0), float, "run.series_interval_s")
    if series < 0:
        raise ValueError("run.series_interval_s must be nonnegative")
    paths = {}
    for field, env in (("catalog", "BLIS_CATALOG"), ("registry", "BLIS_REGISTRY")):
        value = raw.get(field, os.environ.get(env))
        if value is None:
            raise ValueError(f"set {field} in the config or {env} in the environment")
        paths[field] = path_from(value, config.parent if field in raw else caller, field)
        if not paths[field].is_dir():
            raise ValueError(f"{field} is not a directory: {paths[field]}")
    paths["defaults"] = path_from(raw.get("defaults", str(REPOSITORY / "defaults.yaml")),
                                  config.parent, "defaults")
    if not paths["defaults"].is_file():
        raise ValueError(f"defaults is not a file: {paths['defaults']}")
    if output is not None:
        output = path_from(str(output), caller, "--output")
    elif "output" in raw:
        output = path_from(raw["output"], config.parent, "output")
    else:
        stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
        output = REPOSITORY / "research/outputs/semiclosed" / f"{stamp}-{os.getpid()}"
    if os.path.lexists(output):
        raise ValueError(f"output already exists: {output}")
    interpreter = python if python is not None else raw.get("python", os.environ.get("PYTHON"))
    if interpreter is not None:
        typed(interpreter, str, "python")
        if "/" in interpreter:
            base = config.parent if python is None and "python" in raw else caller
            interpreter = str(path_from(interpreter, base, "python"))
    python_argv = python_command(interpreter, needs_arrow=trace_format == "freeinference")
    cluster = {"hardware": layout.hardware, "nodes": layout.nodes,
               "gpus_per_node": layout.gpus_per_node}
    if layout.fabric:
        cluster["fabric"] = layout.fabric
    scenario = {"kind": "Scenario", "name": "semiclosed-config", "engine_version": version,
                "model": serving_model, "coefficients": coefficients, "cluster": cluster}
    deployment = {"kind": "Deployment", "name": "semiclosed-config", "pools": [{
        "role": "colocated", "nodes": layout.nodes,
        "parallel": {"tp": layout.tp, "pp": 1, "dp": 1,
                     "enable_expert_parallel": layout.enable_expert_parallel}, "engine": engine}]}
    if router_config is not None:
        deployment["router"] = {"endpoint_picker_config": "router.yaml"}
    scenario_text = json_text(scenario) + "---\n" + json_text(deployment)
    input_path = output / "input"
    if sources:
        input_path = output / "input.jsonl"
        conversion = [*python_argv, "-m", "research.semiclosed.mix", "build",
                      "--config", str(output / "config.resolved.json")]
        resolved_trace = {"format": trace_format, "sources": [source.resolved() for source in sources],
                          "input": str(input_path), "composition": str(output / "composition.json")}
    elif trace_format == "weka-dag":
        input_path = output / ("input.jsonl.zst" if source.suffix == ".zst" else "input.jsonl")
        conversion = [*python_argv, "-c", "import shutil,sys; shutil.copyfile(sys.argv[1],sys.argv[2])", str(source), str(input_path)]
        resolved_trace = {"format": trace_format, "path": str(source), "input": str(input_path)}
    else:
        conversion = [*python_argv, "-m", "research.freeinference_trace", "--source", str(source)]
        for ordinal in selected:
            conversion += ["--week", WEEKS[ordinal - 1]]
        conversion += ["--model", model, "--output", str(input_path)]
        for name in ("start_ms", "end_ms"):
            if name in trace:
                conversion += ["--" + name.replace("_", "-"), str(trace[name])]
        resolved_trace = {**trace, "source": str(source), "weeks": selected}
    replay = [str(output / "blis"), "replay", "--semi-closed-input", str(input_path),
              "--semi-closed-report", str(output / "workload.json"), "--metrics-path", str(output / "metrics.json"),
              "--semi-closed-join-policy", join, "--scenario", "scenario.yaml", "--scenarios", str(output),
              "--num-instances", str(layout.replicas), "--seed", str(seed),
              "--server-series-interval-s", str(series), "--defaults-filepath", str(paths["defaults"]),
              "--catalog", str(paths["catalog"]), "--registry", str(paths["registry"])]
    if "arrivals" in run:
        replay += ["--semi-closed-arrivals", arrivals["mode"], "--semi-closed-arrival-seed", str(arrivals["seed"])]
        for field, flag in (("interval_s", "--semi-closed-arrival-interval-s"), ("rate_per_s", "--semi-closed-arrival-rate")):
            if field in arrivals:
                replay += [flag, str(arrivals[field])]
    if acceptance:
        replay += ["--speculative-category", acceptance["category"], "--speculative-thinking", acceptance["thinking"]]
    verifier = "verify_dag.py" if trace_format == "weka-dag" else "verify.py"
    verify = [*python_argv, str(REPOSITORY / "research/semiclosed" / verifier), "--input", str(input_path),
              "--report", str(output / "workload.json"), "--metrics", str(output / "metrics.json")]
    if trace_format == "freeinference":
        verify += ["--source", str(source)]
    if sources:
        verify = [*python_argv, "-m", "research.semiclosed.mix", "verify",
                  "--input", str(input_path), "--manifest", resolved_trace["composition"],
                  "--report", str(output / "workload.json"), "--metrics", str(output / "metrics.json")]
    resolved = {"version": 1, "config_path": str(config), "output": str(output),
                "trace": resolved_trace,
                "week_mapping": {str(i): name for i, name in enumerate(WEEKS, 1)},
                "unavailable_weeks": unavailable,
                "serving": {"model": serving_model, "engine_version": version, "coefficients": coefficients,
                            "cluster": {**cluster, "replicas": layout.replicas, "tp": layout.tp, "pp": 1, "dp": 1,
                                        "enable_expert_parallel": layout.enable_expert_parallel},
                            "engine": engine, "acceptance": acceptance},
                "capacity": {"allocated_gpus": layout.nodes * layout.gpus_per_node,
                             "active_gpus": layout.replicas * layout.tp,
                             "spare_gpus": layout.nodes * layout.gpus_per_node - layout.replicas * layout.tp},
                "run": {"seed": seed, "join_policy": join, "series_interval_s": series, "arrivals": arrivals},
                "python_argv": python_argv, **{key: str(value) for key, value in paths.items()}}
    if router_config is not None:
        resolved["serving"]["router"] = {
            "endpoint_picker_config": str(router_source), "saved_config": str(output / "router.yaml"),
            "sha256": hashlib.sha256(router_config.encode()).hexdigest()}
    commands = (tuple(conversion), ("go", "build", "-o", str(output / "blis"), "."),
                tuple(replay), tuple(verify))
    return RunPlan(output, resolved, original, scenario_text, commands, router_config)


def describe(plan: RunPlan) -> None:
    print(f"Trace model: {plan.resolved['trace'].get('model', 'from explicit DAG input')}")
    print(f"Serving model: {plan.resolved['serving']['model']}")
    if "router" in plan.resolved["serving"]:
        print(f"Router config: {plan.resolved['serving']['router']['endpoint_picker_config']}")
    for source in plan.resolved["trace"].get("sources", []):
        print(f"DAG source: {source['path']} ({source['copies']} copies)")
    for ordinal in plan.resolved["trace"].get("weeks", []):
        print(f"Week {ordinal}: {WEEKS[ordinal - 1]}")
    if plan.resolved["unavailable_weeks"]:
        print("Unavailable weeks skipped by all: " + ", ".join(map(str, plan.resolved["unavailable_weeks"])))
    print(f"Run directory: {plan.output}")


def execute(plan: RunPlan) -> int:
    plan.output.parent.mkdir(parents=True, exist_ok=True)
    plan.output.mkdir()
    (plan.output / "config.original.json").write_bytes(plan.original)
    (plan.output / "config.resolved.json").write_text(json_text(plan.resolved))
    (plan.output / "scenario.yaml").write_text(plan.scenario)
    if plan.router_config is not None:
        (plan.output / "router.yaml").write_text(plan.router_config)
    (plan.output / "commands.json").write_text(json_text(plan.commands))
    (plan.output / "commands.log").write_text("".join(shlex.join(argv) + "\n" for argv in plan.commands))
    with (plan.output / "run.log").open("w") as log:
        for index, argv in enumerate(plan.commands):
            message = "+ " + shlex.join(argv)
            print(message, flush=True)
            log.write(message + "\n")
            log.flush()
            try:
                if index == len(plan.commands) - 1:
                    result = subprocess.run(argv, cwd=REPOSITORY, capture_output=True, text=True)
                    (plan.output / "verification.json").write_text(result.stdout)
                    print(result.stdout, end="")
                    print(result.stderr, end="", file=sys.stderr)
                    log.write(result.stdout + result.stderr)
                    status = result.returncode
                else:
                    with subprocess.Popen(argv, cwd=REPOSITORY, stdout=subprocess.PIPE,
                                          stderr=subprocess.STDOUT, text=True) as process:
                        for line in process.stdout:
                            print(line, end="", flush=True)
                            log.write(line)
                        status = process.wait()
            except OSError as error:
                log.write(f"Unable to run command: {error}\n")
                raise
            if status:
                status = 128 - status if status < 0 else status
                log.write(f"Exit status: {status}\n")
                print(f"Run failed with exit status {status}; artifacts retained in {plan.output}", file=sys.stderr)
                return status
        log.write("Exit status: 0\n")
    return 0


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("config", type=Path)
    parser.add_argument("--output", type=Path, help="new output directory, relative to invocation directory")
    parser.add_argument("--python", help="Python executable; overrides config and PYTHON")
    parser.add_argument("--dry-run", action="store_true", help="resolve and print without writing or provisioning")
    args = parser.parse_args(argv)
    try:
        if sys.version_info < (3, 11):
            raise ValueError("Python 3.11+ is required")
        plan = prepare(args.config, output=args.output, python=args.python)
        describe(plan)
        if args.dry_run:
            print("Resolved configuration:\n" + json_text(plan.resolved), end="")
            print("Generated scenario:\n" + plan.scenario, end="")
            if plan.router_config is not None:
                print("Router configuration:\n" + plan.router_config, end="")
            print(f"Command working directory: {REPOSITORY}")
            for command in plan.commands:
                print(shlex.join(command))
            return 0
        return execute(plan)
    except (OSError, ValueError) as error:
        print(f"semiclosed: {error}", file=sys.stderr)
        return 2
    except KeyboardInterrupt:
        print("semiclosed: interrupted; existing run artifacts retained", file=sys.stderr)
        return 130


if __name__ == "__main__":
    raise SystemExit(main())
