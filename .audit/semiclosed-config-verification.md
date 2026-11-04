# Config runner verification — 2026-11-04

Implementation is commit 836fc89 in the isolated implementation worktree; only its four changed files were applied to the primary checkout. Parent-owned examples and documentation are additional scoped changes. No converter or Go simulator files changed for this feature.

`python3 -B -m unittest discover -s research/scripts/tests -v` passed all 20 cases (12 config runner and 8 existing comparison cases). Bash syntax and commit whitespace checks passed. Independent review found no blocking correctness issues. The Python 3.11+ bootstrap requirement was added to the guide.

Real runs are retained under `research/outputs/semiclosed/`:

| Directory | Requests | Prompt tokens | Output tokens | Result |
| --- | ---: | ---: | ---: | --- |
| config-glm52-smoke-20261104 | 175 | 9,077,878 | 169,589 | Complete; entire metrics.json exactly equals legacy runner checks 20261103 baseline |
| config-deepseek-smoke-20261104 | 5 | 276,266 | 783 | Complete; DeepSeek V4 → V4.1, H200 TP4 with DSpark |
| config-multi-node-20261104 | 3 | 12,575 | 467 | Complete; GLM B200 TP16 across two eight-GPU nodes |
| config-partial-h200-20261104 | 5 | 276,266 | 783 | Complete; H200 TP4 on one eight-GPU node, four unused GPUs |

Each successful run passed the existing verifier against the original Parquet. Original/resolved config, generated scenario, argv, logs, binary, converted input, workload/metrics and verification artifacts exist. The resolved active/allocated GPU counts were checked separately.

The deliberately infeasible `config-deepseek-failure-20261104` TP2 run returned status 1, retained its config, commands, input and native memory diagnostic, and produced no successful verification artifact. An initial GLM TP4 partial-node test also failed native memory feasibility (175.0 GiB fixed occupancy versus a 160.5 GiB budget); partial occupancy was then tested successfully with the already-feasible DeepSeek TP4 model. No model parameters were altered to conceal those failures.

Legacy smoke, full, checks and verify dry-run output exactly matches captured pre-change output after normalizing generated timestamp/PID directory names. The legacy verify command successfully reverified the existing 175-request source smoke result.

All-week dry-run resolved all 16 pinned release weeks. A real sparse release containing only week 9 retained ordinal 9 when invoked from another directory and resolved a config-relative output with spaces without creating it. Unit tests cover incomplete/all selection, missing and duplicate ordinals, malformed fields, paths, interpreter precedence, layout rejection, failed subprocess status and retained provenance. Guide links resolve and all three example JSON configs parse.

The all-week config was previewed, not fully replayed during launcher validation. Existing Go engines and source conversion were left unchanged; no new calibration claim is made for the DeepSeek diagnostic example.
