# FreeInference to Weka

Offline conversion for the BLIS AgentX pilot. Output contains complete exported
families for one model, including ordinary families that provide background load.
It changes the representation, not AgentX's replay behavior. See
[known replay limitations](../AGENTX_LIMITATIONS.md).

From the repository root, with Python 3.11 or newer:

```bash
python -m pip install -r research/freeinference_trace/requirements.txt
python -m research.freeinference_trace \
  --week 2026-07-12_2026-07-18 --model glm-5.2 \
  --output research/outputs/glm52-pilot
```

The default source is `traces/freeinference_agentic_trace/release/`. Override it
with `--source`; repeat `--week` for multiple weeks. `--start-ms` and `--end-ms`
select roots by Unix milliseconds in a half-open interval. Descendants and later
turns remain intact, even outside that interval. The output directory must be new.

Outputs are `corpus.jsonl`, `manifest.json`, and `audit.jsonl`. The manifest records
input and output digests, converter source hashes, selection, assumptions, and
conservation counts. The audit records every excluded family and each included
family's original request identities/times, candidate joins, residuals, and prefix
sharing. Identical inputs and converter/environment versions produce identical
artifacts; destination paths and wall-clock timestamps are excluded.

Conversion verifies the published `tables.json` digests. The declared dataset
revision defaults to the research pin; `--source-revision` changes that label,
while file digests identify the bytes actually read. Completeness means all rows
in the published weekly tables, not proof that the publisher observed all work.

Mixed-model families are excluded whole. Unsupported families (nested delegation,
overlapping turns within a session, missing steps/spawn links, invalid counts,
or children preceding their spawning request) are reported and excluded.
`--unsupported error` instead stops on the first such eligible family. Broken
parent graphs, duplicate identities and invalid prefix data fail conversion.

Times use seconds relative to the root's first arrival; `api_time` keeps the
observed LLM duration. Children follow their identified spawning parent turn in
the Weka array, with common-clock timestamps. Their duration ends at the last LLM
completion, omitting unobserved final tools. Audit joins mirror the loader's
timing heuristic and do not establish causal barriers. No gaps are retimed.

Prefix mapping uses every fourth **chained** 16-token identity as a 64-token
identity, capped at `floor(prompt_tokens / 64)`. This retains source sharing at
64-token boundaries without inventing extra shared blocks. Provider token counts
remain absolute, including compaction. Tokenizers are not rescaled: uncovered
tokens become AgentX's synthetic suffix, and its template/marker adds overhead.
Sharing across families is not retained by AgentX.

Use `split_flattened_agents: false` when preserving the published sessions;
AgentX's default `true` can infer different agents from prefix changes. Both use
the existing loader. Validate the output against this checkout with:

```bash
source .inference-sim-env/activate  # If using the documented local Go environment
go run ./research/freeinference_trace/checkweka -input research/outputs/glm52-pilot
# Also exercise AgentX's default chain inference:
go run ./research/freeinference_trace/checkweka \
  -input research/outputs/glm52-pilot -split-flattened-agents
```

This checks the digest and loader acceptance, not simulation correctness or a
hardware context limit. Use the [research replay launcher](../freeinference_replay/README.md)
to run the corpus through BLIS with FreeInference provenance.

Large prefix lists are staged in a temporary SQLite index so Parquet row order
does not dictate family grouping. `--temp-dir` chooses scratch storage;
`--batch-size` bounds prefix rows read per batch. One converted family is held in
memory at a time. Generated output belongs under the ignored `research/outputs/`.

```bash
python -m unittest discover -s research/freeinference_trace/tests -v
```
