# Config-driven semi-closed runs

- [x] `how` over the affected subsystem. Grounding in semiclosed-config-grounding.md and prior source/model walkthrough.
- [x] `architect` design exploration. Compared independent Astra and parent sketches, obtained independent review, selected isolated Python runner. Full model panel unavailable after thread-limit rejection; capacity fallback recorded in design note.
- [x] Blocking first steps. Read existing shell/converter/serving configuration and settle stable week numbering before implementation.
- [x] Independent workstreams. Independent design sketches while parent captures legacy behavior and prepares real verification.
- [x] Shared mutable state. Sketches use separate temporary locations; one implementation owner gets isolated worktree. Parent owns user checkout and final runs/docs.
- [x] Smallest safe decomposition. One owner for config parser, command planning, shell dispatch and tests, because their contract is shared.
- [x] Delegate implementation to isolated owner. Layout/RunPlan plus validated dictionaries; commit 836fc89 over isolated baseline 3e82850.
- [x] Verify real CLI run, malformed configs, dry-run, path semantics, numbered/all weeks, complete source/token conservation and unchanged legacy commands. Evidence in semiclosed-config-verification.md.
- [x] Review independently. Reviewer found no blocking correctness issues under chosen bounded engine contract; documented Python 3.11+ bootstrap requirement.
- [x] Import only the four scoped files from 836fc89; preserve preexisting work. Parent added examples and documentation separately. No unrelated baseline commits imported.
- [x] Design settled in synthesis; additional interrogate round not needed.
- [x] Local review and validation complete. External PR creation skipped because this request is local workspace implementation and no remote publication is needed.
- [x] Prepare final runnable example, config guide link, verification and layout scope.
