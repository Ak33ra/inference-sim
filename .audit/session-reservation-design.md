# Session reservation design

The user wants to compare a clairvoyant reservation rule against cache-off JSQ on the existing eight-B200 mega trace at four-second Poisson root gaps. Each session is one job. The shortest 40% by elapsed time may use any server. Long sessions may use six servers. The requested subgroup statistics are by session.

The frozen oracle contains a class and one reference elapsed duration for each runtime session ID. Rank by minimum elapsed duration observed across the 35 cache-off isolated-family runs, then by runtime session ID. Those isolated runs still allow competition among siblings. The reference includes tool delays and waits for dependencies, and is not a proven session-alone optimum. Exactly 954 of 2,385 sessions are short. A family reference map is retained for an additional overall family comparison.

The native `session-reservation` router filters candidates before using the existing JSQ chooser. Reserved endpoint IDs are fixed to `instance_0` and `instance_1` and checked at startup. It retains the existing arrival latency, outstanding-request ledger, completion timing and tie-breaking. Missing session classes and impossible placements fail explicitly. No engine scheduling, replay lifecycle or token work changes.

Model the Domain led to one immutable session-class manifest rather than scattered size checks. Separate Before Serializing Shared State led to an isolated implementation worktree with one owner. Prove It Works requires actual routing audits and complete workload accounting before reporting results.

Independent source/design readers agreed on a focused policy in `sim/researchrouter/session_reservation.go`. A generic candidate-eligibility interface and a separate gateway wrapper were rejected because they would add another lifecycle and state owner for a single policy. Existing JSQ remains its own unchanged branch.

A new `research/session_reservation` package owns the experiment and its analysis. Existing Python helpers in the frozen study remain unchanged. Its explicit plan adapter supplies the new native router configuration and records the actual execution plan. Ten new loaded runs use one frozen binary and five paired arrival seeds. The five new JSQ runs must reproduce their saved counterparts. The previous study is read only.

The analyzer reports session elapsed time and slowdown for all, short and long sessions, with per-run mean, median and p99 and their median/range across five seeds. It saves paired changes and individual rows for later plotting. It also reports overall family makespan and slowdown with the same old family denominators. No family short/long classification is introduced.
