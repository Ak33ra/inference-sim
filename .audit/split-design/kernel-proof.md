# Kernel boundary evidence

Root independently inspected the installed dependencies used by the frozen study.

Preparation is pure. `sim/kernelmodel/kernelmodel.go:418` calls `AdmissionOverhead(InputLen)`. In `blis-latency-kernel@v0.3.0/kernel.go:1793`, the function returns zero for nonpositive input and otherwise `admissionPerRequest + admissionPerToken * promptTokens`. It reads immutable coefficients and does not mutate state or draw randomness. Homogeneous engines with identical model settings therefore have the same preparation cost.

The lifecycle state bound is conservative. `sim/kernelmodel/settings.go:185` converts `SequenceBoundedBytesAt(computed,inFlight)` to whole pool blocks. Its gate is the minimum of `SequenceBoundedBytesAt(0,tokens)` and the admission cap. The kernel's `kernel.go:386` sets seen to computed plus inFlight, capped by maxModelLen. `caches.go:302` adds window pages and fixed state. `blis-schemas@v0.7.0/rules/rules.go:354` computes each ring as `ceil(min(window-1+extraRetained+inFlight, seen)/block)+1`. For a non-speculative request with total accepted token bound N, every actual step has computed+inFlight <= N. Thus each step's ring token bound is <= N. Hold(0,N) has ring token bound exactly N for a positive window, so it upper-bounds every step, including the extra page. Fixed and recurrent bounded state is independent of computed/inFlight here. The native admission gate is <= Hold(0,N). Ceiling and summation preserve the inequality.

The resulting promise `ceil(N/blockSize) + max(Hold(0,N), Gate(N))` covers both variable token blocks and bounded per-sequence state. It does not justify arbitrary custom Hold callbacks; supported kernel construction or explicit capability validation must establish the contract. Tight-capacity tests and a real-model lifecycle test remain required. Do not physically allocate the promise or include it in physical UsedBlocks.

The host was lightly loaded before evaluation preparation, with load averages 4.65/4.67/4.63 on 256 cores. Reported measurements are deterministic simulated time, not host runtime; run-level host duration is provenance only.
