# Independent core review

Reviewer: Sol, read-only, against the pre-request-SPLIT root snapshot.

Required finding: require exact MaxOutputLen=max(1, oracle output). The writer added the guard and mismatch behavior test.

Reviewed priority direction and ties, exactly eight replicas, first-allocation ownership, runtime input/output identity, independent GPU capacity and client terminal clocks, and callback ordering. OnDone lookup is safe because gateway callback precedes the semi-closed callback and request tokens remain intact. No other correctness blocker. Old session event JSON remains structurally identical for valid manifests, subject to full saved-output parity. Event oracle linkage uses request_id through frozen manifest, not a misleading reference_run value.
