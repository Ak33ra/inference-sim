from __future__ import annotations

from collections.abc import Sequence

from .model import TraceError

SOURCE_BLOCK_TOKENS = 16
TARGET_BLOCK_TOKENS = 64


def map_prefix(block_ids: Sequence[int], prompt_tokens: int) -> list[int]:
    """Use each fourth chained identity for a complete 64-token prefix block.

    A chained ID identifies the entire prefix ending there, so the fourth ID
    already distinguishes every preceding group member. Cap coverage at the
    serving length; BLIS synthesizes the uncovered suffix.
    """
    if any(type(block) is not int or not 0 <= block < 2**63 for block in block_ids):
        raise TraceError("block_ids must contain nonnegative signed int64 identities")
    count = min(len(block_ids) // 4, prompt_tokens // TARGET_BLOCK_TOKENS)
    return list(block_ids[3:count * 4:4])


def common_prefix(a: Sequence[int], b: Sequence[int]) -> int:
    for i, (left, right) in enumerate(zip(a, b)):
        if left != right:
            return i
    return min(len(a), len(b))
