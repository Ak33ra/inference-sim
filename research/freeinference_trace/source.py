from __future__ import annotations

from array import array
from contextlib import closing, contextmanager
import hashlib
import json
from pathlib import Path
import sqlite3
import tempfile
from typing import Iterator

import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq

from .model import Family, Request, Session, TraceError, group_families
from .prefix import SOURCE_BLOCK_TOKENS

REQUEST_COLUMNS = list(Request.__dataclass_fields__)
SESSION_COLUMNS = list(Session.__dataclass_fields__)


def sha256_file(path: Path) -> str:
    with path.open("rb") as stream:
        return hashlib.file_digest(stream, "sha256").hexdigest()


class WeekSource:
    def __init__(self, release: Path, week: str):
        self.week = week
        self.path = release / week
        self.processed = self.path / "processed"
        self.manifest = json.loads((self.path / "manifest.json").read_text())
        if (self.manifest.get("format_version") != 1 or
                self.manifest.get("week") != week or
                self.manifest.get("block_tokens") != SOURCE_BLOCK_TOKENS or
                self.manifest.get("tokenizer") != "o200k_base"):
            raise TraceError(f"{week}: unsupported release format, tokenizer or block size")

    def provenance(self) -> dict:
        tables = json.loads((self.processed / "tables.json").read_text())
        manifest_hash = sha256_file(self.path / "manifest.json")
        if tables.get("manifest_sha256") != manifest_hash:
            raise TraceError(f"{self.week}: manifest digest differs from tables.json")
        files = {}
        for name in ("sessions.parquet", "requests.parquet", "tool_calls.parquet"):
            path = self.processed / name
            digest = sha256_file(path)
            if tables.get("files", {}).get(name, {}).get("sha256") != digest:
                raise TraceError(f"{self.week}: {name} digest differs from tables.json")
            files[name] = {"sha256": digest, "bytes": path.stat().st_size}
        return {"week": self.week, "manifest_sha256": manifest_hash,
                "tables_sha256": sha256_file(self.processed / "tables.json"),
                "salt_series": self.manifest.get("salt_series"), "files": files}

    def families(self) -> list[Family]:
        sessions = [Session(**row) for row in pq.read_table(
            self.processed / "sessions.parquet", columns=SESSION_COLUMNS).to_pylist()]
        requests = (Request(**row) for batch in pq.ParquetFile(
            self.processed / "requests.parquet").iter_batches(
                columns=REQUEST_COLUMNS, batch_size=4096)
                    for row in batch.to_pylist())
        return group_families(self.week, sessions, requests)

    def subagent_tool_keys(self) -> set[tuple[str, int, int]]:
        rows = pq.read_table(self.processed / "tool_calls.parquet", columns=[
            "session_id", "step", "call_index", "category"]).to_pylist()
        return {(r["session_id"], r["step"], r["call_index"])
                for r in rows if r["category"] == "Subagent"}

    @contextmanager
    def prefixes(self, families: list[Family], batch_size: int,
                 temp_dir: Path | None = None) -> Iterator["PrefixStore"]:
        # Family members need not be adjacent in Parquet. A temporary disk index
        # avoids keeping a week's expanded Python block arrays in memory.
        with tempfile.TemporaryDirectory(prefix="freeinference-", dir=temp_dir) as folder:
            with closing(sqlite3.connect(Path(folder) / "prefixes.sqlite")) as db:
                db.execute("CREATE TABLE prefixes (session TEXT, step INTEGER, blocks BLOB, "
                           "PRIMARY KEY (session, step)) WITHOUT ROWID")
                wanted = {r.key for family in families for r in family.all_requests()}
                session_ids = pa.array(sorted({sid for sid, _ in wanted}), type=pa.string())
                count = 0
                if wanted:
                    parquet = pq.ParquetFile(self.processed / "requests.parquet")
                    for batch in parquet.iter_batches(
                            columns=["session_id", "step", "block_ids"], batch_size=batch_size):
                        selected = batch.filter(pc.is_in(batch.column("session_id"), value_set=session_ids))
                        for row in selected.to_pylist():
                            key = row["session_id"], row["step"]
                            if key not in wanted:
                                raise TraceError(f"{self.week}: unexpected prefix row {key}")
                            blocks = row["block_ids"]
                            if blocks is None or any(type(b) is not int or not 0 <= b < 2**63 for b in blocks):
                                raise TraceError(f"{self.week}: invalid block_ids at {key}")
                            db.execute("INSERT INTO prefixes VALUES (?, ?, ?)",
                                       (*key, array("q", blocks).tobytes()))
                            count += 1
                        db.commit()
                if count != len(wanted):
                    raise TraceError(f"{self.week}: expected {len(wanted)} prefix rows, got {count}")
                yield PrefixStore(db)


class PrefixStore:
    def __init__(self, db: sqlite3.Connection):
        self.db = db

    def __call__(self, request: Request) -> array:
        row = self.db.execute("SELECT blocks FROM prefixes WHERE session=? AND step=?",
                              request.key).fetchone()
        if row is None:
            raise TraceError(f"missing prefix for {request.key}")
        result = array("q")
        result.frombytes(row[0])
        return result
