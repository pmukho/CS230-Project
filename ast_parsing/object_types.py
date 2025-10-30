from dataclasses import dataclass, field
from typing import List, Dict, Any, Optional, Set
import json
import os

@dataclass
class UDFInfo:
    name: str
    decorator: bool = False
    snippet: str = ""
    return_type: Optional[str] = None
    applied_to_df: bool = False
    registered_sql: bool = False
    third_party_dependencies: Set[str] = field(default_factory=set)

    def __repr__(self) -> str:
        return (
            f"UDFInfo(name={self.name}, decorator={self.decorator}, "
            f"applied_to_df={self.applied_to_df}, registered_sql={self.registered_sql})"
        )

@dataclass
class AnalysisResult:
    path: str
    snippet: str
    pyspark_ops: List[str] = field(default_factory=list)
    udfs: Dict[str, str] = field(default_factory=dict) #TODO: we store the print but might want to store entire obj in the future? depends on our use case for metrics.
    third_party_libs: List[str] = field(default_factory=list)

    def to_dict(self) -> Dict[str, Any]:
        """
        Convert to a JSON-serializable dict. Keep keys stable for metrics.
        """
        return {
            "path": self.path,
            "snippet": self.snippet,
            "pyspark_ops": list(self.pyspark_ops),
            "udfs": dict(self.udfs),
            "third_party_libs": list(self.third_party_libs),
        }

    def to_json(self) -> str:
        """Return compact JSON string (useful for writing JSONL)."""
        return json.dumps(self.to_dict(), ensure_ascii=False)

    def write_json_file(self, out_path: str, pretty: bool = False) -> None:
        """write to json out file (overwrites previous if we run again)"""
        os.makedirs(os.path.dirname(out_path) or ".", exist_ok=True)
        with open(out_path, "w", encoding="utf-8") as f:
            if pretty:
                json.dump(self.to_dict(), f, indent=2, ensure_ascii=False)
            else:
                json.dump(self.to_dict(), f, ensure_ascii=False)

    def write_jsonl_append(self, fp) -> None:
        """Append JSONL line to an open file-like object."""
        fp.write(self.to_json() + "\n")
