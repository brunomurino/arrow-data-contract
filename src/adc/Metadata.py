from __future__ import annotations
import json
from typing import Any, Dict, List
from typing_extensions import Self


class Metadata:
    def __init__(self):
        self.metadata: Dict[str, Dict[str, Any]] = dict()

    def done(self) -> dict[bytes, bytes]:
        return {
            k.encode("utf8"): json.dumps(v).encode("utf8")
            for k, v in self.metadata.items()
        }

    def add_test_allowed_values(self, allowed_values: List[str]) -> Self:
        tests = self.metadata.get("tests", dict())
        tests.update({"allowed_values": allowed_values})
        self.metadata["tests"] = tests
        return self

    def add_test_not_null(self) -> Self:
        tests = self.metadata.get("tests", dict())
        tests.update({"not_null": True})
        self.metadata["tests"] = tests
        return self
