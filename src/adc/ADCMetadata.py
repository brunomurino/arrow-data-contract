import json
from typing import List


class ADCMetadata:
    def __init__(self):
        self.metadata = dict()

    def done(self):
        return {
            k.encode("utf8"): json.dumps(v).encode("utf8")
            for k, v in self.metadata.items()
        }

    def add_test_allowed_values(self, allowed_values: List):
        tests = self.metadata.get("tests", dict())
        tests.update({"allowed_values": allowed_values})
        self.metadata["tests"] = tests

        return self

    def add_test_not_null(self):
        tests = self.metadata.get("tests", dict())
        tests.update({"not_null": True})
        self.metadata["tests"] = tests

        return self
