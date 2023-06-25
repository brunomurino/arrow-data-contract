from pathlib import Path
import importlib
from typing import Dict, Optional

from adc import DataContract


class ContractNotFound(Exception):
    def __init__(self, name):
        message = f"Contract named {name} is not on the catalog"
        super().__init__(message)


class ServiceCatalog:
    def __init__(self):
        self.all_contracts: Dict[str, DataContract] = dict()

    def load(self, path: Path):
        for python_file in path.glob("**/*.py"):
            module_path = str(python_file.parent / python_file.stem).replace("/", ".")
            module = importlib.import_module(module_path)

            data_contracts_in_module = {
                name: cls
                for name, cls in module.__dict__.items()
                if isinstance(cls, DataContract)
            }

            self.all_contracts.update(data_contracts_in_module)

    def get(self, name) -> Optional[DataContract]:
        if contract := self.all_contracts.get(name):
            return contract
        raise ContractNotFound(name)
