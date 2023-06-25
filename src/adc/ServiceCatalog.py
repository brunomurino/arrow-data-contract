from pathlib import Path
import importlib
from typing import Dict, Optional

from adc import DataContract


class ContractNotFound(Exception):
    def __init__(self, name):
        message = f"Contract named {name} is not in the catalog"
        super().__init__(message)


class ContractAlreadyRegistered(Exception):
    def __init__(self, name):
        message = f"Contract named {name} already registered to catalog"
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

            for name, data_contract in data_contracts_in_module.items():
                if name in self.all_contracts:
                    raise ContractAlreadyRegistered(name)

                self.all_contracts.update({name: data_contract})

    def get(self, name) -> Optional[DataContract]:
        if contract := self.all_contracts.get(name):
            return contract
        raise ContractNotFound(name)
