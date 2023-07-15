from pathlib import Path
import importlib
from typing import Dict, List, Optional
import pyarrow as pa

from .data_contract import DataContract, Direction


class ContractNotFound(Exception):
    def __init__(self, name):
        message = f"Contract named {name} is not in the catalog"
        super().__init__(message)


class ContractAlreadyRegistered(Exception):
    def __init__(self, name):
        message = f"Contract named {name} already registered to catalog"
        super().__init__(message)


class ContractServiceDoesntMatchServiceCatalogName(Exception):
    def __init__(self, name, service_name, service_catalog_name):
        message = f"Contract named {name} with 'service' = '{service_name}' is trying to be added to service catalog named '{service_catalog_name}'."
        super().__init__(message)


class ServiceCatalog:
    """

    Examples:
        >>> service_1_catalog = ServiceCatalog("service_1")
        >>> service_1_catalog
        ServiceCatalog('service_1')

        >>> my_contract_1 = DataContract(pa.schema([
        ...         pa.field("n_legs", pa.int64(),metadata=FieldMetadata().add_test_not_null().done()),
        ...     ],
        ...     metadata=TableMetadata(name="FOO",direction='CONSUMER').done()
        ... ))
        >>> service_1_catalog.add_contract(my_contract_1)
        >>> service_1_catalog.contracts
        {'FOO': DataContract(FOO,CONSUMER,service_1)}

        >>> my_contract_2 = DataContract(pa.schema([
        ...         pa.field("n_legs", pa.int64(),metadata=FieldMetadata().add_test_not_null().done()),
        ...     ],
        ...     metadata=TableMetadata(name="BAR",direction='CONSUMER',service='service_BLA').done()
        ... ))
        >>> service_1_catalog.add_contract(my_contract_2)
        Traceback (most recent call last):
        adc.service_catalog.ContractServiceDoesntMatchServiceCatalogName: Contract named BAR with 'service' = 'service_BLA' is trying to be added to service catalog named 'service_1'.



    """

    def __init__(self, name: str):
        self.name = name
        self.contracts: Dict[str, DataContract] = dict()

    def __repr__(self):
        return f"ServiceCatalog('{self.name}')"

    def add_contract(self, contract: DataContract) -> None:
        contract_name = contract.name

        if contract_name in self.contracts:
            raise ContractAlreadyRegistered(contract_name)

        if contract.service and contract.service != self.name:
            raise ContractServiceDoesntMatchServiceCatalogName(
                contract_name, contract.service, self.name
            )
        else:
            contract.service = self.name

        self.contracts.update({contract_name: contract})

    def load(self, path: Path):
        for python_file in path.glob("**/*.py"):
            module_path = str(python_file.parent / python_file.stem).replace("/", ".")
            module = importlib.import_module(module_path)

            data_contracts_in_module = [
                module_object
                for module_object in module.__dict__.values()
                if isinstance(module_object, DataContract)
            ]

            for data_contract in data_contracts_in_module:
                self.add_contract(data_contract)

    def get(self, name) -> Optional[DataContract]:
        if contract := self.contracts.get(name):
            return contract
        raise ContractNotFound(name)

    def generate_files(self, base_path) -> List[Path]:
        all_files = [
            contract.to_file(base_path) for _, contract in self.contracts.items()
        ]
        return all_files

    def get_consumers(self) -> Dict[str, DataContract]:
        result = {
            name: contract
            for name, contract in self.contracts.items()
            if contract.direction == Direction.CONSUMER
        }
        return result

    def get_producers(self) -> Dict[str, DataContract]:
        result = {
            name: contract
            for name, contract in self.contracts.items()
            if contract.direction == Direction.PRODUCER
        }
        return result
