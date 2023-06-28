from pathlib import Path
import pytest
from adc import (
    ServiceCatalog,
    ContractNotFound,
    DataContract,
)


def test_catalog_fetch_direction():
    catalog = ServiceCatalog()
    catalog.load(Path("./tests/ServiceCatalog/Repo1"))

    all_consumer_contracts = catalog.get_consumers()

    assert "repo_1_contract_1" in all_consumer_contracts
    assert "repo_1_contract_2" not in all_consumer_contracts

    all_producer_contracts = catalog.get_producers()

    assert "repo_1_contract_1" not in all_producer_contracts
    assert "repo_1_contract_2" in all_producer_contracts
