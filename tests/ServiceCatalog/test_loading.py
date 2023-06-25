from pathlib import Path
import pytest
from adc import ServiceCatalog, ContractNotFound, DataContract


def test_catalog_load():
    catalog = ServiceCatalog()
    catalog.load(Path("./tests/ServiceCatalog/Repo1"))

    assert len(catalog.all_contracts) == 1
    assert isinstance(catalog.get("repo_1_contract_1"), DataContract)

    with pytest.raises(ContractNotFound):
        catalog.get("repo_1_contract_2")
