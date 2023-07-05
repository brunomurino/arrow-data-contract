from pathlib import Path
import pytest
import shutil
import uuid
from adc import (
    ServiceCatalog,
    ContractNotFound,
    DataContract,
    ContractAlreadyRegistered,
)


def test_catalog_load():
    catalog = ServiceCatalog("repo_1")
    catalog.load(Path("./tests/ServiceCatalog/Repo1"))

    assert len(catalog.contracts) == 2
    assert isinstance(catalog.get("repo_1_contract_1"), DataContract)

    with pytest.raises(ContractNotFound):
        catalog.get("repo_1_contract_99")


def test_catalog_load_duplicated():
    catalog = ServiceCatalog("repo_1")
    with pytest.raises(ContractAlreadyRegistered):
        catalog.load(Path("./tests/ServiceCatalog/Repo1_duplicated"))


def test_catalog_generate_files():
    catalog = ServiceCatalog("repo_1")
    catalog.load(Path("./tests/ServiceCatalog/Repo1"))
    base_path = Path(str(uuid.uuid4()))
    catalog_files = catalog.generate_files(base_path)
    expected_catalog_files = [
        base_path / Path("CONSUMER/repo_1_contract_1.parquet"),
        base_path / Path("PRODUCER/repo_1_contract_2.parquet"),
    ]
    assert catalog_files == expected_catalog_files
    # TODO: add cleanup step
    shutil.rmtree(base_path)
