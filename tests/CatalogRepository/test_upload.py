from pathlib import Path
from adc import (
    CatalogRepository,
    DataContractCompatibility,
    ServiceCatalog,
    Direction,
    CatalogRepositoryBackendLocal,
    CatalogRepositoryEntry,
)
import uuid
import shutil
import pytest


def test_upload_repo_1():
    catalog = ServiceCatalog("repo_1")
    catalog.load(Path("./tests/CatalogRepository/Repo1"))
    catalog_base_path = Path(str(uuid.uuid4()))

    catalog_files = catalog.generate_files(catalog_base_path)

    repo_path = Path("CATALOG_REPOSITORY")
    backend = CatalogRepositoryBackendLocal(base_path=repo_path)

    repo = CatalogRepository(backend=backend)
    all_catalog_files = repo.upload_catalog_files("repo_1", catalog_files)

    assert all_catalog_files == [repo_path / Path("repo_1/PRODUCER/contract_1.parquet")]

    # TODO: add cleanup step
    shutil.rmtree(catalog_base_path)
    shutil.rmtree(repo_path)


@pytest.fixture()
def mock_catalog_repository():
    repo_path = Path("_CATALOG_REPOSITORY_MULTIPLE")
    backend = CatalogRepositoryBackendLocal(base_path=repo_path)
    repo = CatalogRepository(backend=backend)

    catalog_repo_1 = ServiceCatalog("repo_1")
    catalog_repo_1.load(Path("./tests/CatalogRepository/Repo1"))
    catalog_repo_1_base_path = Path(str(uuid.uuid4()))
    catalog_repo_1_files = catalog_repo_1.generate_files(catalog_repo_1_base_path)
    repo.upload_catalog_files("repo_1", catalog_repo_1_files)
    shutil.rmtree(catalog_repo_1_base_path)

    catalog_repo_2 = ServiceCatalog("repo_2")
    catalog_repo_2.load(Path("./tests/CatalogRepository/Repo2"))
    catalog_repo_2_base_path = Path(str(uuid.uuid4()))
    catalog_repo_2_files = catalog_repo_2.generate_files(catalog_repo_2_base_path)
    repo.upload_catalog_files("repo_2", catalog_repo_2_files)
    shutil.rmtree(catalog_repo_2_base_path)

    return repo


def test_list_catalog_repo(mock_catalog_repository):
    repo = mock_catalog_repository

    cr_list_contracts = repo.list_contracts()

    expected_cr_list_contracts = [
        CatalogRepositoryEntry(
            repo="repo_2",
            direction=Direction.PRODUCER,
            name="contract_2",
            path_in_backend=Path("repo_2/PRODUCER/contract_2.parquet"),
        ),
        CatalogRepositoryEntry(
            repo="repo_1",
            direction=Direction.PRODUCER,
            name="contract_1",
            path_in_backend=Path("repo_1/PRODUCER/contract_1.parquet"),
        ),
    ]

    assert expected_cr_list_contracts == cr_list_contracts


# def test_upload_repo_1_2_3(mock_catalog_repository):
#     repo = mock_catalog_repository

#     catalog_repo_3 = ServiceCatalog("repo_3")
#     catalog_repo_3.load(Path("./tests/CatalogRepository/Repo3"))
#     catalog_repo_3_base_path = Path(str(uuid.uuid4()))
#     catalog_repo_3_files = catalog_repo_3.generate_files(catalog_repo_3_base_path)

#     # breakpoint()

#     # Need to fetch relevant contracts from the CatalogRepository into a temp folder locally

#     # all_service_consumers = catalog_repo_3.get_consumers()
#     res = repo.get_required_contracts_to_check(catalog_repo_3, Path("_STAGING"))

#     comp_rep = DataContractCompatibility(res).run()
#     # for contract_name, _ in res.items():

#     # repo.upload_catalog_files("repo_3", catalog_repo_3_files)
#     # shutil.rmtree(catalog_repo_3_base_path)

#     breakpoint()

#     # TODO: add cleanup step
#     shutil.rmtree(repo_path)
