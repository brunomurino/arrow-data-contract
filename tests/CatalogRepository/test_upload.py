from pathlib import Path
from adc import (
    CatalogRepository,
    DataContractCompatibility,
    ServiceCatalog,
    CatalogRepositoryBackendLocal,
)
import uuid
import shutil


def test_upload_repo_1():
    catalog = ServiceCatalog("repo_1")
    catalog.load(Path("./tests/CatalogRepository/Repo1"))
    catalog_base_path = Path(str(uuid.uuid4()))

    catalog_files = catalog.generate_files(catalog_base_path)

    repo_path = Path("CATALOG_REPOSITORY")
    backend = CatalogRepositoryBackendLocal(base_path=repo_path)

    repo = CatalogRepository(backend=backend)
    all_catalog_files = repo.upload_catalog_files("repo_1", catalog_files)

    assert all_catalog_files == [
        repo_path / Path("repo_1/PRODUCER/repo_1_contract_1.parquet")
    ]

    # TODO: add cleanup step
    shutil.rmtree(catalog_base_path)
    shutil.rmtree(repo_path)


# def test_upload_repo_1_2_3():
#     catalog_repo_1 = ServiceCatalog()
#     catalog_repo_1.load(Path("./tests/CatalogRepository/Repo1"))
#     catalog_repo_1_base_path = Path(str(uuid.uuid4()))
#     catalog_repo_1_files = catalog_repo_1.generate_files(catalog_repo_1_base_path)

#     catalog_repo_2 = ServiceCatalog()
#     catalog_repo_2.load(Path("./tests/CatalogRepository/Repo2"))
#     catalog_repo_2_base_path = Path(str(uuid.uuid4()))
#     catalog_repo_2_files = catalog_repo_2.generate_files(catalog_repo_2_base_path)

#     repo_path = Path("_CATALOG_REPOSITORY_MULTIPLE")
#     backend = CatalogRepositoryBackendLocal(base_path=repo_path)

#     repo = CatalogRepository(backend=backend)
#     repo.upload_catalog_files("repo_1", catalog_repo_1_files)
#     shutil.rmtree(catalog_repo_1_base_path)
#     repo.upload_catalog_files("repo_2", catalog_repo_2_files)
#     shutil.rmtree(catalog_repo_2_base_path)

#     # Setup is done above

#     catalog_repo_3 = ServiceCatalog()
#     catalog_repo_3.load(Path("./tests/CatalogRepository/Repo3"))
#     catalog_repo_3_base_path = Path(str(uuid.uuid4()))
#     catalog_repo_3_files = catalog_repo_3.generate_files(catalog_repo_3_base_path)

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
