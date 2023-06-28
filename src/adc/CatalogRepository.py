from pathlib import Path
import shutil
from typing import List, Protocol, Dict
from adc import DataContract, ServiceCatalog

from adc.DataContract import Direction


class CatalogRepositoryBackend(Protocol):
    def get_complete_path(self, service_name: str, catalog_filepath: Path) -> Path:
        pass

    def upload_file(self, service_name: str, local_filepath: Path) -> Path:
        pass

    def list_files(self, glob_to_search: str) -> List[Path]:
        pass

    def get_file(self, filepath: Path, target_base_path: Path) -> Path:
        pass


class CatalogRepositoryBackendLocal:
    def __init__(self, base_path: Path):
        self.base_path = base_path

    def get_complete_path(self, service_name: str, catalog_filepath: Path) -> Path:
        catalog_filepath_second_parent = catalog_filepath.parent.parent
        complete_path = (
            self.base_path
            / service_name
            / catalog_filepath.relative_to(catalog_filepath_second_parent)
        )
        complete_path.parent.mkdir(parents=True, exist_ok=True)
        return complete_path

    def upload_file(self, service_name: str, local_filepath: Path) -> Path:
        landing_filepath = self.get_complete_path(service_name, local_filepath)
        shutil.copy(local_filepath, landing_filepath)
        return landing_filepath

    def list_files(self, glob_to_search) -> List[Path]:
        return [
            filepath.relative_to(self.base_path)
            for filepath in self.base_path.glob(glob_to_search)
        ]

    def get_file(self, filepath: Path, target_base_path: Path) -> Path:
        target_filepath = target_base_path / filepath
        target_filepath.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy(self.base_path / filepath, target_filepath.parent)
        return target_filepath


class CatalogRepository:
    def __init__(self, backend: CatalogRepositoryBackend):
        self.backend = backend

    def upload_catalog_files(self, service_name, catalog_files) -> List[Path]:
        repository_filepaths = [
            self.backend.upload_file(service_name, filepath)
            for filepath in catalog_files
        ]
        return repository_filepaths

    def find_contract_with_name(self, name):
        pass

    def verify_matches(
        self, searched_direction: Direction, list_of_matches_found: List[Path]
    ) -> None:
        num_matches = len(list_of_matches_found)
        if searched_direction.PRODUCER and num_matches >= 2:
            raise Exception(
                "More than 1 PRODUCER found with the same name. Catalog Repository is corrupted."
            )
        if searched_direction.PRODUCER and num_matches == 0:
            raise Exception("Could not find a PRODUCER.")

    def find_relevant_contracts(self, search_direction, name):
        list_of_matches_found = self.backend.list_files(
            f"**/{search_direction.name}/{name}.*"
        )
        self.verify_matches(search_direction, list_of_matches_found)
        return list_of_matches_found

    def get_required_contracts_to_check(
        self, service_catalog: ServiceCatalog, staging_path: Path
    ) -> List[Dict[str, DataContract]]:
        return [
            {
                "incoming_contract": contract,
                "existing_contract": DataContract.from_file(
                    self.backend.get_file(match_found, staging_path)
                ),
            }
            for requested_contract_name, contract in service_catalog.all_contracts.items()
            for match_found in self.find_relevant_contracts(
                contract.direction.opposite(), requested_contract_name
            )
        ]


# Assume you're on Repo3
# On Repo3's code you declare a few data contracts
# Now there are a few things you want to do
# Check if the contracts you declared can be added to CatalogRepository
# If declaring a PRODUCER, check if there are existing PRODUCER with the same name
# If declaring a CONSUMER, check if a PRODUCER exists
# If both conditions are met, then you can actually run the compatibility checks between the Contracts
# For an incoming PRODUCER, run compatibility check against all CONSUMERS
# For an incoming CONSUMER, run compatibility check against single PRODUCER
