from pathlib import Path
import shutil
from typing import List

from .catalog_repository_entry import CatalogRepositoryEntry


class CatalogRepositoryBackendLocal:
    def __init__(self, base_path: Path):
        self.base_path = base_path

    def __repr__(self):
        return f"Local: {str(self.base_path)}"

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

    def list_contracts(self) -> List[CatalogRepositoryEntry]:
        all_files = [
            filepath.relative_to(self.base_path)
            for filepath in self.base_path.glob("**/*.parquet")
        ]

        contracts = [
            CatalogRepositoryEntry(
                repo=file.parents[1].name,
                direction=file.parents[0].name,
                name=file.stem,
                path_in_backend=file,
            )
            for file in all_files
        ]

        return contracts

    def get_file(self, filepath: Path, target_base_path: Path) -> Path:
        target_filepath = target_base_path / filepath
        target_filepath.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy(self.base_path / filepath, target_filepath.parent)
        return target_filepath
