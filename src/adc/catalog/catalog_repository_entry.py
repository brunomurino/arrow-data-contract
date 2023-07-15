from pathlib import Path
from attrs import define, field

from ..metadata import Direction


@define
class CatalogRepositoryEntry:
    repo: str
    direction: Direction = field(converter=lambda x: Direction[x])
    name: str
    path_in_backend: Path
