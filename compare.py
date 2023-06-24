from DataContract import DataContract, Direction
from pathlib import Path
from typing import Any
from dataclasses import dataclass

@dataclass
class DataContractCatalogEntry:
    name: str
    producer: Any
    consumers: list[Any]


Catalog = {
    data_contract_producer.stem: DataContractCatalogEntry(
        name = data_contract_producer.stem,
        producer = data_contract_producer,
        consumers = list(Path(".").glob("**/DATA_CONTRACTS/CONSUMER/*.parquet")),
    )
    for data_contract_producer in Path(".").glob("**/DATA_CONTRACTS/PRODUCER/*.parquet")
}


breakpoint()


MR_REPO = "Repo2"

# Look at all CONSUMERS in the MR_REPO
mr_repo_consumer_folderpath = Path(MR_REPO) / "CONSUMER"

# For each CONSUMER, find the PRODUCER
for consumer_filepath in mr_repo_consumer_folderpath.glob("*.parquet"):
    list_of_producers = [
        producer_filepath
        for producer_filepath in Path('.').glob("**/PRODUCER/*.parquet")
        if producer_filepath.stem == consumer_filepath.stem
    ]

    assert len(list_of_producers) == 1

    producer_filepath = list_of_producers[0]

    source_repo = producer_filepath.parents[-2]

    breakpoint()