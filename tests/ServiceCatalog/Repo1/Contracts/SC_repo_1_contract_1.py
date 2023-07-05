import pyarrow as pa
from adc import DataContract, Direction
from adc.metadata.table_metadata import TableMetadata

repo_1_contract_1 = DataContract(pa.schema(
    [
        pa.field("field_1", pa.int64()),
    ],
    metadata=TableMetadata(name="repo_1_contract_1", direction=Direction.CONSUMER).done()
))

repo_1_contract_2 = DataContract(pa.schema(
    [
        pa.field("field_1", pa.int64()),
    ],
    metadata=TableMetadata(name="repo_1_contract_2", direction=Direction.PRODUCER).done()
))