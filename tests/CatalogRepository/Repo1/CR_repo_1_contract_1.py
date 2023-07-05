import pyarrow as pa
from adc import DataContract, Direction, TableMetadata

schema = pa.schema(
    [
        pa.field("field_1", pa.int64()),
    ],
    metadata=TableMetadata(name="repo_1_contract_1", direction=Direction.PRODUCER).done()
)

repo_1_contract_1 = DataContract(schema)
