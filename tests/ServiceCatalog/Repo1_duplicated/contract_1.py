import pyarrow as pa
from adc import DataContract, Direction

schema = pa.schema(
    [
        pa.field("field_1", pa.int64()),
    ]
)

repo_1_duplicated_contract_1 = DataContract(
    name="repo_1_duplicated_contract_1",
    schema=schema,
    direction=Direction.CONSUMER,
)
