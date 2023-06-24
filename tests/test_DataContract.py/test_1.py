import pyarrow as pa

from adc import DataContract, Direction


def test_init():
    my_schema = pa.schema(
        [
            pa.field("n_legs", pa.int64()),
        ]
    )

    data_contract = DataContract(
        name="test__DataContract__init", schema=my_schema, direction=Direction.PRODUCER
    )

    assert data_contract.name == "test__DataContract__init"
    assert data_contract.schema == my_schema
    assert data_contract.direction == Direction.PRODUCER

    DataContract.instances = dict()
