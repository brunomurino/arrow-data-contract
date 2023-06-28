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
    assert repr(data_contract) == "DataContract(test__DataContract__init,PRODUCER)"


def test_direction_opposite():
    my_direction = Direction.CONSUMER

    assert my_direction.opposite() == Direction.PRODUCER
    assert my_direction.opposite().opposite() == my_direction
