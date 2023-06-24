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


def test_export_instances():
    my_schema = pa.schema(
        [
            pa.field("n_legs", pa.int64()),
        ]
    )

    data_contract_1 = DataContract(
        name="test_export_instances_1", schema=my_schema, direction=Direction.PRODUCER
    )

    data_contract_2 = DataContract(
        name="test_export_instances_2", schema=my_schema, direction=Direction.CONSUMER
    )

    assert data_contract_1.name in DataContract.instances
    assert data_contract_2.name in DataContract.instances
    assert data_contract_1 in DataContract.instances.values()
    assert data_contract_2 in DataContract.instances.values()
    assert len(DataContract.instances) == 2

    DataContract.instances = dict()
