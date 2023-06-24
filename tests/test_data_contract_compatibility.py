import pyarrow as pa

from adc import DataContract, Direction


def test_schema_comparison_fields_present_consumer_less_than_producer():
    producer_schema = pa.schema(
        [
            pa.field("n_legs", pa.int64()),
            pa.field("year", pa.int64()),
        ]
    )

    consumer_schema = pa.schema(
        [
            pa.field("n_legs", pa.int64()),
        ]
    )

    compatible = DataContract.schema_compatibility(producer_schema, consumer_schema)
    assert compatible == True


def test_schema_comparison_fields_present_consumer_wants_missing_from_producer():
    producer_schema = pa.schema(
        [
            pa.field("year", pa.int64()),
        ]
    )

    consumer_schema = pa.schema(
        [
            pa.field("n_legs", pa.int64()),
        ]
    )

    compatible = DataContract.schema_compatibility(producer_schema, consumer_schema)
    assert compatible == False
