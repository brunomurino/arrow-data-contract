import pyarrow as pa
from adc import SchemaCompatibility


def test_class_init():
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

    schema_compatibility = SchemaCompatibility(producer_schema, consumer_schema)

    assert schema_compatibility.producer_schema == producer_schema
    assert schema_compatibility.consumer_schema == consumer_schema


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

    schema_compatibility = SchemaCompatibility(producer_schema, consumer_schema)

    assert schema_compatibility.consumer_columns_in_producer() == True


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

    schema_compatibility = SchemaCompatibility(producer_schema, consumer_schema)

    assert schema_compatibility.consumer_columns_in_producer() == False
