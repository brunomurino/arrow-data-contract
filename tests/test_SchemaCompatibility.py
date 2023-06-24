import pyarrow as pa
from adc import SchemaCompatibility, SchemaTestResult


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


def test_schema_comparison_check_field_in_producer():
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

    field = consumer_schema.field("n_legs")

    result = schema_compatibility.check_field_in_producer(field)

    assert result.passed()


def test_schema_comparison_consumer_columns_not_in_producer():
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

    field = consumer_schema.field("n_legs")

    result = schema_compatibility.check_field_in_producer(field)

    assert not result.passed()


def test_schema_comparison_producer_field_match_type():
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
    field = consumer_schema.field("n_legs")
    result = schema_compatibility.producer_field_match_type(field)
    assert result.passed()


def test_schema_comparison_producer_field_not_match_type():
    producer_schema = pa.schema(
        [
            pa.field("n_legs", pa.string()),
            pa.field("year", pa.int64()),
        ]
    )

    consumer_schema = pa.schema(
        [
            pa.field("n_legs", pa.int64()),
        ]
    )

    schema_compatibility = SchemaCompatibility(producer_schema, consumer_schema)
    field = consumer_schema.field("n_legs")
    result = schema_compatibility.producer_field_match_type(field)
    assert not result.passed()


def test_schema_comparison_producer_field_match_type_skipped():
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
    field = consumer_schema.field("n_legs")
    result = schema_compatibility.producer_field_match_type(field)
    assert result.result == SchemaTestResult.SKIP


def test_schema_comparison_is_not_compatible():
    producer_schema = pa.schema(
        [
            pa.field("n_legs", pa.int64()),
            pa.field("year", pa.int64()),
            pa.field("foo", pa.int64()),
        ]
    )

    consumer_schema = pa.schema(
        [
            pa.field("n_legs", pa.int64()),
            pa.field("foo", pa.string()),
        ]
    )

    schema_compatibility = SchemaCompatibility(producer_schema, consumer_schema)
    result, compatibility_report = schema_compatibility.is_compatible()

    assert compatibility_report["n_legs"].passed()
    assert not compatibility_report["foo"].passed()

    assert result == SchemaTestResult.FAIL


def test_schema_comparison_is_compatible():
    producer_schema = pa.schema(
        [
            pa.field("n_legs", pa.int64()),
        ]
    )

    consumer_schema = pa.schema(
        [
            pa.field("n_legs", pa.int64()),
        ]
    )

    schema_compatibility = SchemaCompatibility(producer_schema, consumer_schema)
    result, _ = schema_compatibility.is_compatible()
    assert result == SchemaTestResult.PASS
