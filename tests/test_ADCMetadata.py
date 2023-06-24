import pyarrow as pa
from adc import ADCMetadata
import json


def test_create_empty_metadata():
    metadata = ADCMetadata().done()
    assert metadata == dict()


def test_create_metadata_with_allowed_values_test():
    metadata = ADCMetadata().add_test_allowed_values(["FOO", "BAR"]).done()

    expected_metadata = {
        b"tests": json.dumps({"allowed_values": ["FOO", "BAR"]}).encode("utf-8")
    }

    assert metadata == expected_metadata


def test_create_metadata_with_allowed_values_and_not_null_test():
    metadata = (
        ADCMetadata().add_test_allowed_values(["FOO", "BAR"]).add_test_not_null().done()
    )

    expected_metadata = {
        b"tests": json.dumps(
            {
                "allowed_values": ["FOO", "BAR"],
                "not_null": True,
            }
        ).encode("utf-8")
    }

    assert metadata == expected_metadata


def test_create_schema_with_ADCMetadata():
    animals_metadata = (
        ADCMetadata().add_test_allowed_values(["Flamingo", "Horses"]).done()
    )

    my_schema = pa.schema(
        [
            pa.field("animals", pa.string(), metadata=animals_metadata),
        ]
    )

    assert my_schema.field("animals").metadata == animals_metadata
