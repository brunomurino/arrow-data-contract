import pyarrow as pa
import json

from adc import DataContract, Direction, ADCMetadata


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
