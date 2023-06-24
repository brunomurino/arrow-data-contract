import pyarrow as pa
import json

import adc
from adc import DataContract, Direction

def test__DataContract__init():

    n_legs_schema_tests = {
        'allowed_values': [2,102, 203]
    }

    animals_schema_tests = {
        'allowed_values': ["Flamingo","Horses"]
    }

    my_schema = pa.schema([
        pa.field('n_legs', pa.int64(), metadata={
            b'tests': json.dumps(n_legs_schema_tests).encode('utf-8')
        }),
        pa.field('animals', pa.string(), metadata={
            b'tests': json.dumps(animals_schema_tests).encode('utf-8')
        })
    ],metadata={
        b'tests': json.dumps([]).encode('utf-8')
    })

    data_contract = DataContract(
        name = "ANIMALS",
        schema = my_schema,
        direction = Direction.PRODUCER
    )

    assert data_contract.name == 'ANIMALS'
    assert data_contract.schema == my_schema
    assert data_contract.direction == Direction.PRODUCER
