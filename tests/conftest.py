from adc import Direction, DataContract, TableMetadata, FieldTestMetadata, FieldMetadata
import pytest
import pyarrow as pa


@pytest.fixture(autouse=True)
def add_np(doctest_namespace):
    doctest_namespace["TableMetadata"] = TableMetadata
    doctest_namespace["FieldTestMetadata"] = FieldTestMetadata
    doctest_namespace["FieldMetadata"] = FieldMetadata
    doctest_namespace["Direction"] = Direction

    # doctest_namespace["my_contract_1"] = DataContract(pa.schema([
    #         pa.field("n_legs", pa.int64(),metadata=FieldMetadata().add_test_not_null().done()),
    #     ],
    #     metadata=TableMetadata(name="FOO",direction='CONSUMER').done()
    # ))
