import pyarrow as pa
import pyarrow.parquet as pq
from pathlib import Path
from enum import Enum

Direction = Enum("Direction", ["CONSUMER", "PRODUCER"])


class DataContract:
    base_path: Path = Path("DATA_CONTRACTS")

    def __init__(
        self,
        name: str,
        schema: pa.Schema,
        direction: Direction,
    ):
        self.name = name
        self.schema = schema
        self.direction = direction

    def __str__(self):
        return f"DataContract({self.name},{self.direction.name})"

    def __repr__(self):
        return str(self)

    # @staticmethod
    # def schema_test(tbl, column_name, column_index, test_name, test_value):
    #     if test_name == "allowed_values":
    #         dictionary_encoded_column = tbl.column(column_name).dictionary_encode()
    #         tbl = tbl.set_column(column_index, column_name, dictionary_encoded_column)
    #         allowed_values = pa.array(test_value)
    #         existing_values = dictionary_encoded_column.chunk(0).dictionary
    #         test_result = all(ev in allowed_values for ev in existing_values)
    #         return tbl, {
    #             test_name: {
    #                 "allowed_values": allowed_values.to_pylist(),
    #                 "existing_values": existing_values.to_pylist(),
    #                 "result": test_result,
    #             }
    #         }

    # def validate_table(self, incoming_tbl):
    #     my_schema = self.schema

    #     casted_tbl = incoming_tbl.cast(my_schema)

    #     table_test_results = dict()

    #     for field in my_schema:
    #         field_metadata = field.metadata
    #         if not field_metadata:
    #             continue
    #         if b"tests" not in field_metadata:
    #             continue

    #         # // Deserialize the category information
    #         column_tests = json.loads(field_metadata[b"tests"])
    #         column_name = field.name
    #         column_index = my_schema.get_field_index(column_name)

    #         column_test_results = dict()
    #         for test_name, test_value in column_tests.items():
    #             casted_tbl, test_result = DataContract.schema_test(
    #                 casted_tbl, column_name, column_index, test_name, test_value
    #             )
    #             column_test_results = column_test_results | test_result

    #         table_test_results = table_test_results | {column_name: column_test_results}

    #     return casted_tbl, table_test_results

    # def to_file(self):
    #     filepath = DataContract.base_path / self.direction.name / self.name
    #     filepath.parent.mkdir(exist_ok=True, parents=True)
    #     pq.write_metadata(self.schema, filepath.with_suffix(".parquet"))

    # @classmethod
    # def from_file(cls, name):
    #     filepath = DataContract.base_path / Direction.CONSUMER.name / name
    #     file_metadata = pq.read_metadata(filepath.with_suffix(".parquet"))
    #     arrow_schema = file_metadata.schema.to_arrow_schema()

    #     return cls(
    #         name=name,
    #         schema=arrow_schema,
    #         direction=Direction.CONSUMER,
    #     )

    # @staticmethod
    # def consumer_columns_in_producer(columns_in_producer, columns_in_consumer):
    #     columns_present_on_consumer_but_not_on_producer = (
    #         columns_in_consumer.difference(columns_in_producer)
    #     )
    #     columns_present_on_producer_but_not_on_consumer = (
    #         columns_in_producer.difference(columns_in_consumer)
    #     )

    #     if len(columns_present_on_consumer_but_not_on_producer) > 0:
    #         print(
    #             f"Consumer expects the following columns not found in the Producer: {columns_present_on_consumer_but_not_on_producer}"
    #         )
    #         # raise Exception(f"Consumer expects the following columns not found in the Producer: {columns_present_on_consumer_but_not_on_producer}")
    #         return False

    #     return True

    # @staticmethod
    # def schema_compatibility(producer_schema, consumer_schema):
    #     columns_in_producer = set(producer_schema.names)
    #     columns_in_consumer = set(consumer_schema.names)

    #     if not DataContract.consumer_columns_in_producer(
    #         columns_in_producer, columns_in_consumer
    #     ):
    #         return False

    #     return True
