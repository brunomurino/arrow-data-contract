import pyarrow as pa
from dataclasses import dataclass


@dataclass
class SchemaCompatibility:
    producer_schema: pa.Schema
    consumer_schema: pa.Schema

    def consumer_columns_in_producer(self):
        columns_in_producer = set(self.producer_schema.names)
        columns_in_consumer = set(self.consumer_schema.names)

        columns_present_on_consumer_but_not_on_producer = (
            columns_in_consumer.difference(columns_in_producer)
        )
        # columns_present_on_producer_but_not_on_consumer = (
        #     columns_in_producer.difference(columns_in_consumer)
        # )

        if len(columns_present_on_consumer_but_not_on_producer) > 0:
            print(
                f"Consumer expects the following columns not found in the Producer: {columns_present_on_consumer_but_not_on_producer}"
            )
            # raise Exception(f"Consumer expects the following columns not found in the Producer: {columns_present_on_consumer_but_not_on_producer}")
            return False

        return True

    def compatible(self):
        self.consumer_columns_in_producer()

        if not self.consumer_columns_in_producer():
            return False

        return True
    
    def foo(self):
        pass
