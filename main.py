import json
import pyarrow as pa

from DataContract import DataContract, Direction

from Repo2.DATA_CONTRACTS.CONSUMER.ANIMALS import data_contract as animals_data_contract_consumer
from Repo1.DATA_CONTRACTS.PRODUCER.ANIMALS import data_contract as animals_data_contract_producer

# Find the files that are in CONSUMER but not in PRODUCER -- this is an issue
set(animals_data_contract_consumer.schema).difference(set(animals_data_contract_producer.schema))


# Find the files that are in PRODUCER but not in CONSUMER -- this is NOT an issue
set(animals_data_contract_producer.schema).difference(set(animals_data_contract_consumer.schema))

breakpoint()

# pylist = [
#     {'n_legs': 2, 'animals': 'Flamingo'},
#     {'n_legs': 102, 'animals': 'Centipede'},
#     {'n_legs': 203, 'animals': 'Centipede'},
# ]




# data_contract.to_file()

# incoming_tbl = pa.Table.from_pylist(pylist)
# data_contract = DataContract.from_file("ANIMALS")

# final_tbl, test_results = animals_data_contract_consumer.validate_table(incoming_tbl)

    


# final_tbl, test_results = validate_table_against_data_contract(incoming_tbl, data_contract)

# casted_tbl = casted_tbl.replace_schema_metadata(my_schema.metadata)

# for field in casted_tbl.schema:
#     updating_field = casted_tbl.schema.field(field.name)
#     updating_field = updating_field.with_metadata(my_schema.field(field.name).metadata)
#     breakpoint()

breakpoint()

# tbl.cast(my_schema)