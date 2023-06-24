from DataContract import DataContract
DataContract.export_all_instances()

# for data_contract_filepath in Path(".").glob("**/DATA_CONTRACTS/**/*.py"):
#     module_path = str(data_contract_filepath.parent / data_contract_filepath.stem).replace("/",'.')
#     my_module = importlib.import_module(module_path)

# for k,v in DataContract.instances.items():
#     v.to_file()
