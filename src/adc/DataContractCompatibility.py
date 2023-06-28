from dataclasses import dataclass
from typing import Any
from adc import DataContract, SchemaCompatibility, Direction


class DataContractCompatibility:
    def __init__(self, res: Any):
        self.res = res

    # producer_data_contract: DataContract
    # consumer_data_contract: DataContract

    # def run(self):
    #     return SchemaCompatibility(
    #         self.producer_data_contract.schema,
    #         self.consumer_data_contract.schema,
    #     ).compatibility_report()

    def run(self):
        for check in self.res:
            incoming_contract = check["incoming_contract"]
            existing_contract = check["existing_contract"]

            if incoming_contract.direction == Direction.PRODUCER:
                producer_contract = incoming_contract
                consumer_contract = existing_contract
            else:
                consumer_contract = incoming_contract
                producer_contract = existing_contract

            result, report = SchemaCompatibility(
                producer_contract.schema,
                consumer_contract.schema,
            ).is_compatible()

            check["compatibility_report"] = {
                "result": result,
                "report": report,
            }

        return self.res
