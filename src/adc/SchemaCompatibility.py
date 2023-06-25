import pyarrow as pa
from dataclasses import dataclass
from typing import Any, Dict

from enum import Enum

SchemaTestResult = Enum("SchemaTestResult", ["PASS", "FAIL", "SKIP"])

AvailableFieldTest = Enum(
    "AvailableFieldTest",
    [
        "PRESENT_IN_PRODUCER",
        "PRODUCER_TYPE_MATCHES",
    ],
)


@dataclass
class FieldCompatibilitySchemaTestResult:
    name: AvailableFieldTest
    consumer_data: Any
    producer_data: Any
    result: SchemaTestResult

    def passed(self):
        return self.result == SchemaTestResult.PASS


@dataclass
class FieldCompatibilityTestsSumary:
    report: Dict[AvailableFieldTest, FieldCompatibilitySchemaTestResult]
    result: SchemaTestResult

    def passed(self):
        return self.result == SchemaTestResult.PASS


@dataclass
class SchemaCompatibility:
    producer_schema: pa.Schema
    consumer_schema: pa.Schema

    def check_field_in_producer(
        self, field: pa.Field
    ) -> FieldCompatibilitySchemaTestResult:
        is_present = field.name in self.producer_schema.names
        return FieldCompatibilitySchemaTestResult(
            name=AvailableFieldTest.PRESENT_IN_PRODUCER,
            consumer_data=field.name,
            producer_data=field.name if is_present else None,
            result=SchemaTestResult.PASS if is_present else SchemaTestResult.FAIL,
        )

    def producer_field_match_type(
        self, field: pa.Field
    ) -> FieldCompatibilitySchemaTestResult:
        consumer_data = field.type
        producer_data = None
        result = SchemaTestResult.SKIP

        is_present = field.name in self.producer_schema.names
        if is_present:
            producer_data = self.producer_schema.field(field.name).type
            same_type = consumer_data == producer_data
            result = SchemaTestResult.PASS if same_type else SchemaTestResult.FAIL

        return FieldCompatibilitySchemaTestResult(
            name=AvailableFieldTest.PRODUCER_TYPE_MATCHES,
            consumer_data=consumer_data,
            producer_data=producer_data,
            result=result,
        )

    def get_field_tests(
        self, field: pa.Field
    ) -> Dict[AvailableFieldTest, FieldCompatibilitySchemaTestResult]:
        all_tests = [
            self.check_field_in_producer(field),
            self.producer_field_match_type(field),
        ]
        final = {test.name: test for test in all_tests}
        return final

    def get_field_tests_summary(self, field: pa.Field) -> FieldCompatibilityTestsSumary:
        field_tests = self.get_field_tests(field)
        all_passed = all(test_result.passed() for _, test_result in field_tests.items())
        return FieldCompatibilityTestsSumary(
            result=SchemaTestResult.PASS if all_passed else SchemaTestResult.FAIL,
            report=field_tests,
        )

    def compatibility_report(self) -> Dict[str, FieldCompatibilityTestsSumary]:
        report = {
            field.name: self.get_field_tests_summary(field)
            for field in self.consumer_schema
        }
        return report

    def is_compatible(self):
        report = self.compatibility_report()
        all_passed = all(
            field_test_summary.passed() for _, field_test_summary in report.items()
        )

        result = SchemaTestResult.FAIL
        if all_passed:
            result = SchemaTestResult.PASS

        return result, report
