import attrs
from attrs import define, field
from typing import Optional, List, Any, overload
from typing_extensions import Self
import json


@define
class FieldTestMetadata:
    not_null: Optional[bool] = None
    allowed_values: Optional[List[str]] = None

    @classmethod
    def from_dict(cls, incoming_args: dict[str, Any] | Self) -> Self:
        if type(incoming_args) == dict:
            return cls(**incoming_args)
        elif type(incoming_args) == cls:
            return incoming_args
        else:
            raise Exception(
                f"Type of incoming_args not Self or dict[str, Any]"
            )  # pragma: no cover


@define
class FieldMetadata:
    """

    Examples:
        >>> FieldTestMetadata(not_null=True)
        FieldTestMetadata(not_null=True, allowed_values=None)

        >>> FieldMetadata(tests=FieldTestMetadata(not_null=True))
        FieldMetadata(tests=FieldTestMetadata(not_null=True, allowed_values=None))

        >>> FieldMetadata().add_test_not_null()
        FieldMetadata(tests=FieldTestMetadata(not_null=True, allowed_values=None))

    """

    tests: FieldTestMetadata = field(
        default=FieldTestMetadata(), converter=FieldTestMetadata.from_dict
    )

    def done(self) -> dict[bytes, bytes]:
        return {
            k.encode("utf8"): json.dumps(v, default=str).encode("utf8")
            for k, v in attrs.asdict(self).items()
        }

    @classmethod
    def from_encoded(cls, encoded_metadata: dict[bytes, bytes]) -> Self:
        raw_dict = {
            k.decode("utf-8"): json.loads(v.decode("utf-8"))
            for k, v in encoded_metadata.items()
        }
        return cls(**raw_dict)

    def add_test_not_null(self) -> Self:
        self.tests.not_null = True
        return self
