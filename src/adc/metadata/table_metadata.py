import attrs
from attrs import define, field
from typing import Optional
from typing_extensions import Self
from .direction import Direction
import json


def _direction_converter(x):
    # if not x:
    #     return None
    if type(x) == Direction:
        return x
    else:
        return Direction[x]


@define
class TableMetadata:
    """

    Examples:
        >>> TableMetadata(name="FOO", direction='CONSUMER')
        TableMetadata(name='FOO', direction=CONSUMER, service=None)

        >>> TableMetadata(name="FOO", direction='CONSUMER').direction
        CONSUMER

    """

    name: str
    direction: Direction = field(converter=_direction_converter)
    service: Optional[str] = None

    def done(self) -> dict[bytes, bytes]:
        return {
            k.encode("utf8"): json.dumps(v, default=str).encode("utf8")
            for k, v in attrs.asdict(self).items()
        }

    @classmethod
    def from_encoded(cls, encoded_metadata) -> Self:
        raw_dict = {
            k.decode("utf-8"): json.loads(v.decode("utf-8"))
            for k, v in encoded_metadata.items()
        }
        return cls(**raw_dict)
