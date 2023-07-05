from enum import Enum, auto

class Direction(str, Enum):
    """
    Examples:
        >>> consumer = Direction['CONSUMER']
        >>> consumer
        CONSUMER

        >>> consumer.opposite()
        PRODUCER

        >>> consumer.opposite().opposite() == consumer
        True

    """

    CONSUMER = 'CONSUMER'
    PRODUCER = 'PRODUCER'

    def opposite(self):
        return Direction.PRODUCER if self == Direction.CONSUMER else Direction.CONSUMER

    def __repr__(self):
        return self.value
