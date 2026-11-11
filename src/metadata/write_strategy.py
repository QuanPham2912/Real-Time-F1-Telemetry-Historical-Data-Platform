from enum import Enum


class WriteStrategy(str, Enum):
    APPEND = "append"
    MERGE = "merge"
    DYNAMIC_OVERWRITE = "dynamic_overwrite"

    def __str__(self):
        return self.value
