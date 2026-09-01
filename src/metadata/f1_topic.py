from enum import Enum

class F1Topic(str, Enum):
    DIM_DRIVER = "f1.dim.driver"
    DIM_CONSTRUCTOR = "f1.dim.constructor"

    FACT_RACE_RESULT = "f1.fact.race_result"
    FACT_LAP = "f1.fact.lap"

    STREAM_TELEMETRY = "f1.stream.telemetry"
    STREAM_WEATHER = "f1.stream.weather"

    def __str__(self):
        return self.value
    