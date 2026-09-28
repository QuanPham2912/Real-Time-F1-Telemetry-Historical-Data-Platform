from enum import Enum

class F1Topic(str, Enum):
    JOLPICA_DRIVER = "f1.jolpica.dim.driver"
    JOLPICA_CONSTRUCTOR = "f1.jolpica.dim.constructor"
    JOLPICA_RACE = "f1.jolpica.dim.race"
    JOLPICA_RACE_RESULT = "f1.jolpica.fact.race_result"

    STATSF1_DRIVER_STATSF1 = "f1.statsf1.dim.driver"
    STATSF1_CONSTRUCTOR_STATSF1 = "f1.statsf1.dim.constructor"
    STATSF1_ENGINE_SUPPLIER_STATSF1 = "f1.statsf1.dim.engine_supplier"
    STATSF1_CAR_STATSF1 = "f1.statsf1.dim.car"
    STATSF1_RACE_RESULT = "f1.statsf1.fact.race_result"

    FASTF1_LAP = "f1.fastf1.fact.lap"
    FASTF1_TELEMETRY = "f1.fastf1.fact.telemetry"
    FASTF1_WEATHER = "f1.fastf1.stream.weather"

    def __str__(self):
        return self.value
    