from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType, TimestampType, BooleanType

class F1Schemas:
    """
    FAST F1 SCHEMA
    """
    #Schema for telementry data
    telemetry_schema = StructType([
        StructField("DriverNumber", StringType(), True),
        StructField("Time", TimestampType(), True),
        StructField("SesionTime", TimestampType(), True),
        StructField("Speed", FloatType(), True),
        StructField("Throttle", FloatType(), True),
        StructField("Brake", BooleanType(), True),
        StructField("RPM", IntegerType(), True),
        StructField("nGear", IntegerType(), True),
        StructField("DRS", IntegerType(), True),
        StructField("Distance", FloatType(), True),
        StructField("X", FloatType(), True),
        StructField("Y", FloatType(), True),
        StructField("Z", FloatType(), True)
    ])

    #Schemas for Weather data
    weather_schema = StructType([
        StructField("Time", TimestampType(), True),
        StructField("AirTemp", FloatType(), True),
        StructField("TrackTemp", FloatType(), True),
        StructField("Humidity", FloatType(), True),
        StructField("RainFall", BooleanType(), True),
        StructField("RainSpeed", FloatType(), True),
        StructField("WindDirection", IntegerType(), True),
        StructField("Pressure", FloatType(), True)
    ])

    #Schemas for Lap data
    lap_schema = StructType([
        StructField("Driver", StringType(), True),
        StructField("LapNumber", IntegerType(), True),
        StructField("LapTime", StringType(), True),
        StructField("Stint", IntegerType(), True),
        StructField("Compound", StringType(), True),
        StructField("TyreLife", IntegerType(), True),
        StructField("FreshTyre", BooleanType(), True),
        StructField("Sector1Time", TimestampType(), True),
        StructField("Sector2Time", TimestampType(), True),
        StructField("Sector3Time", TimestampType(), True),
        StructField("PitInTime", TimestampType(), True),
        StructField("PitOutTime", TimestampType(), True),
        StructField("TrackStatus", StringType(), True),
        StructField("IsAccurate", BooleanType(), True)
    ])

    """
    API-Jolpica
    """
    #Schema for Driver data
    driver_schema = StructType([
        StructField("driverId", StringType(), True),
        StructField("permanentNumber", IntegerType(), True),
        StructField("code", StringType(), True),
        StructField("givenName", StringType(), True),
        StructField("familyName", StringType(), True),
        StructField("dateOfBirth", TimestampType(), True),
        StructField("nationality", StringType(), True)
    ]) 

    #Schema for Constructor data
    constructor_schema = StructType([
        StructField("constructorId", StringType(), True),
        StructField("name", StringType(), True),
        StructField("nationality", StringType(), True)
    ])

    #Schema for rave result
    result_schema = StructType([
        StructField("number", IntegerType(), True),
        StructField("position", IntegerType(), True),
        StructField("positionText", StringType(), True),
        StructField("points", FloatType(), True),
        StructField("Driver", driver_schema, True),
        StructField("Constructor", constructor_schema, True),
    ])

    #Schema for race data
    race_schema = StructType([
        StructField("season", IntegerType(), True),
        StructField("round", IntegerType(), True),
        StructField("raceName", StringType(), True),
        StructField("Circuit", StructType([
            StructField("circuitId", StringType(), True),
            StructField("url", StringType(), True),
            StructField("circuitName", StringType(), True),
            StructField("Location", StructType([
                StructField("lat", FloatType(), True),
                StructField("long", FloatType(), True),
                StructField("locality", StringType(), True),
                StructField("country", StringType(), True)
            ]), True)
        ]), True),
        StructField("date", TimestampType(), True),
        StructField("time", TimestampType(), True),
    ])

    """
    StatsF1
    """

    #Schema for Driver data

    driver_statsf1_schema = StructType([
        StructField("Driver", StringType(), True),
        StructField("Constructor", StringType(), True),
        StructField("Engine_Manufacturer", StringType(), True),
        StructField("Best_Result", StringType(), True),
    ])

    #Schema for Car data
    car_statsf1_schema = StructType([
        StructField("Constructor", StringType(), True),
        StructField("Chassis", StringType(), True),
        StructField("Engine", StringType(), True),
    ])

    #Schema for Engine Supplier data
    engine_supplier_statsf1_schema = StructType([
        StructField("Engine_Manufacturer", StringType(), True),
        StructField("Nation", StringType(), True),
        StructField("Started_time", StringType(), True),
    ])

    #Schema for Constructor data
    constructor_statsf1_schema = StructType([
        StructField("Constructor", StringType(), True),
        StructField("Nation", StringType(), True),
        StructField("Started_time", StringType(), True),
    ])

    #Schema for Race Result data
    result_statsf1_schema = StructType([
        StructField("Position", IntegerType(), True),
        StructField("Driver_number", StringType(), True),
        StructField("Driver", StringType(), True),
        StructField("Chassis", StringType(), True),
        StructField("Engine_manufacturer", StringType(), True),
        StructField("Total_lap", IntegerType(), True),
        StructField("Race_time", StringType(), True),
    ])
