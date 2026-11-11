import os
import fastf1
from extract.base import Extractor
from metadata.logger import ETLLogger
from metadata.f1_topic import F1Topic as T
from utils.config_reader import load_config

logger = ETLLogger.get_logger()

CONFIG_PATH = "extractors.yaml"
CONFIG = load_config(CONFIG_PATH)

CACHE_DIR = CONFIG["fastf1"]["cache_dir"]
# ----------------------------------------------------
LAP_COL = CONFIG["fastf1"]["columns"]["lap"]
LAP_TIME_COL = CONFIG["fastf1"]["columns"]["lap_time"]
# -----------------------------------------------------
WEATHER_COL = CONFIG["fastf1"]["columns"]["weather"]
WEATHER_TIME_COL = CONFIG["fastf1"]["columns"]["weather_time"]
# -----------------------------------------------------
TELEMETRY_COL = CONFIG["fastf1"]["columns"]["telemetry"]
TELEMETRY_TIME_COL = CONFIG["fastf1"]["columns"]["telemetry_time"]


class FastF1Extractor(Extractor):
    topic_mapping = {
        "lap_data": T.FASTF1_LAP,
        "weather_data": T.FASTF1_WEATHER,
        "telemetry_stream": T.FASTF1_TELEMETRY,
    }

    def __init__(self):
        super().__init__(source_name="FastF1")
        self.source_name = "FastF1"
        self.session_id = None
        self.season = None
        # Have own volume in docker-compose.yml to store cache data, so that we don't have to download the same data again and again
        os.makedirs(CACHE_DIR, exist_ok=True)
        fastf1.Cache.enable_cache(CACHE_DIR)

    def extract_session_data(self, season: int, round: int, session_type: str):
        self.session_id = f"{season}_{round}_{session_type}"
        self.season = season
        session = fastf1.get_session(season, round, session_type)
        session.load()
        return session

    def add_detail_info(self, result: list):
        result["Session_id"] = self.session_id
        result["Source"] = self.source_name
        result["Season"] = self.season

    def cast_time_columns_to_string(self, result: list, time_cols: list):
        for col in time_cols:
            if col in result.columns:
                result[col] = result[col].astype(str)

    def extract_lap_data(self, session):
        laps = session.laps
        result = laps[LAP_COL].copy()
        self.add_detail_info(result)
        self.cast_time_columns_to_string(result, LAP_TIME_COL)
        return result.to_dict(orient="records")

    def extract_weather_data(self, session):
        weather_data = session.weather_data
        weather_data = weather_data[WEATHER_COL].copy()
        self.add_detail_info(weather_data)
        self.cast_time_columns_to_string(weather_data, WEATHER_TIME_COL)
        return weather_data.to_dict(orient="records")

    def extract_telemetry_stream(self, session):
        for PermanentNumber in session.drivers:
            driver_lap = session.laps.pick_driver(PermanentNumber)
            if driver_lap.empty:
                continue
            try:
                # Extract telemetry data for the full race of the driver
                telemetry = driver_lap.get_telemetry()
                telemetry["PermanentNumber"] = PermanentNumber
                # Check if in that did that race not have any col
                valid_cols = [col for col in TELEMETRY_COL if col in telemetry.columns]
                data = telemetry[valid_cols].copy()

                # Edit time data to string because Json can handle date time type
                self.cast_time_columns_to_string(data, TELEMETRY_TIME_COL)

                self.add_detail_info(data)

                driver_record = data.to_dict(orient="records")

                yield from driver_record
            except Exception as e:
                logger.error(
                    f"Error extracting telemetry for driver {PermanentNumber}: {e}"
                )
                continue

    def extract(self, season: int, round: int, session_type: str):
        session = self.extract_session_data(season, round, session_type)
        lap_data = self.extract_lap_data(session)
        weather_data = self.extract_weather_data(session)
        telemetry_stream = self.extract_telemetry_stream(session)

        return {
            "lap_data": lap_data,
            "weather_data": weather_data,
            "telemetry_stream": telemetry_stream,
        }
