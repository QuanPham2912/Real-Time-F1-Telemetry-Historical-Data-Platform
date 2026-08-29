import fastf1
import pandas as pd
from src.extract.base import Extractor
from src.metadata.logger import ETLLogger
import os
from pathlib import Path

logger = ETLLogger.get_logger()

class FastF1Extractor(Extractor):
    def __init__(self, cache_dir: str = None):
        super().__init__(source_name = "FastF1")
        default_cache = Path(__file__).resolve().parents[2] / ".cache" / "fastf1"
        cache_path = Path(cache_dir or os.getenv("FASTF1_CACHE_DIR", default_cache))
        cache_path.mkdir(parents=True, exist_ok=True)
        fastf1.Cache.enable_cache(str(cache_path))

    def extract_session_data(self, season: int, round: int, session_type: str):
        session = fastf1.get_session(season, round, session_type)
        session.load()
        return session

    def extract_lap_data(self, session):
        laps = session.laps
        result = laps[['Driver','LapNumber', 'LapTime', 'Stint', 'Compound', 'TyreLife',
                          'FreshTyre', 'Sector1Time', 'Sector2Time', 'Sector3Time',
                          'PitInTime', 'PitOutTime', 'TrackStatus', 'IsAccurate']].copy()

        # cast time data to string type 
        time_cols = ['LapTime', 'Sector1Time', 'Sector2Time', 'Sector3Time', 'PitInTime', 'PitOutTime']
        for col in time_cols:
            result[col] = result[col].astype(str)

        return result.to_dict(orient='records')

    def extract_weather_data(self, session):
        weather_data = session.weather_data
        weather_data = weather_data[['Time', 'AirTemp', 'TrackTemp',
                                 'Humidity', 'Rainfall', 'WindSpeed',
                                 'WindDirection', 'Pressure']].copy()
        weather_data['Time'] = weather_data['Time'].astype(str)
        return weather_data.to_dict(orient='records')

    def extract_telemetry_stream(self, session):
        for driver in session.drivers:
            driver_lap = session.laps.pick_driver(driver)
            if driver_lap.empty:
                continue
            try:
                # Extract telemetry data for the full race of the driver
                telemetry = driver_lap.get_telemetry()
                telemetry["DriverNumber"] = driver
                selected_cols = ['DriverNumber', 'Time', 'SessionTime',
                                'Speed', 'Throttle', 'Brake',
                                'RPM', 'nGear', 'DRS',
                                'Distance', 'X', 'Y', 'Z']
                # Check if in that did that race not have any col
                valid_cols = [col for col in selected_cols if col in telemetry.columns]

                driver_record = telemetry[valid_cols].to_dict(orient='records')

                for record in driver_record:
                    yield record
            except Exception as e:
                logger.error(f"Error extracting telemetry for driver {driver}: {e}")
                continue
    def extract(self, season: int, round: int, session_type: str):
        session = self.extract_session_data(season, round, session_type)
        lap_data = self.extract_lap_data(session)
        weather_data = self.extract_weather_data(session)
        telemetry_stream = self.extract_telemetry_stream(session)

        return {
            "lap_data": lap_data,
            "weather_data": weather_data,
            "telemetry_stream": telemetry_stream
        }