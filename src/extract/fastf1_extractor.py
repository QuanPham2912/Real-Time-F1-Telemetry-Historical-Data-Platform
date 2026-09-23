import os

import fastf1
from extract.base import Extractor
from metadata.logger import ETLLogger

logger = ETLLogger.get_logger()

class FastF1Extractor(Extractor):
    def __init__(self):
        super().__init__(source_name = "FastF1")
        self.source_name = "FastF1"
        self.session_id = None
        # Have own volume in docker-compose.yml to store cache data, so that we don't have to download the same data again and again
        CACHE_DIR = "/tmp/fastf1_cache"
        os.makedirs(CACHE_DIR, exist_ok=True)
        fastf1.Cache.enable_cache(CACHE_DIR)

    def extract_session_data(self, season: int, round: int, session_type: str):
        self.session_id = f"{season}_{round}_{session_type}"
        session = fastf1.get_session(season, round, session_type)
        session.load()
        return session

    def extract_lap_data(self, session):
        laps = session.laps
        result = laps[['Driver','LapNumber', 'LapTime', 'Stint', 'Compound', 'TyreLife',
                          'FreshTyre', 'Sector1Time', 'Sector2Time', 'Sector3Time',
                          'PitInTime', 'PitOutTime', 'TrackStatus', 'IsAccurate']].copy()
        # Add session_id and source information to recognize each lap data belongs to each year, round, session type and source system.
        result['Session_id'] = self.session_id
        result['Source'] = self.source_name
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
        # Add session_id and source information to recognize each weather data belongs to each year, round, session type and source system.
        weather_data['Session_id'] = self.session_id
        weather_data['Source'] = self.source_name
        weather_data['Time'] = weather_data['Time'].astype(str)
        return weather_data.to_dict(orient='records')

    def extract_telemetry_stream(self, session):
        for PermanentNumber in session.drivers:
            driver_lap = session.laps.pick_driver(PermanentNumber)
            if driver_lap.empty:
                continue
            try:
                # Extract telemetry data for the full race of the driver
                telemetry = driver_lap.get_telemetry()
                telemetry["PermanentNumber"] = PermanentNumber
                selected_cols = ['PermanentNumber', 'Time', 'SessionTime',
                                'Speed', 'Throttle', 'Brake',
                                'RPM', 'nGear', 'DRS',
                                'Distance', 'X', 'Y', 'Z']
                # Check if in that did that race not have any col
                valid_cols = [col for col in selected_cols if col in telemetry.columns]

                data = telemetry[valid_cols].copy()
                data['Session_id'] = self.session_id
                data['Source'] = self.source_name
                

                driver_record = data.to_dict(orient='records')

                yield from driver_record
            except Exception as e:
                logger.error(f"Error extracting telemetry for driver {PermanentNumber}: {e}")
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