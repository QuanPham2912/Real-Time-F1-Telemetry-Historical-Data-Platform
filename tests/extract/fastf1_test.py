import pytest
from unittest.mock import Mock, MagicMock, patch, PropertyMock
import pandas as pd
from datetime import timedelta
from src.extract.fastf1_extractor import FastF1Extractor


class TestFastF1ExtractorInit:
    """Test FastF1Extractor initialization"""
    
    @patch('src.extract.fastf1_extractor.fastf1.Cache.enable_cache')
    def test_init_enables_cache(self, mock_cache):
        """Test that __init__ enables cache with proper path"""
        extractor = FastF1Extractor()
        # Verify enable_cache was called once with a string path
        assert mock_cache.call_count == 1
        # Get the argument passed to enable_cache
        call_arg = mock_cache.call_args[0][0]
        # Should be a string path containing 'fastf1'
        assert isinstance(call_arg, str)
        assert 'fastf1' in call_arg
    
    @patch('src.extract.fastf1_extractor.fastf1.Cache.enable_cache')
    def test_init_sets_source_name(self, mock_cache):
        """Test that source_name is set to 'FastF1'"""
        extractor = FastF1Extractor()
        assert extractor.source_name == "FastF1"


class TestExtractSessionData:
    """Test extract_session_data method"""
    
    @patch('src.extract.fastf1_extractor.fastf1.get_session')
    @patch('src.extract.fastf1_extractor.fastf1.Cache.enable_cache')
    def test_extract_session_data_success(self, mock_cache, mock_get_session):
        """Test successful session data extraction"""
        # Setup mock session
        mock_session = MagicMock()
        mock_get_session.return_value = mock_session
        
        extractor = FastF1Extractor()
        result = extractor.extract_session_data(2023, 1, "Race")
        
        # Verify calls
        mock_get_session.assert_called_once_with(2023, 1, "Race")
        mock_session.load.assert_called_once()
        assert result == mock_session
    
    @patch('src.extract.fastf1_extractor.fastf1.get_session')
    @patch('src.extract.fastf1_extractor.fastf1.Cache.enable_cache')
    def test_extract_session_data_different_session_types(self, mock_cache, mock_get_session):
        """Test extraction with different session types"""
        mock_session = MagicMock()
        mock_get_session.return_value = mock_session
        
        extractor = FastF1Extractor()
        
        # Test different session types
        for session_type in ["Race", "Qualifying", "FP1", "FP2", "FP3", "Sprint"]:
            extractor.extract_session_data(2023, 1, session_type)
        
        assert mock_get_session.call_count == 6
        assert mock_session.load.call_count == 6


class TestExtractLapData:
    """Test extract_lap_data method"""
    
    @patch('src.extract.fastf1_extractor.fastf1.Cache.enable_cache')
    def test_extract_lap_data_success(self, mock_cache):
        """Test successful lap data extraction"""
        # Create mock session with lap data
        mock_session = MagicMock()
        lap_data_df = pd.DataFrame({
            'Driver': ['VER', 'HAM', 'LEC'],
            'LapNumber': [1, 1, 1],
            'LapTime': pd.to_timedelta([90, 91, 92], unit='s'),
            'Stint': [1, 1, 1],
            'Compound': ['SOFT', 'SOFT', 'SOFT'],
            'TyreLife': [1, 1, 1],
            'FreshTyre': [True, True, True],
            'Sector1Time': pd.to_timedelta([30, 30.5, 31], unit='s'),
            'Sector2Time': pd.to_timedelta([30, 30.5, 31], unit='s'),
            'Sector3Time': pd.to_timedelta([30, 30.5, 31], unit='s'),
            'PitInTime': pd.to_timedelta([0, 0, 0], unit='s'),
            'PitOutTime': pd.to_timedelta([0, 0, 0], unit='s'),
            'TrackStatus': ['1', '1', '1'],
            'IsAccurate': [True, True, True]
        })
        mock_session.laps = lap_data_df
        
        extractor = FastF1Extractor()
        result = extractor.extract_lap_data(mock_session)
        
        # Verify result structure
        assert isinstance(result, list)
        assert len(result) == 3
        assert all(isinstance(record, dict) for record in result)
        
        # Verify data content
        assert result[0]['Driver'] == 'VER'
        assert result[0]['LapNumber'] == 1
        
        # Verify time columns are converted to strings
        assert isinstance(result[0]['LapTime'], str)
        assert isinstance(result[0]['Sector1Time'], str)
    
    @patch('src.extract.fastf1_extractor.fastf1.Cache.enable_cache')
    def test_extract_lap_data_empty_dataframe_raises_error(self, mock_cache):
        """Test that extraction with empty lap data raises KeyError"""
        mock_session = MagicMock()
        mock_session.laps = pd.DataFrame()
        
        extractor = FastF1Extractor()
        with pytest.raises(KeyError):
            extractor.extract_lap_data(mock_session)


class TestExtractWeatherData:
    """Test extract_weather_data method"""
    
    @patch('src.extract.fastf1_extractor.fastf1.Cache.enable_cache')
    def test_extract_weather_data_success(self, mock_cache):
        """Test successful weather data extraction"""
        mock_session = MagicMock()
        weather_df = pd.DataFrame({
            'Time': pd.to_timedelta([100, 200], unit='s'),
            'AirTemp': [20.5, 21.0],
            'TrackTemp': [35.0, 36.0],
            'Humidity': [60, 65],
            'Rainfall': [0, 0.1],
            'WindSpeed': [5, 6],
            'WindDirection': [180, 185],
            'Pressure': [1013, 1012]
        })
        mock_session.weather_data = weather_df
        
        extractor = FastF1Extractor()
        result = extractor.extract_weather_data(mock_session)
        
        # Verify result structure
        assert isinstance(result, list)
        assert len(result) == 2
        
        # Verify time is converted to string
        assert isinstance(result[0]['Time'], str)
        assert result[0]['AirTemp'] == 20.5
        assert result[0]['Pressure'] == 1013
    
    @patch('src.extract.fastf1_extractor.fastf1.Cache.enable_cache')
    def test_extract_weather_data_empty_raises_error(self, mock_cache):
        """Test that extraction with empty weather data raises KeyError"""
        mock_session = MagicMock()
        mock_session.weather_data = pd.DataFrame()
        
        extractor = FastF1Extractor()
        with pytest.raises(KeyError):
            extractor.extract_weather_data(mock_session)


class TestExtractTelemetryStream:
    """Test extract_telemetry_stream method"""
    
    @patch('src.extract.fastf1_extractor.fastf1.Cache.enable_cache')
    def test_extract_telemetry_stream_success(self, mock_cache):
        """Test successful telemetry stream extraction"""
        mock_session = MagicMock()
        
        # Setup mock drivers - only one driver for this test
        mock_session.drivers = ['1']
        
        # Create mock lap data
        mock_laps = MagicMock()
        mock_picked_laps = MagicMock()
        mock_laps.pick_driver.return_value = mock_picked_laps
        mock_session.laps = mock_laps
        
        # Mock telemetry data
        telemetry_df = pd.DataFrame({
            'Time': pd.to_timedelta([1, 2], unit='s'),
            'SessionTime': pd.to_timedelta([1, 2], unit='s'),
            'Speed': [200, 205],
            'Throttle': [100, 100],
            'Brake': [0, 0],
            'RPM': [10000, 10500],
            'nGear': [6, 7],
            'DRS': [0, 1],
            'Distance': [100, 150],
            'X': [1.0, 2.0],
            'Y': [3.0, 4.0],
            'Z': [5.0, 6.0]
        })
        mock_picked_laps.empty = False
        mock_picked_laps.get_telemetry.return_value = telemetry_df
        
        extractor = FastF1Extractor()
        result = list(extractor.extract_telemetry_stream(mock_session))
        
        # Verify telemetry extraction
        assert len(result) == 2
        assert all('DriverNumber' in record for record in result)
        assert result[0]['DriverNumber'] == '1'
        assert result[0]['Speed'] == 200
    
    @patch('src.extract.fastf1_extractor.fastf1.Cache.enable_cache')
    def test_extract_telemetry_stream_empty_driver_lap(self, mock_cache):
        """Test telemetry extraction when driver lap is empty"""
        mock_session = MagicMock()
        mock_session.drivers = ['1']
        
        mock_laps = MagicMock()
        mock_picked_laps = MagicMock()
        mock_picked_laps.empty = True
        mock_laps.pick_driver.return_value = mock_picked_laps
        mock_session.laps = mock_laps
        
        extractor = FastF1Extractor()
        result = list(extractor.extract_telemetry_stream(mock_session))
        
        assert result == []
    
    @patch('src.extract.fastf1_extractor.fastf1.Cache.enable_cache')
    @patch('src.extract.fastf1_extractor.logger')
    def test_extract_telemetry_stream_error_handling(self, mock_logger, mock_cache):
        """Test error handling in telemetry extraction"""
        mock_session = MagicMock()
        mock_session.drivers = ['1', '44']
        
        mock_laps = MagicMock()
        mock_picked_laps = MagicMock()
        mock_picked_laps.empty = False
        mock_picked_laps.get_telemetry.side_effect = Exception("API Error")
        mock_laps.pick_driver.return_value = mock_picked_laps
        mock_session.laps = mock_laps
        
        extractor = FastF1Extractor()
        result = list(extractor.extract_telemetry_stream(mock_session))
        
        # Should continue despite error
        assert result == []
        mock_logger.error.assert_called()


class TestExtractMethod:
    """Test main extract method"""
    
    @patch('src.extract.fastf1_extractor.FastF1Extractor.extract_telemetry_stream')
    @patch('src.extract.fastf1_extractor.FastF1Extractor.extract_weather_data')
    @patch('src.extract.fastf1_extractor.FastF1Extractor.extract_lap_data')
    @patch('src.extract.fastf1_extractor.FastF1Extractor.extract_session_data')
    @patch('src.extract.fastf1_extractor.fastf1.Cache.enable_cache')
    def test_extract_returns_all_data_types(self, mock_cache, mock_session_data, 
                                             mock_lap_data, mock_weather_data, mock_telemetry):
        """Test that extract method returns all required data types"""
        # Mock the return values
        mock_session = MagicMock()
        mock_session_data.return_value = mock_session
        mock_lap_data.return_value = [{'Driver': 'VER', 'LapNumber': 1}]
        mock_weather_data.return_value = [{'Time': '1:30', 'AirTemp': 20.5}]
        mock_telemetry.return_value = iter([{'DriverNumber': '1', 'Speed': 200}])
        
        extractor = FastF1Extractor()
        result = extractor.extract(2023, 1, "Race")
        
        # Verify result structure
        assert 'lap_data' in result
        assert 'weather_data' in result
        assert 'telemetry_stream' in result
        
        # Verify data types
        assert isinstance(result['lap_data'], list)
        assert isinstance(result['weather_data'], list)
        assert hasattr(result['telemetry_stream'], '__iter__')
    
    @patch('src.extract.fastf1_extractor.fastf1.get_session')
    @patch('src.extract.fastf1_extractor.fastf1.Cache.enable_cache')
    def test_extract_with_different_seasons_and_rounds(self, mock_cache, mock_get_session):
        """Test extract method with different seasons and rounds"""
        mock_session = MagicMock()
        mock_get_session.return_value = mock_session
        
        # Setup minimal non-empty data to avoid errors
        lap_df = pd.DataFrame({
            'Driver': [],
            'LapNumber': [],
            'LapTime': [],
            'Stint': [],
            'Compound': [],
            'TyreLife': [],
            'FreshTyre': [],
            'Sector1Time': [],
            'Sector2Time': [],
            'Sector3Time': [],
            'PitInTime': [],
            'PitOutTime': [],
            'TrackStatus': [],
            'IsAccurate': []
        })
        
        weather_df = pd.DataFrame({
            'Time': [],
            'AirTemp': [],
            'TrackTemp': [],
            'Humidity': [],
            'Rainfall': [],
            'WindSpeed': [],
            'WindDirection': [],
            'Pressure': []
        })
        
        mock_session.laps = lap_df
        mock_session.weather_data = weather_df
        mock_session.drivers = []
        
        extractor = FastF1Extractor()
        
        # Test different seasons and rounds
        test_cases = [(2023, 1, "Race"), (2022, 10, "Qualifying"), (2021, 5, "FP1")]
        
        for season, round_num, session_type in test_cases:
            extractor.extract(season, round_num, session_type)
        
        assert mock_get_session.call_count == 3
        calls = mock_get_session.call_args_list
        assert calls[0][0] == (2023, 1, "Race")
        assert calls[1][0] == (2022, 10, "Qualifying")
        assert calls[2][0] == (2021, 5, "FP1")