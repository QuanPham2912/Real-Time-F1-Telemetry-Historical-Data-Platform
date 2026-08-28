from unittest.mock import Mock

import pytest

import extract.api_client as api_client_module
from extract.api_client import JolicaClient


def make_response(payload):
	response = Mock()
	response.json.return_value = payload
	return response


def test_extract_result_requests_and_returns_results(monkeypatch):
	response = make_response(
		{"MRData": {"RaceTable": {"Races": [{"Results": [{"position": "1"}]}]}}}
	)
	get = Mock(return_value=response)
	monkeypatch.setattr(api_client_module.requests, "get", get)

	result = JolicaClient().extract_result(2024, 3)

	assert result == [{"position": "1"}]
	get.assert_called_once_with(
		"https://api.jolpi.ca/ergast/f1/2024/3/results.json", timeout=10
	)
	response.raise_for_status.assert_called_once_with()


def test_extract_driver_requests_and_returns_drivers(monkeypatch):
	response = make_response(
		{"MRData": {"DriverTable": {"Drivers": [{"driverId": "max_verstappen"}]}}}
	)
	get = Mock(return_value=response)
	monkeypatch.setattr(api_client_module.requests, "get", get)

	result = JolicaClient().extract_driver(2024)

	assert result == [{"driverId": "max_verstappen"}]
	get.assert_called_once_with(
		"https://api.jolpi.ca/ergast/f1/2024/drivers.json", timeout=10
	)
	response.raise_for_status.assert_called_once_with()


def test_extract_race_requests_and_returns_races(monkeypatch):
	response = make_response(
		{"MRData": {"RaceTable": {"Races": [{"round": "3"}]}}}
	)
	get = Mock(return_value=response)
	monkeypatch.setattr(api_client_module.requests, "get", get)

	result = JolicaClient().extract_race(2024, 3)

	assert result == [{"round": "3"}]
	get.assert_called_once_with("https://api.jolpi.ca/ergast/f1/2024/3.json", timeout=10)
	response.raise_for_status.assert_called_once_with()


def test_extract_combines_race_driver_and_result_data(monkeypatch):
	client = JolicaClient()
	race = [{"round": "3"}]
	drivers = [{"driverId": "max_verstappen"}]
	results = [{"position": "1"}]
	monkeypatch.setattr(client, "extract_race", Mock(return_value=race))
	monkeypatch.setattr(client, "extract_driver", Mock(return_value=drivers))
	monkeypatch.setattr(client, "extract_result", Mock(return_value=results))

	assert client.extract(2024, 3) == {
		"Race": race,
		"Driver": drivers,
		"Results": results,
	}
	client.extract_race.assert_called_once_with(2024, 3)
	client.extract_driver.assert_called_once_with(2024)
	client.extract_result.assert_called_once_with(2024, 3)


def test_extract_result_propagates_http_errors(monkeypatch):
	response = make_response({})
	error = RuntimeError("source unavailable")
	response.raise_for_status.side_effect = error
	monkeypatch.setattr(api_client_module.requests, "get", Mock(return_value=response))

	with pytest.raises(RuntimeError, match="source unavailable"):
		JolicaClient().extract_result(2024, 3)
