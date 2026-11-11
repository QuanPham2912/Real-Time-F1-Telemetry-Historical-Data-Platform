from unittest.mock import Mock

import pytest

import extract.api_client as api_client_module
from extract.api_client import Endpoint, JolpicaClient


def make_response(payload):
	response = Mock()
	response.json.return_value = payload
	return response


def test_fetch_requests_endpoint_and_applies_lineage_tags(monkeypatch):
	response = make_response(
		{"MRData": {"RaceTable": {"Races": [{"Results": [{"position": "1"}]}]}}}
	)
	get = Mock(return_value=response)
	monkeypatch.setattr(api_client_module.requests, "get", get)

	client = JolpicaClient()
	result = client.fetch(
		Endpoint(
			key="Result",
			path="{season}/{round}/results.json",
			records_path=["RaceTable", "Races", 0, "Results"],
			tags=["race_id", "source", "season"],
		),
		2024,
		3,
	)

	assert result == [{
		"position": "1",
		"race_id": "2024_3",
		"source": "jolpica_api",
		"season": 2024,
	}]
	get.assert_called_once_with(
		"https://api.jolpi.ca/ergast/f1/2024/3/results.json", timeout=10
	)
	response.raise_for_status.assert_called_once_with()


def test_fetch_only_adds_configured_lineage_tags(monkeypatch):
	response = make_response(
		{"MRData": {"DriverTable": {"Drivers": [{"driverId": "max_verstappen"}]}}}
	)
	get = Mock(return_value=response)
	monkeypatch.setattr(api_client_module.requests, "get", get)
	client = JolpicaClient()
	endpoint = Endpoint(
		key="Driver",
		path="{season}/drivers.json",
		records_path=["DriverTable", "Drivers"],
		tags=["source"],
		cache_per_season=False,
	)

	result = client.fetch(endpoint, 2024)

	assert result == [{"driverId": "max_verstappen", "source": "jolpica_api"}]
	get.assert_called_once_with(
		"https://api.jolpi.ca/ergast/f1/2024/drivers.json", timeout=10
	)
	response.raise_for_status.assert_called_once_with()


def test_extract_uses_configured_endpoints(monkeypatch):
	client = JolpicaClient()
	endpoints = (
		Endpoint("Race", "{season}/{round}.json", ["RaceTable", "Races"]),
		Endpoint("Driver", "{season}/drivers.json", ["DriverTable", "Drivers"]),
	)
	results = {"Race": [{"round": "3"}], "Driver": [{"driverId": "max_verstappen"}]}
	monkeypatch.setattr(client, "convert_endpoints", Mock(return_value=endpoints))
	monkeypatch.setattr(
		client,
		"fetch",
		Mock(side_effect=[results["Race"], results["Driver"]]),
	)

	assert client.extract(2024, 3) == results
	assert client.fetch.call_args_list == [
		((endpoints[0], 2024, 3),),
		((endpoints[1], 2024, 3),),
	]


def test_fetch_propagates_http_errors(monkeypatch):
	response = make_response({})
	error = RuntimeError("source unavailable")
	response.raise_for_status.side_effect = error
	monkeypatch.setattr(api_client_module.requests, "get", Mock(return_value=response))

	with pytest.raises(RuntimeError, match="source unavailable"):
		JolpicaClient().fetch(
			Endpoint("Race", "{season}/{round}.json", ["RaceTable", "Races"]),
			2024,
			3,
		)
