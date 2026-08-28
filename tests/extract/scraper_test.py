from unittest.mock import Mock

import pytest

import extract.scraper as scraper_module
from extract.scraper import StatsF1


def make_response(html):
	response = Mock()
	response.text = html
	return response


def test_init_sets_source_name_base_url_and_user_agent():
	client = StatsF1()

	assert client.source_name == "Stats_F1"
	assert client.BaseURL == "https://www.statsf1.com/en"
	assert "User-Agent" in client.headers


def test_clean_gp_title_to_statsf1_slug_normalizes_title():
	client = StatsF1()

	assert client.clean_gp_title_to_statsf1_slug("Grand Prix de São Paulo") == "sao-paulo"
	assert client.clean_gp_title_to_statsf1_slug("GP d'Italie") == "italie"


def test_extract_driver_detail_parses_driver_table(monkeypatch):
	html = """
	<table class="datatable"><tbody>
		<tr><td>Max Verstappen</td><td>Red Bull</td><td>Honda</td><td>1</td></tr>
	</tbody></table>
	"""
	response = make_response(html)
	get = Mock(return_value=response)
	monkeypatch.setattr(scraper_module.requests, "get", get)

	result = StatsF1().extract_driver_detail(2024)

	assert result == [{
		"Driver": "Max Verstappen",
		"Constructor": "Red Bull",
		"Engine_Manufacturer": "Honda",
		"Best_Result": "1",
	}]
	get.assert_called_once_with(
		"https://www.statsf1.com/en/2024/pilotes.aspx",
		headers=StatsF1().headers,
		timeout=10,
	)
	response.raise_for_status.assert_called_once_with()


def test_extract_car_detail_parses_and_normalizes_car_table(monkeypatch):
	html = """
	<table class="datatable"><tbody>
		<tr><td><a>Renault</a> Alpine A524</td><td>unused</td><td>Renault</td></tr>
	</tbody></table>
	"""
	response = make_response(html)
	monkeypatch.setattr(scraper_module.requests, "get", Mock(return_value=response))

	assert StatsF1().extract_car_detail(2024) == [{
		"Constructor": "Renault",
		"Chassis": "Renault Alpine A524",
		"Engine": "Renault",
	}]


def test_extract_constructor_nation_parses_sort_keys(monkeypatch):
	html = """
	<table class="sortable"><tbody>
		<tr><td>Ferrari</td><td sorttable_customkey="ITA">Italy</td><td>1950</td></tr>
	</tbody></table>
	"""
	response = make_response(html)
	monkeypatch.setattr(scraper_module.requests, "get", Mock(return_value=response))

	assert StatsF1().extract_constructor_nation(2024) == [{
		"Constructor": "Ferrari",
		"Nation": "ITA",
		"Started_time": "1950",
	}]


def test_get_france_name_circut_removes_canceled_rounds(monkeypatch):
	html = """
	<table class="ms-schedule-table">
		<tbody class="ms-schedule-table__item"><tr><td><span>GP de Monaco</span></td></tr></tbody>
		<tbody class="canceled"><tr><td><span>GP de France</span></td></tr></tbody>
		<tbody class="ms-schedule-table__item"><tr><td><span>GP d'Italie</span></td></tr></tbody>
	</table>
	"""
	response = make_response(html)
	monkeypatch.setattr(scraper_module.requests, "get", Mock(return_value=response))

	assert StatsF1().get_france_name_circut(2024, 2) == "italie"


def test_extract_detail_gp_information_uses_circuit_slug_and_parses_result(monkeypatch):
	gp_html = """
	<table class="datatable"><tbody>
		<tr><td>1</td><td>1</td><td>Max Verstappen</td><td>RB20</td>
		<td>Honda</td><td>70</td><td>1:30:00</td></tr>
	</tbody></table>
	"""
	client = StatsF1()
	client.get_france_name_circut = Mock(return_value="monaco")
	response = make_response(gp_html)
	get = Mock(return_value=response)
	monkeypatch.setattr(scraper_module.requests, "get", get)

	assert client.extract_detail_GP_information(2024, 1) == [{
		"Position": "1",
		"Driver_number": "1",
		"Driver": "Max Verstappen",
		"Chassis": "RB20",
		"Engine_manufacturer": "Honda",
		"Total_lap": "70",
		"Race_time": "1:30:00",
	}]
	get.assert_called_once_with(
		"https://www.statsf1.com/en/2024/monaco/classement.aspx",
		headers=client.headers,
		timeout=10,
	)


def test_extract_combines_driver_constructor_and_result_data(monkeypatch):
	client = StatsF1()
	driver = [{"Driver": "Max Verstappen"}]
	constructor = [{"Constructor": "Red Bull"}]
	engine_supplier = [{"Engine_Manufacturer": "Honda"}]
	car = [{"Chassis": "RB20"}]
	result = [{"Position": "1"}]
	monkeypatch.setattr(client, "extract_driver_detail", Mock(return_value=driver))
	monkeypatch.setattr(client, "extract_constructor_nation", Mock(return_value=constructor))
	monkeypatch.setattr(client, "extract_engine_supplier_detail", Mock(return_value=engine_supplier))
	monkeypatch.setattr(client, "extract_car_detail", Mock(return_value=car))
	monkeypatch.setattr(client, "extract_detail_GP_information", Mock(return_value=result))

	assert client.extract(2024, 1) == {
		"Driver": driver,
		"Constructor": constructor,
		"Engine_Supplier": engine_supplier,
		"Car": car,
		"Result": result,
	}
	client.extract_driver_detail.assert_called_once_with(2024)
	client.extract_constructor_nation.assert_called_once_with(2024)
	client.extract_engine_supplier_detail.assert_called_once_with(2024)
	client.extract_car_detail.assert_called_once_with(2024)
	client.extract_detail_GP_information.assert_called_once_with(2024, 1)


def test_extract_driver_detail_propagates_http_errors(monkeypatch):
	response = make_response("")
	error = RuntimeError("source unavailable")
	response.raise_for_status.side_effect = error
	monkeypatch.setattr(scraper_module.requests, "get", Mock(return_value=response))

	with pytest.raises(RuntimeError, match="source unavailable"):
		StatsF1().extract_driver_detail(2024)
