from unittest.mock import Mock

import pytest

import extract.base as base_module
from extract.base import Extractor


class ConcreteExtractor(Extractor):
	def __init__(self, source_name, data=None, error=None):
		super().__init__(source_name)
		self.data = data if data is not None else {}
		self.error = error

	def extract(self, **kwargs):
		if self.error is not None:
			raise self.error
		return self.data


def test_init_sets_source_name():
	extractor = ConcreteExtractor("test-source")

	assert extractor.source_name == "test-source"


def test_run_returns_extracted_data_and_logs_success(monkeypatch):
	logger = Mock()
	monkeypatch.setattr(base_module, "logger", logger)
	data = {"laps": [{"lap": 1}]}
	extractor = ConcreteExtractor("test-source", data=data)

	result = extractor.run(year=2024)

	assert result == data
	logger.info.assert_any_call("Starting extraction process: [test-source]")
	logger.info.assert_any_call("[test-source] Successfully extracted 1 rows")


def test_run_logs_failure_and_reraises_exception(monkeypatch):
	logger = Mock()
	monkeypatch.setattr(base_module, "logger", logger)
	error = RuntimeError("source unavailable")
	extractor = ConcreteExtractor("test-source", error=error)

	with pytest.raises(RuntimeError, match="source unavailable") as raised:
		extractor.run()

	assert raised.value is error
	logger.exception.assert_called_once_with(
		"[test-source] Extraction failed: source unavailable"
	)
