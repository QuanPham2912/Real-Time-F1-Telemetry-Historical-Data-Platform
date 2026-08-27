import logging
import re

from metadata.logger import ETLLogger


def remove_logger(name):
	logger = logging.getLogger(name)
	for handler in logger.handlers[:]:
		handler.close()
		logger.removeHandler(handler)
	ETLLogger.loggers.pop(name, None)


def test_get_log_format_formats_log_record():
	formatter = ETLLogger.get_log_format()
	record = logging.LogRecord(
		name="test.logger",
		level=logging.INFO,
		pathname=__file__,
		lineno=1,
		msg="extraction started",
		args=(),
		exc_info=None,
	)

	formatted = formatter.format(record)

	assert re.match(
		r"^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2},\d{3} - INFO - test\.logger - extraction started$",
		formatted,
	)


def test_add_console_handler_adds_stdout_handler():
	logger = logging.getLogger("test.console.handler")
	logger.handlers.clear()
	formatter = ETLLogger.get_log_format()

	ETLLogger.add_console_handler(logger, formatter)

	assert len(logger.handlers) == 1
	assert logger.handlers[0].stream is not None
	assert logger.handlers[0].formatter is formatter
	logger.handlers[0].close()
	logger.handlers.clear()


def test_add_file_handler_creates_directory_and_writes_log(tmp_path):
	logger = logging.getLogger("test.file.handler")
	logger.handlers.clear()
	formatter = ETLLogger.get_log_format()

	ETLLogger.add_file_handler(logger, tmp_path / "nested", "etl.log", formatter)
	logger.setLevel(logging.INFO)
	logger.info("file message")
	logger.handlers[0].flush()

	log_file = tmp_path / "nested" / "etl.log"
	assert log_file.is_file()
	assert "file message" in log_file.read_text(encoding="utf-8")
	assert logger.handlers[0].formatter is formatter
	logger.handlers[0].close()
	logger.handlers.clear()


def test_get_logger_configures_logger_and_returns_singleton(tmp_path):
	name = "test.singleton"

	try:
		logger = ETLLogger.get_logger(
			name=name,
			level=logging.DEBUG,
			log_to_file=False,
			log_dir=tmp_path,
		)
		same_logger = ETLLogger.get_logger(
			name=name,
			level=logging.WARNING,
			log_to_file=True,
			log_dir=tmp_path,
		)

		assert same_logger is logger
		assert logger.level == logging.DEBUG
		assert logger.propagate is False
		assert len(logger.handlers) == 1
		assert isinstance(logger.handlers[0], logging.StreamHandler)
	finally:
		remove_logger(name)
