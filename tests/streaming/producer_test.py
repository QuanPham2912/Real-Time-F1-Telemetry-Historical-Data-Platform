from unittest.mock import MagicMock, patch

from metadata.f1_topic import F1Topic
from streaming.base import baseProducer
from streaming.producer import KafkaProducer


class TestKafkaProducer:
	@staticmethod
	def build_producer(mock_client):
		producer = KafkaProducer.__new__(KafkaProducer)
		producer.producer = mock_client
		return producer

	@patch("streaming.producer.KafkaClient")
	def test_is_base_producer_and_initializes_kafka_client_from_environment(
		self, mock_client, monkeypatch
	):
		monkeypatch.setenv("KAFKA_BOOTSTRAP_SERVERS", "broker-from-env:9092")
		with patch("streaming.producer.load_dotenv") as mock_load_dotenv:
			producer = KafkaProducer()

		assert isinstance(producer, baseProducer)
		mock_load_dotenv.assert_called_once_with()
		mock_client.assert_called_once()
		assert mock_client.call_args.kwargs["bootstrap_servers"] == "broker-from-env:9092"
		assert mock_client.call_args.kwargs["batch_size"] == 32768
		assert mock_client.call_args.kwargs["linger_ms"] == 10

	@patch("streaming.producer.KafkaClient")
	def test_initializes_with_default_bootstrap_server_when_environment_is_missing(
		self, mock_client, monkeypatch
	):
		monkeypatch.delenv("KAFKA_BOOTSTRAP_SERVERS", raising=False)
		with patch("streaming.producer.load_dotenv"):
			KafkaProducer()

		assert mock_client.call_args.kwargs["bootstrap_servers"] == "kafka1:19092"

	@patch("streaming.producer.KafkaClient")
	def test_explicit_bootstrap_server_overrides_environment(self, mock_client, monkeypatch):
		monkeypatch.setenv("KAFKA_BOOTSTRAP_SERVERS", "broker-from-env:9092")
		with patch("streaming.producer.load_dotenv") as mock_load_dotenv:
			KafkaProducer(bootstrap_servers="explicit-broker:9092")

		mock_load_dotenv.assert_not_called()
		assert mock_client.call_args.kwargs["bootstrap_servers"] == "explicit-broker:9092"

	def test_send_sends_topic_value_message_and_key(self):
		mock_client = MagicMock()
		producer = self.build_producer(mock_client)
		message = {"driver": "VER"}

		producer.send(F1Topic.FASTF1_TELEMETRY, message, key="car-1")

		mock_client.send.assert_called_once_with(
			F1Topic.FASTF1_TELEMETRY.value,
			key="car-1",
			value=message,
		)

	def test_send_many_sends_all_messages_with_built_keys(self):
		mock_client = MagicMock()
		producer = self.build_producer(mock_client)
		messages = [{"driver": "VER", "lap": 1}, {"driver": "HAM", "lap": 2}]

		producer.send_many(
			F1Topic.FASTF1_LAP,
			messages=messages,
			key_builder=lambda message: f"{message['driver']}-{message['lap']}",
		)

		assert mock_client.send.call_args_list[0].kwargs == {
			"key": "VER-1",
			"value": messages[0],
		}
		assert mock_client.send.call_args_list[1].kwargs == {
			"key": "HAM-2",
			"value": messages[1],
		}
		assert mock_client.send.call_count == 2

	def test_send_many_without_key_builder_uses_no_key(self):
		mock_client = MagicMock()
		producer = self.build_producer(mock_client)

		producer.send_many(F1Topic.FASTF1_LAP, messages=[{"lap": 1}])

		mock_client.send.assert_called_once_with(
			F1Topic.FASTF1_LAP.value,
			key=None,
			value={"lap": 1},
		)

	def test_send_many_with_no_messages_does_not_send(self):
		mock_client = MagicMock()
		producer = self.build_producer(mock_client)

		producer.send_many(F1Topic.FASTF1_LAP)

		mock_client.send.assert_not_called()

	def test_flush_and_close_delegate_to_kafka_client(self):
		mock_client = MagicMock()
		producer = self.build_producer(mock_client)

		producer.flush()
		producer.close()

		mock_client.flush.assert_called_once_with()
		mock_client.close.assert_called_once_with()
