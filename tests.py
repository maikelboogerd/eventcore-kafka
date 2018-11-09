import unittest
from collections import namedtuple
from unittest.mock import Mock

from eventcore_kafka import KafkaConsumer


def consumer_settings():
    return {"servers": "localhost", "group_id": 1, "topics": ["hot"]}


Message = namedtuple("Message", (
    "key",
    "value",
    "error",
))


def mock_message() -> Message:
    return Message(
        key=Mock(return_value=b"MyKey"),
        value=Mock(return_value=b'{"data": []}'),
        error=Mock(return_value=False)
    )


def mock_handler(name, subject, data):
    raise NotImplementedError()


class KafkaConsumerTest(unittest.TestCase):
    def test_auto_commit_enabled(self):
        """When auto_commit is enabled on the server, it shouldn't commit."""
        settings = consumer_settings()
        consumer = KafkaConsumer(**settings)
        kafka_consumer = Mock()
        kafka_consumer.poll = Mock(return_value=mock_message())
        kafka_consumer.commit = Mock()

        consumer.process_event = mock_handler
        consumer.kafka_consumer = kafka_consumer

        with self.assertRaises(NotImplementedError):
            consumer.poll_and_process()

        assert kafka_consumer.poll.called
        assert kafka_consumer.commit.called is False

    def test_auto_commit_disabled(self):
        """When auto_commit is disabled on the server, it should commit."""
        consumer = KafkaConsumer(**consumer_settings())
        kafka_consumer = Mock()
        kafka_consumer.poll = Mock(return_value=mock_message())
        kafka_consumer.commit = Mock()

        consumer.process_event = mock_handler
        consumer.kafka_consumer = kafka_consumer

        with self.assertRaises(NotImplementedError):
            consumer.poll_and_process()

        assert kafka_consumer.poll.called
        assert kafka_consumer.commit.called

    def test_consume_commit_after_processing(self):
        """When server_auto_commit disabled don't commit a failed handler."""
        settings = consumer_settings()
        settings["commit_after_processing"] = True
        consumer = KafkaConsumer(**settings)
        kafka_consumer = Mock()
        kafka_consumer.poll = Mock(return_value=mock_message())
        kafka_consumer.commit = Mock()

        consumer.process_event = mock_handler
        consumer.kafka_consumer = kafka_consumer

        with self.assertRaises(NotImplementedError):
            consumer.poll_and_process()

        assert kafka_consumer.poll.called
        assert kafka_consumer.commit.called is False
