import unittest
from collections import namedtuple
from unittest.mock import Mock, patch

from confluent_kafka.cimpl import KafkaException

from eventcore_kafka import KafkaConsumer, BlockingKafkaConsumer

from eventcore.exceptions import FatalConsumerError


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
        error=Mock(return_value=False))


def mock_handler(name, subject, data):
    raise NotImplementedError()


class KafkaConsumerTest(unittest.TestCase):
    def test_is_valid_message(self):
        message = mock_message()
        valid = KafkaConsumer.is_valid_message(message)

        assert valid

    def test_is_invalid_message_error(self):
        error = Mock()
        message = Message(
            key=Mock(return_value=b"MyKey"),
            value=Mock(return_value=b'{"data": []}'),
            error=Mock(return_value=error))
        self.assertRaises(
            KafkaException, KafkaConsumer.is_valid_message, message=message)

    def test_is_invalid_message(self):
        valid = KafkaConsumer.is_valid_message(b"")
        assert valid is False

    def test_parse_message(self):
        message = mock_message()
        subject, message_body = KafkaConsumer.parse_message(message)
        assert subject == "MyKey"
        assert len(message_body['data']) == 0


class ConsumerMock(Mock):
    pass


class BlockingKafkaConsumerTest(unittest.TestCase):
    @patch("eventcore_kafka.consumer.kafka")
    def test_commit_message(self, kafka):
        message = mock_message()

        self._patch_consumer(kafka, message)

        settings = consumer_settings()
        consumer = BlockingKafkaConsumer(**settings)
        consumer.process_event = Mock()

        consumer.poll_and_process()

        assert kafka.Consumer.commit.called is True

    @patch("eventcore_kafka.consumer.kafka")
    def test_blocking_message(self, kafka):
        message = mock_message()

        self._patch_consumer(kafka, message)

        settings = consumer_settings()
        consumer = BlockingKafkaConsumer(**settings)
        consumer.process_event = mock_handler

        with self.assertRaises(FatalConsumerError):
            consumer.poll_and_process()

        assert kafka.Consumer.commit.called is False

    @staticmethod
    def _patch_consumer(kafka, message):
        ConsumerMock.poll = Mock(return_value=message)
        ConsumerMock.subscribe = Mock()
        ConsumerMock.commit = Mock()
        kafka.KafkaError._PARTITION_EOF = None
        kafka.KafkaException = NotImplementedError
        kafka.Consumer = ConsumerMock
