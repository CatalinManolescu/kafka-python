# tests/test_load_test.py

import unittest
from unittest.mock import patch, MagicMock

from src import kafka_load_test
from src.kafka_commons import generate_key, generate_message

class TestLoadTest(unittest.TestCase):

    @patch('src.kafka_load_test.parse_args')
    @patch('src.kafka_load_test.load_env')
    @patch('src.kafka_load_test.logging_init')
    @patch('src.kafka_load_test.load_config')
    @patch('threading.Thread')
    def test_main_function(self, mock_thread, mock_load_config, mock_logging_init, mock_load_env, mock_parse_args):
        # Mock arguments
        args = MagicMock()
        args.env = None
        args.config = None
        args.send_mode = None
        mock_parse_args.return_value = args

        # Mock configuration
        mock_load_config.return_value = {
            'num_keys': 2,
            'min_messages_per_key': 1,
            'max_messages_per_key': 2,
            'message_size': 20,
            'num_parallel_runs': 1,
            'send_mode': 'per_key',
        }

        # Mock thread
        mock_thread_instance = MagicMock()
        mock_thread.return_value = mock_thread_instance

        # Run main
        kafka_load_test.main()

        # Assertions
        mock_parse_args.assert_called_once()
        mock_load_env.assert_called_once_with(None)
        mock_logging_init.assert_called_once()
        mock_load_config.assert_called_once_with(None)
        mock_thread.assert_called_once()
        mock_thread_instance.start.assert_called_once()
        mock_thread_instance.join.assert_called_once()

    @patch('src.kafka_load_test.Producer')
    @patch('src.kafka_load_test.env_confluent_kafka_config')
    def test_producer_worker(self, mock_env_config, mock_producer_class):
        # Mock environment configuration
        mock_env_config.return_value = {}

        # Mock producer instance
        mock_producer = MagicMock()
        mock_producer_class.return_value = mock_producer

        # Define test configuration
        config = {
            'num_keys': 6,
            'min_messages_per_key': 3,
            'max_messages_per_key': 20,
            'message_size': 45,
            'send_mode': 'round_robin',
        }

        # Mock environment variable for topic
        with patch.dict('os.environ', {'KAFKA_PRODUCER_TOPIC': 'test_topic'}):
            # Run producer_worker
            kafka_load_test.producer_worker(config)

        # Assertions
        mock_env_config.assert_called_once()
        mock_producer_class.assert_called_once_with(**mock_env_config.return_value)
        mock_producer.produce.assert_called()
        mock_producer.poll.assert_called()
        mock_producer.flush.assert_called_once()


    @patch('src.kafka_load_test.stop_event')
    @patch('src.kafka_load_test.logging')
    def test_sigint_handler(self, mock_logging, mock_stop_event):
        # Simulate SIGINT
        kafka_load_test.sigint_handler(None, None)
        # Assertions
        mock_logging.info.assert_called_with('Received SIGINT, stopping...')
        mock_stop_event.set.assert_called_once()

if __name__ == '__main__':
    unittest.main()