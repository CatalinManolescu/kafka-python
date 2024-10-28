# tests/test_kafka_commons.py

import logging
import os
import unittest
from unittest.mock import patch

from src.kafka_commons import (
    env_confluent_kafka_config,
    load_env,
    generate_key,
    generate_message,
    logging_init,
    logging_update_level,
)


class TestKafkaCommons(unittest.TestCase):
    def test_env_confluent_kafka_config(self):
        # Mock environment variables
        with patch.dict(os.environ, {
            'KAFKA_BOOTSTRAP_SERVERS': 'localhost:9092',
            'KAFKA_SASL_USER': 'test_user',
            'KAFKA_SASL_PASSWORD': 'test_password',
            'KAFKA_TRUSTSTORE': '/path/to/truststore.pem',
        }):
            config = env_confluent_kafka_config()
            self.assertEqual(config['bootstrap.servers'], 'localhost:9092')
            self.assertEqual(config['sasl.username'], 'test_user')
            self.assertEqual(config['sasl.password'], 'test_password')
            self.assertEqual(config['ssl.ca.location'], '/path/to/truststore.pem')
            self.assertEqual(config['security.protocol'], 'SASL_SSL')
            self.assertEqual(config['sasl.mechanisms'], 'SCRAM-SHA-256')
            self.assertEqual(config['ssl.cipher.suites'], 'ALL:@SECLEVEL=0')
            self.assertEqual(config['debug'], 'security,broker')
            self.assertEqual(config['ssl.endpoint.identification.algorithm'], 'none')

    @patch('os.path.exists')
    @patch('src.kafka_commons.load_dotenv')
    def test_load_env_existing_file(self, mock_load_dotenv, mock_exists):
        mock_exists.return_value = True
        env_path = '.env'
        load_env(env_path)
        mock_load_dotenv.assert_called_with(dotenv_path=env_path, override=False)

    @patch('os.path.exists')
    def test_load_env_missing_file(self, mock_exists):
        mock_exists.return_value = False
        with self.assertRaises(FileNotFoundError):
            load_env('nonexistent.env')

    @patch('os.path.exists')
    @patch('src.kafka_commons.load_dotenv')
    def test_load_env_default_no_file(self, mock_load_dotenv, mock_exists):
        mock_exists.return_value = False
        load_env()
        mock_load_dotenv.assert_not_called()

    def test_generate_key_default(self):
        key = generate_key()
        self.assertEqual(len(key), 8)
        self.assertIsInstance(key, str)

    def test_generate_key_custom_size(self):
        size = 16
        key = generate_key(size=size)
        self.assertEqual(len(key), size)
        self.assertIsInstance(key, str)

    def test_generate_message_no_prefix(self):
        message_size = 50
        message = generate_message(message_size)
        self.assertEqual(len(message), message_size)
        self.assertIsInstance(message, str)

    def test_generate_message_with_prefix(self):
        message_size = 50
        prefix = "TestPrefix:"
        message = generate_message(message_size, prefix=prefix)
        self.assertEqual(len(message), message_size)
        self.assertTrue(message.startswith(prefix))

    def test_generate_message_prefix_longer_than_size(self):
        message_size = 10
        prefix = "ThisIsAVeryLongPrefix"
        message = generate_message(message_size, prefix=prefix)
        self.assertEqual(len(message), message_size)
        self.assertEqual(message, prefix[:message_size])

    @patch('src.kafka_commons.logging.getLogger')
    def test_logging_update_level_env_variable_priority(self, mock_getLogger):
        with patch.dict(os.environ, {'LOG_LEVEL': 'DEBUG'}):
            logging_update_level()
            mock_getLogger.return_value.setLevel.assert_called_with(logging.DEBUG)

    @patch('src.kafka_commons.logging.getLogger')
    def test_logging_update_level_parameter_used_if_no_env(self, mock_getLogger):
        with patch.dict(os.environ, {}, clear=True):
            logging_update_level('WARNING')
            mock_getLogger.return_value.setLevel.assert_called_with(logging.WARNING)

    @patch('src.kafka_commons.logging.getLogger')
    def test_logging_update_level_default_to_INFO(self, mock_getLogger):
        with patch.dict(os.environ, {}, clear=True):
            logging_update_level()
            mock_getLogger.return_value.setLevel.assert_called_with(logging.INFO)

    @patch('src.kafka_commons.logging.getLogger')
    def test_logging_update_level_invalid_log_level(self, mock_getLogger):
        with patch.dict(os.environ, {'LOG_LEVEL': 'INVALID'}):
            with self.assertRaises(ValueError):
                logging_update_level()

    @patch('src.kafka_commons.logging.getLogger')
    def test_logging_update_level_invalid_type(self, mock_getLogger):
        with self.assertRaises(TypeError):
            logging_update_level(log_level=['NOT_A_STRING_OR_INT'])

    @patch('src.kafka_commons.logging_update_level')
    @patch('src.kafka_commons.logging.basicConfig')
    def test_logging_init_calls_basicConfig_and_update_level(self, mock_basicConfig, mock_logging_update_level):
        logging_init('INFO')
        mock_basicConfig.assert_called_with(format="%(asctime)s [%(levelname)s] %(message)s")
        mock_logging_update_level.assert_called_with('INFO')

    @patch('src.kafka_commons.logging_update_level')
    @patch('src.kafka_commons.logging.basicConfig')
    def test_logging_init_without_parameters(self, mock_basicConfig, mock_logging_update_level):
        logging_init()
        mock_basicConfig.assert_called_with(format="%(asctime)s [%(levelname)s] %(message)s")
        mock_logging_update_level.assert_called_with(None)


if __name__ == '__main__':
    unittest.main()
