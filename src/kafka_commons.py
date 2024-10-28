import logging
import os
import random
import string
from typing import Optional, Union

from dotenv import load_dotenv


def env_confluent_kafka_config():
    return {
        'bootstrap.servers': os.environ.get('KAFKA_BOOTSTRAP_SERVERS', ''),
        'security.protocol': 'SASL_SSL',
        'sasl.mechanisms': 'SCRAM-SHA-256',
        'sasl.username': os.environ.get('KAFKA_SASL_USER', ''),
        'sasl.password': os.environ.get('KAFKA_SASL_PASSWORD', ''),
        'ssl.ca.location': os.environ.get('KAFKA_TRUSTSTORE', ''),
        'ssl.cipher.suites': 'ALL:@SECLEVEL=0',
        'debug': 'security,broker',
        'ssl.endpoint.identification.algorithm': 'none',  # Disable SSL hostname verification
        # 'enable.ssl.certificate.verification': False
    }


def logging_update_level(log_level: Optional[Union[str, int]] = None) -> None:
    """
    Initialize logging with the given log level.

    The environment variable 'LOG_LEVEL' has the highest priority.
    If it's not set, the function uses the 'log_level' parameter.
    Defaults to 'INFO' level if neither is provided.

    :param log_level: Log level as a string (e.g., 'DEBUG') or an integer.
    """
    log_level_env_val = os.environ.get('LOG_LEVEL', None)
    if log_level_env_val is not None:
        log_level = log_level_env_val

    if log_level is None:
        log_level = 'INFO'  # Default level

    if isinstance(log_level, int):
        numeric_level = log_level
    elif isinstance(log_level, str):
        log_level = log_level.upper()
        numeric_level = getattr(logging, log_level, None)
        if not isinstance(numeric_level, int):
            raise ValueError(f"Invalid log level: {log_level}")
    else:
        raise TypeError(f"Log level must be a string or an integer, got {type(log_level)}")

    # Update root logger's level
    logging.getLogger().setLevel(numeric_level)


def logging_init(log_level: Optional[Union[str, int]] = None) -> None:
    """
    Initialize logging configuration with the given log level.

    :param log_level: Log level as a string (e.g., 'DEBUG') or an integer.
    """

    logging.basicConfig(format="%(asctime)s [%(levelname)s] %(message)s")
    logging_update_level(log_level)


def logging_get_file_logger(name: str, prefix: Optional[str] = None, log_dir: Optional[str] = None) -> logging.Logger:
    # Determine log filename with optional prefix
    log_filename = f"{prefix}_{name}.log" if prefix and prefix.strip() else f"{name}.log"

    # Set default log directory if not provided
    if not log_dir:
        project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
        log_dir = os.path.join(project_root, 'logs')
        os.makedirs(log_dir, exist_ok=True)  # Ensure log directory exists

    # NOTE: If log_dir is specified, user must ensure it exists
    # Expand user home directory symbol (~) if present
    expanded_dir = os.path.expanduser(log_dir)
    # Convert relative paths like '../../' to absolute paths
    absolute_dir = os.path.abspath(expanded_dir)

    # Full path for the log file
    log_file_path = os.path.join(absolute_dir, log_filename)

    # Set up file handler with formatting
    file_handler = logging.FileHandler(log_file_path)
    file_handler.setLevel(logging.INFO)
    file_handler.setFormatter(logging.Formatter("%(asctime)s %(message)s"))

    logger = logging.getLogger(name)
    logger.setLevel(logging.INFO)
    logger.addHandler(file_handler)

    return logger


def load_env(env_path: Optional[str] = None) -> None:
    if env_path is None:
        env_path = '.env'
        if not os.path.exists(env_path):
            return  # Exit if default .env file does not exist
    elif not os.path.exists(env_path):
        raise FileNotFoundError(f"Environment file '{env_path}' does not exist")

    logging.info(f"Loading environment variables from '{env_path}'")
    load_dotenv(dotenv_path=env_path, override=False)


# Generate a random key
def generate_key(size: Optional[int] = 8):
    return ''.join(random.choices(string.ascii_letters + string.digits, k=size))


def generate_message(message_size: int, prefix: Optional[str] = None):
    if prefix:
        prefix_bytes = prefix
    else:
        prefix_bytes = ''

    prefix_length = len(prefix_bytes)
    if prefix_length > message_size:
        # Truncate the prefix to fit the message_size
        prefix_bytes = prefix_bytes[:message_size]
        random_payload = ''
    else:
        payload_size = message_size - prefix_length
        random_payload = ''.join(random.choices(string.ascii_letters + string.digits, k=payload_size))
    return prefix_bytes + random_payload
