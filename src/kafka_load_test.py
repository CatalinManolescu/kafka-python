import argparse
import json
import signal
import threading
from datetime import datetime

from confluent_kafka import Producer

from src.kafka_commons import *

LOAD_DEFAULT_CONFIG = {
    'num_keys': 10,
    'min_messages_per_key': 3,
    'max_messages_per_key': 10,
    'message_size': 100,  # in bytes
    'num_parallel_runs': 1,
    'send_mode': 'per_key',  # 'per_key' or 'round_robin'
}

stop_event = threading.Event()
process_start_time = datetime.now()
logger_kafka_messages = None


# Parse command-line arguments
def parse_args():
    parser = argparse.ArgumentParser(description='Kafka Load Test')
    parser.add_argument('--env', type=str, help='Path to environment file')
    parser.add_argument('--config', type=str, help='Path to JSON config file')
    parser.add_argument('--send-mode', type=str, choices=['per_key', 'round_robin'],
                        help='Message sending mode: per_key or round_robin')
    parser.add_argument('--run-once', action='store_true', help='Run the producer only once')
    parser.add_argument('--log-dir', type=str, help='Path to log files')
    return parser.parse_args()


# Load configuration from JSON file
def load_config(config_path: Optional[str] = None):
    config = LOAD_DEFAULT_CONFIG.copy()
    if config_path and os.path.exists(config_path):
        with open(config_path, 'r') as f:
            user_config = json.load(f)
        config.update({k: v for k, v in user_config.items() if v is not None})
    return config


# Signal handler for SIGINT
def sigint_handler(signum, frame):
    logging.info("Received SIGINT, stopping...")
    stop_event.set()


# Callback function to get delivery reports
def on_message_send(err, msg):
    global logger_kafka_messages
    msg_value_small = msg.value()[0:40]
    if err is not None:
        logging.error(f"Delivery failed for Message {msg_value_small}: {err}")
    else:
        logger_kafka_messages.info(
            f"Message {msg_value_small} successfully produced to {msg.topic()} [{msg.partition()}] at offset {msg.offset()}")


# Producer worker
def producer_worker(config, run_once):
    kafka_config = env_confluent_kafka_config()
    producer = Producer(**kafka_config)

    topic = os.environ.get('KAFKA_PRODUCER_TOPIC', '')

    num_keys = config['num_keys']
    min_messages = config['min_messages_per_key']
    max_messages = config['max_messages_per_key']
    message_size = config['message_size']
    send_mode = config['send_mode']

    logging.info(f"Starting producer thread with topic '{topic}' and send_mode '{send_mode}'")

    while not stop_event.is_set():
        keys = [generate_key() for _ in range(num_keys)]
        logging.debug(f"Generated keys: {keys}")

        messages_per_key = {}
        for key in keys:
            num_messages = random.randint(min_messages, max_messages)
            messages = []
            for seq_num in range(num_messages):
                value = generate_message(message_size=message_size, prefix=f"{seq_num}:{key}:")
                messages.append((key.encode('utf-8'), value))
            messages_per_key[key] = messages
            logging.debug(f"{num_messages} messages for key {key}")

        if send_mode == 'per_key':
            # Send all messages per key
            for key in keys:
                if stop_event.is_set():
                    break
                for key_bytes, value in messages_per_key[key]:
                    if stop_event.is_set():
                        break
                    producer.produce(topic, key=key_bytes, value=value, callback=on_message_send)
                    producer.poll(0)
        elif send_mode == 'round_robin':
            # Send one message per key in round-robin fashion
            iterators = [iter(messages_per_key[key]) for key in keys]
            while iterators and not stop_event.is_set():
                # Copy iterators list to avoid modifying it while iterating
                for it in iterators[:]:
                    if stop_event.is_set():
                        break
                    try:
                        key_bytes, value = next(it)
                        producer.produce(topic, key=key_bytes, value=value, callback=on_message_send)
                        producer.poll(0)
                    except StopIteration:
                        iterators.remove(it)
        else:
            logging.error(f"Invalid send_mode: {send_mode}")
            raise ValueError(f"Invalid send_mode: {send_mode}")

        producer.flush()
        logging.info("Producer flush completed")

        if run_once:
            break


def main():
    # Set up signal handler for SIGINT
    signal.signal(signal.SIGINT, sigint_handler)
    args = parse_args()

    load_env(args.env)
    logging_update_level()

    # Get the current date and time in the format {year}{month}{day}{hour}{min}{sec}
    current_time = process_start_time.strftime('%Y%m%d%H%M%S')
    # Create logger for message push logs
    global logger_kafka_messages
    logger_kafka_messages = logging_get_file_logger("msg_push", current_time, args.log_dir)

    config = load_config(args.config)
    if args.send_mode:
        config['send_mode'] = args.send_mode

    threads = []
    for i in range(config['num_parallel_runs']):
        t = threading.Thread(target=producer_worker, args=(config, args.run_once), name=f"ProducerThread-{i}")
        t.start()
        logging.info(f"Started thread {t.name}")
        threads.append(t)

    # Wait for threads to finish
    try:
        for t in threads:
            t.join()
            logging.info(f"Thread {t.name} has finished")
    except KeyboardInterrupt:
        logging.info("Main thread received KeyboardInterrupt, setting stop event.")
        stop_event.set()
        for t in threads:
            t.join()
            logging.info(f"Thread {t.name} has finished")

    logging.info("All threads have completed")


if __name__ == '__main__':
    logging_init(logging.INFO)
    logger_msg_push = logging.getLogger()
    main()
