from confluent_kafka import Consumer, KafkaError

from src.kafka_commons import *

load_env()
logging_init()

# Configuration for Kafka Producer/Consumer
kafka_config = env_confluent_kafka_config()

# Producer example
# producer = Producer(**config)
# producer.produce(os.environ.get('KAFKA_PRODUCER_TOPIC', ''), 'some test message - it works')
# producer.flush()

# Consumer example
kafka_config.update({
    'group.id': os.environ.get('KAFKA_CONSUMER_GROUP_ID', ''),
    'auto.offset.reset': os.environ.get('KAFKA_CONSUMER_OFFSET_RESET', '')
})
consumer = Consumer(**kafka_config)
consumer.subscribe(os.environ.get('KAFKA_CONSUMER_TOPICS', '').split(','))

try:
    while True:
        msg = consumer.poll(1.0)
        if msg is None:
            continue
        if msg.error():
            if msg.error().code() == KafkaError._PARTITION_EOF:
                continue
            else:
                print(msg.error())
                break
        print(f'> {msg.value()}')
finally:
    consumer.close()
