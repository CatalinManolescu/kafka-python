from confluent_kafka import Producer, Consumer, KafkaError

import logging
import json
import os

from dotenv import load_dotenv
load_dotenv(override=False)

logging.basicConfig(level=os.environ.get('LOG_LEVEL', 'ERROR'))

# Configuration for Kafka Producer/Consumer
config = {
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

# Producer example
# producer = Producer(**config)
# producer.produce('your_topic', 'your_message')
# producer.flush()

# Consumer example
config.update({
    'group.id': os.environ.get('KAFKA_CONSUMER_GROUP_ID', ''),
    'auto.offset.reset': os.environ.get('KAFKA_CONSUMER_OFFSET_RESET', '')
})
consumer = Consumer(**config)
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
        print('Received message: {}'.format(msg.value().decode('utf-8')))
finally:
    consumer.close()
    