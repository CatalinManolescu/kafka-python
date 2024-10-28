import logging
import os
import ssl

from dotenv import load_dotenv
from kafka import KafkaConsumer

load_dotenv(override=False)

logging.basicConfig(level=os.environ.get('LOG_LEVEL', 'ERROR'))

ctx = ssl.create_default_context()
ctx.check_hostname = False
ctx.load_verify_locations(os.environ.get('KAFKA_TRUSTSTORE', ''))
ctx.options &= ~ssl.OP_NO_SSLv3
ctx.set_ciphers('ALL:@SECLEVEL=0')
# ctx.set_ciphers('DHE-DSS-AES256-GCM-SHA384')

# Kafka configuration
kafka_config = {
    'bootstrap_servers': os.environ.get('KAFKA_BOOTSTRAP_SERVERS', ''),
    'security_protocol': 'SASL_SSL',
    'sasl_mechanism': 'SCRAM-SHA-256',
    'sasl_plain_username': os.environ.get('KAFKA_SASL_USER', ''),
    'sasl_plain_password': os.environ.get('KAFKA_SASL_PASSWORD', ''),
    'ssl_context': ctx
}

# Producer
# producer = KafkaProducer(**kafka_config)
# producer.send(os.environ.get('KAFKA_PRODUCER_TOPIC', ''), 'some test message - it works')
# producer.flush()

# Consumer
consumer = KafkaConsumer(
    os.environ.get('KAFKA_CONSUMER_TOPICS', ''),
    **kafka_config,
    auto_offset_reset=os.environ.get('KAFKA_CONSUMER_OFFSET_RESET', ''),
    group_id=os.environ.get('KAFKA_CONSUMER_GROUP_ID', '')
)

for message in consumer:
    print(f"Received message: {message.value}")
