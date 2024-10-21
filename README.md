# Kafka Python Examples

## Dev

```shell
python -m venv .venv
```

Activate virtual env

```shell
source .venv/bin/activate # Linux shell (Bash, ZSH, etc.) only
```

Install dependencies

```shell
python -m pip install -r requirements.txt
```

Deactivate virtual env

```shell
deactivate
```

## Usage

NOTE: Environment variables can be defined in `.env` file

### Environment Variables

| Variable                      | Description                                |
|-------------------------------|--------------------------------------------|
| `KAFKA_BOOTSTRAP_SERVERS`     | dns/ip and port. Ex <private-node-ip>:9093 |
| `KAFKA_TRUSTSTORE`            | path to truststore in pem format           |
| `KAFKA_SASL_USER`             | sasl username                              |
| `KAFKA_SASL_PASSWORD`         | sasl password                              |
| `KAFKA_CONSUMER_GROUP_ID`     | consumer group name                        |
| `KAFKA_CONSUMER_OFFSET_RESET` | value for auto.offset.reset                |
| `KAFKA_CONSUMER_TOPICS`       | topics to read from                        |
| `LOG_LEVEL`                   | Log Level. Default ERROR                   |

## Debug

### Debug with kafka docker 

```shell
docker run --rm -it --user $(id -u):$(id -g) -v $PWD:/workspace -w /workspace catalinm/kafka:2.13-3.4.1 bash
```

#### Test Console Consumer

```shell
kafka-console-consumer.sh --bootstrap-server your.kafka.broker:9093 --topic your_topic --consumer.config consumer.properties
```