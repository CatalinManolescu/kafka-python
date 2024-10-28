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

| Variable                       | Description                                |
|--------------------------------|--------------------------------------------|
| `KAFKA_BOOTSTRAP_SERVERS`      | dns/ip and port. Ex <private-node-ip>:9093 |
| `KAFKA_TRUSTSTORE`             | path to truststore in pem format           |
| `KAFKA_SASL_USER`              | sasl username                              |
| `KAFKA_SASL_PASSWORD`          | sasl password                              |
| `KAFKA_CONSUMER_GROUP_ID`      | consumer group name                        |
| `KAFKA_CONSUMER_OFFSET_RESET`  | value for auto.offset.reset                |
| `KAFKA_CONSUMER_TOPICS`        | topics to read from                        |
| `KAFKA_PRODUCER_TOPIC`         | topic to write to                          |
| `LOG_LEVEL`                    | Log Level. Default ERROR                   |

### Tests

#### Run Tests

```shell
./scripts/run_tests.sh
```

### Exec

#### Run Load Test

Exec: `src.kafka_load_test`

Params:
- `--env` : Path to file containing the Environment variables. Defaults to `.env`
- `--config` : Path to JSON config file
- `--send-mode` : Message sending mode: per_key or round_robin
- `--run-once` : Run the producer only once. 
- `--log-dir` : Path to log folder. If not specified, the project/logs folder will be used. 

Example config file

```json
{
    'num_keys': 10,
    'min_messages_per_key': 3,
    'max_messages_per_key': 10,
    'message_size': 100,  # in bytes
    'num_parallel_runs': 1,
    'send_mode': 'per_key',     # 'per_key' or 'round_robin'
}
```

```shell
python -m src.kafka_load_test --config config/config.json
```

## Debug

### Debug with kafka docker 

```shell
docker run --rm -it --user $(id -u):$(id -g) -v $PWD:/workspace -w /workspace catalinm/kafka:2.13-3.4.1 bash
```

#### Test Console Consumer

```shell
kafka-console-consumer.sh --bootstrap-server your.kafka.broker:9093 --topic your_topic --consumer.config consumer.properties
```