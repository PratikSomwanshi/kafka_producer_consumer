# Make sure to install confluent-kafka python package
# pip install confluent-kafka

import threading
from confluent_kafka import DeserializingConsumer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroDeserializer
from confluent_kafka.serialization import StringDeserializer
import os
from dotenv import load_dotenv
from pathlib import Path

load_dotenv(Path(__file__).with_name(".env"))


def required_env(name):
    value = os.getenv(name)
    if not value:
        raise RuntimeError(f"Missing required environment variable: {name}")
    return value

CA_PATH = os.path.abspath(os.getenv("KAFKA_CA_PATH", "ca_re.pem"))
KAFKA_BOOTSTRAP_SERVERS = required_env("KAFKA_BOOTSTRAP_SERVERS")
KAFKA_USERNAME = required_env("KAFKA_USERNAME")
KAFKA_PASSWORD = required_env("KAFKA_PASSWORD")
SCHEMA_REGISTRY_URL = required_env("SCHEMA_REGISTRY_URL")
SCHEMA_REGISTRY_USERNAME = required_env("SCHEMA_REGISTRY_USERNAME")
SCHEMA_REGISTRY_PASSWORD = required_env("SCHEMA_REGISTRY_PASSWORD")

# Define Kafka configuration
kafka_config = {
    'bootstrap.servers': KAFKA_BOOTSTRAP_SERVERS,
    'sasl.mechanisms': 'SCRAM-SHA-256',
    'security.protocol': 'SASL_SSL',
    'sasl.username': KAFKA_USERNAME,
    'sasl.password': KAFKA_PASSWORD,
    'ssl.ca.location': CA_PATH,
    'group.id': 'group1',
    'auto.offset.reset': 'earliest'
}


# Create a Schema Registry client
schema_registry_client = SchemaRegistryClient({
  'url': SCHEMA_REGISTRY_URL,
  'basic.auth.user.info': '{}:{}'.format(SCHEMA_REGISTRY_USERNAME, SCHEMA_REGISTRY_PASSWORD)
})
# Fetch the latest Avro schema for the value
subject_name = 'retail_data_dev-value'
schema_str = schema_registry_client.get_version(subject_name, 1).schema.schema_str

# Create Avro Deserializer for the value
key_deserializer = StringDeserializer('utf_8')
avro_deserializer = AvroDeserializer(schema_registry_client, schema_str)

# Define the DeserializingConsumer
consumer = DeserializingConsumer({
    'bootstrap.servers': kafka_config['bootstrap.servers'],
    'security.protocol': kafka_config['security.protocol'],
    'sasl.mechanisms': kafka_config['sasl.mechanisms'],
    'sasl.username': kafka_config['sasl.username'],
    'sasl.password': kafka_config['sasl.password'],
    'ssl.ca.location': CA_PATH,
    'key.deserializer': key_deserializer,
    'value.deserializer': avro_deserializer,
    'group.id': kafka_config['group.id'],
    'auto.offset.reset': kafka_config['auto.offset.reset']
    # 'enable.auto.commit': True,
    # 'auto.commit.interval.ms': 5000 # Commit every 5000 ms, i.e., every 5 seconds
})

# Subscribe to the 'retail_data' topic
consumer.subscribe(['test'])

#Continually read messages from Kafka
try:
    while True:
        msg = consumer.poll(1.0) # How many seconds to wait for message

        if msg is None:
            continue
        if msg.error():
            print('Consumer error: {}'.format(msg.error()))
            continue

        print('Successfully consumed record with key {} and value {}'.format(msg.key(), msg.value()))

except KeyboardInterrupt:
    pass
except Exception as e:
    print(e)
finally:
    consumer.close()
