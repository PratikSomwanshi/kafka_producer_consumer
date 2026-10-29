# Make sure to install confluent-kafka python package
# pip install confluent-kafka
# pip install pandas

import datetime
import threading
from decimal import *
from time import sleep
from uuid import uuid4, UUID
import time
import os
from pathlib import Path
from dotenv import load_dotenv

from confluent_kafka import SerializingProducer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer
from confluent_kafka.serialization import StringSerializer
import pandas as pd





def delivery_report(err, msg):
    """
    Reports the failure or success of a message delivery.

    Args:
        err (KafkaError): The error that occurred on None on success.

        msg (Message): The message that was produced or failed.

    Note:
        In the delivery report callback the Message.key() and Message.value()
        will be the binary format as encoded by any configured Serializers and
        not the same object that was passed to produce().
        If you wish to pass the original object(s) for key and value to delivery
        report callback we recommend a bound callback or lambda where you pass
        the objects along.

    """
    if err is not None:
        print("Delivery failed for User record {}: {}".format(msg.key(), err))
        return
    print('User record {} successfully produced to {} [{}] at offset {}'.format(
        msg.key(), msg.topic(), msg.partition(), msg.offset()))
    print("=====================")

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
    'ssl.ca.location': CA_PATH
}



# Create a Schema Registry client
schema_registry_client = SchemaRegistryClient({
  'url': SCHEMA_REGISTRY_URL,
  'basic.auth.user.info': '{}:{}'.format(SCHEMA_REGISTRY_USERNAME, SCHEMA_REGISTRY_PASSWORD)
})

# Fetch the latest Avro schema for the value
subject_name = 'retail_data_dev-value'
schema_str = schema_registry_client.get_latest_version(subject_name).schema.schema_str
print("Schema from Registery---")
print(schema_str)
print("=====================")

# Create Avro Serializer for the value
key_serializer = StringSerializer('utf_8')
avro_serializer = AvroSerializer(schema_registry_client, schema_str)

# Define the SerializingProducer
producer = SerializingProducer({
    'bootstrap.servers': kafka_config['bootstrap.servers'],
    'security.protocol': kafka_config['security.protocol'],
    'sasl.mechanisms': kafka_config['sasl.mechanisms'],
    'sasl.username': kafka_config['sasl.username'],
    'sasl.password': kafka_config['sasl.password'],
    'ssl.ca.location': kafka_config['ssl.ca.location'],
    'key.serializer': key_serializer,  # Key will be serialized as a string
    'value.serializer': avro_serializer,  # Value will be serialized as Avro,
})



# Load the CSV data into a pandas DataFrame
df = pd.read_csv('retail_data.csv')
df = df.fillna('null')
# print(df.head(5))
# print("=====================")

# Iterate over DataFrame rows and produce to Kafka
for index, row in df.iterrows():
    # Create a dictionary from the row values
    data_value = row.to_dict()
    print(data_value)
    # Produce to Kafka
    producer.produce(
        topic='test', 
        key=str(index), 
        value=data_value, 
        on_delivery=delivery_report
    )
    producer.flush()
    time.sleep(10)

print("All Data successfully published to Kafka")
