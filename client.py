from confluent_kafka import Producer, Consumer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer, AvroDeserializer
from confluent_kafka.serialization import SerializationContext, MessageField
import os
import random
import time

# Read Avro schema from file
with open(os.path.join(os.path.dirname(__file__), "user.avsc")) as schema_file:
    avro_schema_str = schema_file.read()

def user_to_dict(user, ctx):
    return user

def dict_to_user(obj, ctx):
    return obj

def read_config():
  # reads the client configuration from client.properties
  # and returns it as a key-value map
  config = {}
  with open("client.properties") as fh:
    for line in fh:
      line = line.strip()
      if len(line) != 0 and line[0] != "#":
        parameter, value = line.strip().split('=', 1)
        config[parameter] = value.strip()
  return config

def filter_kafka_config(config):
    return {k: v for k, v in config.items() if not k.startswith("schema.registry.")}

def avro_produce(topic, config, sr_config):
    schema_registry_client = SchemaRegistryClient(sr_config)
    avro_serializer = AvroSerializer(schema_registry_client, avro_schema_str, user_to_dict)
    producer = Producer(config)
    names = ["Alice", "Bob", "Charlie", "Diana", "Eve"]
    domains = ["example.com", "test.org", "mail.net"]
    try:
        while True:
            name = random.choice(names)
            email = f"{name.lower()}@{random.choice(domains)}"
            user = {
                "active": random.choice([True, False]),
                "name": name,
                "email": email,
                "age": random.randint(18, 80)
            }
            producer.produce(
                topic=topic,
                key=name,
                value=avro_serializer(user, SerializationContext(topic, MessageField.VALUE))
            )
            print(f"Produced Avro message to topic {topic}: {user}")
            producer.poll(0)
            time.sleep(1)
    except KeyboardInterrupt:
        print("Stopped producing.")
    finally:
        producer.flush()

def avro_consume(topic, config, sr_config):
    config["group.id"] = "python-avro-group-1"
    config["auto.offset.reset"] = "earliest"
    schema_registry_client = SchemaRegistryClient(sr_config)
    avro_deserializer = AvroDeserializer(schema_registry_client, avro_schema_str, dict_to_user)
    consumer = Consumer(config)
    consumer.subscribe([topic])
    try:
        while True:
            msg = consumer.poll(1.0)
            if msg is not None and msg.error() is None:
                user = avro_deserializer(msg.value(), SerializationContext(topic, MessageField.VALUE))
                print(f"Consumed Avro message from topic {topic}: {user}")
                break  # For demo, consume one message and exit
    except KeyboardInterrupt:
        pass
    finally:
        consumer.close()

def read_sr_config():
    # Reads schema registry config from client.properties
    sr_config = {}
    with open("client.properties") as fh:
        for line in fh:
            line = line.strip()
            if len(line) != 0 and line[0] != "#":
                if line.startswith("schema.registry."):
                    parameter, value = line.strip().split('=', 1)
                    sr_config[parameter.replace("schema.registry.", "")] = value.strip()
    return sr_config

def main():
    config = read_config()
    sr_config = read_sr_config()
    topic = "avro-topic"
    kafka_config = filter_kafka_config(config)
    avro_produce(topic, kafka_config, sr_config)


main()