import json

from kafka import KafkaConsumer

with open("config.json") as file:
    config = json.load(file)


consumer = KafkaConsumer(
    config["topic"],
    bootstrap_servers=config["bootstrap_servers"],
    security_protocol=config["security_protocol"],
    sasl_mechanism=config["sasl_mechanism"],
    sasl_plain_username=config["sasl_plain_username"],
    sasl_plain_password=config["sasl_plain_password"],
    group_id=config["group_id"],
    auto_offset_reset="earliest",
)

for msg in consumer:
    print(json.loads(msg.value))
    message = json.loads(msg.value)
