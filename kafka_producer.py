import json
import time

# pip install pandasfrom generator_fake import get_registered_user
import pandas as pd
from kafka import KafkaProducer


def json_serializer(data):
    return json.dumps(data).encode("utf-8")


with open("config.json") as file:
    config = json.load(file)


def get_data():
    df = pd.read_csv("products.csv")
    df2 = df.to_dict(orient="records")
    return df2


producer = KafkaProducer(
    bootstrap_servers=config["bootstrap_servers"],
    security_protocol=config["security_protocol"],
    sasl_mechanism=config["sasl_mechanism"],
    sasl_plain_username=config["sasl_plain_username"],
    sasl_plain_password=config["sasl_plain_password"],
    # group_id = "my-group",
    value_serializer=json_serializer,
)


if __name__ == "__main__":
    data = get_data()
    print(len(data))
    i = 1
    while i < len(data):
        # registered_user = get_registered_user()
        print(data[i])
        producer.send("SupplyChain", data[i])
        time.sleep(10)
        i += 1
