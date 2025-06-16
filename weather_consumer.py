import os
import json
import uuid
from cassandra.cluster import Cluster
from cassandra.query import SimpleStatement
from datetime import datetime
from dotenv import load_dotenv
from kafka import KafkaConsumer

load_dotenv()
SERVER_ADDRESS = os.getenv("SERVER_ADDRESS")

consumer = KafkaConsumer(
    'weatherdata',
    bootstrap_servers = f'{SERVER_ADDRESS}:9092',
    value_deserializer = lambda m: json.loads(m.decode('utf-8')),
    auto_offset_reset='earliest',
    enable_auto_commit=True
)
cluster = Cluster([SERVER_ADDRESS])
session = cluster.connect('weather')

insert_query = session.prepare("""
    INSERT INTO weatherdata (id, city, temperature, humidity, timestamp)
    VALUES (?, ?, ?, ?, ?)
""")

def consume_data():
    for message in consumer:
        data = message.value
        try:
            session.execute(
                insert_query,
                (
                    uuid.uuid4(),
                    data['city'],
                    float(data['temperature']),
                    float(data['humidity']),
                    datetime.now()
                )
            )
            print(f"Inserted: {data}")
        except Exception as e:
            print(f"Error inserting: {e}")