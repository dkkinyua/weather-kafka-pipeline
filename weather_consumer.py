import os
import json
import psycopg2
from confluent_kafka import Consumer
from dotenv import load_dotenv

load_dotenv()
API_KEY = os.getenv("API_KEY")
API_SECRET_KEY = os.getenv("API_SECRET_KEY")
BOOTSTRAP_SERVER = os.getenv("BOOTSTRAP_SERVER")
TOPIC = os.getenv("CONFLUENT_TOPIC")
CLIENT_ID = os.getenv("CLIENT_ID")
GROUP_ID = os.getenv("GROUP_ID")

consumer_config = {
    'bootstrap.servers': BOOTSTRAP_SERVER,
    'security.protocol': 'SASL_SSL',
    'sasl.mechanism': 'PLAIN',
    'sasl.username': API_KEY,
    'sasl.password': API_SECRET_KEY,
    'group.id': GROUP_ID,
    'auto.offset.reset': 'earliest',
    'enable.auto.commit': True
}

consumer = Consumer(consumer_config)
consumer.subscribe([TOPIC])

def load_to_db(weather, conn, cur):
    try:
        cur.execute(
            """
            INSERT INTO african_weather_data (city, description, temperature, timestamp)
            VALUES (%s, %s, %s, %s)
            """,
            (weather['city'], weather['description'], weather['temperature'], weather['timestamp'])
        )
        conn.commit()
    except Exception as e:
        print(f"Database error: {e}")

def consume_data(consumer):
    try:
        conn = psycopg2.connect(
            host=os.getenv("DB_HOST"),
            port=os.getenv("DB_PORT"),
            dbname=os.getenv("DB_NAME"),
            user=os.getenv("DB_USERNAME"),
            password=os.getenv("DB_PWD"),
            sslmode='require'
        )
        cur = conn.cursor()

        while True:
            msg = consumer.poll(1.0)
            if not msg:
                continue
            if msg.error():
                print(f"Consumer error: {msg.error()}")
                continue
            try:
                weather = json.loads(msg.value().decode('utf-8'))
                load_to_db(weather, conn, cur)
                print(f"{weather['city']} loaded into db at {weather['timestamp']}")
            except Exception as e:
                print(f"Error processing message: {e}")
    finally:
        cur.close()
        conn.close()


if __name__ == '__main__':
    consume_data(consumer)
