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

def load_to_db(weather):
    try:
        conn = psycopg2.connect(
            host=os.getenv("DB_HOST"),
            dbname=os.getenv("DB_NAME"),
            user=os.getenv("DB_USERNAME"),
            password=os.getenv("DB_PWD")
        )
        cur = conn.cursor()
        cur.execute(
            """
            INSERT INTO african_weather_data (city, description, temperature, timestamp)
            VALUES (%s, %s, %s, %s)
            """,
            (weather['city'], weather['description'], weather['temperature'], weather['timestamp'])
        )
        conn.commit()
        cur.close()
        conn.close()
        print(f"Inserted weather data for {weather['city']} into DB.")
    except Exception as e:
        print(f"Database error: {e}")

def consume_data(consumer):
    while True:
        msg = consumer.poll(1.0) # poll data for 1 second before consuming again
        if not msg:
            continue
        if msg.error():
            print(f"Consumer error: {msg.error()}")
            continue
        weather = json.loads(msg.value().decode('utf-8'))
        print(weather)

        try:
            load_to_db(weather)
            print(f"{weather['city']}, loaded to db at {weather['timestamp']}")
        except Exception as e:
            print(f"Loading to db error: {e}")

if __name__ == '__main__':
    consume_data(consumer)
