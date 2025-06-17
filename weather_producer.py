import os
import json
import time
import requests
from confluent_kafka import Producer
from dotenv import load_dotenv

load_dotenv()
API_KEY = os.getenv("API_KEY")
API_SECRET_KEY = os.getenv("API_SECRET_KEY")
BOOTSTRAP_SERVER = os.getenv("BOOTSTRAP_SERVER")
OPENWEATHER_KEY = os.getenv("OPENWEATHER_KEY")
TOPIC = os.getenv("CONFLUENT_TOPIC")
CLIENT_ID = os.getenv("CLIENT_ID")

producer_config = {
    'bootstrap.servers': BOOTSTRAP_SERVER,
    'security.protocol': 'SASL_SSL',
    'sasl.mechanism': 'PLAIN',
    'sasl.username': API_KEY,
    'sasl.password': API_SECRET_KEY,  
    'client.id': CLIENT_ID
}

producer = Producer(producer_config)

cities = ['Nairobi', 'Pretoria', 'Cairo', 'Lagos', 'Mombasa']

def produce_data(city):
    url = f'https://api.openweathermap.org/data/2.5/weather?q={city}&appid={OPENWEATHER_KEY}'
    data = requests.get(url).json()
    return {
        "city": city,
        "timestamp": int(time.time()),
        "temperature": data["main"]["temp"],
        "description": data["weather"][0]["description"]
    }
    
if __name__ == '__main__':
    while True:
        for city in cities:
            weather = produce_data(city)
            print(f"Weather: {weather}")
            producer.produce(TOPIC, json.dumps(weather).encode('utf-8'))
            producer.flush()
        print("Data sent successfully!")
