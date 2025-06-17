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

cities = [('Nairobi', 'KE'), ('Pretoria', 'ZA'), ('Cairo', 'EG'), ('Lagos', 'NG'), ('Dar es Salaam', 'TZ')]

def produce_data(city, country):
    #https://api.openweathermap.org/data/2.5/weather?q={city name},{country code}&appid={API key}
    url = f'https://api.openweathermap.org/data/2.5/weather?q={city},{country}&appid={OPENWEATHER_KEY}'
    response = requests.get(url)
    if response.status_code == 200:
        data = response.json()
        return {
            "city": city,
            "country": country,
            "timestamp": int(time.time()),
            "temperature": data["main"]["temp"],
            "description": data["weather"][0]["description"]
        }
    else:
        print(f"Requests to API error: {response.status_code}, {response.text}")
    
if __name__ == '__main__':
    while True:
        for city, country in cities:
            weather = produce_data(city, country)
            print(f"Weather: {weather}")
            producer.produce(TOPIC, json.dumps(weather).encode('utf-8'))
            producer.flush()
        time.sleep(3) # sleep for 3s before making another request
