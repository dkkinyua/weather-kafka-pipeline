import os
import json
import requests
from dotenv import load_dotenv
from kafka import KafkaProducer

load_dotenv()
API_KEY = os.getenv("API_KEY")
SERVER_ADDRESS = os.getenv("SERVER_ADDRESS")

producer = KafkaProducer(
    bootstrap_servers = f'{SERVER_ADDRESS}:9092',
    value_serializer = lambda v: json.dumps(v).encode('utf-8')
)
cities = ['Nairobi', 'Pretoria', 'Cairo', 'Lagos', 'Mombasa']

def produce_data(cities):
    for city in cities:
        url = f'https://api.openweathermap.org/data/2.5/weather?q={city}&appid={API_KEY}'
        data = requests.get(url).json()
        return data
    
if __name__ == '__main__':
    weather = produce_data(cities)
    for i in weather:
        print(f'City weather: {i}')
        producer.send('weatherdata', i)
    producer.flush()
    print("Data sent successfully!")
