import requests
import json
import logging
import time
import os
from kafka import KafkaProducer

def get_weather():
    response = requests.get(
        "https://api.open-meteo.com/v1/forecast",
        params={
            "latitude": 51.3,
            "longitude": 0.43,
            "current":"temperature_2m",
        }
    )
    return response.json()

def main():

    KAFKA_IP_ADDRESS = os.environ['KAFKA_IP_ADDRESS']
    KAFKA_PORT = os.environ['KAFKA_PORT']
    bootstrap_server = f"{KAFKA_IP_ADDRESS}:{KAFKA_PORT}"

    producer = KafkaProducer(
        bootstrap_servers=bootstrap_server,
        ssl_cafile='/home/guillaume/projects/certs/ca.crt',
        security_protocol='SSL',
        ssl_check_hostname=True,
    )

    while True:
        try:
            weather = get_weather()
            logging.debug("Got weather: %s", weather)
            producer.send('weather', value=json.dumps(weather).encode('utf-8'))
            producer.flush()
            logging.info("Produced... Sleeping...")
            time.sleep(10)
        except Exception as e:
            logging.error(f"Failed to send message: {e}")
            time.sleep(10)


if __name__ == "__main__":
    logging.basicConfig(level="DEBUG")
    main()
