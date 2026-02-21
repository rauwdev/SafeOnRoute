import json
import random
import time
from datetime import datetime, timezone

from kafka import KafkaProducer

TOPIC = "SOR_sensores"
KAFKA_BOOTSTRAP = "localhost:9094"
DEVICE_ID = "arduino-01"

producer = KafkaProducer(
    bootstrap_servers=KAFKA_BOOTSTRAP,
    value_serializer=lambda v: json.dumps(v).encode("utf-8"),
)

try:
    while True:
        payload = {
            "device_id": DEVICE_ID,
            "ts": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
            "temp": round(random.uniform(20.0, 26.0), 1),
            "hum": round(random.uniform(45.0, 60.0), 1),
        }
        producer.send(TOPIC, payload)
        producer.flush()
        print(payload)
        time.sleep(2)

except KeyboardInterrupt:
    print("\n[STOP]")
