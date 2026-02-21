import json
from kafka import KafkaConsumer
import psycopg2

TOPIC = "SOR_sensores"
KAFKA_BOOTSTRAP = "localhost:9094"

PG_HOST = "localhost"
PG_PORT = 5432
PG_DB = "SOR_db"
PG_USER = "SOR_dev"
PG_PASS = "Los-Boscos-SOR-5439"

GROUP_ID = "SOR-consumer-1"

def main():
    conn = psycopg2.connect(
        host=PG_HOST, port=PG_PORT, dbname=PG_DB, user=PG_USER, password=PG_PASS
    )
    conn.autocommit = True
    cur = conn.cursor()

    consumer = KafkaConsumer(
        TOPIC,
        bootstrap_servers=KAFKA_BOOTSTRAP,
        group_id=GROUP_ID,
        auto_offset_reset="earliest",
        enable_auto_commit=True,
        value_deserializer=lambda v: v.decode("utf-8"),
    )

    print(f"[OK] Consumer <- topic={TOPIC} from {KAFKA_BOOTSTRAP}")
    print(f"[OK] Inserting into Postgres: {PG_DB} on {PG_HOST}:{PG_PORT}")

    inserted = 0

    for msg in consumer:
        raw = msg.value

        try:
            data = json.loads(raw)

            device_id = data.get("device_id")
            ts = data.get("ts")
            temp = data.get("temp")
            hum = data.get("hum")

            if device_id is None or ts is None:
                print(f"[SKIP] Falta device_id/ts: {raw}")
                continue

            cur.execute(
                """
                INSERT INTO measurements (device_id, ts, temp, hum, raw)
                VALUES (%s, %s, %s, %s, %s::jsonb)
                """,
                (device_id, ts, temp, hum, json.dumps(data)),
            )

            inserted += 1
            print(f"[INSERT #{inserted}] {device_id} {ts} temp={temp} hum={hum}")

        except Exception as e:
            print(f"[ERROR] {e} | raw={raw}")

if __name__ == "__main__":
    main()
