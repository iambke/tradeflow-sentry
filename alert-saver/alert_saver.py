import json
import time
import psycopg2
from kafka import KafkaConsumer
from kafka.errors import NoBrokersAvailable

def wait_for_postgres(retries=10, delay=2):
    for _ in range(retries):
        try:
            conn = psycopg2.connect(
                host="localhost",
                database="trades_db",
                user="trader",
                password="securepass"
            )
            print("✅ Connected to PostgreSQL")
            return conn
        except psycopg2.OperationalError:
            print("⏳ Waiting for PostgreSQL...")
            time.sleep(delay)
    raise Exception("❌ Could not connect to PostgreSQL.")

def wait_for_kafka(retries=10, delay=2):
    for _ in range(retries):
        try:
            consumer = KafkaConsumer(
                'alerts',
                bootstrap_servers='localhost:9092',
                value_deserializer=lambda m: json.loads(m.decode('utf-8')),
                auto_offset_reset='latest',
                enable_auto_commit=True
            )
            print("✅ Connected to Kafka. 📥 Listening for alerts...")
            return consumer
        except NoBrokersAvailable:
            print("⏳ Waiting for Kafka...")
            time.sleep(delay)
    raise Exception("❌ Could not connect to Kafka.")

def main():
    conn = wait_for_postgres()
    cursor = conn.cursor()
    consumer = wait_for_kafka()

    try:
        for message in consumer:
            alert = message.value
            user_id = alert.get("user_id")
            msg = alert.get("alert_msg")
            score = float(alert.get("anomaly_score", 0))

            try:
                cursor.execute("""
                    INSERT INTO alerts (user_id_hash, alert_msg, anomaly_score)
                    VALUES (%s, %s, %s)
                """, (user_id, msg, score))
                conn.commit()
                print(f"💾 Saved alert for {user_id[:8]}: {msg}")
            except Exception as e:
                print("❌ DB insert failed:", e)

    except KeyboardInterrupt:
        print("\n🛑 Stopped by user.")
    finally:
        consumer.close()
        cursor.close()
        conn.close()

if __name__ == "__main__":
    main()
