from kafka import KafkaConsumer, KafkaProducer
import json
import pandas as pd

user_history = {}
last_alerted_amount = {}

MULTIPLIER_THRESHOLD = 1.5
Z_SCORE_THRESHOLD = 2.0

consumer = KafkaConsumer(
    'trades',
    bootstrap_servers='localhost:9092',
    value_deserializer=lambda m: json.loads(m.decode('utf-8')),
    auto_offset_reset='latest',
    enable_auto_commit=True
)

producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

print("📡 Listening to trade stream...")

for message in consumer:
    trade = message.value
    user_id = trade['user_id']
    amount = float(trade['amount'])

    if user_id not in user_history:
        user_history[user_id] = []
        last_alerted_amount[user_id] = None

    history = pd.Series(user_history[user_id])
    mean = history.mean() if not history.empty else 0
    std = history.std() if len(history) > 1 else 0

    is_anomalous = False
    anomaly_score = 0

    if len(history) >= 3:
        if mean > 0 and amount > MULTIPLIER_THRESHOLD * mean:
            is_anomalous = True
            anomaly_score = round(amount / mean, 2)
        elif std > 0 and abs((amount - mean) / std) > Z_SCORE_THRESHOLD:
            is_anomalous = True
            anomaly_score = round(abs((amount - mean) / std), 2)

    if is_anomalous and last_alerted_amount[user_id] != amount:
        alert = {
            "user_id": user_id,
            "alert_msg": f"🚨 Unusual trade: ${amount} (Avg: ${round(mean, 2)})",
            "anomaly_score": anomaly_score
        }
        producer.send("alerts", alert)
        last_alerted_amount[user_id] = amount
        print("⚠️  Alert sent:", alert)
    else:
        print(f"✅ Normal trade: ${amount} | Mean: {round(mean, 2)} | Std: {round(std, 2)}")

    user_history[user_id].append(amount)
