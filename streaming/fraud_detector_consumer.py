import json
from kafka import KafkaConsumer

consumer = KafkaConsumer(
    'financial_transactions',
    bootstrap_servers=['localhost:9092'],
    auto_offset_reset='latest',
    value_serializer=lambda x: json.loads(x.decode('utf-8'))
)

print("⚡ Fraud Engine Active. Monitoring Kafka Stream...")

for message in consumer:
    tx = message.value
    is_fraud = 0
    reasons = []

    # Rule 1: High Transaction Limit Violation
    if tx['amount'] > 5000.0:
        is_fraud = 1
        reasons.append("HIGH_AMOUNT_VIOLATION")

    # Rule 2: Sector Risk Management
    if tx['category'] == "Gambling" and tx['amount'] > 1000.0:
        is_fraud = 1
        reasons.append("HIGH_RISK_CATEGORY_LIMIT")

    tx['is_fraud_flag'] = is_fraud
    tx['fraud_reasons'] = reasons

    # BATCHING LOGIC FOR HADOOP (Concept)
    # In production, you write this directly into HDFS using Spark Streaming or pyHive
    if is_fraud == 1:
        print(f"🚨 ALERT: Potential Fraud Found! ID: {tx['transaction_id']} | Reason: {reasons}")
    else:
        print(f"✅ Safe Transaction Logged: {tx['transaction_id']}")

