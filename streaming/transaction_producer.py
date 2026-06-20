import time
import json
import random
from datetime import datetime
from kafka import KafkaProducer
from faker import Faker

fake = Faker()

# Initialize Kafka Producer
producer = KafkaProducer(
    bootstrap_servers=['localhost:9092'],
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

TOPIC_NAME = 'financial_transactions'

def generate_transaction():
    """Generates realistic transaction data mixed with anomalies/fraud indicators."""
    account_id = f"ACC-{random.randint(10000, 99999)}"
    
    # Introduce an occasional anomaly: massive transaction amount
    if random.random() < 0.05:  # 5% chance of high risk anomaly
        amount = round(random.uniform(5000.0, 25000.0), 2)
    else:
        amount = round(random.uniform(5.0, 1200.0), 2)

    return {
        "transaction_id": fake.uuid4(),
        "account_id": account_id,
        "timestamp": datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S'),
        "amount": amount,
        "merchant": fake.company(),
        "category": random.choice(["Retail", "Groceries", "Entertainment", "Electronics", "Travel", "Gambling"]),
        "location_country": random.choice(["US", "IN", "UK", "CA", "DE", "RU"]),
        "device_id": f"DEV-{random.randint(100, 999)}"
    }

print("🚀 Starting real-time financial transaction stream to Kafka...")
try:
    while True:
        tx_data = generate_transaction()
        producer.send(TOPIC_NAME, value=tx_data)
        print(f"Sent: {tx_data['transaction_id']} | ${tx_data['amount']} | {tx_data['location_country']}")
        
        # Simulates throughput (change to 0.01 to match enterprise-level speeds)
        time.sleep(random.uniform(0.1, 0.5)) 
except KeyboardInterrupt:
    print("\n🛑 Stream stopped by user.")
finally:
    producer.close()

