#!/usr/bin/env python3
"""
Generate transactions with OBVIOUS fraud patterns for demo
Run this WHILE fraud-detector.py is running to see real-time detection
Usage: python3 scripts/generate-fraud-demo.py
"""

import json
import random
import time
import sys
from datetime import datetime
from confluent_kafka import Producer
from confluent_kafka.serialization import SerializationContext, MessageField
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer

def load_config():
    """Load configuration from config.env file"""
    config = {}
    try:
        with open('scripts/config.env', 'r') as f:
            for line in f:
                line = line.strip()
                if line and not line.startswith('#') and '=' in line:
                    key, value = line.split('=', 1)
                    config[key] = value
        return config
    except FileNotFoundError:
        print("❌ Error: scripts/config.env not found")
        sys.exit(1)

def generate_normal_transaction(txn_id, user_id):
    """Generate a normal transaction"""
    return {
        'transaction_id': txn_id,
        'user_id': user_id,
        'amount': round(random.uniform(10, 500), 2),
        'merchant_id': random.randint(100, 999),
        'currency': 'USD',
        'payment_method': random.choice(['CREDIT_CARD', 'DEBIT_CARD', 'DIGITAL_WALLET']),
        'ip_address': f"{random.randint(1,255)}.{random.randint(1,255)}.{random.randint(1,255)}.{random.randint(1,255)}",
        'device_id': f"device_{random.randint(1000, 9999)}",
        'latitude': round(random.uniform(-90, 90), 6),
        'longitude': round(random.uniform(-180, 180), 6),
        'created_at': int(time.time() * 1000)
    }

def generate_high_amount_fraud(txn_id, user_id):
    """Generate a high-amount fraudulent transaction"""
    return {
        'transaction_id': txn_id,
        'user_id': user_id,
        'amount': round(random.uniform(8000, 25000), 2),  # Very high amount
        'merchant_id': random.randint(100, 999),
        'currency': 'USD',
        'payment_method': 'CREDIT_CARD',
        'ip_address': f"{random.randint(1,255)}.{random.randint(1,255)}.{random.randint(1,255)}.{random.randint(1,255)}",
        'device_id': f"device_{random.randint(1000, 9999)}",
        'latitude': round(random.uniform(-90, 90), 6),
        'longitude': round(random.uniform(-180, 180), 6),
        'created_at': int(time.time() * 1000)
    }

def generate_velocity_fraud(txn_id, user_id):
    """Generate rapid transactions (velocity fraud)"""
    return {
        'transaction_id': txn_id,
        'user_id': user_id,
        'amount': round(random.uniform(500, 2000), 2),
        'merchant_id': random.randint(100, 999),
        'currency': 'USD',
        'payment_method': random.choice(['CREDIT_CARD', 'DEBIT_CARD']),
        'ip_address': f"{random.randint(1,255)}.{random.randint(1,255)}.{random.randint(1,255)}.{random.randint(1,255)}",
        'device_id': f"device_{user_id}",  # Same device
        'latitude': round(random.uniform(-90, 90), 6),
        'longitude': round(random.uniform(-180, 180), 6),
        'created_at': int(time.time() * 1000)
    }

def delivery_report(err, msg):
    """Callback for message delivery reports"""
    if err is not None:
        print(f'❌ Delivery failed: {err}')

def main():
    print("🎭 Fraud Detection Demo - Transaction Generator")
    print("="*70)
    print("This will generate transactions with OBVIOUS fraud patterns")
    print("Run fraud-detector.py in another terminal to see real-time detection!")
    print("="*70)
    print()
    
    # Load configuration
    config = load_config()
    
    # Configure producer
    producer_conf = {
        'bootstrap.servers': config['BOOTSTRAP_SERVER'],
        'security.protocol': 'SASL_SSL',
        'sasl.mechanisms': 'PLAIN',
        'sasl.username': config['KAFKA_API_KEY'],
        'sasl.password': config['KAFKA_API_SECRET'],
    }
    
    # Configure Schema Registry
    schema_registry_conf = {
        'url': config['SR_ENDPOINT'],
        'basic.auth.user.info': f"{config['SR_API_KEY']}:{config['SR_API_SECRET']}"
    }
    
    try:
        schema_registry_client = SchemaRegistryClient(schema_registry_conf)
        schema_str = schema_registry_client.get_latest_version('postgres-transactions-value').schema.schema_str
        avro_serializer = AvroSerializer(schema_registry_client, schema_str)
        producer = Producer(producer_conf)
        
        print("✅ Connected to Kafka")
        print()
        
        txn_id = 1000  # Start from 1000 to avoid conflicts
        
        # Demo scenarios
        print("📋 Demo Scenario:")
        print()
        
        # Scenario 1: Normal transactions
        print("1️⃣  Sending 5 normal transactions...")
        normal_user = 5000
        for i in range(5):
            txn = generate_normal_transaction(txn_id, normal_user)
            producer.produce(
                topic='postgres-transactions',
                key=str(txn['transaction_id']),
                value=avro_serializer(txn, SerializationContext('postgres-transactions', MessageField.VALUE)),
                callback=delivery_report
            )
            print(f"   ✅ Normal: User {normal_user}, ${txn['amount']:.2f}")
            txn_id += 1
            time.sleep(0.5)
        
        producer.flush()
        print()
        time.sleep(2)
        
        # Scenario 2: High-amount fraud
        print("2️⃣  Sending HIGH-AMOUNT fraud transaction...")
        fraud_user = 6000
        txn = generate_high_amount_fraud(txn_id, fraud_user)
        producer.produce(
            topic='postgres-transactions',
            key=str(txn['transaction_id']),
            value=avro_serializer(txn, SerializationContext('postgres-transactions', MessageField.VALUE)),
            callback=delivery_report
        )
        print(f"   🚨 FRAUD: User {fraud_user}, ${txn['amount']:.2f} (HIGH AMOUNT)")
        txn_id += 1
        producer.flush()
        print()
        time.sleep(3)
        
        # Scenario 3: Velocity fraud (rapid transactions)
        print("3️⃣  Sending VELOCITY fraud (15 rapid transactions from same user)...")
        velocity_user = 7000
        for i in range(15):
            txn = generate_velocity_fraud(txn_id, velocity_user)
            producer.produce(
                topic='postgres-transactions',
                key=str(txn['transaction_id']),
                value=avro_serializer(txn, SerializationContext('postgres-transactions', MessageField.VALUE)),
                callback=delivery_report
            )
            print(f"   🚨 Transaction {i+1}/15: User {velocity_user}, ${txn['amount']:.2f}")
            txn_id += 1
            time.sleep(0.2)  # Very fast!
        
        producer.flush()
        print()
        time.sleep(2)
        
        # Scenario 4: Combined fraud (high amount + velocity)
        print("4️⃣  Sending COMBINED fraud (high amount + velocity)...")
        combined_user = 8000
        for i in range(8):
            if i < 3:
                txn = generate_normal_transaction(txn_id, combined_user)
                print(f"   ✅ Normal: User {combined_user}, ${txn['amount']:.2f}")
            else:
                txn = generate_high_amount_fraud(txn_id, combined_user)
                print(f"   🚨 FRAUD: User {combined_user}, ${txn['amount']:.2f}")
            
            producer.produce(
                topic='postgres-transactions',
                key=str(txn['transaction_id']),
                value=avro_serializer(txn, SerializationContext('postgres-transactions', MessageField.VALUE)),
                callback=delivery_report
            )
            txn_id += 1
            time.sleep(0.3)
        
        producer.flush()
        print()
        
        print("="*70)
        print("✅ Demo Complete!")
        print("="*70)
        print()
        print("Check your fraud-detector.py terminal to see the alerts!")
        print()
        
    except Exception as e:
        print(f"❌ Error: {e}")
        import traceback
        traceback.print_exc()

if __name__ == '__main__':
    main()

# Made with Bob