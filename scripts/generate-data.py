#!/usr/bin/env python3
"""
Generate sample transaction data for fraud detection demo
Run from: confluent-fraud-detection directory
Usage: python3 scripts/generate-data.py
"""

import json
import random
import time
import sys
from datetime import datetime
from confluent_kafka import Producer
from confluent_kafka.serialization import StringSerializer, SerializationContext, MessageField
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
        print("   Please run ./setup.sh first")
        sys.exit(1)

def generate_transaction(txn_id, is_fraud=False):
    """Generate a sample transaction"""
    if is_fraud:
        # Fraudulent transaction patterns
        amount = random.uniform(5000, 50000)  # Large amounts
        user_id = random.randint(1000, 1500)  # Limited user pool
    else:
        # Normal transaction
        amount = random.uniform(10, 1000)
        user_id = random.randint(1000, 9999)
    
    return {
        'transaction_id': txn_id,
        'user_id': user_id,
        'amount': round(amount, 2),
        'merchant_id': random.randint(100, 999),
        'currency': 'USD',
        'payment_method': random.choice(['CREDIT_CARD', 'DEBIT_CARD', 'DIGITAL_WALLET']),
        'ip_address': f"{random.randint(1,255)}.{random.randint(1,255)}.{random.randint(1,255)}.{random.randint(1,255)}",
        'device_id': f"device_{random.randint(1000, 9999)}",
        'latitude': round(random.uniform(-90, 90), 6),
        'longitude': round(random.uniform(-180, 180), 6),
        'created_at': int(time.time() * 1000)
    }

def delivery_report(err, msg):
    """Callback for message delivery reports"""
    if err is not None:
        print(f'❌ Message delivery failed: {err}')
    else:
        print(f'✅ Transaction {msg.key().decode("utf-8")} sent to {msg.topic()} [{msg.partition()}]')

def main():
    print("🚀 Fraud Detection Data Generator")
    print("=" * 50)
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
        
        # Get schema
        print("📋 Fetching schema from Schema Registry...")
        schema_str = schema_registry_client.get_latest_version('postgres-transactions-value').schema.schema_str
        print("✅ Schema loaded")
        print()
        
        avro_serializer = AvroSerializer(schema_registry_client, schema_str)
        producer = Producer(producer_conf)
        
        print("📊 Generating transactions...")
        print("   Normal transactions: 95%")
        print("   Fraudulent transactions: 5%")
        print()
        
        num_transactions = 100
        fraud_count = 0
        normal_count = 0
        
        for i in range(1, num_transactions + 1):
            # 5% fraud rate
            is_fraud = random.random() < 0.05
            transaction = generate_transaction(i, is_fraud)
            
            if is_fraud:
                fraud_count += 1
            else:
                normal_count += 1
            
            # Serialize and produce
            try:
                producer.produce(
                    topic='postgres-transactions',
                    key=str(transaction['transaction_id']),
                    value=avro_serializer(
                        transaction,
                        SerializationContext('postgres-transactions', MessageField.VALUE)
                    ),
                    callback=delivery_report
                )
            except Exception as e:
                print(f"❌ Error producing message: {e}")
            
            # Flush every 10 messages
            if i % 10 == 0:
                producer.flush()
                print(f"📈 Progress: {i}/{num_transactions} transactions sent")
                print()
            
            # Rate limiting: 10 TPS
            time.sleep(0.1)
        
        # Final flush
        producer.flush()
        
        print()
        print("=" * 50)
        print("✅ Data Generation Complete!")
        print("=" * 50)
        print()
        print(f"Total transactions: {num_transactions}")
        print(f"Normal: {normal_count} ({normal_count/num_transactions*100:.1f}%)")
        print(f"Fraudulent: {fraud_count} ({fraud_count/num_transactions*100:.1f}%)")
        print()
        print("Next steps:")
        print("1. View fraud alerts: ./scripts/view-alerts.sh")
        print("2. Deploy ksqlDB queries: ./scripts/deploy-ksqldb.sh")
        print()
        
    except Exception as e:
        print(f"❌ Error: {e}")
        sys.exit(1)

if __name__ == '__main__':
    main()

# Made with Bob
