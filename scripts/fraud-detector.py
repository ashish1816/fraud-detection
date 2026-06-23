#!/usr/bin/env python3
"""
Real-time Fraud Detection Consumer
Consumes transactions and detects fraud patterns in real-time
Run from: confluent-fraud-detection directory
Usage: python3 scripts/fraud-detector.py
"""

import json
import sys
from collections import defaultdict
from datetime import datetime, timedelta
from confluent_kafka import Consumer, Producer
from confluent_kafka.serialization import SerializationContext, MessageField
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroDeserializer, AvroSerializer

# Fraud detection thresholds
HIGH_AMOUNT_THRESHOLD = 5000
MEDIUM_AMOUNT_THRESHOLD = 1000
VELOCITY_WINDOW_MINUTES = 5
MAX_TRANSACTIONS_PER_WINDOW = 10
MAX_AMOUNT_PER_WINDOW = 10000

# Track user activity (in-memory for demo)
user_transactions = defaultdict(list)

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

def calculate_fraud_score(transaction, user_history):
    """Calculate fraud score based on multiple factors"""
    score = 0
    reasons = []
    
    # Factor 1: High transaction amount
    amount = transaction['amount']
    if amount > HIGH_AMOUNT_THRESHOLD:
        score += 40
        reasons.append(f"High amount: ${amount:.2f}")
    elif amount > MEDIUM_AMOUNT_THRESHOLD:
        score += 20
        reasons.append(f"Medium-high amount: ${amount:.2f}")
    
    # Factor 2: Transaction velocity (count)
    recent_txns = [t for t in user_history 
                   if (datetime.now() - t['timestamp']).seconds < VELOCITY_WINDOW_MINUTES * 60]
    
    if len(recent_txns) > MAX_TRANSACTIONS_PER_WINDOW:
        score += 40
        reasons.append(f"High velocity: {len(recent_txns)} txns in {VELOCITY_WINDOW_MINUTES}min")
    elif len(recent_txns) > MAX_TRANSACTIONS_PER_WINDOW // 2:
        score += 20
        reasons.append(f"Medium velocity: {len(recent_txns)} txns in {VELOCITY_WINDOW_MINUTES}min")
    
    # Factor 3: Total amount in window
    total_amount = sum(t['amount'] for t in recent_txns)
    if total_amount > MAX_AMOUNT_PER_WINDOW:
        score += 30
        reasons.append(f"High total in window: ${total_amount:.2f}")
    
    return score, reasons

def get_decision(score):
    """Determine action based on fraud score"""
    if score >= 70:
        return "BLOCK", "🚫"
    elif score >= 50:
        return "REVIEW", "⚠️"
    else:
        return "APPROVE", "✅"

def print_fraud_alert(transaction, score, reasons, decision, emoji):
    """Print formatted fraud alert"""
    print("\n" + "="*70)
    print(f"{emoji} FRAUD ALERT - {decision}")
    print("="*70)
    print(f"Transaction ID: {transaction['transaction_id']}")
    print(f"User ID: {transaction['user_id']}")
    print(f"Amount: ${transaction['amount']:.2f}")
    print(f"Merchant: {transaction['merchant_id']}")
    print(f"Payment Method: {transaction['payment_method']}")
    print(f"Device: {transaction['device_id']}")
    print(f"Location: ({transaction['latitude']:.4f}, {transaction['longitude']:.4f})")
    print(f"\nFraud Score: {score}/100")
    print(f"Decision: {decision}")
    print("\nReasons:")
    for reason in reasons:
        print(f"  • {reason}")
    print("="*70)

def main():
    print("🔍 Real-time Fraud Detection System")
    print("="*70)
    print("Monitoring transactions for fraud patterns...")
    print("Press Ctrl+C to stop")
    print("="*70)
    print()
    
    # Load configuration
    config = load_config()
    
    # Configure consumer
    consumer_conf = {
        'bootstrap.servers': config['BOOTSTRAP_SERVER'],
        'security.protocol': 'SASL_SSL',
        'sasl.mechanisms': 'PLAIN',
        'sasl.username': config['KAFKA_API_KEY'],
        'sasl.password': config['KAFKA_API_SECRET'],
        'group.id': 'fraud-detection-consumer',
        'auto.offset.reset': 'earliest'
    }
    
    # Configure Schema Registry
    schema_registry_conf = {
        'url': config['SR_ENDPOINT'],
        'basic.auth.user.info': f"{config['SR_API_KEY']}:{config['SR_API_SECRET']}"
    }
    
    try:
        schema_registry_client = SchemaRegistryClient(schema_registry_conf)
        
        # Get schema for deserialization
        schema_str = schema_registry_client.get_latest_version('postgres-transactions-value').schema.schema_str
        avro_deserializer = AvroDeserializer(schema_registry_client, schema_str)
        
        consumer = Consumer(consumer_conf)
        consumer.subscribe(['postgres-transactions'])
        
        print("✅ Connected to Kafka")
        print("✅ Subscribed to postgres-transactions topic")
        print()
        
        transaction_count = 0
        fraud_detected = 0
        review_flagged = 0
        approved = 0
        
        while True:
            msg = consumer.poll(1.0)
            
            if msg is None:
                continue
            
            if msg.error():
                print(f"❌ Consumer error: {msg.error()}")
                continue
            
            # Deserialize transaction
            transaction = avro_deserializer(
                msg.value(),
                SerializationContext('postgres-transactions', MessageField.VALUE)
            )
            
            transaction_count += 1
            user_id = transaction['user_id']
            
            # Get user history
            user_history = user_transactions[user_id]
            
            # Calculate fraud score
            score, reasons = calculate_fraud_score(transaction, user_history)
            decision, emoji = get_decision(score)
            
            # Update statistics
            if decision == "BLOCK":
                fraud_detected += 1
            elif decision == "REVIEW":
                review_flagged += 1
            else:
                approved += 1
            
            # Print alert for suspicious transactions
            if decision in ["BLOCK", "REVIEW"]:
                print_fraud_alert(transaction, score, reasons, decision, emoji)
            else:
                # Just show approved transactions briefly
                print(f"✅ Transaction {transaction['transaction_id']} - User {user_id} - ${transaction['amount']:.2f} - APPROVED (Score: {score})")
            
            # Update user history
            user_transactions[user_id].append({
                'amount': transaction['amount'],
                'timestamp': datetime.now()
            })
            
            # Clean old history (keep last 1 hour)
            user_transactions[user_id] = [
                t for t in user_transactions[user_id]
                if (datetime.now() - t['timestamp']).seconds < 3600
            ]
            
            # Print statistics every 10 transactions
            if transaction_count % 10 == 0:
                print(f"\n📊 Statistics: Total={transaction_count} | Blocked={fraud_detected} | Review={review_flagged} | Approved={approved}\n")
    
    except KeyboardInterrupt:
        print("\n\n🛑 Stopping fraud detection system...")
        print(f"\n📊 Final Statistics:")
        print(f"   Total Transactions: {transaction_count}")
        print(f"   Blocked: {fraud_detected} ({fraud_detected/transaction_count*100:.1f}%)")
        print(f"   Flagged for Review: {review_flagged} ({review_flagged/transaction_count*100:.1f}%)")
        print(f"   Approved: {approved} ({approved/transaction_count*100:.1f}%)")
        print("\n✅ Fraud detection system stopped")
    
    except Exception as e:
        print(f"❌ Error: {e}")
        import traceback
        traceback.print_exc()
    
    finally:
        consumer.close()

if __name__ == '__main__':
    main()

# Made with Bob