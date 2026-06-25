#!/usr/bin/env python3
"""
Simple Real-time Fraud Detection Consumer
Consumes orders from Datagen connector and detects fraud patterns
Run from: confluent-fraud-detection directory
Usage: python3 scripts/fraud-detector-simple.py
"""

import json
import sys
from collections import defaultdict
from datetime import datetime
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.json_schema import JSONDeserializer
from confluent_kafka.serialization import SerializationContext, MessageField

# Fraud detection thresholds (adapted for orders)
HIGH_AMOUNT_THRESHOLD = 500  # High order value
MEDIUM_AMOUNT_THRESHOLD = 200  # Medium order value
VELOCITY_WINDOW_MINUTES = 5
MAX_ORDERS_PER_WINDOW = 10
MAX_AMOUNT_PER_WINDOW = 1000

# Track user activity (in-memory for demo)
user_orders = defaultdict(list)

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

def calculate_fraud_score(order, user_history):
    """Calculate fraud score based on multiple factors"""
    score = 0
    reasons = []
    
    # Factor 1: High order amount
    amount = order.get('orderunits', 0)
    if amount > HIGH_AMOUNT_THRESHOLD:
        score += 40
        reasons.append(f"High order value: ${amount:.2f}")
    elif amount > MEDIUM_AMOUNT_THRESHOLD:
        score += 20
        reasons.append(f"Medium-high order value: ${amount:.2f}")
    
    # Factor 2: Order velocity (count)
    recent_orders = [o for o in user_history 
                     if (datetime.now() - o['timestamp']).seconds < VELOCITY_WINDOW_MINUTES * 60]
    
    if len(recent_orders) > MAX_ORDERS_PER_WINDOW:
        score += 40
        reasons.append(f"High velocity: {len(recent_orders)} orders in {VELOCITY_WINDOW_MINUTES}min")
    elif len(recent_orders) > MAX_ORDERS_PER_WINDOW // 2:
        score += 20
        reasons.append(f"Medium velocity: {len(recent_orders)} orders in {VELOCITY_WINDOW_MINUTES}min")
    
    # Factor 3: Total amount in window
    total_amount = sum(o['amount'] for o in recent_orders)
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

def print_fraud_alert(order, score, reasons, decision, emoji):
    """Print formatted fraud alert"""
    print("\n" + "="*70)
    print(f"{emoji} FRAUD ALERT - {decision}")
    print("="*70)
    print(f"Order ID: {order.get('orderid', 'N/A')}")
    print(f"Item: {order.get('itemid', 'N/A')}")
    print(f"Amount: ${order.get('orderunits', 0):.2f}")
    
    address = order.get('address', {})
    if isinstance(address, dict):
        city = address.get('city', 'N/A')
        state = address.get('state', 'N/A')
        print(f"Address: {city}, {state}")
    
    print(f"Order Time: {order.get('ordertime', 'N/A')}")
    print(f"\nFraud Score: {score}/100")
    print(f"Decision: {decision}")
    print("\nReasons:")
    for reason in reasons:
        print(f"  • {reason}")
    print("="*70)

def main():
    print("🔍 Real-time Fraud Detection System (Orders)")
    print("="*70)
    print("Monitoring orders for fraud patterns...")
    print("Press Ctrl+C to stop")
    print("="*70)
    print()
    
    # Load configuration
    config = load_config()
    
    # Import Kafka consumer
    try:
        from confluent_kafka import Consumer
    except ImportError:
        print("❌ Error: confluent-kafka package not installed")
        print("Install with: pip3 install confluent-kafka")
        sys.exit(1)
    
    # Configure consumer
    consumer_conf = {
        'bootstrap.servers': config['BOOTSTRAP_SERVER'],
        'security.protocol': 'SASL_SSL',
        'sasl.mechanisms': 'PLAIN',
        'sasl.username': config['KAFKA_API_KEY'],
        'sasl.password': config['KAFKA_API_SECRET'],
        'group.id': 'fraud-detection-simple',
        'auto.offset.reset': 'earliest'
    }
    
    # Configure Schema Registry for Avro deserialization
    schema_registry_conf = {
        'url': config['SR_ENDPOINT'],
        'basic.auth.user.info': f"{config['SR_API_KEY']}:{config['SR_API_SECRET']}"
    }
    
    consumer = None
    order_count = 0
    fraud_detected = 0
    review_flagged = 0
    approved = 0
    
    try:
        # Initialize Schema Registry client
        schema_registry_client = SchemaRegistryClient(schema_registry_conf)
        
        # Get the latest schema for the topic
        schema_str = schema_registry_client.get_latest_version('postgres-transactions-value').schema.schema_str
        
        # Define a simple from_dict function that returns the dict as-is
        def dict_to_order(obj, ctx):
            return obj
        
        # Initialize JSON Schema deserializer with the schema and from_dict function
        json_deserializer = JSONDeserializer(schema_str, from_dict=dict_to_order)
        
        consumer = Consumer(consumer_conf)
        consumer.subscribe(['postgres-transactions'])
        
        print("✅ Connected to Kafka")
        print("✅ Connected to Schema Registry")
        print("✅ Subscribed to postgres-transactions topic")
        print("✅ Using JSON Schema deserialization")
        print()
        
        while True:
            msg = consumer.poll(1.0)
            
            if msg is None:
                continue
            
            if msg.error():
                print(f"❌ Consumer error: {msg.error()}")
                continue
            
            # Deserialize order using JSON Schema
            try:
                order = json_deserializer(
                    msg.value(),
                    SerializationContext('postgres-transactions', MessageField.VALUE)
                )
            except Exception as e:
                print(f"⚠️ Failed to deserialize message: {e}")
                continue
            
            order_count += 1
            
            # Use orderid as identifier
            order_id = order.get('orderid', 'unknown')
            
            # Extract address info for user tracking
            address = order.get('address', {})
            if isinstance(address, dict):
                user_key = f"{address.get('city', 'unknown')}_{address.get('zipcode', 'unknown')}"
            else:
                user_key = 'unknown'
            
            # Get user history
            user_history = user_orders[user_key]
            
            # Calculate fraud score
            score, reasons = calculate_fraud_score(order, user_history)
            decision, emoji = get_decision(score)
            
            # Update statistics
            if decision == "BLOCK":
                fraud_detected += 1
            elif decision == "REVIEW":
                review_flagged += 1
            else:
                approved += 1
            
            # Print alert for suspicious orders
            if decision in ["BLOCK", "REVIEW"]:
                print_fraud_alert(order, score, reasons, decision, emoji)
            else:
                # Just show approved orders briefly
                print(f"✅ Order {order_id} - {order.get('itemid', 'N/A')} - ${order.get('orderunits', 0):.2f} - APPROVED (Score: {score})")
            
            # Update user history
            user_orders[user_key].append({
                'amount': order.get('orderunits', 0),
                'timestamp': datetime.now()
            })
            
            # Clean old history (keep last 1 hour)
            user_orders[user_key] = [
                o for o in user_orders[user_key]
                if (datetime.now() - o['timestamp']).seconds < 3600
            ]
            
            # Print statistics every 10 orders
            if order_count % 10 == 0:
                print(f"\n📊 Statistics: Total={order_count} | Blocked={fraud_detected} | Review={review_flagged} | Approved={approved}\n")
    
    except KeyboardInterrupt:
        print("\n\n🛑 Stopping fraud detection system...")
        print(f"\n📊 Final Statistics:")
        print(f"   Total Orders: {order_count}")
        if order_count > 0:
            print(f"   Blocked: {fraud_detected} ({fraud_detected/order_count*100:.1f}%)")
            print(f"   Flagged for Review: {review_flagged} ({review_flagged/order_count*100:.1f}%)")
            print(f"   Approved: {approved} ({approved/order_count*100:.1f}%)")
        print("\n✅ Fraud detection system stopped")
    
    except Exception as e:
        print(f"❌ Error: {e}")
        import traceback
        traceback.print_exc()
    
    finally:
        if consumer:
            consumer.close()

if __name__ == '__main__':
    main()

# Made with Bob