#!/usr/bin/env python3
"""
Transaction Generator - Inserts transactions into PostgreSQL
The JDBC Source Connector will then stream these to Kafka
"""

import os
import time
import random
from datetime import datetime
from faker import Faker
import psycopg2
from psycopg2.extras import execute_values

fake = Faker()

# Database configuration from environment
DB_CONFIG = {
    'host': os.getenv('POSTGRES_HOST', 'localhost'),
    'port': int(os.getenv('POSTGRES_PORT', 5432)),
    'database': os.getenv('POSTGRES_DB', 'transactions'),
    'user': os.getenv('POSTGRES_USER', 'frauduser'),
    'password': os.getenv('POSTGRES_PASSWORD', 'fraudpass123')
}

GENERATION_RATE = int(os.getenv('GENERATION_RATE', 10))  # transactions per second

# Merchant data
MERCHANTS = [
    (1001, 'Amazon', 'E-commerce'),
    (1002, 'Walmart', 'Retail'),
    (1003, 'Shell Gas Station', 'Gas Station'),
    (1004, 'Starbucks', 'Restaurant'),
    (1005, 'Best Buy', 'Electronics'),
    (1006, 'Luxury Casino Online', 'Gambling'),
    (1007, 'Crypto Exchange XYZ', 'Cryptocurrency'),
    (1008, 'Target', 'Retail'),
    (1009, 'Apple Store', 'Electronics'),
    (1010, 'Netflix', 'Streaming')
]

TRANSACTION_TYPES = ['purchase', 'refund', 'withdrawal', 'deposit']
LOCATIONS = ['New York, NY', 'Los Angeles, CA', 'Chicago, IL', 'Houston, TX', 
             'Phoenix, AZ', 'Philadelphia, PA', 'San Antonio, TX', 'San Diego, CA']


def connect_db():
    """Connect to PostgreSQL database"""
    max_retries = 5
    retry_delay = 5
    
    for attempt in range(max_retries):
        try:
            conn = psycopg2.connect(**DB_CONFIG)
            print(f"✅ Connected to PostgreSQL at {DB_CONFIG['host']}:{DB_CONFIG['port']}")
            return conn
        except psycopg2.OperationalError as e:
            if attempt < max_retries - 1:
                print(f"⚠️  Connection attempt {attempt + 1} failed. Retrying in {retry_delay}s...")
                time.sleep(retry_delay)
            else:
                print(f"❌ Failed to connect to database after {max_retries} attempts")
                raise


def generate_transaction(user_id_base=5000):
    """Generate a single transaction"""
    user_id = random.randint(user_id_base, user_id_base + 1000)
    merchant_id, merchant_name, merchant_category = random.choice(MERCHANTS)
    
    # Generate amount based on merchant category
    if merchant_category in ['Gambling', 'Cryptocurrency']:
        # High-risk merchants - higher amounts
        amount = round(random.uniform(100, 10000), 2)
    elif merchant_category == 'Electronics':
        amount = round(random.uniform(50, 2000), 2)
    else:
        amount = round(random.uniform(5, 500), 2)
    
    # Occasionally generate fraud patterns
    fraud_pattern = random.random()
    if fraud_pattern < 0.05:  # 5% fraud rate
        # High amount fraud
        amount = round(random.uniform(5000, 20000), 2)
    
    transaction = {
        'user_id': user_id,
        'amount': amount,
        'merchant_id': merchant_id,
        'merchant_name': merchant_name,
        'merchant_category': merchant_category,
        'transaction_type': random.choice(TRANSACTION_TYPES),
        'location': random.choice(LOCATIONS),
        'device_id': fake.uuid4(),
        'ip_address': fake.ipv4()
    }
    
    return transaction


def insert_transactions(conn, transactions):
    """Batch insert transactions into database"""
    query = """
        INSERT INTO transactions 
        (user_id, amount, merchant_id, merchant_name, merchant_category, 
         transaction_type, location, device_id, ip_address)
        VALUES %s
    """
    
    values = [
        (t['user_id'], t['amount'], t['merchant_id'], t['merchant_name'],
         t['merchant_category'], t['transaction_type'], t['location'],
         t['device_id'], t['ip_address'])
        for t in transactions
    ]
    
    with conn.cursor() as cursor:
        execute_values(cursor, query, values)
    conn.commit()


def main():
    """Main transaction generation loop"""
    print("=" * 70)
    print("🏦 Transaction Generator for Fraud Detection")
    print("=" * 70)
    print(f"Database: {DB_CONFIG['host']}:{DB_CONFIG['port']}/{DB_CONFIG['database']}")
    print(f"Generation Rate: {GENERATION_RATE} transactions/second")
    print("=" * 70)
    
    conn = connect_db()
    
    transaction_count = 0
    batch_size = max(1, GENERATION_RATE // 2)  # Insert in batches
    batch = []
    
    try:
        print("\n🚀 Starting transaction generation...")
        print("Press Ctrl+C to stop\n")
        
        while True:
            # Generate transaction
            transaction = generate_transaction()
            batch.append(transaction)
            
            # Insert batch when ready
            if len(batch) >= batch_size:
                insert_transactions(conn, batch)
                transaction_count += len(batch)
                
                # Print summary
                avg_amount = sum(t['amount'] for t in batch) / len(batch)
                print(f"✅ Inserted {len(batch)} transactions | "
                      f"Total: {transaction_count} | "
                      f"Avg Amount: ${avg_amount:.2f}")
                
                batch = []
            
            # Sleep to maintain generation rate
            time.sleep(1.0 / GENERATION_RATE)
            
    except KeyboardInterrupt:
        print("\n\n⏹️  Stopping transaction generator...")
        
        # Insert remaining batch
        if batch:
            insert_transactions(conn, batch)
            transaction_count += len(batch)
        
        print(f"\n📊 Final Statistics:")
        print(f"   Total Transactions Generated: {transaction_count}")
        print("\n✅ Transaction generator stopped cleanly")
        
    finally:
        conn.close()


if __name__ == '__main__':
    main()

# Made with Bob
