#!/bin/bash

# ============================================================================
# Create Kafka Topics
# ============================================================================
# Run from: confluent-fraud-detection directory
# Usage: ./scripts/create-topics.sh
# ============================================================================

set -e

# Load configuration
source scripts/config.env

echo "📝 Creating Kafka Topics..."
echo ""

# Use the cluster
confluent kafka cluster use "$CLUSTER_ID"

# Topics to create
TOPICS=(
    "postgres-transactions"
    "mongo-user_profiles"
    "device-fingerprints"
    "fraud-alerts"
    "fraud-scores"
    "fraud-alerts-comprehensive"
)

for topic in "${TOPICS[@]}"; do
    echo "Creating topic: $topic"
    
    # Check if topic exists
    if confluent kafka topic describe "$topic" &> /dev/null; then
        echo "  ✅ Topic already exists"
    else
        confluent kafka topic create "$topic" \
            --partitions 3 \
            --config retention.ms=604800000
        echo "  ✅ Topic created"
    fi
    echo ""
done

echo "================================================"
echo "✅ All Topics Created!"
echo "================================================"
echo ""
echo "Topics:"
for topic in "${TOPICS[@]}"; do
    echo "  • $topic"
done
echo ""
echo "Next step: ./scripts/register-schemas.sh"
echo ""

# Made with Bob
