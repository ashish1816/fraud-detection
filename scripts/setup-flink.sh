#!/bin/bash
# Setup Flink SQL for Fraud Detection
# This script enables Flink and creates the necessary SQL statements

set -e

echo "🚀 Setting up Flink SQL for Fraud Detection"
echo "============================================"
echo ""

# Check if confluent CLI is installed
if ! command -v confluent &> /dev/null; then
    echo "❌ Error: confluent CLI not found"
    echo "Install from: https://docs.confluent.io/confluent-cli/current/install.html"
    exit 1
fi

echo "✅ Confluent CLI found"
echo ""

# Get current environment
ENV_ID=$(confluent environment list -o json | jq -r '.[0].id')
echo "📍 Using environment: $ENV_ID"

# Get current Kafka cluster
CLUSTER_ID=$(confluent kafka cluster list -o json | jq -r '.[0].id')
echo "📍 Using Kafka cluster: $CLUSTER_ID"

echo ""
echo "⚠️  IMPORTANT: Flink setup requires manual steps in Confluent Cloud UI"
echo ""
echo "Please follow these steps:"
echo ""
echo "1. Go to: https://confluent.cloud"
echo "2. Navigate to your environment: $ENV_ID"
echo "3. Click 'Flink' in the left menu"
echo "4. Click 'Create Compute Pool'"
echo "   - Name: fraud-detection-pool"
echo "   - Region: us-east-1 (same as Kafka)"
echo "   - Max CFUs: 5"
echo "5. Click 'Continue' and 'Launch'"
echo ""
echo "After creating the compute pool, press ENTER to continue..."
read

echo ""
echo "📝 Creating fraud-alerts-flink topic..."
confluent kafka topic create fraud-alerts-flink \
    --partitions 6 \
    --if-not-exists

if [ $? -eq 0 ]; then
    echo "✅ fraud-alerts-flink topic created"
else
    echo "⚠️  Topic may already exist"
fi

echo ""
echo "============================================"
echo "✅ Setup complete!"
echo ""
echo "Next steps:"
echo ""
echo "1. Open Flink SQL Workspace:"
echo "   https://confluent.cloud/environments/$ENV_ID/flink"
echo ""
echo "2. Copy and run the SQL from: scripts/flink-fraud-detection.sql"
echo ""
echo "3. Monitor results:"
echo "   confluent kafka topic consume fraud-alerts-flink --from-beginning"
echo ""
echo "============================================"

# Made with Bob
