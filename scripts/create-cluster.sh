#!/bin/bash

# ============================================================================
# Create Kafka Cluster and Schema Registry
# ============================================================================
# Run from: confluent-fraud-detection directory
# Usage: ./scripts/create-cluster.sh
# ============================================================================

set -e

# Load configuration
source scripts/config.env

echo "🏗️  Creating Kafka Cluster..."
echo ""

CLUSTER_NAME="fraud-detection-demo"
CLUSTER_ID=$(confluent kafka cluster list -o json | jq -r ".[] | select(.name==\"$CLUSTER_NAME\") | .id")

if [ -z "$CLUSTER_ID" ]; then
    echo "Creating new Basic cluster (free tier eligible)..."
    confluent kafka cluster create "$CLUSTER_NAME" \
        --cloud aws \
        --region us-east-1 \
        --type basic
    
    # Wait for cluster to be ready
    echo "⏳ Waiting for cluster to be ready..."
    sleep 30
    
    CLUSTER_ID=$(confluent kafka cluster list -o json | jq -r ".[] | select(.name==\"$CLUSTER_NAME\") | .id")
    echo "✅ Cluster created: $CLUSTER_ID"
else
    echo "✅ Cluster already exists: $CLUSTER_ID"
fi

confluent kafka cluster use "$CLUSTER_ID"

# Get bootstrap server
BOOTSTRAP_SERVER=$(confluent kafka cluster describe "$CLUSTER_ID" -o json | jq -r '.endpoint' | sed 's/SASL_SSL:\/\///')

echo ""
echo "🔑 Creating API Keys..."
echo ""

# Create Kafka API key
API_KEY_OUTPUT=$(confluent api-key create --resource "$CLUSTER_ID" -o json)
KAFKA_API_KEY=$(echo "$API_KEY_OUTPUT" | jq -r '.api_key')
KAFKA_API_SECRET=$(echo "$API_KEY_OUTPUT" | jq -r '.api_secret')

echo "✅ Kafka API Key created"
echo "   Key: $KAFKA_API_KEY"
echo "   Secret: $KAFKA_API_SECRET"

# Wait for API key to propagate
echo "⏳ Waiting for API key to propagate..."
sleep 10

# Use the API key
confluent api-key use "$KAFKA_API_KEY" --resource "$CLUSTER_ID"

echo ""
echo "🔧 Enabling Schema Registry..."
echo ""

SR_CLUSTER_ID=$(confluent schema-registry cluster describe -o json 2>/dev/null | jq -r '.cluster_id' || echo "")

if [ -z "$SR_CLUSTER_ID" ] || [ "$SR_CLUSTER_ID" == "null" ]; then
    echo "⚠️  Schema Registry needs to be enabled manually via Confluent Cloud UI"
    echo "   1. Go to https://confluent.cloud"
    echo "   2. Navigate to your environment: $ENVIRONMENT_ID"
    echo "   3. Click 'Stream Governance' and enable Schema Registry"
    echo "   4. Select AWS and US region"
    echo ""
    echo "   After enabling, press Enter to continue..."
    read -p ""
    
    sleep 5
    SR_CLUSTER_ID=$(confluent schema-registry cluster describe -o json | jq -r '.cluster_id')
    
    if [ -z "$SR_CLUSTER_ID" ] || [ "$SR_CLUSTER_ID" == "null" ]; then
        echo "❌ Schema Registry not found. Please enable it in the UI first."
        exit 1
    fi
    
    echo "✅ Schema Registry found: $SR_CLUSTER_ID"
else
    echo "✅ Schema Registry already enabled: $SR_CLUSTER_ID"
fi

# Get Schema Registry endpoint
SR_ENDPOINT=$(confluent schema-registry cluster describe -o json | jq -r '.endpoint_url')

# Create Schema Registry API key
SR_API_KEY_OUTPUT=$(confluent api-key create --resource "$SR_CLUSTER_ID" -o json)
SR_API_KEY=$(echo "$SR_API_KEY_OUTPUT" | jq -r '.api_key')
SR_API_SECRET=$(echo "$SR_API_KEY_OUTPUT" | jq -r '.api_secret')

echo "✅ Schema Registry API Key created"
echo "   Key: $SR_API_KEY"
echo "   Secret: $SR_API_SECRET"

# Save all configuration
cat >> scripts/config.env << EOF

# Kafka Cluster
CLUSTER_ID=$CLUSTER_ID
CLUSTER_NAME=$CLUSTER_NAME
BOOTSTRAP_SERVER=$BOOTSTRAP_SERVER
KAFKA_API_KEY=$KAFKA_API_KEY
KAFKA_API_SECRET=$KAFKA_API_SECRET

# Schema Registry
SR_CLUSTER_ID=$SR_CLUSTER_ID
SR_ENDPOINT=$SR_ENDPOINT
SR_API_KEY=$SR_API_KEY
SR_API_SECRET=$SR_API_SECRET
EOF

echo ""
echo "✅ Configuration updated in scripts/config.env"
echo ""

# Create client configuration file
cat > scripts/client.properties << EOF
# Kafka Client Configuration
bootstrap.servers=$BOOTSTRAP_SERVER
security.protocol=SASL_SSL
sasl.mechanisms=PLAIN
sasl.username=$KAFKA_API_KEY
sasl.password=$KAFKA_API_SECRET

# Schema Registry
schema.registry.url=$SR_ENDPOINT
basic.auth.credentials.source=USER_INFO
basic.auth.user.info=$SR_API_KEY:$SR_API_SECRET
EOF

echo "✅ Client configuration saved to scripts/client.properties"
echo ""

echo "================================================"
echo "✅ Cluster Setup Complete!"
echo "================================================"
echo ""
echo "Cluster Details:"
echo "  Cluster ID: $CLUSTER_ID"
echo "  Bootstrap:  $BOOTSTRAP_SERVER"
echo "  SR URL:     $SR_ENDPOINT"
echo ""
echo "Next step: ./scripts/create-topics.sh"
echo ""

# Made with Bob
