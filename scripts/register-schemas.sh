#!/bin/bash

# ============================================================================
# Register Avro Schemas
# ============================================================================
# Run from: confluent-fraud-detection directory
# Usage: ./scripts/register-schemas.sh
# ============================================================================

set -e

# Load configuration
source scripts/config.env

echo "📋 Registering Avro Schemas..."
echo ""

# Transaction schema
cat > data/transaction-schema.json << 'EOF'
{
  "schema": "{\"type\":\"record\",\"name\":\"Transaction\",\"namespace\":\"com.frauddetection\",\"fields\":[{\"name\":\"transaction_id\",\"type\":\"long\"},{\"name\":\"user_id\",\"type\":\"long\"},{\"name\":\"amount\",\"type\":\"double\"},{\"name\":\"merchant_id\",\"type\":\"long\"},{\"name\":\"currency\",\"type\":\"string\"},{\"name\":\"payment_method\",\"type\":\"string\"},{\"name\":\"ip_address\",\"type\":\"string\"},{\"name\":\"device_id\",\"type\":\"string\"},{\"name\":\"latitude\",\"type\":\"double\"},{\"name\":\"longitude\",\"type\":\"double\"},{\"name\":\"created_at\",\"type\":\"long\"}]}"
}
EOF

echo "Registering transaction schema..."
curl -X POST "$SR_ENDPOINT/subjects/postgres-transactions-value/versions" \
  -u "$SR_API_KEY:$SR_API_SECRET" \
  -H "Content-Type: application/vnd.schemaregistry.v1+json" \
  -d @data/transaction-schema.json

echo ""
echo "✅ Transaction schema registered"
echo ""

# User profile schema
cat > data/user-profile-schema.json << 'EOF'
{
  "schema": "{\"type\":\"record\",\"name\":\"UserProfile\",\"namespace\":\"com.frauddetection\",\"fields\":[{\"name\":\"user_id\",\"type\":\"long\"},{\"name\":\"email\",\"type\":\"string\"},{\"name\":\"account_age_days\",\"type\":\"int\"},{\"name\":\"kyc_verified\",\"type\":\"boolean\"},{\"name\":\"risk_score\",\"type\":\"int\"},{\"name\":\"country\",\"type\":\"string\"},{\"name\":\"total_transactions\",\"type\":\"int\"},{\"name\":\"total_amount_lifetime\",\"type\":\"double\"},{\"name\":\"updated_at\",\"type\":\"long\"}]}"
}
EOF

echo "Registering user profile schema..."
curl -X POST "$SR_ENDPOINT/subjects/mongo-user_profiles-value/versions" \
  -u "$SR_API_KEY:$SR_API_SECRET" \
  -H "Content-Type: application/vnd.schemaregistry.v1+json" \
  -d @data/user-profile-schema.json

echo ""
echo "✅ User profile schema registered"
echo ""

echo "================================================"
echo "✅ All Schemas Registered!"
echo "================================================"
echo ""
echo "Next step: python3 scripts/generate-data.py"
echo ""

# Made with Bob
