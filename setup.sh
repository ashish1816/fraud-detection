#!/bin/bash

# ============================================================================
# Fraud Detection Platform - Setup Script
# ============================================================================
# Run this script from the confluent-fraud-detection directory
# Usage: ./setup.sh
# ============================================================================

set -e  # Exit on error

echo "🚀 Fraud Detection Platform Setup"
echo "=================================="
echo ""

# Check if we're in the right directory
if [ ! -f "HACKATHON_QUICKSTART.md" ]; then
    echo "❌ Error: Please run this script from the confluent-fraud-detection directory"
    echo "   cd confluent-fraud-detection"
    echo "   ./setup.sh"
    exit 1
fi

# Create working directory
mkdir -p scripts
mkdir -p data
mkdir -p logs

echo "✅ Directory structure created"
echo ""

# Check prerequisites
echo "📋 Checking prerequisites..."
echo ""

# Check Confluent CLI
if ! command -v confluent &> /dev/null; then
    echo "❌ Confluent CLI not found. Installing..."
    echo "   Visit: https://docs.confluent.io/confluent-cli/current/install.html"
    exit 1
else
    echo "✅ Confluent CLI found: $(confluent version)"
fi

# Check Python
if ! command -v python3 &> /dev/null; then
    echo "❌ Python 3 not found. Please install Python 3.8+"
    exit 1
else
    echo "✅ Python found: $(python3 --version)"
fi

# Check jq
if ! command -v jq &> /dev/null; then
    echo "⚠️  jq not found. Installing..."
    if [[ "$OSTYPE" == "darwin"* ]]; then
        brew install jq
    elif [[ "$OSTYPE" == "linux-gnu"* ]]; then
        sudo apt-get install -y jq
    fi
fi

echo ""
echo "✅ All prerequisites met!"
echo ""

# Login to Confluent Cloud
echo "🔐 Logging into Confluent Cloud..."
echo "   (You'll be redirected to browser for authentication)"
echo ""

confluent login --save

echo ""
echo "✅ Successfully logged in!"
echo ""

# Create environment
echo "🏗️  Creating Confluent Cloud environment..."
echo ""

ENV_NAME="fraud-detection-hackathon"
ENV_ID=$(confluent environment list -o json | jq -r ".[] | select(.name==\"$ENV_NAME\") | .id")

if [ -z "$ENV_ID" ]; then
    confluent environment create "$ENV_NAME" --governance-package essentials
    ENV_ID=$(confluent environment list -o json | jq -r ".[] | select(.name==\"$ENV_NAME\") | .id")
    echo "✅ Environment created: $ENV_ID"
else
    echo "✅ Environment already exists: $ENV_ID"
fi

confluent environment use "$ENV_ID"

# Save configuration
cat > scripts/config.env << EOF
# Confluent Cloud Configuration
# Generated: $(date)
ENVIRONMENT_ID=$ENV_ID
ENVIRONMENT_NAME=$ENV_NAME
EOF

echo ""
echo "✅ Configuration saved to scripts/config.env"
echo ""

echo "================================================"
echo "✅ Setup Complete!"
echo "================================================"
echo ""
echo "Next steps:"
echo "1. Create Kafka cluster:    ./scripts/create-cluster.sh"
echo "2. Create topics:           ./scripts/create-topics.sh"
echo "3. Deploy ksqlDB queries:   ./scripts/deploy-ksqldb.sh"
echo "4. Generate sample data:    python3 scripts/generate-data.py"
echo ""
echo "Or follow the step-by-step guide in HACKATHON_QUICKSTART.md"
echo ""

# Made with Bob
