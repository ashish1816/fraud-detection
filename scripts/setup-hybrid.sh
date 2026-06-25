#!/bin/bash
# Setup script for hybrid fraud detection system
# Creates the fraud-alerts topic in Confluent Cloud

echo "🚀 Setting up Hybrid Fraud Detection System"
echo "=========================================="
echo ""

# Check if confluent CLI is installed
if ! command -v confluent &> /dev/null; then
    echo "❌ Error: confluent CLI not found"
    echo "Install from: https://docs.confluent.io/confluent-cli/current/install.html"
    exit 1
fi

echo "✅ Confluent CLI found"
echo ""

# Create fraud-alerts topic
echo "📝 Creating fraud-alerts topic..."
confluent kafka topic create fraud-alerts \
    --partitions 6 \
    --if-not-exists

if [ $? -eq 0 ]; then
    echo "✅ fraud-alerts topic created successfully"
else
    echo "⚠️  Topic may already exist or creation failed"
fi

echo ""
echo "📋 Listing all topics:"
confluent kafka topic list

echo ""
echo "=========================================="
echo "✅ Setup complete!"
echo ""
echo "Next steps:"
echo "1. Run the hybrid fraud detector:"
echo "   python3 scripts/fraud-detector-hybrid.py"
echo ""
echo "2. Monitor fraud-alerts topic:"
echo "   confluent kafka topic consume fraud-alerts --from-beginning"
echo ""
echo "3. (Optional) Add a sink connector for visualization"
echo "=========================================="

# Made with Bob
