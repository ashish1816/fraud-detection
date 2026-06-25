#!/bin/bash

# Deploy Kafka Connectors to Confluent Cloud
# This script deploys JDBC Source and Elasticsearch Sink connectors

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(dirname "$SCRIPT_DIR")"

# Load configuration
if [ -f "$SCRIPT_DIR/config.env" ]; then
    source "$SCRIPT_DIR/config.env"
else
    echo "❌ Error: config.env not found. Run setup.sh first."
    exit 1
fi

echo "========================================================================"
echo "🔌 Deploying Kafka Connectors to Confluent Cloud"
echo "========================================================================"

# Check if connectors are supported in Confluent Cloud
echo ""
echo "📋 Checking Confluent Cloud connector availability..."

# Note: Confluent Cloud has managed connectors
# For fully managed connectors, use Confluent Cloud UI or CLI
# For self-managed, we need Kafka Connect cluster

echo ""
echo "⚠️  IMPORTANT: Connector Deployment Options"
echo "========================================================================"
echo ""
echo "Option 1: Confluent Cloud Managed Connectors (Recommended)"
echo "   - Go to: https://confluent.cloud"
echo "   - Navigate to your cluster"
echo "   - Click 'Connectors' in left menu"
echo "   - Add 'PostgreSQL CDC Source' connector"
echo "   - Add 'Elasticsearch Sink' connector"
echo "   - Configure using the JSON templates in connectors/ directory"
echo ""
echo "Option 2: Self-Managed Kafka Connect (Advanced)"
echo "   - Deploy Kafka Connect cluster (see below)"
echo "   - Install connector plugins"
echo "   - Deploy connectors via REST API"
echo ""
echo "========================================================================"

# Function to deploy connector via REST API (for self-managed Connect)
deploy_connector() {
    local connector_file=$1
    local connect_url=${KAFKA_CONNECT_URL:-"http://localhost:8083"}
    
    if [ ! -f "$connector_file" ]; then
        echo "❌ Connector file not found: $connector_file"
        return 1
    fi
    
    echo "📤 Deploying connector from: $connector_file"
    
    # Extract connector name from JSON
    connector_name=$(jq -r '.name' "$connector_file")
    
    # Check if connector already exists
    if curl -s "$connect_url/connectors/$connector_name" > /dev/null 2>&1; then
        echo "⚠️  Connector '$connector_name' already exists. Updating..."
        curl -X PUT \
            -H "Content-Type: application/json" \
            --data @"$connector_file" \
            "$connect_url/connectors/$connector_name/config"
    else
        echo "➕ Creating new connector '$connector_name'..."
        curl -X POST \
            -H "Content-Type: application/json" \
            --data @"$connector_file" \
            "$connect_url/connectors"
    fi
    
    echo ""
}

# Create simplified connector configurations for deployment
create_postgres_source_config() {
    cat > "$SCRIPT_DIR/postgres-source-connector.json" <<EOF
{
  "name": "postgres-transactions-source",
  "config": {
    "connector.class": "io.confluent.connect.jdbc.JdbcSourceConnector",
    "tasks.max": "1",
    "connection.url": "jdbc:postgresql://localhost:5432/transactions",
    "connection.user": "frauduser",
    "connection.password": "fraudpass123",
    "mode": "timestamp+incrementing",
    "incrementing.column.name": "transaction_id",
    "timestamp.column.name": "created_at",
    "topic.prefix": "postgres-",
    "table.whitelist": "transactions",
    "poll.interval.ms": "1000",
    "batch.max.rows": "1000",
    "validate.non.null": "true",
    "key.converter": "org.apache.kafka.connect.converters.LongConverter",
    "value.converter": "io.confluent.connect.avro.AvroConverter",
    "value.converter.schema.registry.url": "${SCHEMA_REGISTRY_URL}",
    "value.converter.basic.auth.credentials.source": "USER_INFO",
    "value.converter.basic.auth.user.info": "${SCHEMA_REGISTRY_API_KEY}:${SCHEMA_REGISTRY_API_SECRET}"
  }
}
EOF
    echo "✅ Created PostgreSQL source connector config"
}

create_elasticsearch_sink_config() {
    cat > "$SCRIPT_DIR/elasticsearch-sink-connector.json" <<EOF
{
  "name": "elasticsearch-fraud-alerts-sink",
  "config": {
    "connector.class": "io.confluent.connect.elasticsearch.ElasticsearchSinkConnector",
    "tasks.max": "1",
    "topics": "fraud-alerts,fraud-alerts-comprehensive",
    "connection.url": "http://localhost:9200",
    "type.name": "_doc",
    "key.ignore": "false",
    "schema.ignore": "false",
    "batch.size": "1000",
    "max.buffered.records": "10000",
    "linger.ms": "1000",
    "flush.timeout.ms": "10000",
    "max.retries": "5",
    "retry.backoff.ms": "100",
    "key.converter": "org.apache.kafka.connect.storage.StringConverter",
    "value.converter": "io.confluent.connect.avro.AvroConverter",
    "value.converter.schema.registry.url": "${SCHEMA_REGISTRY_URL}",
    "value.converter.basic.auth.credentials.source": "USER_INFO",
    "value.converter.basic.auth.user.info": "${SCHEMA_REGISTRY_API_KEY}:${SCHEMA_REGISTRY_API_SECRET}"
  }
}
EOF
    echo "✅ Created Elasticsearch sink connector config"
}

# Main deployment logic
echo ""
read -p "Do you want to create connector configuration files? (y/n): " create_configs

if [ "$create_configs" = "y" ]; then
    echo ""
    echo "📝 Creating connector configuration files..."
    create_postgres_source_config
    create_elasticsearch_sink_config
    echo ""
    echo "✅ Connector configurations created in scripts/ directory"
    echo ""
fi

echo ""
read -p "Do you have a Kafka Connect cluster running? (y/n): " has_connect

if [ "$has_connect" = "y" ]; then
    read -p "Enter Kafka Connect REST API URL (default: http://localhost:8083): " connect_url
    KAFKA_CONNECT_URL=${connect_url:-"http://localhost:8083"}
    
    echo ""
    echo "🔌 Deploying connectors to $KAFKA_CONNECT_URL..."
    
    # Deploy PostgreSQL Source
    if [ -f "$SCRIPT_DIR/postgres-source-connector.json" ]; then
        deploy_connector "$SCRIPT_DIR/postgres-source-connector.json"
    fi
    
    # Deploy Elasticsearch Sink
    if [ -f "$SCRIPT_DIR/elasticsearch-sink-connector.json" ]; then
        deploy_connector "$SCRIPT_DIR/elasticsearch-sink-connector.json"
    fi
    
    echo ""
    echo "✅ Connector deployment complete!"
    echo ""
    echo "📊 Check connector status:"
    echo "   curl $KAFKA_CONNECT_URL/connectors"
    echo "   curl $KAFKA_CONNECT_URL/connectors/postgres-transactions-source/status"
    echo "   curl $KAFKA_CONNECT_URL/connectors/elasticsearch-fraud-alerts-sink/status"
else
    echo ""
    echo "📚 To deploy connectors, you need:"
    echo ""
    echo "1. Kafka Connect Cluster Options:"
    echo "   a) Use Confluent Cloud UI (easiest)"
    echo "   b) Run self-managed Connect with Docker:"
    echo ""
    echo "      docker run -d \\"
    echo "        --name kafka-connect \\"
    echo "        --network fraud-network \\"
    echo "        -p 8083:8083 \\"
    echo "        -e CONNECT_BOOTSTRAP_SERVERS=\"$BOOTSTRAP_SERVERS\" \\"
    echo "        -e CONNECT_REST_PORT=8083 \\"
    echo "        -e CONNECT_GROUP_ID=\"fraud-connect-cluster\" \\"
    echo "        -e CONNECT_CONFIG_STORAGE_TOPIC=\"fraud-connect-configs\" \\"
    echo "        -e CONNECT_OFFSET_STORAGE_TOPIC=\"fraud-connect-offsets\" \\"
    echo "        -e CONNECT_STATUS_STORAGE_TOPIC=\"fraud-connect-status\" \\"
    echo "        -e CONNECT_KEY_CONVERTER=\"org.apache.kafka.connect.storage.StringConverter\" \\"
    echo "        -e CONNECT_VALUE_CONVERTER=\"io.confluent.connect.avro.AvroConverter\" \\"
    echo "        -e CONNECT_VALUE_CONVERTER_SCHEMA_REGISTRY_URL=\"$SCHEMA_REGISTRY_URL\" \\"
    echo "        -e CONNECT_REST_ADVERTISED_HOST_NAME=\"localhost\" \\"
    echo "        -e CONNECT_PLUGIN_PATH=\"/usr/share/java,/usr/share/confluent-hub-components\" \\"
    echo "        -e CONNECT_SECURITY_PROTOCOL=\"SASL_SSL\" \\"
    echo "        -e CONNECT_SASL_MECHANISM=\"PLAIN\" \\"
    echo "        -e CONNECT_SASL_JAAS_CONFIG=\"org.apache.kafka.common.security.plain.PlainLoginModule required username='$KAFKA_API_KEY' password='$KAFKA_API_SECRET';\" \\"
    echo "        confluentinc/cp-kafka-connect:7.5.0"
    echo ""
    echo "2. Install connector plugins:"
    echo "   docker exec -it kafka-connect confluent-hub install confluentinc/kafka-connect-jdbc:latest"
    echo "   docker exec -it kafka-connect confluent-hub install confluentinc/kafka-connect-elasticsearch:latest"
    echo ""
    echo "3. Deploy connectors:"
    echo "   ./scripts/deploy-connectors.sh"
fi

echo ""
echo "========================================================================"
echo "📖 Next Steps:"
echo "========================================================================"
echo ""
echo "1. Start PostgreSQL and Elasticsearch:"
echo "   docker-compose up -d postgres elasticsearch"
echo ""
echo "2. Deploy connectors (Confluent Cloud UI or self-managed)"
echo ""
echo "3. Start fraud detector:"
echo "   python3 scripts/fraud-detector.py"
echo ""
echo "4. Monitor data flow in Confluent Cloud UI"
echo ""
echo "========================================================================"

# Made with Bob
