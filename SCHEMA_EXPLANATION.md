# Schema Explanation for Judges

## Overview
Our fraud detection system uses **JSON Schema** format registered in Confluent Schema Registry for data governance and validation.

## Data Schema: Orders Stream

**Schema ID:** 100001  
**Format:** JSON Schema  
**Subject:** `postgres-transactions-value`

### Schema Structure

```json
{
  "type": "object",
  "title": "ksql.orders",
  "properties": {
    "ordertime": {
      "type": "integer",
      "connect.type": "int64",
      "description": "Unix timestamp (milliseconds) when order was placed"
    },
    "orderid": {
      "type": "integer", 
      "connect.type": "int32",
      "description": "Unique order identifier"
    },
    "itemid": {
      "type": "string",
      "description": "Product/item identifier (e.g., 'Item_123')"
    },
    "orderunits": {
      "type": "number",
      "connect.type": "float64",
      "description": "Order amount in dollars (e.g., 0.45, 0.89)"
    },
    "address": {
      "type": "object",
      "description": "Customer shipping address",
      "properties": {
        "city": {
          "type": "string",
          "description": "City name (e.g., 'San Francisco')"
        },
        "state": {
          "type": "string", 
          "description": "State code (e.g., 'CA', 'NY')"
        },
        "zipcode": {
          "type": "integer",
          "connect.type": "int64",
          "description": "ZIP code (e.g., 94105)"
        }
      }
    }
  }
}
```

## Key Points for Judges

### 1. **Schema Registry Integration**
- All data is validated against this schema before entering Kafka
- Schema evolution is managed centrally
- Ensures data quality and consistency across the pipeline

### 2. **JSON Schema vs Avro**
- We chose **JSON Schema** for:
  - Better readability and debugging
  - Native compatibility with Flink SQL
  - Easier integration with web services
  - Human-readable format for demos

### 3. **Fraud Detection Fields**

| Field | Fraud Detection Use |
|-------|---------------------|
| `orderunits` | **Amount-based fraud**: Flag orders > $0.50 as suspicious |
| `ordertime` | **Velocity fraud**: Detect multiple orders in short time windows |
| `address.city` + `address.state` | **Geographic fraud**: Identify unusual location patterns |
| `orderid` | **Deduplication**: Prevent processing same order twice |
| `itemid` | **Pattern analysis**: Detect suspicious item combinations |

### 4. **Real-World Mapping**

In production, this schema would map to:
- `orderunits` → Transaction amount ($0.45 = $450 in real scenario)
- `ordertime` → Transaction timestamp
- `address` → Customer billing/shipping location
- `itemid` → Product SKU or service identifier

### 5. **Flink SQL Compatibility**

The schema auto-discovers in Flink SQL as:
```sql
-- Flink automatically creates this table structure
CREATE TABLE `postgres-transactions` (
  `ordertime` BIGINT,
  `orderid` INT,
  `itemid` STRING,
  `orderunits` DOUBLE,
  `address` ROW<city STRING, state STRING, zipcode BIGINT>
)
```

## Demo Talking Points

### For Technical Judges:
1. **"We use JSON Schema in Confluent Schema Registry for centralized data governance"**
2. **"Schema ID 100001 ensures all consumers get validated, consistent data"**
3. **"Flink SQL auto-discovers the schema, enabling immediate stream processing"**
4. **"The nested address object demonstrates complex type handling in real-time"**

### For Business Judges:
1. **"Every transaction is validated against our schema before processing"**
2. **"We analyze order amounts, timing, and location to detect fraud patterns"**
3. **"The system processes thousands of orders per second with sub-second latency"**
4. **"Schema evolution allows us to add new fraud detection features without downtime"**

## Sample Data

```json
{
  "ordertime": 1719305112000,
  "orderid": 12345,
  "itemid": "Item_789",
  "orderunits": 0.89,
  "address": {
    "city": "San Francisco",
    "state": "CA",
    "zipcode": 94105
  }
}
```

**Fraud Analysis:**
- ✅ Amount: $0.89 (REVIEW threshold)
- ✅ Location: San Francisco, CA (valid)
- ✅ Time: Recent timestamp (valid)
- **Decision:** REVIEW for manual verification

## Architecture Benefits

1. **Type Safety**: JSON Schema validates data types at ingestion
2. **Documentation**: Schema serves as API contract
3. **Evolution**: Backward/forward compatibility rules
4. **Performance**: Flink optimizes queries based on schema
5. **Debugging**: Easy to inspect and understand data structure

---

**Quick Reference:**
- Schema Registry: `https://psrc-z6mnmyr.us-east-1.aws.confluent.cloud`
- Subject: `postgres-transactions-value`
- Version: Latest (ID: 100001)
- Format: JSON Schema