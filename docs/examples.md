# Examples

Heddle is designed for complex orchestration tasks. Below is a comprehensive example demonstrating cross-service integration, database enrichment, and error handling.

## Real-time Fraud Detection

This workflow demonstrates:
- Consuming from Kafka
- Relational filtering using PRQL
- Integration with external steps (PostgreSQL, ClickHouse)
- LLM-powered audit generation (OpenAI)
- Hierarchical error handling

```heddle
import "fhub/kafka" kafka
import "fhub/postgresql" pg
import "fhub/clickhouse" ch
import "fhub/llm" openai
import "fraud-score/detect" fraud_detection

// 1. Centralized Resources (State/Connections)
// PostgreSQL
resource pg_db = pg.connection {
  host: "pg.internal"
}

// Clickhouse
resource ch_db = ch.connection {
  host: "ch.internal"
}

// Kafka
resource kf_broker = kafka.connection {
  broker: "kafka.internal:9092"
}

// 2. Bound Imperative Steps with Resource Injection
step fetch_user_data = pg.query <connection=pg_db> {
  query: "SELECT id AS user_id, country FROM users WHERE id = @id"
}

step fetch_risk_profile = ch.query <connection=ch_db> {
  query: "SELECT user_id, velocity_score FROM risk_metrics WHERE user_id = @user_id"
}

step generate_audit = openai.prompt {
  system: "Analyze transaction, location, and velocity score. Generate a fraud audit text report."
}

// Global error catcher
handler alert_on_fail {
  *
    | kafka.produce <broker=kf_broker> { topic: "dlq_alerts" }
}

// Step error catcher
handler alert_step_fail {
  *
    | kafka.produce <broker=kf_broker> { topic: "dlq_alerts" }
}


// 3. Strict DAG Workflow
workflow FraudDetection ? alert_on_fail {

  kafka.consume <broker=kf_broker> { topic: "live_transactions" }
  > tx_stream

  // 1. Filter: High-value txns isolated via native PRQL
  // 2. Process: Imperative logic with localized error trap
  // 3. Enrich: Joined with user data & risk metrics
  // 4. Audit: LLM generates natural language report
  tx_stream
    | (
        from input
        filter amount > 10000
        select {user_id, amount}
      ) 
    | fraud_detection.process ? alert_step_fail
    | (
        from input
        join fetch_user_data (==user_id)
        join fetch_risk_profile (==user_id)
      )
    | generate_audit
    | kafka.produce <broker=kf_broker> { topic: "fraud_audits" }
}
```
