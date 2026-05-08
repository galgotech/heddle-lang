# Heddle: The High-Performance Orchestration Weave

Heddle is a strictly-typed, domain-specific programming language (**DSL**) and high-performance orchestration engine built to eliminate "orchestration debt", the maintainability nightmares caused by fragmented microservices, untyped data pipelines, and excessive serialization overhead.

Whether coordinating **real-time Machine Learning (ML) inference**, **asynchronous data processing**, or **complex polyglot microservices**, Heddle bridges the gap between functional safety and imperative utility. It provides a unified orchestration layer for engineers tackling modern infrastructure challenges:

*   **Reliable ML Pipelines:** Seamlessly integrate sophisticated model logic with high-speed data delivery. By enforcing strict data contracts, Heddle ensures that data moving from preprocessing to inference remain consistent, eliminating the common "silent failures" found in loosely-typed systems.
*   **Structured Data Workflows:** Transform tangled, imperative scripts into clear, declarative pipelines. Heddle allows developers to model complex logical dependencies with a readable syntax, providing full visibility into execution flows and simplifying the maintenance of large-scale ingestion systems.
*   **Seamless Polyglot Coordination:** Unify services written in different languages under a single orchestration standard. Teams can leverage the strengths of Python, Go, Rust, or Node.js within a single workflow, ensuring that every handoff is validated through a strictly-typed shared interface.

---

## The Heddle Advantage

Heddle replaces tangled scripts with verifiable, high-performance pipelines. It prioritizes Developer Experience (**DX**) by eliminating the friction between local development and production scale.

*   **Zero-Copy Efficiency:** Utilizes **Apache Arrow** and **Arrow Flight** for universal memory exchange, allowing data to flow between Go, Python, Rust, and Node.js at the speed of local RAM.
*   **Static Safety:** A strictly-typed DSL catches logic errors at compile-time, while JIT type inference adapts schemas dynamically based on step configurations.
*   **Invisible Local-to-Cloud Bridge:** Execute workflows locally on your laptop or deploy to a global cluster with zero configuration changes.
*   **Native Relational Logic:** Embeds **PRQL** (Pipelined Relational Query Language) for side-effect-free data transformations directly within the orchestration layer.
*   **Radical Reusability:** The **fhub** (Functions Hub) ecosystem provides a "Lego-style" library of modular connectors and steps.

---

## Heddle in Action

Heddle uses explicit contracts and readable pipelines. Below is a comprehensive example demonstrating `resources`, `steps`, and native `PRQL` integration.

### The Orchestration Logic (`fraud_detection.he`)

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
  query: "SELECT id AS user_id, country FROM users WHERE id = @user_id"
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

---

## Core Architecture

Heddle utilizes a decoupled, high-performance architecture designed to separate logical orchestration from physical execution. This separation ensures that the system maintains high fault tolerance while delivering near-zero data serialization overhead.

### 1. The Smart Control Plane (Orchestrator)
The Control Plane manages global execution state and routing without processing raw payload data.

*   **Operator Fusion and DAG Optimization:** The compiler transforms the Heddle DSL into an Intermediate Representation (**IR**). It performs aggressive topology pruning and "Operator Fusion," merging continuous nodes into atomic **Super Steps**. This optimization minimizes the I/O bounds and inter-process communication overhead between workers.
*   **Dynamic JIT Provisioning:** Step implementations are dynamically loaded into stateless workers via a Just-In-Time (**JIT**) model. This ensures that workers remain lightweight and specialized for the current task. By caching binaries locally and using proactive injection, the system achieves **zero warm-up time** for new workflows.
*   **Secure Environment Injection:** The Control Plane acts as a secure proxy for infrastructure secrets and environment variables. It injects these sensitive configurations directly into worker memory at runtime, ensuring that no secrets are ever persisted on the worker nodes.
*   **Hierarchical State Isolation (CHASM):** Using the **CHASM** engine, Heddle implements strict state isolation. External failures trigger exclusive retries only for the problematic connector, bypassing massive **DAG** re-executions and ensuring deterministic recovery.


### 2. Polyglot Workers (Execution)
The execution layer uses a bifurcated architecture to support multi-language environments with maximum efficiency:

*   **Main Worker:** A central orchestrating daemon running on each host. It coordinates data retrieval from the **Data Locality Registry**, manages local plugin lifecycles, and synchronizes state with the Control Plane.
*   **Polyglot Plugins:** Language-specific execution units (Python, Go, Rust, Node.js) that execute business logic. The Main Worker delegates tasks to these plugins via Remote Procedure Call (**RPC**), passing data through zero-copy memory pointers.
*   **P2P Data Resolution:** Workers resolve data dependencies through Peer-to-Peer (**P2P**) communication, bypassing the Control Plane to avoid throughput bottlenecks.
*   **Cross-Workflow Batching:** To maximize efficiency, the Main Worker identifies lexical signature overlaps across independent pipelines. It consolidates parallel data flows into a single table (`HeddleFrame`), invoking the imperative code only once per batch, processing as single instruction multiple data (SIMD).
*   **Resource:** It manages the lifecycle of stateful resources (e.g., database handles).

### 3. Data Locality Registry work (Data Management)
Inspired by the **Vineyard (v6d)** architecture, this subsystem acts as an optimized memory-mapping layer.

*   **Zero-Copy Routing:** Maps **DAG** outputs to physical locations (e.g., Host IP, Memory Handle). This allows workers to access data directly from RAM without copying or serialization.
*   **Memory Offloading:** Dynamically manages data that exceeds local RAM capacity by offloading it to persistent storage while maintaining performance during large-scale aggregations.
*   **Garbage Collection:** Automatically tracks memory allocations and releases resources once the immutable intermediate data is no longer required by the workflow.

---

## Communication Protocols

Heddle enforces a strict physical separation between the Control Plane and the Data Plane to eliminate architectural bottlenecks and ensure consistent performance under load.

### 1. Control Plane ↔ Worker (The Control Channel)
This channel handles the administrative lifecycle of the Directed Acyclic Graph (**DAG**) without touching raw data payloads.

*   **Protocol:** Utilizes high-performance **gRPC** for low-latency command and control.
*   **Payload:** Transmits execution state, task assignments, resource metadata, and **JIT** code instructions.
*   **Optimization:** By isolating control logic from data traffic, the Control Plane remains responsive during data processing, as it never serializes or deserializes business data.

### 2. Worker ↔ Worker / Data Manager (The Data Plane)
The Data Plane handles the high-throughput transmission of **Apache Arrow** records between workers and the **Data Locality Registry**.

*   **Protocol:** Employs **Apache Arrow Flight**, a framework specifically optimized for high-performance transport of large columnar datasets.
*   **Flight Tickets:** Instead of sending raw data, the system passes lightweight **Flight Tickets**. These metadata objects contain the `RouteType`, `Address`, and `ResourceID` required for a worker to resolve and retrieve a physical memory handle.
*   **Dual-Mode Transport Strategy:**
    *   **Local (`LOCAL`)**: For workers on the same physical host, Heddle uses **Unix Domain Sockets (UDS)** and shared memory. This results in near-zero latency data resolution by passing memory pointers instead of copying bytes.
    *   **Remote (`REMOTE`)**: For cross-cluster communication, the system utilizes high-speed network paths over **gRPC**, optimized for concurrent stream processing of **Arrow RecordBatches**.


### The Polyglot Implementation

Heddle provides native SDKs for the most common systems languages. Each SDK leverages **Apache Arrow** for zero-copy memory exchange, ensuring that data transformations happen at the speed of local RAM regardless of the language.

#### Python SDK
Utilizes a decorator-based API for quick implementation of functions and resources.

```python
from heddle.sdk.plugin import Plugin
from heddle.core.table import Table
from heddle.core.step import StepConfig
from heddle.core.resource import ResourceConfig, Resource

# 1. Define Specific Table Types
class CredentialsTable(Table):
    """Input: contains 'id' and 'password'."""
    pass

class SecureTable(Table):
    """Output: contains 'id' and 'hash'."""
    pass

# 2. Define Resource & Step Configurations
class VaultConfig(ResourceConfig):
    api_key: str

class HashConfig(StepConfig[VaultConfig]):
    algorithm: str

# 3. Define the Resource Instance
class Vault(Resource):
    def __init__(self, api_key: str):
        self.api_key = api_key

# 4. Initialize and Register
plugin = Plugin(namespace="security")

@plugin.resource(name="vault")
def init_vault(config: VaultConfig) -> Vault:
    return Vault(config.api_key)

@plugin.step(name="hash", resource="vault")
def hash_password(config: HashConfig, input: CredentialsTable) -> SecureTable:
    # The 'vault' resource is automatically injected into the config
    vault = config.resource 
    
    df = input.to_pandas() # Contains 'id' and 'password'
    # ... use vault.api_key for salt or pepper ...
    return SecureTable.from_pandas(df) # Returns 'id' and 'hash'

if __name__ == "__main__":
    plugin.serve()
```

#### Go SDK
Optimized for high-concurrency and native performance within the Go ecosystem.

```go
import (
	"context"
	"github.com/galgotech/heddle-lang/sdk/go/core"
	"github.com/galgotech/heddle-lang/sdk/go/plugin"
)

// 1. Define Specific Table Types
type CredentialsTable struct { core.Table } // Input: id, password
type SecureTable struct { core.Table }      // Output: id, hash

// 2. Define Resource & Step Configurations
type VaultConfig struct { ApiKey string `json:"api_key"` }
type HashConfig struct {
	Algorithm string `json:"algorithm"`
	Resource  *Vault `json:"-"` // Automatically injected
}

// 3. Define the Resource Instance
type Vault struct { ApiKey string }
func (v *Vault) Start(ctx context.Context) error { return nil }

// 4. Define the Step Logic with Resource Injection
func HashPassword(ctx context.Context, cfg HashConfig, input CredentialsTable) (SecureTable, error) {
	record := input.Native() // Contains 'id' and 'password'
	// The resource is now a key of the config
	vault := cfg.Resource 
	
	// ... use vault.ApiKey for salt or pepper ...
	return SecureTable{core.NewTableFromRecord(record)}, nil
}

func main() {
	p := plugin.New("security")
	plugin.RegisterResource(p, "vault", func(ctx context.Context, cfg VaultConfig) (*Vault, error) {
		return &Vault{ApiKey: cfg.ApiKey}, nil
	})
	plugin.RegisterStep(p, "hash", HashPassword, plugin.WithResource("vault"))
	p.Serve()
}
```

#### Node.js SDK
Class-based approach using TypeScript decorators for enterprise-grade orchestration.

```typescript
import { Plugin, Step, Resource, Table } from '@heddle/sdk';

// 1. Define Specific Table Types
class CredentialsTable extends Table {} // Input: id, password
class SecureTable extends Table {}      // Output: id, hash

// 2. Define Resource & Step Configurations
interface VaultConfig { api_key: string; }
interface HashConfig { algorithm: string; resource?: Vault; }

// 3. Define the Resource Instance
class Vault {
  constructor(public apiKey: string) {}
  async start() {}
}

// 4. Define the Plugin logic
class SecurityPlugin {
  @Resource({ name: 'vault' })
  initVault(config: VaultConfig): Vault {
    return new Vault(config.api_key);
  }

  @Step({ name: 'hash', resource: 'vault' })
  async hash(config: HashConfig, input: CredentialsTable): Promise<SecureTable> {
    const vault = config.resource!; // Injected from the vault resource
    const df = input.toDataFrame(); // Contains 'id' and 'password'
    // ... perform hashing logic (password -> hash) ...
    return new SecureTable(df);
  }
}

const plugin = new Plugin('security');
plugin.register(SecurityPlugin);
plugin.serve();
```

#### Rust SDK
Aggressive memory safety and performance using zero-copy trait-based implementation.

```rust
use heddle_sdk::{plugin, Step, Resource, Table, Result};
use serde::Deserialize;

// 1. Define Specific Table Types
struct CredentialsTable(Table); // Input: id, password
struct SecureTable(Table);      // Output: id, hash

// 2. Define Resource & Step Configurations
#[derive(Deserialize)]
struct VaultConfig { api_key: String }
#[derive(Deserialize)]
struct HashConfig { 
    algorithm: String,
    resource: Option<Vault>, // Injected from the vault resource
}

// 3. Define the Resource Instance
struct Vault { api_key: String }
impl Resource for Vault {
    async fn start(&self) -> Result<()> { Ok(()) }
}

// 4. Define the Plugin logic
#[plugin(namespace = "security")]
pub struct SecurityPlugin;

#[Step(name = "hash", resource = "vault")]
impl SecurityPlugin {
    async fn hash(&self, config: HashConfig, input: CredentialsTable) -> Result<SecureTable> {
        let vault = config.resource.as_ref().unwrap();
        let df = input.0.as_polars()?; // Contains 'id' and 'password'
        // ... use vault.api_key for high-speed hashing ...
        Ok(SecureTable(Table::try_from(df)?))
    }
}
```

---

## Project Structure

Heddle is a polyglot monorepo designed for integration between the control plane and multi-language SDKs.

```plaintext
/
├── cmd/                  # CLI entrypoints (cluster, development, local)
├── internal/services/    # Core services (Control Plane, LSP, Debug Adapter)
├── sdk/                  # Polyglot SDKs (Python, Go, Rust, Node.js)
├── pkg/                  # Shared libraries (Parser, IR, Arrow utilities)
├── editors/              # Editor integrations (VS Code extension)
└── web/                  # Playground
```

---

## Getting Started

Visit the [official documentation](https://docs.heddle.dev) for installation guides, language specifications, and tutorials.

**Heddle: Join the Weave.**
