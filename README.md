# Heddle: The High-Performance Orchestration Weave

Heddle is a strictly-typed, domain-specific programming language (**DSL**) and orchestration engine engineered for high-performance pipelines independently of business domains. It bridges the gap between functional safety and imperative utility, delivering execution through a native Go core integrated with Python, Node.js, Rust, and Go with zero-copy data transmission.

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

### The Orchestration Logic (`auth_service.he`)

```heddle
import "fhub/http" http
import "fhub/database" db
import "std/io" io

// 1. Define Reusable Resources
resource pg_db = db.connection {
  host: "db.internal"
  port: 5432
}

// 2. Bind Imperative Steps
step fetch_user = db.query <connection=pg_db> {
  query: "SELECT id, username FROM users WHERE id = @id"
}

step send_welcome = io.print {
  message: "User authenticated successfully."
}

// 3. Orchestrate the Workflow
workflow Login {
  http.post { path: "/login" }
    | (from input select id) // Relational transformation
    | fetch_user
    > authenticated_user

  authenticated_user
    | send_welcome
}
```

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

## Core Architecture

Heddle operates on a decoupled architecture that ensures robust fault tolerance and performance.

### 1. The Smart Control Plane
A self-contained Go binary that reads the logical topology (**DAG**), optimizes execution plans through operator fusion, and routes metadata via a **Data Locality Registry**. It manages execution state without handling raw payload traffic.

### 2. Polyglot Workers
Stateless execution units that run on the same host or across a cluster. Workers utilize **JIT Code Injection** to load imperative functions (Steps) dynamically.

### 3. Data Locality Registry
An optimized memory-mapping subsystem that maps DAG outputs to physical locations (Memory Handles). It enables zero-copy data routing and handles memory offloading for large datasets.

---

## Project Structure

Heddle is a polyglot monorepo designed for tight integration between the control plane and multi-language SDKs.

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
