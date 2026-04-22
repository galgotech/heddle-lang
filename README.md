# **Heddle: The Language for Orchestration Logic**

_Simplicity, Order, and Radical Reuse._

Heddle is a strictly-typed, domain-specific programming language (DSL) and orchestration engine designed for orchestrating logic, whether it be data pipelines, microservices, or other tasks. It bridges the gap between the rigorous safety of functional data pipelines and the pragmatic utility of imperative code (Go/Python/NodeJS/Rust).

**Join our community of adopters, contributors, and visionaries building a massive ecosystem of reusable Steps and modules!**

---

## **What Does Heddle Look Like?**

Heddle favors explicit contracts and readable pipelines. Below is a comprehensive example demonstrating schemas, resources, complex error handlers, native PRQL integration, and multiple workflows in a single file.

```heddle
// auth_service.he

import "fhub/http" http
import "fhub/database" db
import "fhub/security" security
import "fhub/input" input
import "fhub/console" console
import "fhub/window" window
import "std/io" io

// 1. Define Strict Schemas
schema User {
  id: int
  username: string
}

// 2. Define Reusable Steps (FFI to Go/Python/Rust)
step route_login: void -> User = http.post {
  path: "/login",
  response: { contentType: "application/json" }
}

step validate_login: User -> User = input.validate {
  rules: {
    username: { type: "string", minLength: 1 },
    password: "string"
  }
}

step password_security: User -> User = security.hash {
  algorithm: "argon2",
  config: { rounds: 10 }
}

step user_exists: User -> User = db.query {
  query: "SELECT id, username FROM users WHERE username = $username AND password = $password"
}

step send_welcome_email: User -> void = io.print { message: "Sending email..." }

// 3. Advanced Error Handling
handler global_error_handler {
  * error -> void = console.log
}

handler detailed_error_handler {
  // Chain error processing
  * error -> error = error.process
    | error -> error = error.add_debug_info
  > enriched_error

  enriched_error | error -> void = console.log
  enriched_error | error -> void = kafka.retry_queue
}

// 4. Orchestrate Complex Workflows
workflow Login ? global_error_handler {
  route_login
    | window.accumulate { interval: 50 } // Rate limiting window
    | validate_login ? detailed_error_handler
    | (from input select { username, password }) // PRQL enrichment
    | password_security
    | user_exists ? detailed_error_handler
  > authenticated_user

  // Fan-out execution
  authenticated_user | http.response { status: 200 }
  authenticated_user | send_welcome_email
}

workflow Register ? global_error_handler {
  http.post { path: "/register" }
    | validate_login ? detailed_error_handler
    | db.insert { table: "users" }
    | http.response { status: 201 }
}
```

---

### **The Python Counterpart (SDK)**

Connecting your existing imperative logic to Heddle is seamless. Here is how a custom Step is implemented in Python:

```python
# local/security.py
from typing import TypedDict
from heddle.core import Table

class HashConfig(TypedDict):
    algorithm: str
    rounds: int

def hash_password(config: HashConfig, input_table: Table) -> Table:
    # input_table is a zero-copy Apache Arrow table!
    arrow_data = input_table.to_pandas()
    
    # ... perform hashing logic ...
    
    return Table.from_pandas(arrow_data)
```

---

## **Core Philosophy**

### **The Language: Declarative Orchestration**

Heddle's DSL (`.he`) is a strictly-typed, functional language designed to eliminate the maintainability nightmare of traditional data orchestration and microservices. By enforcing a mathematical boundary between **orchestration logic** (the *what*) and **imperative computation** (the *how*), Heddle replaces brittle scripts with verifiable, high-performance pipelines.

*   **Static Safety:** Catch logic errors at compile-time with a robust, strictly-typed system.
*   **Relational Native:** Embedded PRQL support allows for side-effect-free data enrichment directly within the orchestration layer.
*   **Radical Reuse:** Designed for the `fhub` ecosystem, enabling LEGO-like modularity of data connectors and execution steps.

### **The Transparent Serverless Supercomputer**

The Heddle engine is a high-performance distributed fabric that abstracts away the friction of modern cloud infrastructure. It acts as a **Smart Control Plane**, providing the illusion of a single, infinitely scalable machine where execution is local by default and distributed by necessity.

*   **Zero-Copy Memory Interconnect:** Powered by Apache Arrow, Heddle eliminates serialization overhead, allowing data to flow between polyglot workers (Python, Rust, Node.js) at the speed of local RAM.
*   **The Invisible Bridge:** Erases the boundary between local development and production clusters. Your laptop becomes the head node of a global supercomputer with zero configuration changes.
*   **Deep Observability:** A "transparent" execution model with native DAG visualization and time-travel debugging, allowing you to trace the exact lineage of every data packet.

---

## **Core Architecture: Host-Core Symbiosis**

Heddle operates on a dual-paradigm architecture, functioning as a 100% self-contained "Smart Control Plane" that routes logic to isolated Workers:

* **The Functional Core (Go):** A declarative, strictly-typed orchestration layer operating as a highly concurrent engine. It manages the Directed Acyclic Graph (DAG) logic, static analysis, and task routing.
* **The Imperative Host (Go, Python, Node.js, Rust):** Computations and side-effect-heavy tasks (e.g., API calls, database writes) are encapsulated into "Steps". These steps act as a Foreign Function Interface (FFI), executing business logic in isolated, always-warm stateful workers.

### **How It Works: The Flow of Execution**

```plaintext
[Local Machine / IDE]
         | 
         |  (Heddle CLI: Executes 100% locally OR triggers remote cluster)
         v
+---------------------------------------------------------+
|            SMART CONTROL PLANE (Go Core)                |
|                                                         |
|  1. Parses Functional DSL (.he)                         |
|  2. Analyzes & Optimizes DAG (Operator Fusion)          |
|  3. Routes Data & Injects Code dynamically              |
+---------------------------------------------------------+
         |
         |  (Apache Arrow Flight RPC - Zero-Copy Transfer)
         v
+---------------------------------------------------------+
|                ISOLATED WORKERS                         |
|                                                         |
|  [ Imperative Logic ]       [ Functional Transforms ]   |
|  - Go SDK                   - Go PRQL                   |
|  - Python SDK               - DataFusion (Rust)         |
|  - Node.js SDK                                          |
|  - Rust SDK                                             |
+---------------------------------------------------------+
```

## **The Local-to-Cloud Bridge**

Heddle is fundamentally designed to act as an **"Invisible Local-to-Cloud Bridge"**. Developer Experience (DX) dictates that there should be no friction between local development and production execution:

* **100% Local Execution:** Everything can be run locally. The pipeline can be tested entirely on your machine, leveraging the Go core to manage concurrency without relying on external infrastructure.
* **Transparent Cloud Bridge:** With zero configuration changes, the same local environment acts as a bridge to the cloud. The local CLI can securely stream gigabytes of local data via Arrow Flight to be processed on a remote cluster, or alternatively, it can send just the DAG manifest to serve as a trigger for data already residing in remote Data Lakes.

## **Key Features**

* **Zero-Copy Memory Standard:** Heddle uses Apache Arrow for universal memory exchange. Data flows through the pipeline as immutable HeddleFrames (Arrow Tables), completely eliminating serialization and deserialization overhead.
* **High-Speed Transport:** Network communication between the Go orchestrator and isolated polyglot workers relies on Apache Arrow Flight RPC, allowing columnar data batches to stream.
* **Relational Pushdown (PRQL & DataFusion):** Heddle natively embeds PRQL (Pipelined Relational Query Language) for side-effect-free data derivations. Workers utilize the embedded DataFusion engine to execute these transformations locally over Arrow memory, ensuring mathematical consistency across all languages.
* **Time-Travel Debugging:** Because data flows immutably, Heddle maintains an append-only history log. If a step fails, developers can trace the exact lineage and state of the data at any preceding step.
* **Radical Reusability (fhub):** Heddle promotes a Lego-like modularity. The Functions Hub (fhub) is an open-source standard library of reusable data connectors and steps designed to be written once and shared across any workflow.

## **🛠️ Developer Experience (DX)**

Developer Experience is the single most important metric for Heddle's success.

*   **Trivial Step Creation:** Polyglot SDKs hide the complexity of gRPC and Arrow memory management, allowing developers to focus purely on writing their imperative business logic.
*   **Instant Diagnostics:** A custom Language Server Protocol (LSP) integrated with VS Code provides real-time syntax highlighting, type-checking, and diagnostics backed directly by the Go compiler.
*   **Time-Travel Debugging:** Because data flows immutably, Heddle maintains an append-only history log. If a step fails, developers can trace the exact lineage and state of the data at any preceding step.

---

_Heddle: Orchestrate Logic._