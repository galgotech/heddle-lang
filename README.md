# Heddle: Logic Orchestration Engine

## Abstract

Heddle is a statically-typed, distributed orchestration language designed to reconcile the tension between the rigorous safety of functional data flow and the pragmatic utility of imperative execution. By formalizing the boundary between **Control Flow** (the Workflow) and **Computation** (the Step), Heddle eliminates the "spaghetti code" phenomenon endemic to traditional data engineering and microservices orchestration. It leverages a Host-Core architecture, utilizing Apache Arrow for zero-copy memory interchange and Ray for distributed, out-of-core processing, ensuring that logic is both verifiable at compile-time and scalable at runtime.

---

## 1. The Theoretical Foundation: Bridging Paradigms

In contemporary software architecture, particularly within data engineering and backend automation, developers are often forced to choose between the readability of declarative DSLs (Domain Specific Languages) and the flexibility of general-purpose imperative languages like Python. Heddle bridges these two worlds by enforcing a strict separation of concerns, a philosophy encapsulated in the mantra: **Orchestrate Logic**.

### 1.1. The Functional Core (The Workflow)

The structural backbone of a Heddle program is the **Workflow**. Defined declaratively, a workflow represents a Directed Acyclic Graph (DAG) of data dependencies. Inside the workflow scope, Heddle enforces functional programming principles. Data is immutable; it flows from one operation to the next via the pipe operator (`|`), transforming state without side effects visible to the orchestration layer. This linearizes complex logic, making the lineage of data visually apparent and mathematically verifiable. Because the control flow is declarative, the Heddle compiler can construct an Intermediate Representation (IR) that allows for advanced optimizations, such as parallel execution of independent branches, before a single line of code is executed.

### 1.2. The Imperative Host (The Step)

While the orchestration is functional, the actual units of work—interacting with databases, calling APIs, or processing images—are inherently imperative and side-effect-heavy. Heddle encapsulates these operations within **Steps**. A Step is an atomic binding to a function in the Host environment (currently Python). Within the definition of a step, the developer has full access to the imperative ecosystem, allowing for the configuration of database connections, machine learning models, or system calls. The Heddle runtime treats these steps as black boxes with strict input/output contracts, ensuring that the internal complexity of a step does not bleed into the orchestration logic.

---

## 2. System Architecture: Host-Core Symbiosis

Heddle avoids the monolithic design of traditional ETL tools by adopting an Embedded Core Architecture. This design distinguishes between the **Heddle Core Runtime (HCR)**, which manages state and scheduling, and the **Host Environment**, which executes the business logic.

### 2.1. The Compilation Process

The execution of a Heddle program begins with the parsing of the `.he` source file using a strictly defined EBNF grammar. The `Start` and `Program` parsers generate an Abstract Syntax Tree (AST) that validates the structural integrity of the code. This AST is subsequently lowered into a linear Intermediate Representation (IR) consisting of instructions such as `StepInstruction`, `PipeInstruction`, and `ImmutableInstruction`. This compilation phase is crucial; it performs static analysis to ensure that the output type of one step matches the input requirement of the next, preventing runtime type errors that plague dynamic Python workflows.

### 2.2. Distributed Execution with Ray

The Heddle runtime creates a dynamic execution graph on top of the Ray distributed computing framework. The `RayStreamEngine` translates the static IR into a live, streaming dataflow graph. Each step in the workflow is instantiated as a `StepActor`—a stateful worker that persists across micro-batches of data to maintain resource connections (like database handles) warm. This architecture allows Heddle to scale transparently from a single laptop to a massive cluster. The runtime manages the ingestion of data, partitioning it into micro-batches to optimize throughput and utilizing `streamz` to manage backpressure and concurrency.

### 2.3. Zero-Copy Memory Model (Apache Arrow)

To make the boundary between the Functional Core and the Imperative Host viable, data serialization overhead must be eliminated. Heddle utilizes **Apache Arrow** as its universal memory standard. When data flows from a Heddle workflow into a Python step, it is passed as an Arrow Table. This allows for zero-copy reads and vectorized processing. Whether the data originates from a CSV file, a SQL database, or a Kafka stream, it is normalized into this columnar format, ensuring high-performance data interchange between heterogeneous systems.

---

## 3. Language Specification

Heddle programs are defined in `.he` source files, which serve as the textual representation of the logic topology. The lexical structure is designed to be minimal, reducing syntactic noise to emphasize the data flow graph.

### 3.1. Lexical Structure and Program Organization

A Heddle **Program** is a sequence of top-level declarations. Unlike scripting languages where execution begins at the top of the file, a Heddle file is a declarative manifest. The order of top-level statements (Imports, Schemas, Resources, Steps, and Workflows) is semantically significant for symbol resolution but does not imply imperative execution order. Comments are defined using C-style double slashes (`//`), allowing for inline documentation.

### 3.2. The Type System: Schema Contracts

At the heart of Heddle’s safety guarantees is its static type system. Schemas define the "shape" of the data frames passed between the functional core and the imperative host steps. A **Schema Statement** binds a unique identifier to a structural definition, effectively creating a named contract.

The type system supports both primitive scalars and composite structures. Primitives include `int`, `string`, `float`, `bool`, and `timestamp`, mapping directly to Apache Arrow’s columnar types to ensure zero-copy compatibility. Composite types allow for nested dictionaries and lists, enabling the representation of complex, hierarchical business objects.

```heddle
// A schema representing a user entity with strict typing.
schema UserEntity = {
  id: int,
  username: string,
  email: string,
  meta: {
    signup_date: timestamp,
    is_active: bool
  }
}
```

### 3.3. Module System and Resource Injection

Heddle employs a module system to bridge the gap between the orchestration layer and the host environment

**Import Statements:** The `import` statement establishes a link to an external package. Syntactically, it requires a string literal representing the module path and an alias identifier.

```heddle
import "fhub/postgres" pg
```

**Resource Statements:** While workflows are stateless, real-world operations require stateful contexts such as database connections. Heddle models these as **Resources**. A `resource` statement instantiates a configuration object that persists across the lifecycle of the runtime actors.

```heddle
// Defines a persistent connection to the primary database.
resource PrimaryDB = pg.connection {
  host: "db.internal",
  port: 5432
}
```

### 3.4. Step Definition: The Foreign Function Interface

The **Step** is the fundamental unit of computation in Heddle. It acts as a typed Foreign Function Interface (FFI) to the host language. A step declaration binds a Heddle identifier to a specific function within an imported module and strictly defines its input and output types

The syntax allows for **Resource Injection** using angle brackets (`< ... >`). This mechanism passes the pre-configured resource instances to the underlying host function at runtime, ensuring that the step has access to necessary infrastructure without hardcoding connection logic inside the business logic.

```heddle
// A source step that reads from PostgreSQL.
// It injects the 'PrimaryDB' resource and executes a static query.
step fetch_active_users -> UserEntity = pg.query<connection=PrimaryDB> {
  query: "SELECT * FROM users WHERE status = 'active'"
}
```

### 3.5. Workflow Composition and Control Flow

The `workflow` statement defines the execution scope. Within a workflow, Heddle enforces a declarative, functional style

**Scoped Execution:** Variables defined within a workflow using `let` are immutable and local to that scope. This immutability is key to Heddle's ability to parallelize execution.

**The Pipeline Operator (`|`):** Data flow is expressed using the pipe operator. This operator takes the output of the expression on the left and passes it as the input to the step on the right. This linear syntax eliminates the need for nested function calls or intermediate variables.

```heddle
workflow UserOnboarding {
  // Binds the result of the pipeline to 'processed_users'
  let processed_users = fetch_active_users
    | enrich_user_data
    | send_welcome_email
}
```

### 3.6. Functional Transformations (PRQL Integration)

While Steps handle imperative side effects, Heddle integrates **PRQL (Pipeline Relational Query Language)** for pure data transformations. PRQL expressions are embedded directly within the pipeline using parentheses `( ... )`

These expressions allow for relational algebra operations—`from`, `select`, `filter`, `derive`, `join`, `aggregate`—to be performed on the dataframe in transit. Because these transformations are declarative and side-effect-free, they are executed by the high-performance Heddle Core rather than the slower Host Python environment.

```heddle
let high_value_orders = fetch_orders
  | (
    from input
    filter amount > 1000.0
    derive {
      tax = amount * 0.2,
      total = amount + tax
    }
    select { order_id, total, customer_id }
  )
  | save_report
```

### 3.7. Error Handling and Handlers

Heddle treats error handling as a control flow routing problem. The **Handler** statement defines a specialized step designed to process error contexts. The error routing operator `?` allows developers to attach these handlers to specific steps or entire workflows

```heddle
handler log_error = std.log_failure { level: "error" }

workflow ReliableIngest {
  ingest_data
    | unstable_api_call ? log_error
    | save_data
}
```

---

## 4. Advanced Concepts: Time-Travel and Observability

One of Heddle's most distinguishing features is its approach to program state and debugging

### 4.1. Immutable Frames and History

In the Heddle runtime, data is encapsulated in a `HeddleFrame`. This structure is immutable. When a step processes a frame, it does not overwrite the data; instead, it evolves the frame, appending the new result to a history log contained within the frame object itself. This "append-only" architecture enables **Time-Travel Debugging**. If a workflow fails at the final step, the runtime retains the exact state of the data as it existed at every preceding step. Developers can inspect the history dictionary within the frame to trace the lineage of data transformation precisely

### 4.2. Integrated Metrics

Observability is a first-class citizen in Heddle. The runtime automatically wraps every step execution in a `PrometheusExporter` context. This system tracks granular metrics such as memory usage (RSS), batch size, and request latency for every single step in the workflow. Because this is integrated into the `StepActor`, no user code is required to enable this monitoring; the "Orchestration" layer handles the operational telemetry automatically

---

## 5. Module Ecosystem

Heddle is designed to be extensible through a standard library of modules (`fhub`). These modules provide the imperative bindings for common infrastructure

* **Database Connectivity**: Modules for PostgreSQL, MySQL, and ClickHouse provide high-performance readers and writers that automatically convert SQL result sets into Arrow tables.
* **ETL Utilities**: The `etl` module provides standard transformations, ingestion patterns, and vectorization capabilities for AI workflows.
* **Streaming & Messaging**: Integration with Kafka, NATS, and MQTT allows Heddle to orchestrate event-driven architectures and streaming pipelines as easily as batch jobs.
* **Standard Library**: Utilities for console I/O, error handling, and basic system interactions.

---

## 6. New Horizons: Expanding the Orchestration Paradigm

Heddle’s fundamental premise—**Orchestrate Logic**—extends beyond traditional data engineering. By formalizing the interface between "intent" (Workflow) and "action" (Step), Heddle provides a rigorous substrate for domains where safety, auditability, and distributed execution are paramount

### 6.1. Heddle as the Agentic Intermediate Layer

Large Language Models (LLMs) excel at reasoning and semantic planning but struggle with reliable execution. Heddle serves as the **Agentic Intermediate Layer**, acting as the verifiable bridge between an LLM's probabilistic intent and the deterministic world of APIs and tools

Instead of executing code directly, the AI Agent generates a Heddle Workflow (or parameters for a Heddle Step). Because Heddle compiles to a static graph with strict schema contracts, the system can verify that the AI’s plan is structurally sound and type-safe *before* any action is taken.

### 6.2. Infrastructure as Typed Topology

Infrastructure as Code (IaC) often suffers from a lack of dynamic visibility. Heddle offers a third path: **Infrastructure as Typed Topology**. Cloud infrastructure is inherently a Directed Acyclic Graph (DAG) of dependencies: a Database depends on a Subnet, which depends on a VPC

By treating infrastructure components (Servers, Databases, Load Balancers) as **Steps** that output their configuration state (IDs, ARNs, IP addresses), Heddle allows engineers to provision complex environments using the same composable logic used for data pipelines. A workflow can provision a database, configure a Kubernetes cluster, and set up networking.

---

## 7. Conclusion

Heddle is not merely a syntax for running Python scripts; it is a rigorous framework for defining the topology of logic. By enforcing a compilation step, strict type schemas, and immutable data history, it brings software engineering discipline to the chaotic world of data pipelines and infrastructure management. It allows the architect to reason about the system at a high level—seeing the flow, the dependencies, and the contracts—while allowing the engineer to implement the details using the world's most popular ecosystem. It is the realization of the "Orchestrate Logic" philosophy

