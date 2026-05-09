# Heddle Language Specification

This document defines the theoretical design, semantics, and syntax rules of the Heddle language. It focuses strictly on language constructs. It does not cover underlying implementation details, execution engine optimizations, memory management, or infrastructure. 

The Heddle language architectural pillars:
- **Module Imports**: Loads external steps, resources, handlers and libraries into the workflow namespace.
- **Resource Definition**: Centralizes state and connectivity configurations.
- **Step Definition**: Binds imperative functions to reusable workflow components.
- **Error Handlers**: Defines reusable strategies for failure recovery and data routing.
- **Workflow Orchestration**: Defines the execution DAG and data routing between steps.
- **JIT Code Injection**: Dynamically deploys step implementations into stateless workers.
- **Strict Typing**: Resolves types definitively during the planning phase.
- **Relational Transformations**: Integrates Pipelined Relational Query Language (PRQL) for direct data manipulation.
- **Strict Layout**: Enforces exact indentation and formatting rules for deterministic parsing.
- **Resilience**: Isolates failures using hierarchical state machines.

## Architectural Pillars

### 1. Module Imports
The `import` statement loads external steps, resources, handlers and libraries (imperative functions) into the workflow namespace. It requires a module path and an explicit alias for referencing the module's functions and resources. 

The compiler verifies the existence of imported modules during the planning phase. Explicit aliasing prevents naming collisions when multiple modules provide overlapping function or resource names.

### 2. Resource Definition
The `resource` keyword defines state and connectivity configurations, such as database or message broker connections. This keyword separates infrastructure definitions from execution logic. This separation promotes component reuse across multiple workflows.

To pass a resource to a `step`, developers use the metadata injection syntax: `<key=identifier>`. For example, a PostgreSQL step uses `<connection=resource_pg>`. The key must match the expected plugin configuration name. The Control Plane validates this metadata during Directed Acyclic Graph (DAG) planning and development. This validation allows the Language Server Protocol (LSP) to provide immediate IDE diagnostics. 

### 3. Step Definition
The `step` keyword binds an imperative function implementation (from a module) to a unique Heddle identifier. This declaration enables developers to reuse specific logic, such as data fetching or processing, across multiple workflows. Step definitions follow a standard syntax: `step identifier = module.function <resource_injection> { configuration }`.

### 4. Error Handlers
The `handler` keyword defines a reusable strategy for managing execution failures. Handlers isolate error-recovery logic from the main workflow, ensuring consistent failure management across multiple steps. 

Developers use the catch-all operator (`*`) within a handler to capture both the technical stacktrace and the data payload that triggered the failure. The handler then routes this combined context through a recovery pipeline, such as logging to standard error or sending the payload to a dead-letter queue.

### 5. Workflow Orchestration
The `workflow` keyword defines the high-level orchestration logic for a data pipeline. A workflow arranges steps and relational queries into a Directed Acyclic Graph (DAG) that the Control Plane parses and executes.

Within a workflow block, developers use pipeline statements to route data between execution units. Heddle uses two primary operators to define this data flow:
- **Pipe Operator (`|`)**: Transfers data sequentially from one execution unit to the next.
- **Assignment Operator (`>`)**: Captures the output of a pipeline and assigns it to a named identifier for subsequent reference.
- **Error Handler (`?`)**: Workflows bind a default by workflow or by step error handler using `? handler_identifier`. This handler manages failures triggered during execution.

The Control Plane validates the entire workflow definition during the planning phase to ensure deterministic execution. This process includes verifying module imports, step bindings, resource injections, and error handlers. The compiler also performs strict type checking on all data transfers, including those within PRQL blocks and across pipe operators (`|`). This comprehensive validation eliminates runtime failures and allows the system to build an optimized execution plan where data flows efficiently between workers with minimal overhead.

### 6. JIT Code Injection
Heddle utilizes a Just-In-Time (JIT) execution model to maintain stateless workers. The Control Plane reads the DAG and injects the necessary step implementations into the workers immediately before execution. This dynamic injection ensures that workers remain lightweight and specialized for the current task.

The JIT model enables the system to update logic across a cluster without restarting individual worker nodes. This approach eliminates deployment downtime and ensures that all workers execute the most recent version of the workflow logic.

### 7. JIT Type Inference and Strict Typing
Heddle evaluates a dual-mode typing system during the DAG planning phase to ensure deterministic execution. The system mandates that the workflow define and validate all types before execution starts. This strict validation eliminates runtime type errors.

The two typing modes are:
- **Static Typing**: Defines contracts rigidly within the imperative language (e.g., Python, Rust) function implementation. The system fixes these contracts during plugin registration to ensure immutable data structures.
- **Inference-based Typing**: Adapts schemas dynamically based on the step configuration. For example, modifying the `WHERE` clause in a database query changes the resulting schema. The plugin must provide this dynamically inferred typing metadata to the Heddle interpreter during the planning phase.

### 8. Relational Transformations and PRQL
Heddle natively integrates the PRQL. This integration allows developers to perform relational data transformations directly within the workflow stream. 

Developers can reference a `step` directly in the `from` clause of a PRQL block. For example, developers write `(from fetch_users select id)`. This direct reference allows Heddle to pipe step outputs directly to the relational engine. The direct pipe eliminates intermediate variables and excessive data serialization.

### 9. Strict Layout and Indentation
Heddle enforces a strict syntax layout. This formatting ensures code readability and supports deterministic parsing. The compiler rejects code that violates these formatting rules.

The strict layout rules require:
- **Explicit Block Scoping**: Developers must format all braced blocks (`{}`) across multiple lines. This rule applies to `resource`, `step`, `handler`, and `workflow` declarations. The opening and closing braces require their own newlines. Developers must indent the inner content.
- **Pipeline Transitions**: Developers must place the assignment operator (`>`) and the pipe operator (`|`) on new lines. The compiler rejects same-line assignments or mid-line pipeline operators.

**Invalid Syntax Examples:**
```heddle
// INVALID: Single-line declaration
resource pg_db = pg.connection { host: "localhost" } 

// INVALID: Same-line assignment
fetch_users > users 

// INVALID: Compressed pipeline
users | (select id) > output 
```

### 10. Resilience and State Machines
Heddle isolates failures using Hierarchical State Machines (CHASM). This system catches external errors at the exact point of occurrence. The state machine triggers retries exclusively for the failing connector, which prevents unnecessary full DAG re-executions.

## Pipeline Examples

The following examples demonstrate the syntax rules applied to data validation pipelines. They illustrate resource injection, type inference, and relational transformations.

### Static and Dynamic Typing Pipeline

This example defines a static resource and infers the schema dynamically using PRQL.

```heddle
import "std/io" io
import "database/postgresql" pg
import "email/validate" email_validate

// Centralized connection configuration
resource resource_pg = pg.connection {
  host: "db.internal"
  port: 5432
}

// Step with automatic type inference based on SQL schema
step fetch_users = <connection=resource_pg> pg.query {
  query: "SELECT id, name, email FROM users WHERE status = 'active' and last_login > @timestamp"
}

handler global_error_handler {
  * 
    | io.stderr
}

handler debug_error_handler {
  * 
    | io.stderr
}

workflow validate_emails ? global_error_handler {
  fetch_users
    > users

  users 
    // Inject relational logic via internal PRQL engine
    | (from input select id, email)
    
    // Call external validation step and catch errors
    | email_validate.exists ? debug_error_handler
    
    // Join original input with validation results
    | (from input join users where input.id = users.id and input.exists select concat(input.id, "-", users.email))
    
    // Output final results
    | io.print
}
```

### Direct Step Reference Pipeline

This example routes the step output directly into the PRQL engine.

```heddle
import "std/io" io
import "database/postgresql" pg
import "email/validate" email_validate

resource resource_pg = pg.connection {
  host: "db.internal"
  port: 5432
}

step fetch_users = <connection=resource_pg> pg.query {
  query: "SELECT id, name, email FROM users WHERE status = 'active' and last_login > @timestamp"
}

handler global_error_handler {
  *
    | io.stderr
}

handler debug_error_handler {
  *
    | io.stderr
}

workflow validate_emails ? global_error_handler {
  // Direct reference: The PRQL engine consumes the step output immediately.
  (from fetch_users select id, email)
    | email_validate.exists ? debug_error_handler
    | (from input join users where input.id = users.id and input.exists select concat(input.id, users.email))
    | io.print
}
```
