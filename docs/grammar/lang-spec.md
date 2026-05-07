# Heddle Language Specification

This document presents the theoretical and conceptual specification of the Heddle language. The exclusive focus here is on design, semantics, and language rules, completely abstracting any underlying implementation details, execution engine optimizations (such as memory management), and infrastructure.

The following example demonstrates the specification of a data validation pipeline, illustrating the declaration of external resources, type inference, and native relational transformations from the strict perspective of the language syntax.

## Architectural Pillars

### 1. Resource Definition (Resources)
`resource` definitions centralize state and connectivity configurations (e.g., PostgreSQL, Message Brokers). They enable strict separation between infrastructure and execution logic, facilitating **Radical Reusability** across different workflows.

- **Step Injection**: Passing resources to a `step` uses the metadata syntax `<key=identifier>` (e.g., `<connection=resource_pg>`). The key must match the name expected by the plugin configuration, being validated both during DAG (Directed Acyclic Graph) planning and development time. This allows the LSP (Language Server Protocol) to provide immediate diagnostics in the IDE, ensuring safety before execution.

### 2. JIT (Just-In-Time) Type Inference and Strict Typing
Heddle employs a dual-mode typing system—**Static** and **Inference-based**—both of which are strictly resolved during the DAG planning phase by the Control Plane:

- **Static Typing**: Defined rigidly within the implementation of the function or step in the imperative language (FFI with Python, Rust, etc.). These contracts are fixed at plugin registration, ensuring immutable data structures that are verified before execution.
- **Inference-based Typing**: Dynamically adapts based on the step configuration. For instance, changing selected fields in a database query or modifying `WHERE` parameters triggers a re-evaluation of the schema. The plugin implementation (e.g., PostgreSQL) must provide this typing metadata to the Heddle interpreter during planning.

All types must be fully defined and strictly validated before the workflow starts, ensuring deterministic execution and eliminating runtime type errors.

### 3. Relational Transformations and Integrated PRQL
Heddle natively integrates the PRQL (Pipelined Relational Query Language) engine with DataFusion. This allows complex data transformations directly in the flow with real-time schema inference and optimized efficiency.

- **Direct Step Reference**: A `step` can be referenced directly in the `from` clause of a PRQL block (e.g., `(from fetch_users ...)`). This allows Heddle to orchestrate step execution and data streaming to the relational engine, eliminating the need for intermediate variables or excessive serialization.

### 4. Strict Layout and Indentation
Heddle enforces a strict layout to ensure readability and deterministic parsing. The language follows a "one-way" formatting rule where block scopes and pipeline stages must follow exact indentation patterns.

- **Explicit Block Scoping**: All braced blocks `{}` (used in `resource`, `step`, `handler`, and `workflow`) **must** be multi-line. Opening and closing braces must be followed/preceded by a newline, and the content must be indented.
- **Pipeline Transitions**: The assignment operator (`>`) and the pipe operator (`|`) **must** start on a new line. Same-line assignments or mid-line piping are syntactically invalid.

**Invalid Syntax Examples:**
```heddle
// INVALID: Single-line declaration
resource pg_db = pg.connection { host: "localhost" } 

// INVALID: Same-line assignment
fetch_users > users 

// INVALID: Compressed pipeline
users | (select id) > output 
```

### 5. Resilience and Hierarchical State Machines (CHASM)
Heddle isolates failures at the exact point of occurrence. External errors trigger retries only for the problematic connector, avoiding massive DAG re-executions.
- **Catch-all Operator (`*`)**: The use of an asterisk in an error handler indicates that both the technical details of the error (stacktrace, message) and the data that caused the failure (the payload at that point) will be passed to the next stage of recovery (e.g., sending to `io.stderr` or a Dead Letter Queue).


---
### Static (Plugin) / Dynamic (Plugin + PRQL) Typing Example

```heddle
import "std/io" io
import "database/postgresql" pg
import "email/validate" email_validate

// Centralized connection configuration
resource resource_pg = pg.connection {
  host: "db.internal"
  port: 5432
}

// Step with type inference automatically handled via SQL parsing and database DDL schema inference
step fetch_users = pg.query <connection=resource_pg> {
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
    // Injection of relational logic via internal PRQL engine
    | (from input select id, email)
    
    // External validation with specific error handler
    | email_validate.exists ? debug_error_handler
    | (from input join users where input.id = users.id and input.exists select concat(input.id, "-", users.email))
    | io.print
}
```

### Direct Step Reference

```heddle
import "std/io" io
import "database/postgresql" pg
import "email/validate" email_validate

// Centralized connection configuration
resource resource_pg = pg.connection {
  host: "db.internal"
  port: 5432
}

// Step with type inference automatically handled via SQL parsing and database DDL schema inference
step fetch_users = pg.query <connection=resource_pg> {
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

  // Direct reference: the 'fetch_users' step is executed and its output 
  // is immediately consumed by the PRQL engine via the 'from' clause.
  (from fetch_users select id, email)
    // External validation with specific error handler
    | email_validate.exists ? debug_error_handler
    | (from input join users where input.id = users.id and input.exists select concat(input.id, users.email))
    | io.print
}
```
