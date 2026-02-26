# Heddle: The Language for Orchestration Logic

_Simplicity, Order, and Radical Reuse._

Heddle is a strictly-typed, domain-specific programming language (DSL) built to eliminate the maintainability nightmares endemic to traditional data orchestration and microservices.

Designed for **Backend Developers, Data Engineers, and Data Scientists**, Heddle bridges the gap between the rigorous safety of functional data pipelines and the pragmatic utility of imperative Python code.

**Turn an adopters, contributors, and visionaries to help us build a massive ecosystem of reusable Steps and modules!**

---

## Why Heddle?

Modern data engineering often forces a choice: write clean declarative configurations that lack flexibility, or write glue scripts in Python that inevitably degrade into tangled spaghetti.

Heddle introduces a **Host-Core Architecture** to give you the best of both worlds:

1. **The Functional Core (Heddle DSL):** A clean, declarative, strictly-typed workflow pipeline where data flows immutably.
2. **The Imperative Host (Python):** Atomic, reusable "Steps" where you can fully leverage the power of the Python ecosystem to interact with databases, APIs, or ML models.

By separating the control flow from the computation, Heddle maximizes developer productivity and fosters a highly modular, reusable codebase.

---

## Core Features

- 🛡️ **Strictly Typed DSL:** Catch configuration and type mismatch errors at compile-time with our custom LSP, _before_ your multi-hour distributed data job starts. Features built-in primitives (`int`, `string`, `float`, `bool`, `timestamp`, `void`).
- ♻️ **Radical Reusability:** Define a Step once and reuse it across multiple workflows. Build an internal ecosystem of Lego-like data connectors and transformations.
- 🐍 **Seamless Python Integration:** Bind Python functions to Heddle steps in seconds using standard `TypedDict` and Arrow `Table`. Heddle also supports converting Python dataclasses directly into Heddle schemas.
- 🚀 **Zero-Copy Memory via Apache Arrow:** Data flows between Heddle pipelines and Python steps via Arrow Tables and Ray's Plasma store. Zero serialization overhead ensures unmatched high performance.
- 🌐 **Distributed Execution via Ray:** Execute your workflows locally for debugging, then scale to a massive distributed cluster seamlessly. Heddle's execution engine features micro-batching, Numba JIT optimization for pure functions, and detached persistent actors.

---

## What Does Heddle Look Like?

Heddle favors explicit contracts and readable pipelines. Here is a complete, runnable user onboarding pipeline (`onboarding.he`):

```heddle
// onboarding.he

import "fhub/postgres" pg
import "fhub/email" mail
import "local/transform" tf

// 1. Define Strict Schemas
schema User {
  id: int
  username: string
  email: string
  signup_date: timestamp
  is_active: bool
}

// 2. Configure Persistent Resources
resource PrimaryDB = pg.connection {
  host: "db.internal"
  port: 5432
}

resource Mailgun = mail.provider {
  api_key: "sk-live-..."
}

// 3. Define Reusable Steps (FFI to Python)
step fetch_active_users: void -> User = pg.query <connection=PrimaryDB> {
  query: "SELECT * FROM users WHERE status = 'active'"
}

step enrich_user_data: User -> User = tf.enrich_geo_data

step send_welcome: User -> void = mail.send_template <provider=Mailgun> {
  template_id: "welcome_v2"
}

// 4. Orchestrate Logic Safely
workflow UserOnboarding {
  fetch_active_users
    | enrich_user_data
    | send_welcome
}
```

### The Python Counterpart

Connecting Python to Heddle is completely seamless. Here is the Python implementation of the `tf.enrich_geo_data` Step leveraged above:

```python
# local/transform.py
from typing import TypedDict
from heddle.core import Table

class GeocodingConfig(TypedDict):
    # Add configuration properties bound from Heddle
    pass

def enrich_geo_data(config: GeocodingConfig, input_table: Table) -> Table:
    # input_table is a zero-copy Apache Arrow table!
    # Perform your pandas, polars, or pyarrow operations here
    arrow_table = input_table.arrow_table

    # ... computation ...

    return Table(arrow_table)
```

---

## Advanced Capabilities

### Row-Level Recovery & Time-Travel Debugging

Heddle evaluates workflows via immutable `HeddleFrames` that maintain a robust history mechanism. If a step fails, you have access to the exact state of the data precisely at each prior step. With built-in Handler blocks (`step_that_might_fail? my_error_handler`), you can catch row-level errors and recover gracefully without crashing the entire batch execution.

### Embedded PRQL Native Transforms

Want to do relational transforms without dropping into Python? Heddle natively embeds PRQL (Pipelined Relational Query Language) backed by DataFusion with native Arrow integration.

```heddle
workflow Analytics {
  fetch_active_users
    | (
      from input
      filter is_active == true
      select { id, email }
    )
    | sink_to_s3
}
```

---

## Join the Revolution: Help Us Build the Ecosystem!

Heddle's true power lies in its **reusability**. We envision a world where Data Engineers don't need to write the same Kafka reader or Postgres writer from scratch ten times a week.

We are actively seeking **Contributors, Maintainers, and Partners** to help us refine the compiler and build `fhub` (The Functions Hub)—an open-source standard library of reusable Heddle Steps.

Whether you're dealing with LLM wrappers, complex ETL tasks, or backend micro-orchestration, Heddle gives you the robust structure you need to move fast without introducing tech debt.

- **Found a bug?** Open an issue.
- **Want to build a module?** PRs are highly welcome for integration with modern tools (dbt, Snowflake, HuggingFace, OpenAI, etc.).
- **Interested in compilers or distributed systems?** Heddle's core is ripe for optimization and language design discussions!

---

_Heddle: Orchestrate Logic._
