---
title: "Polyglot SDK Examples"
meta_title: "Heddle Examples"
description: "See how Heddle Lang orchestrates complex logic across Go, Python, and Node.js."
layout: "about"
draft: false
---

Heddle Lang is built on the principle of **Radical Reusability**. You write your complex business logic in the language that best fits the task (Python for Data Science, Go for Systems, Node.js for APIs), and Heddle handles the orchestration, zero-copy data routing, and resilience.

## 1. The Orchestration Layer (.he)
This Heddle script fetches users from a database, validates their emails using a Python step, and prints the results.

```heddle
import "std/io" io
import "db/postgres" pg
import "validator/email" val

resource db_conn = pg.connection {
  host: "prod-db.internal"
}

step fetch_users = pg.query <connection=db_conn> {
  query: "SELECT id, email FROM users WHERE active = true"
}

workflow validate_pipeline {
  fetch_users
    | (from input select email)
    | val.check
    | io.print
}
```

---

## 2. The Imperative Layer (Polyglot SDKs)

### Python (Data Science & Validation)
Python steps use the Heddle SDK to receive and return `HeddleFrames` (Apache Arrow tables) with zero-copy efficiency.

```python
import heddle

@heddle.step()
def check(frame: heddle.HeddleFrame):
    # 'frame' is a zero-copy Arrow table
    # We can use Polars or Pandas for high-speed processing
    df = frame.to_polars()
    
    # Perform complex validation
    df = df.with_columns(
        is_valid = df["email"].str.contains(r"@")
    )
    
    return heddle.from_polars(df)
```

### Go (High-Performance Systems)
Go steps are executed natively within the worker ecosystem, ideal for compute-intensive tasks.

```go
package main

import (
    "github.com/galgotech/heddle/sdk/go"
)

func Check(ctx *heddle.Context, input *heddle.HeddleFrame) (*heddle.HeddleFrame, error) {
    // Process input frame using Go's Apache Arrow implementation
    // No serialization overhead
    output := performValidation(input)
    return output, nil
}
```

### Node.js (API Integration)
Node.js steps allow for easy integration with modern web services and existing Javascript logic.

```javascript
const heddle = require("@heddle/sdk");

/**
 * @param {heddle.HeddleFrame} frame
 */
async function check(frame) {
  const data = frame.toArray();
  
  const results = data.map(row => ({
    ...row,
    valid: row.email.includes("@")
  }));
  
  return heddle.fromArray(results);
}

module.exports = { check };
```

---

## The Heddle Advantage
By separating the **How** (Imperative Steps) from the **When** (Declarative Workflow), Heddle ensures that:
- **Developers focus on logic**, not network plumbing.
- **Data flows at memory speeds** between languages.
- **Errors are isolated** and managed by the control plane.
