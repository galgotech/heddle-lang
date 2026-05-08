---
title: "About Heddle Lang"
meta_title: "Architecture & Design"
description: "High-performance data orchestration with host-core symbiosis and zero-copy data routing."
image: "/images/performance.png"
layout: "about"
draft: false
---

Heddle is a strictly-typed Domain-Specific Language (DSL) and high-performance orchestration engine built to eliminate the complexity of distributed development. It simplifies the integration and reuse of disparate infrastructure—including databases, event streams, and Large Language Models (LLMs)—while ensuring Software, Data, and Infrastructure Engineers retain total control. By automating orchestration and enforcing radical modularity, Heddle allows engineers to focus on domain-specific imperative logic and other engineering challenges. The engine compiles workflows into optimized Directed Acyclic Graphs (DAGs), providing a seamless, invisible bridge from local experimentation to production-scale clusters.

## Architecture and Components

Heddle operates on a decoupled architecture ensuring robust fault tolerance and zero-copy data routing. The system is bifurcated into a **Smart Control Plane** and **Stateless Workers**.

### 1. The Smart Control Plane (The Brain)
The Control Plane is a 100% self-contained, autonomous binary in Go. It manages execution state and routing without processing any payload data.

*   **DAG Compilation:** Compiles Heddle DSL into an optimized Intermediate Representation (IR).
*   **JIT Code Injection:** Dynamically loads imperative functions (steps) into workers at runtime.
*   **Cross-Workflow Batching:** Identifies lexical signature overlaps to consolidate task execution.
*   **Resource Management:** Tracks stateful resources like database handles across the cluster.

### 2. Polyglot Workers (The Muscle)
Stateless workers running across the cluster execute the declarative flow controls defined in the Heddle DSL.

*   **Main Worker:** Orchestrates data retrieval, state synchronization, and plugin lifecycles.
*   **Polyglot Plugins:** Language-specific units (Go, Python, Rust, Node.js) that execute the core logic via RPC.
*   **Zero-Copy Memory:** Plugins communicate with the Main Worker using shared memory pointers, eliminating serialization overhead.

### 3. Data Locality Registry
An optimized memory-mapping subsystem that manages data flow and localization.

*   **Zero-Copy Routing:** Maps outputs to physical handles (Worker ID, Host IP, Memory Handle).
*   **Memory Offloading:** Automatically spills large datasets to NVMe/SSD when RAM limits are reached.
*   **GC Evasion:** Uses aggressive pooling and pointerless structs to minimize Go's Garbage Collector overhead.

---

## Language Design & Syntax

Heddle enforces a strict syntax layout to ensure code readability and deterministic parsing. The language focuses on the **Symbiosis** between the orchestration layer and the developer's core logic.

### Strict Layout Rules
*   **Explicit Block Scoping:** Braced blocks (`{}`) must be formatted across multiple lines.
*   **Pipeline Transitions:** Assignment (`>`) and pipe (`|`) operators must reside on new lines.
*   **Strict Typing:** All types are resolved definitively during the planning phase to prevent runtime failures.

### Host-Core Symbiosis
Heddle doesn't replace the complex logic developers already write; it simplifies and reuses it. By binding imperative functions from Go, Python, or Rust into a strictly-typed orchestration DAG, Heddle allows engineers to focus on their domain expertise while it handles the distributed system complexity.
