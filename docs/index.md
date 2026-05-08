# Heddle Lang

Heddle is a strictly-typed Domain-Specific Language (**DSL**) and high-performance orchestration engine built to eliminate the complexity. It allows engineers to focus on domain-specific requirements, through modularity and orchestration.

### Core Value Propositions

*   **Unified Infrastructure Integration:** Simplifies the reuse of disparate infrastructure—including databases, event streams, and Large Language Models (**LLMs**).
*   **Performance-First Execution:** Compiles workflows into optimized Directed Acyclic Graphs (**DAGs**) with zero-copy data transmission.
*   **Invisible Local-to-Cloud Bridge:** Facilitates a seamless transition from local experimentation to production-scale clusters without code changes.

## Architecture and Components

This document outlines the core architecture and fundamental components of the Heddle Lang distributed execution engine. It defines the responsibilities of the control plane, worker nodes, and data management layer, as well as the communication protocols that bind them.

## 1. Control Plane

The Control Plane is acting as the orchestrator of the Heddle ecosystem. It manages execution state and routing without processing or handling with any data.

* **DAG Compilation and Optimization:** Compiles the Heddle Domain-Specific Language (DSL) into an Intermediate Representation (IR). It evaluates the Directed Acyclic Graph (DAG) topology and prunes/optimizes it to build aggressive execution plans, merging continuous nodes into atomic "Super Steps" and then deciding which workers will process each subgraph.
* **Just-In-Time (JIT) Code Injection:** Dynamically load the imperative functions (steps) and the control logic code into workers based on the generated IR.
* **Resource Management:** Tracks and manages the lifecycle of stateful resources across the cluster, ensuring that external connections (e.g., database handles) remain open and tied to the correct worker until the resource times out.
* **Environment Provisioning:** Distributes required environment variables and secrets to workers, ensuring they can connect to necessary external services securely.
* **Cross-Workflow Batching:** Converges the execution of structurally independent workflows that share the same imperative function. This mechanism includes:
  * **Optimized Task Routing:** Identifies lexical signature overlaps across multiple active pipelines.
  * **Execution Affinity:** Centralizes task scheduling by directing data from varied workflows to the same physical workers via the Data Locality Registry.
  * **Columnar Consolidation:** Allows workers to intercept parallel flows, merging data blocks natively into a single `HeddleFrame` (Apache Arrow table) within local shared memory (`/dev/shm`).
  * **Single Batch Invocation:** Invokes the user's imperative code only once per batch, providing the consolidated `HeddleFrame` as input.
  * **Deployment Isolation:** Enables independent deployment of new logical topologies while maintaining vector processing efficiency, without requiring Control Plane reconfiguration.

## 2. Polyglot Workers

The execution layer employs a bifurcated architecture to maximize efficiency and support multi-language environments:

1. **Main Worker:** A single orchestrating daemon running per host. It manages data retrieval, state synchronization, and local plugin lifecycles.
2. **Polyglot Plugins:** Language-specific execution units (Python, Go, Rust, Node.js, etc) segmented by namespace/module. The Main Worker connects to these plugins via RPC at runtime, delegating processing tasks using zero-copy memory exchange.

Together, they fulfill the following responsibilities:

* **Step Processing:** The Polyglot Plugins execute the imperative logic defined within the Heddle workflow. The Main Worker delegates this processing directly via zero-copy memory pointers.
* **Data Synchronization:** The Main Worker coordinates with the Data Locality Registry to read inputs and write outputs seamlessly.
* **Connection Lifecycle:** The Main Worker maintains open connections for external resources until explicitly instructed to terminate by the Control Plane, avoiding connection churn.
* **Peer-to-Peer Data Retrieval:** The Main Worker performs P2P data resolution for inter-step dependencies. Upon completion of a DAG execution, it garbage-collects all immutable intermediate data, preserving only the explicit outputs defined by the workflow.

## 3. Data Locality Registry

The Data Locality Registry (Data Manager) is an optimized memory-mapping subsystem that manages data flow and localization, drawing architectural inspiration from [Vineyard (v6d)](https://v6d.io/docs.html).

* **Zero-Copy Memory Mapping:** Maps DAG outputs to physical locations (e.g., Worker ID, Host IP, Memory Handle) to enable zero-copy data routing.
* **Memory Offloading:** Handles the offloading of large datasets that exceed local RAM capacity, preserving performance during massive aggregations.
* **Lifecycle Management:** Tracks memory allocations and coordinates with workers to release memory safely when the data is no longer needed.

## 4. Communication Protocols

Heddle relies on strictly defined boundaries to ensure low latency and zero serialization overhead:

* **Control Plane ↔ Worker:** Utilizes high-performance **gRPC** to transmit execution state, metadata, and JIT code instructions. No payload data flows through this channel.
* **Worker ↔ Data Manager / Worker ↔ Worker:** Employs **Apache Arrow Flight** for zero-copy data exchange. Local routes use Unix domain sockets for shared memory, while remote routes utilize high-speed network paths.