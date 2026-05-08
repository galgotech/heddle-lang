---
# Banner
banner:
  title: "Language and Orchestration Engine"
  content: "Heddle is a strictly-typed Domain-Specific Language (DSL) and high-performance orchestration engine built to eliminate the complexity. It simplifies the integration and reuse of disparate infrastructure—including databases, event streams, and LLMs—while providing a seamless bridge from local experimentation to production-scale clusters"
  image: "/images/banner.png"
  button:
    enable: true
    label: "Explore Documentation 🚀"
    link: "/docs"

# Features
features:
  - title: "Embedded PRQL Native Transforms"
    image: "/images/prql.png"
    content: "Heddle integrates the Pipelined Relational Query Language (PRQL) natively to execute data transformations within the workflow stream. This integration eliminates serialization overhead by processing relational logic directly in the stream."
    bulletpoints:
      - "Integrate native PRQL parsers"
      - "Execute zero-copy relational transforms using Apache Arrow memory"
      - "Enforce strict type validation across all pipeline boundaries"
      - "Unify imperative logic and relational queries in a single syntax"
    button:
      enable: false
      label: ""
      link: ""

  - title: "Row-Level Recovery & Time-Travel Debugging"
    image: "/images/recovery.png"
    content: "Heddle isolates execution failures at the source to facilitate deterministic row-level recovery. The engine maintains transparent state tracking for every pipeline step to ensure consistent data tracking and rapid debugging"
    bulletpoints:
      - "Isolate failures within discrete state machine boundaries"
      - "Retry failing connectors independently to prevent full DAG re-execution"
      - "Capture granular state history for time-travel debugging"
      - "Enforce deterministic state transitions across distributed workers"
    button:
      enable: false
      label: ""
      link: ""

  - title: "Engineered for Resilience & Performance"
    image: "/images/performance.png"
    content: "The Smart Control Plane manages workflow topologies while the zero-copy backbone orchestrates data routing. Heddle achieves high throughput by optimizing communication in the Control Plane/Workers and implementing aggressive memory management."
    bulletpoints:
      - "Optimize memory utilization and implement disk-spilling for massive datasets"
      - "Transmit data batches via Apache Arrow Flight RPC with zero-copy efficiency"
      - "Fuse logical steps into atomic execution units to minimize processing overhead"
      - "Establish shared memory boundaries for high-speed local data exchange"
    button:
      enable: false
      label: ""
      link: ""

  - title: "Build the Ecosystem: Simplicite and Reusability"
    image: "/images/reusability.png"
    content: "Heddle simplifies the integration of complex infrastructure while allowing engineers to retain control over domain-specific logic and complexity. The system encourages reusability by packaging imperative code into modular 'Lego-style' components shared via the fhub ecosystem."
    bulletpoints:
      - "Define reusable 'Step' and 'Resource' components for the fhub standard"
      - "Integrate disparate infrastructure including databases, event streams, and LLMs"
      - "Leverage polyglot SDKs for Go, Python, Rust, and Node.js"
      - "Simplify step creation using language-specific SDKs and tools"
    button:
      enable: false
      label: ""
      link: ""
---
