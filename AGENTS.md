# Initial Concept

Heddle is a strictly-typed, domain-specific programming language (DSL) built to eliminate the maintainability nightmares endemic to traditional data orchestration and microservices.

# Product Definition: Heddle Lang

## 1. Primary Orchestration Use Case
Although Heddle is general-purpose, it is first beifng optimized for high-performance data pipelines and microservice orchestration. The immediate focus is to enable the continuous migration of orchestration logic from Python/Ray-based architectures to a highly concurrent Go core, eliminating bottlenecks such as the GIL and external infrastructure complexity.

## 2. 'fhub' (Functions Hub) Expansion Strategy
The central strategy for fhub is Radical Reusability. The goal is to create a "Lego-style" ecosystem of modular components, where data connectors (e.g., Postgres, Mailgun) and execution steps are developed once and reused across multiple workflows. This is supported by:
- Standardized Connectors: Open-source libraries that map external systems directly to Apache Arrow tables.
- Python FFI Development: Creation of "Steps" that use the foreign function interface to efficiently integrate imperative logic into the functional core.

## 3. Most Important Success Metric
The definitive metric for Heddle's success is Developer Experience (DX). The system's value is measured by its ability to reduce friction for engineers, which is achieved through four strategic pillars:
- Invisible Local-to-Cloud Bridge: Seamless transition between local development and production clusters without configuration changes.
- Trivial Step Creation: Polyglot SDKs abstract away the complexities of gRPC and Arrow memory management, making step creation effortless.
- Immediate Feedback: High-quality tooling, including a customized LSP for VS Code with real-time diagnostics.
- Transparent Execution: Data flow visualization via DAG and "time-travel" debugging.


# Product Guidelines: Heddle Lang

## 1. Prose Style & Tone
The primary prose style for documentation and communication is **Pragmatic Technical Clarity** with a high focus on Developer Experience (DX).
- **Mirroring Code Clarity:** Direct, concise, and free of unnecessary jargon. It uses structured scannability (e.g., IEEE 830-1998 format).
- **Balancing Rigor and Utility:** Bridges academic rigor (mathematically verifiable depth for the functional core) with pragmatic utility (approachable, action-oriented language for step creation).
- **Inspirational yet Grounded:** Paints a vision of a "Transparent Serverless Supercomputer" while providing no-nonsense technical blueprints.

## 2. Code Examples
Code examples must be integrated components, not just static snippets, following these principles:
- **Host-Core Symbiosis Dual-View:** Always present both sides of the contract: the strictly-typed Heddle DSL (`.he`) and the imperative step code (Python/Rust/Node.js).
- **Visual DAG Representation:** Accompany code with its corresponding Directed Acyclic Graph (DAG) using tools like React Flow or Mermaid.
- **Zero-Copy Data Previews:** Display `HeddleFrame` samples to show transformations without serialization overhead.
- **Explicit Optimization Context:** Specify the optimization level.
- **Interactive Playgrounds:** Include "Run in Playground" links for real-time browser experimentation.

## 3. Brand Messaging
- **Clarity over Complexity:** Heddle replaces tangled scripts with a strictly-typed, declarative DSL.
- **DX as the North Star:** Emphasize the "Invisible Local-to-Cloud Bridge," trivial onboarding, and immediate feedback (LSP, DAGs).
- **Technical Authority:** Highlight the Go-based "Smart Control Plane," Apache Arrow zero-copy standard, and PRQL/DataFusion native integration.
- **Radical Reusability (`fhub`):** Focus on building Lego-like modular data connectors and steps once to be shared everywhere.

## 4. Visual Identity & UX
The visual identity is an extension of the technical promise: order from chaos. The theme is **"A Trama de Alta Performance"** (The High-Performance Weave).
- **Functional Minimalism (Industrial-Modern):** Less is more. Use thin borders, generous spacing, and technical typography.
- **Color Palette (High-Performance Dark):**
  - Primary Background: Deep Slate / Obsidian (#0D1117)
  - Flow Highlight: Electric Cyan / Neon Green
  - Types/Contracts: Muted Purple / Steel Blue
  - Alerts/Errors: High-Contrast Orange
- **Non-Interruption UX:** Focus on the Flow State. Transparent LSP, real-time data visualization without `print()` statements, and latency feedback micro-animations.
- **Typography:** JetBrains Mono Sans for UI clarity, JetBrains Mono for code/DSL with ligature support.

# Tech Stack: Heddle Lang

## Core Systems
- **Host Language:** Go (latest stable version)
  - Acts as the "Smart Control Plane" and functional core.
  - Handles parsing, compiling, and concurrency (Host-Core Symbiosis).
- **Target Imperative Languages:** Python, Rust, Node.js
  - Used for defining polyglot "Steps" via FFI.

## Data & Execution
- **Memory Standard:** Apache Arrow
  - Used for zero-copy memory exchange between the Go core and Python steps via Ray's Plasma store.
- **Distributed Execution Engine:** Apache Arrow Flight RPC.
  - Uses high-performance gRPC to transmit data batches (RecordBatches) between the Go Core and Workers with near-zero serialization overhead.
- **Shared Memory (Local)**: Use of shared memory for executions on the same node, ensuring zero latency.
- **Embedded Relational Engine:** DataFusion & PRQL
  - PRQL backed by DataFusion with native Arrow integration for relational transforms within the DSL.

## Language Design
- **Domain Specific Language:** Heddle DSL (`.he` files)
  - Strictly-typed configuration and workflow orchestration language.

# Project Workflow

## Guiding Principles

1. **Test-Driven Development:** Write unit tests before implementing functionality
2. **High Code Coverage:** Aim for >80% code coverage for all modules
3. **User Experience First:** Every decision should prioritize user experience

## Task Workflow

All tasks follow a strict lifecycle:

### Standard Task Workflow

1. **Write Failing Tests (Red Phase):**
   - Create a new test file for the feature or bug fix.
   - Write one or more unit tests that clearly define the expected behavior and acceptance criteria for the task.
   - **CRITICAL:** Run the tests and confirm that they fail as expected. This is the "Red" phase of TDD. Do not proceed until you have failing tests.

4. **Implement to Pass Tests (Green Phase):**
   - Write the minimum amount of application code necessary to make the failing tests pass.
   - Run the test suite again and confirm that all tests now pass. This is the "Green" phase.

5. **Refactor (Optional but Recommended):**
   - With the safety of passing tests, refactor the implementation code and the test code to improve clarity, remove duplication, and enhance performance without changing the external behavior.
   - Rerun tests to ensure they still pass after refactoring.

6. **Verify Coverage:** Run coverage reports using the project's chosen tools. For example, in a Python project, this might look like:
   ```bash
   pytest --cov=app --cov-report=html
   ```
   Target: >80% coverage for new code. The specific tools and commands will vary by language and framework.

### Phase Completion Verification and Checkpointing Protocol

**Trigger:** This protocol is executed immediately after a task is completed.

1.  **Announce Protocol Start:** Inform the user that the phase is complete and the verification and checkpointing protocol has begun.

2.  **Ensure Test Coverage:**
    -   **Step 2.1: Determine Scope:** To identify the files changed, you must first find the starting point.
    -   **Step 2.2: List Changed Files:** Get a precise list of all files modified.
    -   **Step 2.3: Verify and Create Tests:** For each file in the list:
        -   **CRITICAL:** First, check its extension. Exclude non-code files (e.g., `.json`, `.md`, `.yaml`).
        -   For each remaining code file, verify a corresponding test file exists.
        -   If a test file is missing, you **must** create one. Before writing the test, **first, analyze other test files in the repository to determine the correct naming convention and testing style.** The new tests **must** validate the functionality described.

3.  **Execute Automated Tests with Proactive Debugging:**
    -   Before execution, you **must** announce the exact shell command you will use to run the tests.
    -   **Example Announcement:** "I will now run the automated test suite to verify the phase. **Command:** `CI=true npm test`"
    -   Execute the announced command.
    -   If tests fail, you **must** inform the user and begin debugging. You may attempt to propose a fix a **maximum of two times**. If the tests still fail after your second proposed fix, you **must stop**, report the persistent failure, and ask the user for guidance.

4.  **Announce Completion:** Inform the user is complete.

### Quality Gates

Before complete, verify:

- [ ] All tests pass
- [ ] Code coverage meets requirements (>80%)
- [ ] All public functions/methods are documented (e.g., docstrings, JSDoc, GoDoc)
- [ ] Type safety is enforced (e.g., type hints, TypeScript types, Go types)
- [ ] No linting or static analysis errors (using the project's configured tools)
- [ ] Documentation updated if needed
- [ ] No security vulnerabilities introduced

## Testing Requirements

### Unit Testing
- Every module must have corresponding tests.
- Use appropriate test setup/teardown mechanisms (e.g., fixtures, beforeEach/afterEach).
- Mock external dependencies.
- Test both success and failure cases.

### Integration Testing
- Test complete user flows
- Verify database transactions
- Test authentication and authorization
- Check form submissions

## Code Review Process

### Self-Review Checklist
Before requesting review:

1. **Functionality**
   - Feature works as specified
   - Edge cases handled
   - Error messages are user-friendly

2. **Code Quality**
   - Follows style guide
   - DRY principle applied
   - Clear variable/function names
   - Appropriate comments

3. **Testing**
   - Unit tests comprehensive
   - Integration tests pass
   - Coverage adequate (>80%)

4. **Security**
   - No hardcoded secrets
   - Input validation present
   - SQL injection prevented
   - XSS protection in place

5. **Performance**
   - Database queries optimized
   - Images optimized
   - Caching implemented where needed

## Definition of Done

A task is complete when:

1. All code implemented to specification
2. Unit tests written and passing
3. Code coverage meets project requirements
4. Documentation complete (if applicable)
5. Code passes all configured linting and static analysis checks

## Emergency Procedures

### Critical Bug in Production
1. Create hotfix branch from main
2. Write failing test for bug
3. Implement minimal fix
5. Deploy immediately
6. Document in plan.md
