# The Vision Doc

## Engram: A Distributed Verifiable Cognitive Substrate

### The Vision Itself

Engram exists to answer a single question:

> How can autonomous systems remember, reason, coordinate, and evolve knowledge across time while remaining scalable, distributed, and verifiable?

Today, AI systems are largely stateless.

Memory is fragmented.

Context is transient.

Knowledge is siloed.

Trust is implicit.

Engram aims to become the infra layer that solves these problems.

Sort of like a foundational system for machine cognition.

---

# Long-Term Goal

Become the memory, coordination, and trust layer for autonomous systems.

Equivalent to:

* Linux for operating systems
* PostgreSQL for structured data
* Redis for transient state

But for machine cognition.

---

# Core Principles

## 1. Memory is Infrastructure

Memory is not a feature.

Memory is a fundamental primitive.

Every autonomous system should be able to:

* store knowledge
* retrieve knowledge
* evolve knowledge
* share knowledge
* verify knowledge

through a common infrastructure layer.

---

## 2. Memory is Distributed

Memory should survive machine failures.

Memory should exist across nodes.

Memory should replicate and synchronize automatically.

Topics:

* replication
* consensus
* partitioning
* fault tolerance
* recovery

---

## 3. Memory Evolves

Knowledge should not remain static.

The system should:

* summarize
* consolidate
* compress
* reorganize
* forget

when necessary.

Memory management becomes an active process.

---

## 4. Memory is Queryable

Memory should support more than retrieval.

Future APIs should resemble:

* remember()
* recall()
* trace()
* reason()
* prove()

rather than simple vector search.

---

## 5. Memory is Temporal

The system should preserve historical state.

Questions such as:

* What was known six months ago?
* Why was this decision made?
* What changed?

should be answerable.

Topics:

* snapshots
* versioning
* timelines
* historical reconstruction

---

## 6. Memory is Shared

Multiple autonomous systems should collaborate through shared memory.

Memory ownership should support:

* private memory
* shared memory
* public memory

with explicit permissions and synchronization guarantees.

---

## 7. Memory is Inference-Aware

The system should understand how memories influence outcomes.

Questions:

* Which memories improve results?
* Which memories are irrelevant?
* Which memories should be cached?

This introduces AI infrastructure concerns:

* retrieval optimization
* inference systems
* serving architectures
* context management

---

## 8. Memory is Verifiable

Trust should not rely on assumptions.

The system should eventually support proofs for:

* memory existence
* retrieval correctness
* provenance
* reasoning traces
* state integrity

Topics:

* cryptography
* verifiable computation
* zkVMs
* proof systems

---

## 9. Memory Enables Collective Intelligence

The long-term goal is to support networks of autonomous systems.

Not one agent.

Not one organization.

Potentially millions.

Each system contributes to and benefits from a shared cognitive substrate.

---

# Development Trajectory

## Stage 1: Distributed Memory ✅

Goal:

Create a fault-tolerant distributed memory system.

Status: complete. Three nodes, Raft consensus (OpenRaft 0.9), gRPC transport (tonic 0.12), leader election, log replication, follower redirect, failover, and cluster observability. All five acceptance criteria pass.

Learn:

* networking
* async systems
* consensus
* OpenRaft

---

## Stage 2: Knowledge Formation ✅

Goal:

Transform memories into structured knowledge.

Status: complete. Entity and relationship extraction from message text (OpenAI GPT-4o-mini or offline mock), per-session in-memory knowledge graph backed by petgraph, leader-only extraction with Raft-replicated `AddKnowledge` command for consistency across all nodes, four knowledge REST endpoints, Graphviz DOT export, four new Prometheus metrics, 146 tests pass.

Learn:

* graph systems
* indexing
* query engines

---

## Stage 3: Collective Memory

### Stage 3A: Persistence and recovery ✅

Status: complete. Persistent redb-backed Raft log and snapshot store, full state machine snapshots (short-term memory, core memory, knowledge graph), startup recovery, InstallSnapshot over gRPC for lagging followers, automatic log compaction. All 10 cluster-verify criteria pass.

### Stage 3B: Collective memory ✅

Status: complete. Session visibility (Private/Shared) controlled via `SetSessionVisibility` Raft command, global cross-session knowledge graph with provenance and conflict tracking, agent registration at session creation, six new global REST endpoints, three new Prometheus gauges (`engram_global_entities`, `engram_global_relationships`, `engram_global_conflicts`), snapshot protocol v2 including global_graph/visibility/session_agents, 17/17 cluster-verify criteria pass.

Goal:

Allow multiple autonomous systems to share knowledge.

Learn:

* synchronization
* permissions
* conflict resolution

---

## Stage 4: Memory Evolution

Goal:

Enable memory consolidation, summarization, and adaptation.

Learn:

* scheduling
* background processing
* AI infrastructure

---

## Stage 5: Cognitive Version Control

Goal:

Track and reconstruct historical states of knowledge.

Learn:

* snapshots
* state machines
* storage engines

---

## Stage 6: Inference-Aware Memory

Goal:

Integrate deeply with inference systems.

Learn:

* vLLM
* serving architectures
* retrieval optimization

---

## Stage 7: Verifiable Cognition

Goal:

Prove memory and reasoning correctness.

Learn:

* zkVMs
* proof systems
* verifiable computation

---

## Stage 8: Distributed Cognitive Infrastructure

Goal:

Support large-scale autonomous organizations.

Learn:

* trust systems
* machine coordination
* distributed governance

---

# Success Metric

In my opinion, success here means becoming capable of solving increasingly difficult problems involving:

* distributed systems
* AI infrastructure
* memory
* coordination
* verification

If Engram forces those skills to emerge, it has succeeded.

Even if the implementation changes.

Even if the architecture changes.

Even if the vision changes.

The trajectory remains.

---
