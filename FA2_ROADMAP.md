# Formative Assessment 2 (FA-2) Extension Roadmap

This document outlines the architectural hooks already established in FA-1 to support FA-2 course topics:

```
FA-1 Baseline (Communication & Offline P2P)
  │
  ├──► FA-2.1: Raft Consensus Cluster (Leader Election & State Machine Replication)
  ├──► FA-2.2: Vector Clock Causality Tracking & Merkle Tree Anti-Entropy
  ├──► FA-2.3: Distributed Transaction Coordinator (Two-Phase Commit / 2PC)
  ├──► FA-2.4: Quorum Replication & Tunable Consistency (Dynamo Model)
  └──► FA-2.5: Byzantine Fault Tolerance (BFT) Simulation for untrusted nodes
```

## Architectural Readiness
1. **Node Independence**: Each node (`node-a`, `node-b`, `node-c`) is already an autonomous process with independent database storage and gRPC interfaces, making it directly pluggable into a Raft consensus state machine.
2. **Versioned Models**: The `Transaction` table already records monotonic `version` integers, prepared to be converted into multidimensional vector clock mappings.
3. **Idempotency & Message Deduplication**: The messaging broker and database already maintain message IDs (`message_id`) to prevent duplicate execution during network replay.
