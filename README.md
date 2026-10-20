# Espresso: Distributed Offline Payment and Wallet System Using Peer-to-Peer Synchronization

> **Academic Prototype Notice:** This system is an academic simulation built for a Distributed Systems course (FA-1). It operates exclusively on simulated virtual digital currency (₹ / DEMO-COINS) and does NOT integrate with real-world banking networks or UPI switches.

---

## Table of Contents
1. [Project Overview](#project-overview)
2. [Distributed Systems Syllabus Concept Mapping](#distributed-systems-syllabus-concept-mapping)
3. [System Architecture](#system-architecture)
4. [Technology Stack](#technology-stack)
5. [Key Distributed Features](#key-distributed-features)
   - [Remote Procedure Call (RPC)](#1-remote-procedure-call-rpc)
   - [Message-Oriented Communication](#2-message-oriented-communication)
   - [Direct Peer-to-Peer (P2P) Synchronization](#3-direct-peer-to-peer-p2p-synchronization)
   - [Stream-Oriented Communication & WebSockets](#4-stream-oriented-communication--websockets)
   - [WebRTC Peer-to-Peer Verification](#5-webrtc-peer-to-peer-verification)
   - [Deterministic Offline Spending & Double-Spend Resolution](#6-deterministic-offline-spending--double-spend-resolution)
6. [Quick Start & Setup Instructions](#quick-start--setup-instructions)
   - [Option A: Full Docker Compose Deployment (Recommended)](#option-a-full-docker-compose-deployment)
   - [Option B: Local Multi-Process Python Runner](#option-b-local-multi-process-python-runner)
7. [Automated Test Suite](#automated-test-suite)
8. [Demonstration Walkthrough (FA-1 Scenarios)](#demonstration-walkthrough-fa-1-scenarios)
9. [FA-2 Extension Roadmap](#fa-2-extension-roadmap)

---

## Project Overview

In intermittent-connectivity and rural environments, centralized payment gateways fail when wide-area network links drop. This project implements an academic prototype of a **Distributed Digital Wallet System** capable of:
1. Processing real-time payments across a 3-node payment cluster via **gRPC** and **RabbitMQ**.
2. Allowing users to enter **Offline Mode**, queue payments locally (`PENDING_OFFLINE`), and reserve balance against a deterministic offline quota (₹500 limit).
3. Automatically synchronizing offline transactions upon reconnection using direct **Peer-to-Peer (P2P) Delta Replication**.
4. Reconciling version vectors and deterministically rejecting double-spend attempts (`REJECTED_DOUBLE_SPEND`).
5. Supporting **WebRTC DataChannels** for direct browser-to-browser payment dispute and verification.

---

## Distributed Systems Syllabus Concept Mapping

| Syllabus Concept (Unit 1 & 2) | Theoretical Definition | Prototype Implementation | Code References |
| :--- | :--- | :--- | :--- |
| **Distributed Architecture** | Autonomous nodes communicating over a network to achieve a common goal. | 3 independent payment nodes (`node-a:5001`, `node-b:5002`, `node-c:5003`) with isolated ledgers. | `backend/node/app.py`, `backend/node/database.py` |
| **Middleware Layer** | Abstraction layer providing uniform services, routing, and transparency. | API Gateway on port 8000 handling JWT auth, load routing, failover, and protocol translation. | `backend/gateway/app.py`, `backend/gateway/router.py` |
| **Remote Procedure Call (RPC)** | Transparent synchronous execution of subroutines on remote network nodes. | Native gRPC services defined via `proto/payment.proto` (`GetBalance`, `ProcessPayment`, `SyncTransactions`). | `proto/payment.proto`, `backend/node/grpc_service.py` |
| **Message-Oriented Communication** | Decoupled asynchronous message passing via persistent message broker queues. | RabbitMQ topic exchange `payment_events` with routing keys `payment.created`, `payment.committed`. | `backend/messaging/broker.py`, `backend/node/app.py` |
| **Stream-Oriented Communication** | Continuous full-duplex transmission of time-sensitive event streams. | WebSockets endpoint `/ws/stream` broadcasting live RPC traces, state changes, and heartbeat telemetry. | `backend/gateway/websocket_manager.py`, `frontend/src/pages/LiveStream.tsx` |
| **Peer-to-Peer (P2P) Communication** | Direct data exchange between equal nodes without centralized master coordination. | Direct P2P Delta Sync engine querying peer version vectors and merging missing transactions. | `backend/node/sync_service.py`, `frontend/src/pages/SyncMonitor.tsx` |
| **WebRTC** | Direct peer browser data channels using signaling solely for connection negotiation. | WebRTC DataChannel for peer-to-peer payment verification and dispute chats. | `backend/gateway/webrtc_signaling.py`, `frontend/src/pages/WebRTCVerification.tsx` |
| **Names, Identifiers & Addresses** | Distinguishing persistent logical entities from transient network location addresses. | Logical IDs (`USER-1001`, `WALLET-1001`, `TX-xxxx`, `NODE-A`) mapped dynamically to network addresses (`node-a:5001`). | `backend/common/models.py`, `backend/common/config.py` |
| **Fault Awareness & Failover** | Monitoring node liveness and failing over transparently when a node crashes. | Dynamic crash simulation (`/simulate-failure`) and automatic Gateway router failover to healthy surviving nodes. | `backend/gateway/router.py`, `frontend/src/pages/DistributedNodes.tsx` |
| **Virtualization & Containers** | Packaging distributed multi-tier applications into containerized environments. | Docker Compose multi-service deployment orchestrating Frontend, Gateway, Nodes A/B/C, and RabbitMQ. | `docker-compose.yml`, `docker/Dockerfile.*` |

---

## System Architecture

```
                          ┌───────────────────────────┐
                          │   React + Vite Frontend   │
                          │   (Port 3000 / Tailwind)  │
                          └─────────────┬─────────────┘
                                        │ HTTP / WebSocket / WebRTC
                                        ▼
                          ┌───────────────────────────┐
                          │  API Gateway / Middleware │
                          │     (FastAPI : 8000)      │
                          └─────────────┬─────────────┘
                                        │ gRPC / REST RPC
                 ┌──────────────────────┼──────────────────────┐
                 ▼                      ▼                      ▼
          ┌──────────────┐       ┌──────────────┐       ┌──────────────┐
          │ Payment Node │       │ Payment Node │       │ Payment Node │
          │    NODE-A    │◄─────►│    NODE-B    │◄─────►│    NODE-C    │
          │  Port: 5001  │  P2P  │  Port: 5002  │  P2P  │  Port: 5003  │
          │  gRPC: 6001  │       │  gRPC: 6002  │       │  gRPC: 6003  │
          └──────┬───────┘       └──────┬───────┘       └──────┬───────┘
                 │                      │                      │
                 ▼                      ▼                      ▼
            Ledger DB A            Ledger DB B            Ledger DB C
          (data_node_a.db)       (data_node_b.db)       (data_node_c.db)
                 │                      │                      │
                 └──────────────────────┼──────────────────────┘
                                        │ AMQP Events
                                        ▼
                          ┌───────────────────────────┐
                          │      Message Broker       │
                          │     (RabbitMQ : 5672)     │
                          └───────────────────────────┘
```

---

## Technology Stack

- **Frontend**: React 18, TypeScript, Vite, Tailwind CSS, Lucide Icons, Recharts
- **Backend Services**: Python 3.11 / 3.14, FastAPI, Pydantic V2, SQLAlchemy 2.0
- **RPC Framework**: gRPC (HTTP/2 + Protocol Buffers)
- **Message Broker**: RabbitMQ (AMQP 0-9-1) with `aio-pika`
- **Stream Communication**: WebSockets (`/ws/stream` and `/ws/signaling`)
- **P2P Client Verification**: WebRTC DataChannel (SCTP/DTLS)
- **Databases**: Independent SQLite / PostgreSQL ledgers per payment node
- **DevOps**: Docker, Docker Compose, Nginx

---

## Key Distributed Features

### 1. Remote Procedure Call (RPC)
Service-to-service communication is defined in `proto/payment.proto`:
- `GetBalance(BalanceRequest) -> BalanceResponse`
- `ProcessPayment(PaymentRequest) -> PaymentResponse`
- `ValidateTransaction(ValidationRequest) -> ValidationResponse`
- `SyncTransactions(SyncRequest) -> SyncResponse`

### 2. Message-Oriented Communication
The `payment_events` topic exchange publishes decoupled lifecycle events:
- `payment.created`: Initiated on origin node
- `payment.pending`: Queued offline payment
- `payment.sync`: Ingested via P2P delta sync
- `payment.committed`: Settled across cluster
- `payment.rejected`: Rejected due to funds or double spend

### 3. Direct Peer-to-Peer (P2P) Synchronization
Payment nodes exchange version deltas directly without a central coordinator:
1. `GET_SYNC_STATE`: Query peer's highest Lamport version.
2. `REQUEST_MISSING`: Query delta transactions `since_version`.
3. `SEND_TRANSACTIONS`: Transmit missing transaction payload.
4. `MERGE & ACK`: Atomically commit delta, update local balances, and write to `sync_records`.

### 4. Deterministic Offline Spending & Double-Spend Resolution
- **Offline Spending Limit**: Max ₹500 per offline transaction.
- **Balance Invariant**:
  $$\text{Available Balance} = \text{Confirmed Balance} - \text{Pending Outgoing}$$
- When a user attempts two offline transactions of ₹400 with a ₹500 balance:
  - **Transaction A (₹400)**: Accepted into offline queue (`PENDING_OFFLINE`). Remaining unreserved balance becomes ₹100.
  - **Transaction B (₹400)**: Rejected locally with `REJECTED_DOUBLE_SPEND` because ₹400 + ₹400 = ₹800 > confirmed ₹500.

---

## Quick Start & Setup Instructions

### Option A: Full Docker Compose Deployment (Recommended)
Run the complete multi-container distributed cluster with a single command:

```bash
docker compose up --build
```

Access the services:
- **Frontend Dashboard**: `http://localhost:3000`
- **API Gateway & Swagger Docs**: `http://localhost:8000/docs`
- **RabbitMQ Management UI**: `http://localhost:15672` (User: `guest`, Pass: `guest`)
- **Payment Node A**: `http://localhost:5001`
- **Payment Node B**: `http://localhost:5002`
- **Payment Node C**: `http://localhost:5003`

---

### Option B: Local Multi-Process Python Runner
If running without Docker:

```bash
# 1. Install Python dependencies
pip install -r requirements.txt

# 2. Install Frontend dependencies & build
cd frontend
npm install
npm run build
cd ..

# 3. Launch all backend nodes and gateway concurrently
python scripts/run_local.py
```

In a separate terminal, launch the frontend dev server:
```bash
cd frontend
npm run dev
```

---

## Automated Test Suite

Run the full pytest suite covering wallet invariants, state machine transitions, double-spend detection, P2P sync, and REST APIs:

```bash
python -m pytest
```

All 13 test suites execute and pass with 100% success.

---

## Demonstration Walkthrough (FA-1 Scenarios)

### Demo Accounts Provided:
- **Alice Sharma** (`WALLET-1001`) — Balance: ₹2,000
- **Bob Verma** (`WALLET-1002`) — Balance: ₹1,500
- **Charlie Gupta** (`WALLET-1003`) — Balance: ₹3,000

1. **Scenario 1 (Normal Online Payment)**:
   - Log in as Alice.
   - Send ₹300 to Bob online.
   - Observe real-time RPC trace on **Live Communication Waterfall** and status transition to `COMMITTED`.
2. **Scenario 2 (Offline Payment & Queue)**:
   - Toggle **Connectivity** to `OFFLINE` in navbar.
   - Send ₹200 to Bob.
   - Observe state `PENDING_OFFLINE` in **Offline Queue** and immediate local balance reservation.
3. **Scenario 3 (Reconnection & P2P Sync)**:
   - Toggle **Connectivity** back to `ONLINE`.
   - Click **Reconcile with Nodes** to commit the queued transaction across distributed nodes.
4. **Scenario 4 (Fault Injection & Failover)**:
   - Go to **Distributed Nodes** and click **Simulate Crash / Failure** on **NODE-A**.
   - Send payment. The Gateway router automatically fails over to **NODE-B**.
   - Restore **NODE-A** and execute P2P sync on **Sync Monitor**.
5. **Scenario 5 (Double-Spend Prevention Lab)**:
   - Open **Double-Spend Lab** and execute Step 1, Step 2, and Step 3 to see `REJECTED_DOUBLE_SPEND` in action.
6. **Scenario 6 (WebRTC Direct Verification)**:
   - Open **WebRTC P2P Demo** in two browser tabs and exchange verification messages over the direct data channel.

---

## FA-2 Extension Roadmap
See [FA2_ROADMAP.md](file:///c:/Users/adity/OneDrive/Desktop/Git/fsdl/CC_MiniProj/DSProj/FA2_ROADMAP.md) for future plans on Raft consensus, vector clocks, and two-phase commit coordinators.
