# Tatkal Task Orchestrator — Design Document

## Table of Contents

1. [Overview](#overview)
2. [Architecture](#architecture)
3. [System Components](#system-components)
4. [Data Model](#data-model)
5. [Core Algorithms](#core-algorithms)
6. [API Specification](#api-specification)
7. [Workflow Simulation](#workflow-simulation)
8. [Failure Handling & Resilience](#failure-handling--resilience)
9. [Crash Recovery](#crash-recovery)
10. [Concurrency Model](#concurrency-model)
11. [Project Structure](#project-structure)
12. [Configuration](#configuration)
13. [Testing](#testing)
14. [Demo Walkthrough](#demo-walkthrough)
15. [Four Key Questions](#four-key-questions)

---

## Overview

**Tatkal Task Orchestrator** is a lightweight, resilient DAG (Directed Acyclic Graph) task runner that simulates the high-demand **IRCTC Tatkal Ticket Booking** workflow. It demonstrates production-grade distributed systems patterns — concurrency control, exponential retry backoff, cascading failure blocking, and crash recovery — in a self-contained, easy-to-run package.

### Key Characteristics

| Property | Value |
|----------|-------|
| Runtime | Node.js v18+ |
| Language | JavaScript (CommonJS) |
| Web Framework | Express 4.x |
| Database | SQLite (better-sqlite3) with WAL mode |
| External Dependencies | None (fully self-contained) |
| Build Step | None |
| Deployment | Single process, embedded DB |

---

## Architecture

### High-Level Diagram

```
┌─────────────────────────────────────────────────────────┐
│                      Client (HTTP)                      │
└──────────────────────────┬──────────────────────────────┘
                           │ REST API
                           ▼
┌─────────────────────────────────────────────────────────┐
│                   Express App (app.js)                   │
│  ┌──────────┐ ┌──────────┐ ┌──────────┐ ┌───────────┐  │
│  │ POST     │ │ GET      │ │ POST     │ │ GET       │  │
│  │ /tasks   │ │ /tasks/:id│ │ /tasks/:id│ │ /stats    │  │
│  │          │ │          │ │ /cancel  │ │           │  │
│  └────┬─────┘ └────┬─────┘ └────┬─────┘ └─────┬─────┘  │
│       │             │            │             │        │
│       └─────────────┴─────┬──────┴─────────────┘        │
│                           │                              │
│                    DAG Validation                        │
│                    (dag.js)                              │
│                           │                              │
│                           ▼                              │
│              ┌────────────────────────┐                  │
│              │   TaskScheduler        │                  │
│              │   (scheduler.js)       │                  │
│              │                        │                  │
│              │  ┌──────────────────┐  │                  │
│              │  │  Worker Pool     │  │                  │
│              │  │  (concurrency=2) │  │                  │
│              │  │                  │  │                  │
│              │  │  Worker 1 ──┐    │  │                  │
│              │  │  Worker 2 ──┤    │  │                  │
│              │  └─────────────┼────┘  │                  │
│              │                │       │                  │
│              │         ┌──────▼─────┐ │                  │
│              │         │  runner.js │ │                  │
│              │         │ (executor) │ │                  │
│              │         └────────────┘ │                  │
│              └───────────┬────────────┘                  │
│                          │                               │
│                          ▼                               │
│              ┌────────────────────────┐                  │
│              │   SQLite (db.js)       │                  │
│              │   WAL Mode             │                  │
│              │                        │                  │
│              │  ┌──────────────────┐  │                  │
│              │  │ tasks            │  │                  │
│              │  │ task_dependencies│  │                  │
│              │  └──────────────────┘  │                  │
│              └────────────────────────┘                  │
└─────────────────────────────────────────────────────────┘
```

### Request Flow

```
Client Request
    │
    ▼
┌─────────┐     ┌──────────┐     ┌───────────┐     ┌──────────┐
│ Express  │────▶│ DAG      │────▶│ Database  │────▶│Scheduler │
│ Route    │     │ Validate │     │ Insert    │     │ Tick     │
└─────────┘     └──────────┘     └───────────┘     └────┬─────┘
                                                        │
                                                        ▼
                                                 ┌──────────┐
                                                 │ Worker   │
                                                 │ Execute  │
                                                 └────┬─────┘
                                                      │
                                                      ▼
                                                 ┌──────────┐
                                                 │ Update   │
                                                 │ Status   │
                                                 └──────────┘
```

---

## System Components

### 1. Server Bootstrap (`src/server.js`)

The entry point that wires everything together.

```
server.js
  │
  ├── initDB()              → Creates tables, enables WAL + foreign keys
  ├── new TaskScheduler()   → Creates scheduler with concurrency limit
  ├── recoverInterruptedTasks() → Crash recovery on startup
  └── app.listen(PORT)      → Starts HTTP server
```

**Responsibilities:**
- Initialize the database schema
- Instantiate the scheduler with configurable concurrency
- Run crash recovery before accepting traffic
- Start the HTTP listener

### 2. Express App (`src/app.js`)

The HTTP API layer.

**Responsibilities:**
- Parse JSON request bodies
- Validate input and return appropriate HTTP status codes
- Use database transactions for atomic batch inserts
- Delegate scheduling logic to the TaskScheduler

### 3. Database Layer (`src/db.js`)

SQLite persistence with WAL mode.

**Responsibilities:**
- Open/create the SQLite database
- Enable WAL mode for concurrent reads during writes
- Enable foreign key constraints
- Create the `tasks` and `task_dependencies` tables
- Create indexes for performance

### 4. Task Scheduler (`src/scheduler.js`)

The core orchestration engine.

**Responsibilities:**
- Maintain a pool of workers (up to concurrency limit)
- Identify "ready" tasks (all dependencies succeeded)
- Execute tasks via the runner
- Handle success/failure with retry logic
- Cascade-block dependent tasks on failure
- Cancel tasks and their dependents
- Recover interrupted tasks on startup

### 5. Task Runner (`src/runner.js`)

The simulated task executor.

**Responsibilities:**
- Simulate work with configurable latency (`duration_ms`)
- Simulate random failures based on `failure_rate` probability
- Enforce operational timeout (`timeout_ms`)
- Return a Promise that resolves on success or rejects on failure

### 6. DAG Validator (`src/dag.js`)

Cycle detection and dependency validation.

**Responsibilities:**
- Detect cycles using Kahn's Algorithm (topological sort)
- Validate that all dependency references exist
- Reject duplicate task IDs
- Reject self-dependencies
- Throw descriptive errors for invalid DAGs

---

## Data Model

### Entity-Relationship Diagram

```
┌──────────────────────┐         ┌──────────────────────────┐
│       tasks          │         │   task_dependencies      │
├──────────────────────┤         ├──────────────────────────┤
│ id (PK, TEXT)        │◄────────┤ task_id (PK, FK)         │
│ name (TEXT, NOT NULL)│         │ depends_on_task_id (PK,  │
│ status (TEXT, CHECK) │         │   FK)                    │
│ duration_ms (INT)    │         └──────────────────────────┘
│ failure_rate (REAL)  │
│ timeout_ms (INT)     │
│ max_retries (INT)    │
│ attempts (INT)       │
│ error_message (TEXT) │
│ created_at (INT)     │
│ updated_at (INT)     │
└──────────────────────┘
```

### Task Status State Machine

```
                    ┌─────────┐
                    │ waiting │
                    └────┬────┘
                         │ scheduler picks up
                         ▼
                    ┌─────────┐
          ┌────────│ running │────────┐
          │        └─────────┘        │
          │ success                   │ failure
          ▼                           ▼
    ┌──────────┐              ┌──────────────┐
    │succeeded │              │   failed     │
    └──────────┘              └──────────────┘
          │                           │
          │                           │ retries remain
          │                           ▼
          │                    ┌──────────┐
          │                    │ waiting  │ (re-queue)
          │                    └──────────┘
          │
          │ all deps succeeded
          ▼
    ┌──────────┐
    │ running  │ (dependent task)
    └──────────┘

    ┌──────────┐
    │ blocked  │ ◀── parent failed (cascade)
    └──────────┘

    ┌──────────┐
    │ canceled │ ◀── user cancellation (cascade)
    └──────────┘
```

### Status Values

| Status | Meaning |
|--------|---------|
| `waiting` | Task is queued, waiting for dependencies or a worker slot |
| `running` | Task is currently being executed by a worker |
| `succeeded` | Task completed successfully |
| `failed` | Task exhausted all retry attempts |
| `blocked` | A dependency failed; this task will never run |
| `canceled` | Task was manually canceled |

---

## Core Algorithms

### 1. DAG Validation — Kahn's Algorithm

```
function validateDAG(tasks):
    1. Build adjacency list from task_dependencies
    2. Calculate in-degree for each task
    3. Initialize queue with all tasks having in-degree = 0
    4. While queue is not empty:
        a. Dequeue a task
        b. Add to sorted list
        c. For each dependent, decrement in-degree
        d. If in-degree becomes 0, enqueue it
    5. If sorted list length < total tasks → CYCLE DETECTED
    6. Validate all dependency references exist
    7. Reject self-dependencies and duplicate IDs
```

**Time Complexity:** O(V + E) where V = tasks, E = dependencies

### 2. Scheduling Loop (Tick)

```
function tick():
    1. Query all tasks with status = 'waiting'
    2. For each waiting task:
        a. Check if ALL dependencies have status = 'succeeded'
        b. If yes → mark as "ready"
    3. Count currently running tasks
    4. availableSlots = concurrencyLimit - runningCount
    5. Assign ready tasks to available slots (FIFO order)
    6. For each assigned task:
        a. Set status = 'running'
        b. Execute via runner
        c. On success → status = 'succeeded'
        d. On failure → retry or fail
```

### 3. Exponential Backoff Retry

```
function calculateBackoff(attempt):
    baseDelay = 100ms
    maxDelay = 30000ms
    delay = min(baseDelay * 2^attempt, maxDelay)
    jitter = random(0, delay * 0.1)  // 10% jitter
    return delay + jitter

function runWorker(task):
    try:
        result = await runner.execute(task)
        markSucceeded(task)
    catch error:
        task.attempts++
        if task.attempts <= task.max_retries:
            backoff = calculateBackoff(task.attempts)
            scheduleRetry(task, backoff)
        else:
            markFailed(task)
            cascadeBlockDependents(task)
```

### 4. Cascading Failure Block

```
function cascadeBlockDependents(failedTask):
    1. Find all tasks that depend on failedTask
    2. For each dependent:
        a. Set status = 'blocked'
        b. Set error_message = "Dependency {failedTask.id} failed"
        c. Recursively cascade to ITS dependents
```

### 5. Crash Recovery

```
function recoverInterruptedTasks():
    1. Find all tasks with status = 'running'
       (these were interrupted by a crash/restart)
    2. For each interrupted task:
        a. If attempts < max_retries:
            → Reset to 'waiting' (will be re-executed)
        b. Else:
            → Mark as 'failed'
            → Cascade block dependents
```

---

## API Specification

### Base URL

```
http://localhost:3000
```

### Endpoints

#### 1. Submit Tasks

```
POST /tasks
Content-Type: application/json

Request Body:
{
  "tasks": [
    {
      "id": "task-1",              // required, unique
      "name": "Verify Passenger",  // required
      "duration_ms": 500,          // optional, default: 300
      "failure_rate": 0.1,         // optional, default: 0.0 (0.0-1.0)
      "timeout_ms": 5000,          // optional, default: 5000
      "max_retries": 3,            // optional, default: 3
      "dependencies": ["task-0"]   // optional, array of task IDs
    }
  ]
}

Response 201:
{
  "message": "Tasks submitted successfully",
  "task_ids": ["task-1", "task-2"]
}

Response 400:
{
  "error": "Cycle detected in task graph"
}
```

#### 2. Get Task Status

```
GET /tasks/:id

Response 200:
{
  "id": "task-1",
  "name": "Verify Passenger",
  "status": "running",
  "duration_ms": 500,
  "failure_rate": 0.1,
  "timeout_ms": 5000,
  "max_retries": 3,
  "attempts": 1,
  "error_message": null,
  "created_at": 1696156800000,
  "updated_at": 1696156801000,
  "dependencies": ["task-0"]
}

Response 404:
{
  "error": "Task not found"
}
```

#### 3. Cancel Task

```
POST /tasks/:id/cancel

Response 200:
{
  "ok": true,
  "taskId": "task-1",
  "status": "canceled"
}

Response 400:
{
  "error": "Cannot cancel task with status 'succeeded'"
}

Response 404:
{
  "error": "Task not found"
}
```

#### 4. Get Stats

```
GET /stats

Response 200:
{
  "running": 2,
  "waiting": 3,
  "concurrencyLimit": 2
}
```

---

## Workflow Simulation

### Tatkal Booking DAG

```
                    ┌──────────────────┐
                    │ verify-passenger │
                    └────────┬─────────┘
                             │
              ┌──────────────┼──────────────┐
              ▼              ▼               ▼
    ┌─────────────────┐  ┌──────────────────┐  ┌───────────────────┐
    │ lock-berth-quota│  │validate-aadhaar  │  │                   │
    │                 │  │      -kyc        │  │                   │
    └────────┬────────┘  └────────┬─────────┘  │                   │
             │                    │             │                   │
             └────────────┬───────┘             │                   │
                          ▼                     │                   │
                ┌──────────────────┐            │                   │
                │ process-upi-     │            │                   │
                │    payment       │            │                   │
                └────────┬─────────┘            │                   │
                         │                      │                   │
                         └──────────┬───────────┘                   │
                                    ▼                               │
                          ┌──────────────────┐                      │
                          │  generate-pnr    │◄─────────────────────┘
                          └──────────────────┘
```

### Task Definitions

| Task ID | Name | Duration | Failure Rate | Dependencies |
|---------|------|----------|-------------|--------------|
| `verify-passenger` | Verify Passenger | 300ms | 0.05 | None |
| `lock-berth-quota` | Lock Berth Quota | 500ms | 0.15 | verify-passenger |
| `validate-aadhaar-kyc` | Validate Aadhaar KYC | 400ms | 0.10 | verify-passenger |
| `process-upi-payment` | Process UPI Payment | 800ms | 0.25 | lock-berth-quota, validate-aadhaar-kyc |
| `generate-pnr` | Generate PNR | 200ms | 0.05 | process-upi-payment |

### Execution Timeline (Happy Path)

```
Time 0ms:   verify-passenger starts
Time 300ms: verify-passenger succeeds
            ├── lock-berth-quota starts (Worker 1)
            └── validate-aadhaar-kyc starts (Worker 2)
Time 700ms: validate-aadhaar-kyc succeeds
Time 800ms: lock-berth-quota succeeds
            └── process-upi-payment starts (Worker 1)
Time 1600ms: process-upi-payment succeeds
             └── generate-pnr starts (Worker 1)
Time 1800ms: generate-pnr succeeds
             └── ALL TASKS COMPLETE
```

### Execution Timeline (With Failures)

```
Time 0ms:   verify-passenger starts
Time 300ms: verify-passenger succeeds
            ├── lock-berth-quota starts (Worker 1)
            └── validate-aadhaar-kyc starts (Worker 2)
Time 700ms: validate-aadhaar-kyc succeeds
Time 800ms: lock-berth-quota FAILS (attempt 1/3)
            └── retry scheduled with backoff (~200ms)
Time 1000ms: lock-berth-quota starts (attempt 2)
Time 1500ms: lock-berth-quota succeeds
             └── process-upi-payment starts
Time 2300ms: process-upi-payment FAILS (attempt 1/3)
             └── retry scheduled with backoff (~400ms)
Time 2700ms: process-upi-payment starts (attempt 2)
Time 3500ms: process-upi-payment succeeds
             └── generate-pnr starts
Time 3700ms: generate-pnr succeeds
             └── ALL TASKS COMPLETE
```

---

## Failure Handling & Resilience

### Failure Scenarios

#### 1. Task Failure with Retries

```
Attempt 1: FAIL → wait 100ms + jitter
Attempt 2: FAIL → wait 200ms + jitter
Attempt 3: FAIL → wait 400ms + jitter
Attempt 4: FAIL → mark as 'failed', cascade block dependents
```

#### 2. Cascading Failure

```
If lock-berth-quota fails:
    └── process-upi-payment → blocked
        └── generate-pnr → blocked
```

#### 3. Task Cancellation

```
If user cancels verify-passenger:
    └── lock-berth-quota → canceled
    └── validate-aadhaar-kyc → canceled
        └── process-upi-payment → canceled
            └── generate-pnr → canceled
```

#### 4. Timeout

```
If a task exceeds timeout_ms:
    → Treated as failure
    → Retry logic applies
    → If all retries exhausted → failed
```

### Retry Backoff Formula

```
delay = min(baseDelay × 2^(attempt-1), maxDelay) + jitter

Where:
  baseDelay = 100ms
  maxDelay  = 30000ms (30 seconds)
  jitter   = random(0, delay × 0.1)
```

| Attempt | Base Delay | Max Delay | With Jitter |
|---------|-----------|-----------|-------------|
| 1 | 100ms | 100ms | 100-110ms |
| 2 | 200ms | 200ms | 200-220ms |
| 3 | 400ms | 400ms | 400-440ms |
| 4 | 800ms | 800ms | 800-880ms |
| 5 | 1600ms | 1600ms | 1600-1760ms |
| 6 | 3200ms | 3200ms | 3200-3520ms |
| 7 | 6400ms | 6400ms | 6400-7040ms |
| 8 | 12800ms | 12800ms | 12800-14080ms |
| 9 | 25600ms | 25600ms | 25600-28160ms |
| 10 | 51200ms | 30000ms | 30000-33000ms |

---

## Crash Recovery

### Problem

If the server crashes while tasks are in `running` state, those tasks are orphaned — no worker will ever complete them.

### Solution

On startup, `recoverInterruptedTasks()`:

```
1. SELECT * FROM tasks WHERE status = 'running'
2. For each orphaned task:
   a. If attempts < max_retries:
      → UPDATE status = 'waiting'
      → Task will be re-executed by the scheduler
   b. Else:
      → UPDATE status = 'failed'
      → cascadeBlockDependents(task)
```

### Recovery Scenarios

| Scenario | Attempts | Max Retries | Action |
|----------|----------|-------------|--------|
| Crashed on first attempt | 1 | 3 | Re-queue (waiting) |
| Crashed on second attempt | 2 | 3 | Re-queue (waiting) |
| Crashed on final attempt | 3 | 3 | Mark failed, block dependents |

---

## Concurrency Model

### Worker Pool Architecture

```
┌─────────────────────────────────────────────┐
│              TaskScheduler                   │
│                                              │
│  Concurrency Limit: 2                        │
│                                              │
│  ┌─────────────┐    ┌─────────────┐         │
│  │  Worker 1   │    │  Worker 2   │         │
│  │  (busy)     │    │  (busy)     │         │
│  └─────────────┘    └─────────────┘         │
│                                              │
│  ┌─────────────┐    ┌─────────────┐         │
│  │  Worker 3   │    │  Worker 4   │         │
│  │  (idle)     │    │  (idle)     │         │
│  └─────────────┘    └─────────────┘         │
│                                              │
│  Ready Queue: [task-5, task-6, task-7]       │
└─────────────────────────────────────────────┘
```

### Concurrency Behavior

- **Default limit:** 2 parallel workers
- **Configurable:** Via `CONCURRENCY_LIMIT` environment variable
- **FIFO ordering:** Ready tasks are assigned in creation order
- **Non-blocking:** The scheduler never blocks; it uses async/await
- **Slot release:** When a task completes (success or failure), its slot is freed for the next ready task

### Database Concurrency

- **WAL mode:** Allows concurrent reads while a write is in progress
- **Transactions:** Batch inserts are atomic
- **Foreign keys:** Enforce referential integrity

---

## Project Structure

```
tatkal-task-orchestrator/
├── src/
│   ├── server.js          # Entry point — bootstrap & startup
│   ├── app.js             # Express app & API routes
│   ├── db.js              # Database initialization & schema
│   ├── scheduler.js       # Core task scheduling engine
│   ├── runner.js          # Task execution simulator
│   └── dag.js             # DAG validation (Kahn's Algorithm)
├── test.js                # Automated test suite
├── package.json           # NPM package manifest
├── package-lock.json      # NPM lock file
├── orchestrator.db        # SQLite database (generated)
├── orchestrator.db-shm    # SQLite shared memory (generated)
├── orchestrator.db-wal    # SQLite write-ahead log (generated)
├── .gitignore             # Git ignore rules
├── README.md              # Project documentation
├── DESIGN.md              # This file
└── TRADEOFFS.md           # Design tradeoffs document
```

---

## Configuration

### Environment Variables

| Variable | Default | Description |
|----------|---------|-------------|
| `PORT` | `3000` | HTTP server port |
| `CONCURRENCY_LIMIT` | `2` | Maximum parallel task workers |
| `DB_PATH` | `orchestrator.db` | SQLite database file path |

### NPM Scripts

| Script | Command | Purpose |
|--------|---------|---------|
| `npm start` | `node src/server.js` | Start the production server |
| `npm test` | `node test.js` | Run the automated test suite |

---

## Testing

### Test Suite (`test.js`)

The test suite uses Node.js built-in `assert` module and covers:

1. **DAG Validation**
   - Valid DAG acceptance
   - Cycle detection
   - Self-dependency rejection
   - Duplicate ID rejection
   - Missing dependency detection

2. **Task Lifecycle**
   - Task submission
   - Status transitions
   - Dependency resolution

3. **Failure Handling**
   - Retry with backoff
   - Cascading failure blocking
   - Max retries exhaustion

4. **Cancellation**
   - Task cancellation
   - Cascade cancellation to dependents

5. **Crash Recovery**
   - Interrupted task recovery
   - Retry count preservation

### Running Tests

```bash
npm test
```

---

## Demo Walkthrough

### Prerequisites

```bash
# 1. Clone the repository
git clone <repo-url>
cd tatkal-task-orchestrator

# 2. Install dependencies
npm install

# 3. Start the server
npm start
```

### Demo Steps

#### Step 1: Submit the Tatkal Booking Workflow

```bash
curl -X POST http://localhost:3000/tasks \
  -H "Content-Type: application/json" \
  -d '{
    "tasks": [
      {
        "id": "verify-passenger",
        "name": "Verify Passenger",
        "duration_ms": 300,
        "failure_rate": 0.05,
        "max_retries": 3
      },
      {
        "id": "lock-berth-quota",
        "name": "Lock Berth Quota",
        "duration_ms": 500,
        "failure_rate": 0.15,
        "max_retries": 3,
        "dependencies": ["verify-passenger"]
      },
      {
        "id": "validate-aadhaar-kyc",
        "name": "Validate Aadhaar KYC",
        "duration_ms": 400,
        "failure_rate": 0.10,
        "max_retries": 3,
        "dependencies": ["verify-passenger"]
      },
      {
        "id": "process-upi-payment",
        "name": "Process UPI Payment",
        "duration_ms": 800,
        "failure_rate": 0.25,
        "max_retries": 3,
        "dependencies": ["lock-berth-quota", "validate-aadhaar-kyc"]
      },
      {
        "id": "generate-pnr",
        "name": "Generate PNR",
        "duration_ms": 200,
        "failure_rate": 0.05,
        "max_retries": 3,
        "dependencies": ["process-upi-payment"]
      }
    ]
  }'
```

#### Step 2: Monitor Progress

```bash
# Check overall stats
curl http://localhost:3000/stats

# Check individual task status
curl http://localhost:3000/tasks/verify-passenger
curl http://localhost:3000/tasks/lock-berth-quota
curl http://localhost:3000/tasks/process-upi-payment
curl http://localhost:3000/tasks/generate-pnr
```

#### Step 3: Observe Concurrency

```bash
# With concurrency limit = 2, you'll see:
# - verify-passenger runs alone first
# - Then lock-berth-quota and validate-aadhaar-kyc run in parallel
# - Then process-upi-payment runs alone
# - Finally generate-pnr runs
```

#### Step 4: Test Cancellation

```bash
# Cancel a task and watch dependents get canceled
curl -X POST http://localhost:3000/tasks/lock-berth-quota/cancel

# Check that process-upi-payment and generate-pnr are now 'canceled'
curl http://localhost:3000/tasks/process-upi-payment
curl http://localhost:3000/tasks/generate-pnr
```

#### Step 5: Test Failure Scenario

```bash
# Submit a workflow with high failure rate
curl -X POST http://localhost:3000/tasks \
  -H "Content-Type: application/json" \
  -d '{
    "tasks": [
      {
        "id": "flaky-task",
        "name": "Flaky Task",
        "duration_ms": 100,
        "failure_rate": 0.9,
        "max_retries": 2
      },
      {
        "id": "dependent-task",
        "name": "Dependent Task",
        "duration_ms": 100,
        "failure_rate": 0.0,
        "dependencies": ["flaky-task"]
      }
    ]
  }'

# Watch flaky-task retry with backoff, then fail
# Watch dependent-task get blocked
```

#### Step 6: Test Crash Recovery

```bash
# 1. Submit a long-running task
curl -X POST http://localhost:3000/tasks \
  -H "Content-Type: application/json" \
  -d '{
    "tasks": [
      {
        "id": "long-task",
        "name": "Long Running Task",
        "duration_ms": 30000,
        "failure_rate": 0.0
      }
    ]
  }'

# 2. Kill the server (Ctrl+C) while the task is running

# 3. Restart the server
npm start

# 4. Check that the task was recovered
curl http://localhost:3000/tasks/long-task
# Status should be 'waiting' (re-queued) or 'failed' (if max retries exceeded)
```

---

## Four Key Questions

### Question 1: How do you make sure your concurrency limit is never exceeded, even when many tasks are submitted at the same moment?

**Answer:**

The concurrency limit is enforced through three mechanisms working together:

1. **Atomic counter (`activeWorkers`):** The `TaskScheduler` maintains an `activeWorkers` counter that is incremented *before* a worker starts and decremented in the `finally` block when it completes. The `tick()` loop checks `this.activeWorkers < this.concurrencyLimit` before assigning any new task.

2. **Re-entrancy guard (`isTicking`):** The `tick()` method sets `this.isTicking = true` at the start and resets it in a `finally` block. If `tick()` is called while already running (e.g., from a retry timer), it returns immediately, preventing double-assignment of slots.

3. **Database-level guard:** The SQL UPDATE that transitions a task from `waiting` to `running` includes `AND status = 'waiting'` in the WHERE clause. If two scheduler instances somehow tried to claim the same task, only one would succeed (the `changes` count would be 0 for the second).

**What could go wrong if you got this wrong:**

- **Resource exhaustion:** If the limit is exceeded, you could spawn unbounded concurrent workers, overwhelming the database connection pool, memory, or downstream services.
- **Race condition:** Without the `isTicking` guard, two concurrent `tick()` calls could both read `activeWorkers = 1`, both see a slot available, and both assign a task — exceeding the limit.
- **Inconsistent state:** If the counter gets out of sync with actual running tasks (e.g., a worker crashes without decrementing), slots leak and the scheduler eventually stops processing new tasks.

**Code location:** `src/scheduler.js` lines 52-88 (`tick()` method) and lines 120-124 (`finally` block in `runWorker()`).

---

### Question 2: If the service is killed while tasks are running, what exactly happens when it starts again? Say clearly whether any work could be lost or accidentally run twice.

**Answer:**

When the service restarts, `recoverInterruptedTasks()` in `src/scheduler.js` (lines 17-44) handles the recovery:

1. It queries all tasks with `status = 'running'` — these are tasks that were actively executing when the process died.
2. For each interrupted task, it checks if the task has remaining retry attempts:
   - **If attempts < max_retries:** The task is reset to `waiting` and its `attempts` counter is incremented. It will be re-executed by the scheduler.
   - **If attempts >= max_retries:** The task is marked as `failed` and its dependents are cascade-blocked.

**Could work be lost?**

Yes, in a specific sense: if a task had *already completed its work* (e.g., the external API call succeeded) but the process died *before* the database was updated to `succeeded`, that work is lost. The task will be re-executed from scratch. This is the classic "at-least-once" execution semantic — there is no way to distinguish "never ran" from "ran but didn't record the result."

**Could work run twice?**

Yes. If a task was actively running when the crash occurred, it will be re-executed on recovery. If that task had side effects (e.g., charging a card, sending an email), those side effects will occur twice. This is inherent to any system that doesn't use idempotent operations or distributed transactions.

**Mitigation in this project:**

- The `attempts` counter is incremented during recovery, so a task that was interrupted multiple times will eventually hit `max_retries` and fail permanently rather than retrying forever.
- The simulated tasks in `runner.js` are idempotent (they just resolve/reject), so double execution is harmless in the demo. In production, you would need idempotent task handlers.

---

### Question 3: When several tasks are ready and one slot frees up, which one runs next? Explain your rule and give an example where it gives a poor result.

**Answer:**

**Rule:** First-In-First-Out (FIFO) by creation time. The SQL query in `tick()` uses `ORDER BY t.created_at ASC LIMIT 1` to select the next ready task.

**Why FIFO?** It's simple, predictable, and fair — tasks are processed in the order they were submitted. For the Tatkal booking scenario, this is reasonable because all tasks in a single booking workflow are submitted together and have similar priority.

**Example where FIFO gives a poor result:**

Imagine two workflows submitted nearly simultaneously:

- **Workflow A** (submitted at T=0): A single long-running task `generate-invoice` with `duration_ms = 30000` (30 seconds).
- **Workflow B** (submitted at T=100): Three quick tasks `send-email` (50ms), `update-db` (50ms), `notify-user` (50ms) with no dependencies.

With concurrency limit = 2 and FIFO ordering:

```
T=0:    generate-invoice starts (Worker 1)
T=100:  send-email starts (Worker 2)
T=150:  send-email completes, update-db starts (Worker 2)
T=200:  update-db completes, notify-user starts (Worker 2)
T=250:  notify-user completes
T=30000: generate-invoice completes
```

This works fine. But consider a different scenario:

- **Workflow A** (submitted at T=0): `task-A1` (30s) → `task-A2` (30s) → `task-A3` (30s)
- **Workflow B** (submitted at T=100): `task-B1` (50ms), `task-B2` (50ms) — independent, no dependencies

With FIFO:

```
T=0:    task-A1 starts (Worker 1)
T=100:  task-B1 starts (Worker 2)
T=150:  task-B1 completes, task-B2 starts (Worker 2)
T=200:  task-B2 completes
T=30000: task-A1 completes, task-A2 starts (Worker 1)
T=60000: task-A2 completes, task-A3 starts (Worker 1)
T=90000: task-A3 completes
```

Total time for Workflow B: 200ms (good)
Total time for Workflow A: 90000ms (expected)

But what if Workflow B was submitted *after* A1 completed?

- **Workflow A** (submitted at T=0): `task-A1` (30s) → `task-A2` (30s)
- **Workflow B** (submitted at T=31000): `task-B1` (50ms), `task-B2` (50ms)

With FIFO:

```
T=0:     task-A1 starts (Worker 1)
T=30000: task-A1 completes, task-A2 starts (Worker 1)
T=31000: task-B1 submitted, but Worker 2 is free → starts immediately
T=31050: task-B1 completes, task-B2 starts (Worker 2)
T=31100: task-B2 completes
T=60000: task-A2 completes
```

This is fine. The real problem arises when a *long-running task blocks the queue*:

- **Workflow A** (submitted at T=0): `task-A1` (30s), `task-A2` (30s) — A2 depends on A1
- **Workflow B** (submitted at T=100): `task-B1` (50ms) — depends on nothing

With concurrency = 1 and FIFO:

```
T=0:    task-A1 starts (Worker 1)
T=100:  task-B1 submitted, but Worker 1 is busy → waits
T=30000: task-A1 completes, task-A2 starts (Worker 1)
T=30000: task-B1 is still waiting (FIFO: A2 was created before B1)
T=60000: task-A2 completes, task-B1 starts
```

**Poor result:** Workflow B waited 60 seconds for a 50ms task because FIFO strictly prioritizes older tasks. A better approach might be **priority-based scheduling** (urgent tasks jump the queue) or **shortest-job-first** (quick tasks run first to clear the queue faster).

---

### Question 4: What is the one thing that must always be true for your service to be considered correct? Point to where in your code that is enforced.

**Answer:**

**The invariant:** A task can only transition to `running` if and only if **all of its dependencies have `status = 'succeeded'`**. No task should ever execute before its prerequisites are met, and no task should be permanently stuck in `waiting` if its dependencies have all succeeded.

**Where it is enforced:**

This is enforced in the SQL query in `src/scheduler.js` lines 58-68:

```sql
SELECT t.* FROM tasks t
WHERE t.status = 'waiting'
  AND NOT EXISTS (
    SELECT 1 FROM task_dependencies td
    JOIN tasks parent ON td.depends_on_task_id = parent.id
    WHERE td.task_id = t.id AND parent.status != 'succeeded'
  )
ORDER BY t.created_at ASC
LIMIT 1
```

The `NOT EXISTS` subquery ensures that a task is only selected for execution if there are **no dependencies** in a non-`succeeded` state. This means:

- If any dependency is `waiting`, `running`, `failed`, `blocked`, or `canceled`, the task will NOT be selected.
- Only when ALL dependencies are `succeeded` does the `NOT EXISTS` return true, making the task eligible.

**Additional enforcement points:**

1. **DAG validation (`src/dag.js`):** Prevents cycles at submission time, ensuring the dependency graph is always a DAG (no circular dependencies that would make the invariant impossible to satisfy).

2. **Cascading failure (`src/scheduler.js` lines 127-146):** When a task fails, all its dependents are immediately set to `blocked`, preventing them from ever running with a failed dependency.

3. **Database constraints (`src/db.js`):** The `CHECK` constraint on `status` ensures only valid states are possible, and foreign keys ensure dependency references are always valid.

**Why this is the critical invariant:**

If this invariant is violated — even once — the system produces incorrect results. For example, if `process-upi-payment` ran before `lock-berth-quota` succeeded, you might charge a user's card without actually reserving a berth. If `generate-pnr` ran before `process-upi-payment` succeeded, you might issue a PNR for an unpaid ticket. The entire correctness of the orchestration depends on this guarantee.

---

## Summary

The Tatkal Task Orchestrator demonstrates a production-grade DAG task orchestration system with:

- **DAG-based dependency resolution** with cycle detection
- **Configurable concurrency** with a worker pool
- **Exponential backoff retry** with jitter
- **Cascading failure blocking** for dependency management
- **Crash recovery** for interrupted tasks
- **Task cancellation** with cascade to dependents
- **Self-contained deployment** with embedded SQLite

All in ~330 lines of clean, modular JavaScript with no external infrastructure requirements.
