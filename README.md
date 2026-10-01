# Tatkal Ticket Booking Task Orchestrator

A lightweight, resilient DAG task runner with concurrency limits, exponential retry backoff, cascading failure blocking, and crash recovery.

## What It Does

Simulates the high-demand **IRCTC Tatkal Ticket Booking** workflow as a Directed Acyclic Graph (DAG) of tasks:

- `verify-passenger` → Validates passenger credentials
- `lock-berth-quota` → Competes for PRS quota inventory
- `validate-aadhaar-kyc` → Runs in parallel with berth reservation
- `process-upi-payment` → Gateway integration (retries under load)
- `generate-pnr` → Final issuance (blocked if payment or quota fails)

## Prerequisites

- Node.js v18 or higher
- No external databases or Docker required

## Running

```bash
# 1. Install dependencies
npm install

# 2. Run the automated test suite
npm test

# 3. Start the server
npm start
```

The API is available at `http://localhost:3000`.

## API

| Method | Path | Description |
|--------|------|-------------|
| `POST` | `/tasks` | Submit a batch of tasks |
| `GET` | `/tasks/:id` | Get task status |
| `POST` | `/tasks/:id/cancel` | Cancel a task |
| `GET` | `/stats` | Get concurrency stats |

## Documentation

- [DESIGN.md](DESIGN.md) — Architecture, algorithms, and design decisions
- [TRADEOFFS.md](TRADEOFFS.md) — Key tradeoffs and alternatives considered
