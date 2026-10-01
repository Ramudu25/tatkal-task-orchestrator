# Tatkal Ticket Booking Task Orchestrator

A lightweight, resilient DAG task runner with concurrency limits, exponential retry backoff, cascading failure blocking, and crash recovery.

## Real-World Scenario
Simulates high-demand **IRCTC Tatkal Ticket Booking**:
- `verify-passenger`: Validates passenger credentials.
- `lock-berth-quota`: Competes for PRS quota inventory.
- `validate-aadhaar-kyc`: Runs in parallel with berth reservation.
- `process-upi-payment`: Gateway integration (demonstrating retries under load).
- `generate-pnr`: Final issuance (blocked if payment or quota fails).

## Prerequisites
- Node.js v18 or higher (No external databases or Docker required).

## Installation & Running

```bash
# 1. Install dependencies
npm install

# 2. Run the automated test suite
npm test

# 3. Start the server
npm start