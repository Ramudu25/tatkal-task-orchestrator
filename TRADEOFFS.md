# Tradeoffs

## 1. Single-Process In-Memory Scheduler vs. External Message Queue

**Chosen approach:** Single-process scheduler with an in-memory `activeWorkers` counter and SQLite for persistence.

**Alternative:** Use an external message queue (RabbitMQ, Kafka, Redis Streams) with a consumer group and prefetch limit for concurrency control.

**Why not the alternative:** The goal is a self-contained demo that runs with zero infrastructure. An external queue would require Docker, additional configuration, and operational knowledge — defeating the "clone and run" requirement. The in-memory counter is sufficient for a single-process demo and keeps the codebase under 350 lines. In a multi-instance production deployment, you'd need a distributed lock (e.g., Redis Redlock) or a queue-based approach, but that's out of scope for this project.

---

## 2. FIFO Task Ordering vs. Priority-Based Scheduling

**Chosen approach:** First-In-First-Out (FIFO) by `created_at` timestamp. Ready tasks are selected with `ORDER BY t.created_at ASC LIMIT 1`.

**Alternative:** Priority-based scheduling where each task has a `priority` field (1-10) and the scheduler picks the highest-priority ready task first. Or shortest-job-first (SJF) to minimize average wait time.

**Why not the alternative:** FIFO is simple, predictable, and fair. For the Tatkal booking scenario, all tasks in a single workflow are submitted together and have similar priority. Priority scheduling adds complexity (priority inversion, starvation of low-priority tasks) without clear benefit for the demo. The DESIGN.md documents a scenario where FIFO performs poorly (a long-running task blocking a quick urgent task), but this is an acceptable tradeoff for simplicity. If needed, adding a `priority` column and changing the `ORDER BY` clause would be a minimal change.

---

## 3. At-Least-Once Execution vs. Exactly-Once Execution

**Chosen approach:** At-least-once execution. On crash recovery, interrupted tasks are re-executed. If a task completed but the DB update was lost, the task runs twice.

**Alternative:** Exactly-once execution using idempotent task handlers with deduplication (e.g., a `processed_task_ids` table or external idempotency store). Or transactional outbox pattern where task execution and status update happen in the same database transaction.

**Why not the alternative:** Exactly-once semantics require either idempotent task handlers (which depend on the specific task implementation) or distributed transactions (2PC) across the task executor and the database. For a simulated demo where tasks just resolve/reject with no real side effects, at-least-once is sufficient and much simpler. The DESIGN.md clearly documents this tradeoff and notes that production systems would need idempotent handlers.

---

## 4. SQLite (Embedded) vs. PostgreSQL (External)

**Chosen approach:** SQLite via better-sqlite3 with WAL mode. Zero configuration, zero infrastructure, file-based persistence.

**Alternative:** PostgreSQL with `SELECT ... FOR UPDATE SKIP LOCKED` for task claiming, or a job queue extension like pgmq.

**Why not the alternative:** SQLite is embedded — no separate process, no connection pooling, no authentication. For a demo that must "run from a clean clone," this is critical. WAL mode provides better concurrency than default journal mode, and better-sqlite3 is synchronous (no async overhead), which is fast enough for this scale. PostgreSQL would provide better concurrency control and monitoring, but requires a running server, connection management, and schema migration tooling. The tradeoff is operational simplicity vs. production-grade reliability. For a demo, simplicity wins.
