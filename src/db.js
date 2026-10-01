const Database = require('better-sqlite3');
const path = require('path');

function initDB(dbPath = path.join(__dirname, '..', 'orchestrator.db')) {
  const db = new Database(dbPath);

  // WAL mode for high concurrency reads & crash safety
  db.pragma('journal_mode = WAL');
  db.pragma('foreign_keys = ON');

  db.exec(`
    CREATE TABLE IF NOT EXISTS tasks (
      id TEXT PRIMARY KEY,
      name TEXT NOT NULL,
      status TEXT NOT NULL CHECK(status IN ('waiting', 'running', 'succeeded', 'failed', 'blocked', 'canceled')),
      duration_ms INTEGER NOT NULL DEFAULT 300,
      failure_rate REAL NOT NULL DEFAULT 0.0,
      timeout_ms INTEGER NOT NULL DEFAULT 5000,
      max_retries INTEGER NOT NULL DEFAULT 3,
      attempts INTEGER NOT NULL DEFAULT 0,
      error_message TEXT,
      created_at INTEGER NOT NULL,
      updated_at INTEGER NOT NULL
    );

    CREATE TABLE IF NOT EXISTS task_dependencies (
      task_id TEXT NOT NULL,
      depends_on_task_id TEXT NOT NULL,
      PRIMARY KEY (task_id, depends_on_task_id),
      FOREIGN KEY (task_id) REFERENCES tasks(id) ON DELETE CASCADE,
      FOREIGN KEY (depends_on_task_id) REFERENCES tasks(id) ON DELETE CASCADE
    );

    CREATE INDEX IF NOT EXISTS idx_tasks_status ON tasks(status);
  `);

  return db;
}

module.exports = { initDB };