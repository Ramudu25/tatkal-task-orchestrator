const assert = require('assert');
const fs = require('fs');
const path = require('path');
const { initDB } = require('./src/db');
const { TaskScheduler } = require('./src/scheduler');
const { validateBatchDAG } = require('./src/dag');

const TEST_DB = path.join(__dirname, 'test.db');

function cleanup() {
  for (const ext of ['', '-journal', '-wal', '-shm']) {
    if (fs.existsSync(TEST_DB + ext)) fs.unlinkSync(TEST_DB + ext);
  }
}

async function sleep(ms) {
  return new Promise(resolve => setTimeout(resolve, ms));
}

async function runTests() {
  console.log('🧪 Starting Tatkal Orchestrator Test Suite...\n');
  cleanup();

  // Test 1: Cycle Detection
  console.log('Test 1: Rejecting Circular Dependencies...');
  assert.throws(() => {
    validateBatchDAG([
      { id: 'A', name: 'A', dependencies: ['B'] },
      { id: 'B', name: 'B', dependencies: ['A'] }
    ]);
  }, /Circular dependency detected/);
  console.log('  ✔ Passed: Circular dependency caught at submission.');

  // Test 2: Concurrency Enforcement
  console.log('\nTest 2: Enforcing Max Concurrency Limit (N = 2)...');
  const db = initDB(TEST_DB);
  const scheduler = new TaskScheduler(db, 2, 50);

  // Insert 4 independent tasks
  const now = Date.now();
  for (let i = 1; i <= 4; i++) {
    db.prepare(`
      INSERT INTO tasks (id, name, status, duration_ms, failure_rate, timeout_ms, max_retries, created_at, updated_at)
      VALUES (?, ?, 'waiting', 200, 0.0, 5000, 3, ?, ?)
    `).run(`task-${i}`, `Parallel Task ${i}`, now + i, now);
  }

  scheduler.tick();
  const stats = scheduler.getStats();
  assert.strictEqual(stats.running, 2, `Expected 2 running tasks, got ${stats.running}`);
  assert.strictEqual(stats.waiting, 2, `Expected 2 waiting tasks, got ${stats.waiting}`);
  console.log('  ✔ Passed: Active workers strictly constrained to N=2.');

  // Wait for all 4 to complete
  await sleep(600);
  const completedCount = db.prepare("SELECT COUNT(*) AS count FROM tasks WHERE status = 'succeeded'").get().count;
  assert.strictEqual(completedCount, 4, `Expected 4 succeeded tasks, got ${completedCount}`);
  console.log('  ✔ Passed: All parallel tasks cleared without exceeding concurrency limit.');

  // Test 3: Retries and Cascading Blocked Status
  console.log('\nTest 3: Retries & Cascading Blocked State on Failure...');
  // A fails (100% failure rate, max 2 retries), B depends on A
  db.prepare(`
    INSERT INTO tasks (id, name, status, duration_ms, failure_rate, timeout_ms, max_retries, created_at, updated_at)
    VALUES ('lock-berth-fail', 'Lock Berth', 'waiting', 50, 1.0, 5000, 2, ?, ?)
  `).run(now, now);

  db.prepare(`
    INSERT INTO tasks (id, name, status, duration_ms, failure_rate, timeout_ms, max_retries, created_at, updated_at)
    VALUES ('generate-pnr-blocked', 'Generate PNR', 'waiting', 50, 0.0, 5000, 3, ?, ?)
  `).run(now + 1, now);

  db.prepare("INSERT INTO task_dependencies (task_id, depends_on_task_id) VALUES ('generate-pnr-blocked', 'lock-berth-fail')").run();

  scheduler.tick();
  // Wait for attempts + backoff retry
  await sleep(400);

  const parent = db.prepare("SELECT status, attempts FROM tasks WHERE id = 'lock-berth-fail'").get();
  const child = db.prepare("SELECT status, error_message FROM tasks WHERE id = 'generate-pnr-blocked'").get();

  assert.strictEqual(parent.status, 'failed', `Expected parent to be failed, got ${parent.status}`);
  assert.strictEqual(parent.attempts, 2, `Expected 2 attempts, got ${parent.attempts}`);
  assert.strictEqual(child.status, 'blocked', `Expected child to be blocked, got ${child.status}`);
  console.log('  ✔ Passed: Parent retried 2 times, failed permanently, and child cascaded to blocked.');

  // Test 4: Cancellation Cascades
  console.log('\nTest 4: Cancellation Cascading...');
  db.prepare(`
    INSERT INTO tasks (id, name, status, duration_ms, failure_rate, timeout_ms, max_retries, created_at, updated_at)
    VALUES ('pay-cancel', 'Pay UPI', 'waiting', 100, 0.0, 5000, 3, ?, ?)
  `).run(now, now);

  db.prepare(`
    INSERT INTO tasks (id, name, status, duration_ms, failure_rate, timeout_ms, max_retries, created_at, updated_at)
    VALUES ('pnr-cancel', 'Final PNR', 'waiting', 100, 0.0, 5000, 3, ?, ?)
  `).run(now + 1, now);

  db.prepare("INSERT INTO task_dependencies (task_id, depends_on_task_id) VALUES ('pnr-cancel', 'pay-cancel')").run();

  scheduler.cancelTask('pay-cancel');
  const canceledTask = db.prepare("SELECT status FROM tasks WHERE id = 'pay-cancel'").get();
  const blockedTask = db.prepare("SELECT status, error_message FROM tasks WHERE id = 'pnr-cancel'").get();

  assert.strictEqual(canceledTask.status, 'canceled');
  assert.strictEqual(blockedTask.status, 'blocked');
  console.log('  ✔ Passed: Canceled task stopped and child marked as blocked.');

  cleanup();
  console.log('\n🎉 ALL TESTS PASSED SUCCESSFULLY!\n');
}

runTests().catch(err => {
  console.error('❌ Test failed:', err);
  process.exit(1);
});