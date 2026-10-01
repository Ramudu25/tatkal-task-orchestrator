const { initDB } = require('./db');
const { TaskScheduler } = require('./scheduler');
const { createApp } = require('./app');

const PORT = process.env.PORT || 3000;
const CONCURRENCY_LIMIT = process.env.CONCURRENCY_LIMIT ? parseInt(process.env.CONCURRENCY_LIMIT, 10) : 2;

const db = initDB();
const scheduler = new TaskScheduler(db, CONCURRENCY_LIMIT);

// Run crash recovery on startup
scheduler.recoverInterruptedTasks();
scheduler.tick();

const app = createApp(db, scheduler);

if (require.main === module) {
  app.listen(PORT, () => {
    console.log(`Tatkal Orchestrator API listening on http://localhost:${PORT}`);
    console.log(`Concurrency Limit: ${CONCURRENCY_LIMIT}`);
  });
}

module.exports = { app, db, scheduler };