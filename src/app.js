const express = require('express');
const { validateBatchDAG } = require('./dag');

function createApp(db, scheduler) {
  const app = express();
  app.use(express.json());

  // 1. Submit Batch of Tasks
  app.post('/tasks', (req, res) => {
    const { tasks } = req.body;
    if (!Array.isArray(tasks) || tasks.length === 0) {
      return res.status(400).json({ error: "Request body must contain a non-empty 'tasks' array." });
    }

    try {
      const existing = new Set(db.prepare("SELECT id FROM tasks").all().map(r => r.id));
      validateBatchDAG(tasks, existing);

      const insertTask = db.prepare(`
        INSERT INTO tasks (id, name, status, duration_ms, failure_rate, timeout_ms, max_retries, created_at, updated_at)
        VALUES (@id, @name, 'waiting', @duration_ms, @failure_rate, @timeout_ms, @max_retries, @now, @now)
      `);

      const insertDep = db.prepare(`
        INSERT INTO task_dependencies (task_id, depends_on_task_id) VALUES (?, ?)
      `);

      const transaction = db.transaction((taskList) => {
        const now = Date.now();
        for (const t of taskList) {
          insertTask.run({
            id: t.id,
            name: t.name,
            duration_ms: t.duration_ms ?? 300,
            failure_rate: t.failure_rate ?? 0.0,
            timeout_ms: t.timeout_ms ?? 5000,
            max_retries: t.max_retries ?? 3,
            now
          });

          for (const depId of (t.dependencies || [])) {
            insertDep.run(t.id, depId);
          }
        }
      });

      transaction(tasks);
      scheduler.tick();

      return res.status(201).json({
        message: 'Tasks submitted successfully',
        task_ids: tasks.map(t => t.id)
      });
    } catch (err) {
      return res.status(400).json({ error: err.message });
    }
  });

  // 2. Task Status
  app.get('/tasks/:id', (req, res) => {
    const task = db.prepare("SELECT * FROM tasks WHERE id = ?").get(req.params.id);
    if (!task) return res.status(404).json({ error: 'Task not found' });

    const deps = db.prepare("SELECT depends_on_task_id FROM task_dependencies WHERE task_id = ?")
      .all(req.params.id)
      .map(r => r.depends_on_task_id);

    return res.json({ ...task, dependencies: deps });
  });

  // 3. Cancel Task
  app.post('/tasks/:id/cancel', (req, res) => {
    const result = scheduler.cancelTask(req.params.id);
    if (!result) return res.status(404).json({ error: 'Task not found' });
    if (!result.ok) return res.status(400).json(result);
    return res.json(result);
  });

  // 4. Concurrency & Queue Stats
  app.get('/stats', (req, res) => {
    return res.json(scheduler.getStats());
  });

  return app;
}

module.exports = { createApp };