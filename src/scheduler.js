const { executeTask } = require('./runner');

class TaskScheduler {
  constructor(db, concurrencyLimit = 2, baseBackoffMs = 150) {
    this.db = db;
    this.concurrencyLimit = concurrencyLimit;
    this.baseBackoffMs = baseBackoffMs;
    this.activeWorkers = 0;
    this.isTicking = false;
  }

  log(emoji, message) {
    const time = new Date().toISOString().substring(11, 19);
    console.log(`[${time}] ${emoji} ${message}`);
  }

  recoverInterruptedTasks() {
    const now = Date.now();
    const runningTasks = this.db.prepare("SELECT * FROM tasks WHERE status = 'running'").all();
    
    if (runningTasks.length > 0) {
      this.log('🔄', `Crash Recovery: Found ${runningTasks.length} interrupted task(s)`);
    }

    for (const t of runningTasks) {
      const nextAttempt = t.attempts + 1;
      if (nextAttempt >= t.max_retries) {
        this.db.prepare(`
          UPDATE tasks 
          SET status = 'failed', attempts = ?, error_message = 'Interrupted by server restart (exceeded max retries)', updated_at = ?
          WHERE id = ?
        `).run(nextAttempt, now, t.id);
        this.log('💥', `Task '${t.id}' marked failed on reboot (exceeded max retries)`);
        this.cascadeBlockDependents(t.id, 'Upstream dependency interrupted by crash and failed permanently');
      } else {
        this.db.prepare(`
          UPDATE tasks 
          SET status = 'waiting', attempts = ?, error_message = 'Interrupted by server restart; requeued', updated_at = ?
          WHERE id = ?
        `).run(nextAttempt, now, t.id);
        this.log('🔁', `Task '${t.id}' recovered and requeued (Attempt ${nextAttempt}/${t.max_retries})`);
      }
    }
  }

  getStats() {
    const running = this.db.prepare("SELECT COUNT(*) AS count FROM tasks WHERE status = 'running'").get().count;
    const waiting = this.db.prepare("SELECT COUNT(*) AS count FROM tasks WHERE status = 'waiting'").get().count;
    return { running, waiting, concurrencyLimit: this.concurrencyLimit };
  }

  tick() {
    if (this.isTicking) return;
    this.isTicking = true;

    try {
      while (this.activeWorkers < this.concurrencyLimit) {
        const readyTask = this.db.prepare(`
          SELECT t.* FROM tasks t
          WHERE t.status = 'waiting'
            AND NOT EXISTS (
              SELECT 1 FROM task_dependencies td
              JOIN tasks parent ON td.depends_on_task_id = parent.id
              WHERE td.task_id = t.id AND parent.status != 'succeeded'
            )
          ORDER BY t.created_at ASC
          LIMIT 1
        `).get();

        if (!readyTask) break;

        const now = Date.now();
        const updated = this.db.prepare(`
          UPDATE tasks 
          SET status = 'running', attempts = attempts + 1, updated_at = ?
          WHERE id = ? AND status = 'waiting'
        `).run(now, readyTask.id);

        if (updated.changes > 0) {
          this.activeWorkers++;
          this.log('🚀', `Starting '${readyTask.id}' (${readyTask.name}) [Active slots: ${this.activeWorkers}/${this.concurrencyLimit}]`);
          this.runWorker(readyTask);
        }
      }
    } finally {
      this.isTicking = false;
    }
  }

  async runWorker(task) {
    try {
      await executeTask(task);
      const now = Date.now();
      this.db.prepare("UPDATE tasks SET status = 'succeeded', updated_at = ? WHERE id = ?").run(now, task.id);
      this.log('✅', `Completed '${task.id}' (${task.name}) successfully`);
    } catch (err) {
      const now = Date.now();
      const current = this.db.prepare("SELECT attempts, max_retries FROM tasks WHERE id = ?").get(task.id);
      
      if (current.attempts < current.max_retries) {
        const delay = this.baseBackoffMs * Math.pow(2, current.attempts - 1);
        this.db.prepare(`
          UPDATE tasks 
          SET status = 'waiting', error_message = ?, updated_at = ?
          WHERE id = ?
        `).run(err.message, now, task.id);

        this.log('⚠️', `Task '${task.id}' failed attempt ${current.attempts}/${current.max_retries}. Retrying in ${delay}ms...`);
        setTimeout(() => this.tick(), delay);
      } else {
        this.db.prepare(`
          UPDATE tasks 
          SET status = 'failed', error_message = ?, updated_at = ?
          WHERE id = ?
        `).run(err.message, now, task.id);

        this.log('❌', `Task '${task.id}' failed permanently (${err.message})`);
        this.cascadeBlockDependents(task.id, `Upstream dependency '${task.id}' failed permanently`);
      }
    } finally {
      this.activeWorkers--;
      this.log('🏁', `Worker freed. [Active slots: ${this.activeWorkers}/${this.concurrencyLimit}]`);
      this.tick();
    }
  }

  cascadeBlockDependents(failedTaskId, reason) {
    const now = Date.now();
    const dependents = this.db.prepare(`
      SELECT task_id FROM task_dependencies WHERE depends_on_task_id = ?
    `).all(failedTaskId);

    for (const row of dependents) {
      const target = this.db.prepare("SELECT status FROM tasks WHERE id = ?").get(row.task_id);
      if (target && (target.status === 'waiting' || target.status === 'running')) {
        this.db.prepare(`
          UPDATE tasks 
          SET status = 'blocked', error_message = ?, updated_at = ?
          WHERE id = ?
        `).run(reason, now, row.task_id);
        
        this.log('⛔', `Blocked '${row.task_id}' -> Reason: ${reason}`);
        this.cascadeBlockDependents(row.task_id, reason);
      }
    }
  }

  cancelTask(taskId) {
    const now = Date.now();
    const task = this.db.prepare("SELECT * FROM tasks WHERE id = ?").get(taskId);
    if (!task) return null;

    if (task.status === 'succeeded' || task.status === 'failed' || task.status === 'blocked') {
      return { ok: false, message: `Cannot cancel task in final state: ${task.status}` };
    }

    this.db.prepare("UPDATE tasks SET status = 'canceled', updated_at = ? WHERE id = ?").run(now, taskId);
    this.log('🛑', `Task '${taskId}' explicitly canceled`);
    this.cascadeBlockDependents(taskId, `Upstream task '${taskId}' was canceled`);
    this.tick();
    return { ok: true, taskId, status: 'canceled' };
  }
}

module.exports = { TaskScheduler };