/**
 * Simulates task execution with latency, failure probability, and operational timeout.
 */
function executeTask(task) {
  return new Promise((resolve, reject) => {
    let completed = false;

    const timer = setTimeout(() => {
      if (!completed) {
        completed = true;
        if (Math.random() < task.failure_rate) {
          reject(new Error(`Simulated transient network/service failure in ${task.name}`));
        } else {
          resolve({ ok: true, duration: task.duration_ms });
        }
      }
    }, task.duration_ms);

    // Added operational improvement: Task Execution Timeout
    const timeout = setTimeout(() => {
      if (!completed) {
        completed = true;
        clearTimeout(timer);
        reject(new Error(`Task exceeded timeout limit of ${task.timeout_ms}ms`));
      }
    }, task.timeout_ms);
  });
}

module.exports = { executeTask };