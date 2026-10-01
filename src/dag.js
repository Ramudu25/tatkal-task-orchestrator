/**
 * Validates dependencies using Kahn's Algorithm (Topological Sort).
 * Rejects circular dependencies and missing tasks upfront.
 */
function validateBatchDAG(newTasks, existingTaskIds = new Set()) {
  const graph = new Map(); // taskId -> list of dependents (edges out)
  const inDegree = new Map(); // taskId -> count of unmet dependencies
  const taskIds = new Set();

  for (const t of newTasks) {
    if (!t.id || !t.name) {
      throw new Error(`Task is missing 'id' or 'name'`);
    }
    if (taskIds.has(t.id) || existingTaskIds.has(t.id)) {
      throw new Error(`Duplicate task ID detected: '${t.id}'`);
    }
    taskIds.add(t.id);
    graph.set(t.id, []);
    inDegree.set(t.id, 0);
  }

  for (const t of newTasks) {
    const deps = t.dependencies || [];
    for (const depId of deps) {
      if (depId === t.id) {
        throw new Error(`Circular dependency detected: Task '${t.id}' cannot depend on itself`);
      }
      if (!taskIds.has(depId) && !existingTaskIds.has(depId)) {
        throw new Error(`Dependency '${depId}' required by '${t.id}' does not exist`);
      }

      if (taskIds.has(depId)) {
        graph.get(depId).push(t.id);
        inDegree.set(t.id, inDegree.get(t.id) + 1);
      }
    }
  }

  // Kahn's Algorithm: process nodes with inDegree 0
  const queue = [];
  for (const [id, deg] of inDegree.entries()) {
    if (deg === 0) queue.push(id);
  }

  let visitedCount = 0;
  while (queue.length > 0) {
    const curr = queue.shift();
    visitedCount++;

    for (const neighbor of graph.get(curr)) {
      inDegree.set(neighbor, inDegree.get(neighbor) - 1);
      if (inDegree.get(neighbor) === 0) {
        queue.push(neighbor);
      }
    }
  }

  if (visitedCount !== newTasks.length) {
    throw new Error('Circular dependency detected within task submission batch');
  }

  return true;
}

module.exports = { validateBatchDAG };