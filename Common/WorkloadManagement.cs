using System;
using System.Collections.Generic;
using System.Linq;
using StackExchange.Redis;

namespace Redis.Workflow.Common
{
    public class WorkflowManagement
    {
        public WorkflowManagement(ITaskHandler taskHandler, Behaviours behaviours = Behaviours.Processor | Behaviours.Submitter)
        {
            _taskHandler = taskHandler;

            var mux = ConnectionMultiplexer.Connect("localhost");

            _db = mux.GetDatabase();

            var sub = mux.GetSubscriber();

            sub.Subscribe("submittedTask", (c, v) =>
            {
                ProcessNextTask();
            });

            sub.Subscribe("workflowComplete", (c, v) =>
            {
                Console.WriteLine("Workflow complete: " + v);

                var taskIds = ((string)_db.HashGet("workflow-" + v, "tasks")).Split(',');
                foreach (var taskId in taskIds)
                {
                    var submitted = _db.HashGet("task-" + taskId, "submitted");
                    var running = _db.HashGet("task-" + taskId, "running");
                    var complete = _db.HashGet("task-" + taskId, "complete");
                    var parents = _db.HashGet("task-" + taskId, "parents");
                    var children = _db.HashGet("task-" + taskId, "children");

                    Console.WriteLine("Task " + taskId + " s: " + submitted + " r: " + running + " c: " + complete + " pa: " + parents + " ch: " + children);
                }

            });
        }

        public void ClearBacklog()
        {
            Console.WriteLine("Clearing backlog");
            while (ProcessNextTask() != null) { }
        }

        private string ProcessNextTask()
        {
            var task = PopTask();

            if (task == null) return null;

            var payload = _db.HashGet("task-" + task, "payload");

            var rh = new SimpleResultHandler(task, CompleteTask);

            _taskHandler.Run(payload, rh);

            return task;
        }

        private string PopTask()
        {
            return Lua.PopTask(_db, DateTime.Now.ToString("dd/MM/yy HH:mm:ss.fff"));
        }

        /// <summary>
        /// Pushes an executable workflow into the system. Task Names are assumed to be unique; there's
        /// no checking of integrity and consistency of child-parent relationships (so no guarantee of transitive
        /// closure and so on). Creates a new instance of this workflow which is available for execution
        /// immediately.
        /// </summary>
        /// <param name="workflow"></param>
        /// <param name="tasks"></param>
        /// <returns>The handle used to identify this workflow instance</returns>
        public long PushWorkflow(Workflow workflow, IEnumerable<Task> tasks)
        {
            long? workflowId = Lua.GetWorkflowId(_db);

            if (!workflowId.HasValue || _db.KeyExists("workflow-" + workflowId)) throw new InvalidOperationException("State may be corrupt; workflow id " + workflowId + " already allocated or is null");
            
            var ids = new Dictionary<string, long>();
            foreach (var task in tasks)
            {
                long? taskId = Lua.GetTaskId(_db);

                if (!taskId.HasValue || _db.KeyExists("task-" + taskId)) throw new InvalidOperationException("State may be corrupt; task id " + workflowId + " already allocated or is null");

                ids[task.Name] = taskId.Value;
            }

            foreach (var task in tasks)
            {
                _db.HashSet("task-" + ids[task.Name],
                    new[] { new HashEntry("name", task.Name),
                        new HashEntry("parents", string.Join(",", task.Parents.Select(p => ids[p]))),
                        new HashEntry("children", string.Join(",", task.Children.Select(c => ids[c]))),
                        new HashEntry("workflow", workflowId),
                        new HashEntry("payload", task.Payload)
                    }
                    );

                foreach (var parent in task.Parents)
                {
                    _db.SetAdd("parents-" + ids[task.Name], ids[parent]);
                }

                foreach (var child in task.Children)
                {
                    _db.ListLeftPush("children-" + ids[task.Name], ids[child]);
                }

                _db.SetAdd("tasks", ids[task.Name]);
            }

            _db.HashSet("workflow-" + workflowId, new[] { new HashEntry("name", workflow.Name), new HashEntry("tasks", string.Join(",", ids.Values)) });
            _db.StringSet("workflow-remaining-" + workflowId, tasks.Count());

            // and finally do this, as this'll actually set thing in motion
            foreach (var task in tasks)
            {
                if (task.Parents.Count().Equals(0))
                {
                    Lua.PushTask(_db, ids[task.Name].ToString(), DateTime.Now.ToString("dd/MM/yyyy HH:mm:ss.fff"));
                }
            }

            _db.SetAdd("workflows", workflowId);

            Console.WriteLine("pushed: " + workflow.Name + " as workflow " + workflowId);

            return workflowId.Value;
        }

        private void PushTask(string task)
        {
            Lua.PushTask(_db, task, DateTime.Now.ToString("dd/MM/yyyy HH:mm:ss.fff"));

            Console.WriteLine("pushed: " + task);
        }

        private void CompleteTask(string task)
        {
            Lua.CompleteTask(_db, task, DateTime.Now.ToString("dd/MM/yyyy HH:mm:ss.fff"));

            Console.WriteLine("completed: " + task);
        }

        private readonly ITaskHandler _taskHandler;

        private readonly IDatabase _db;

        private readonly object _turnstile = new object();
    }
}
