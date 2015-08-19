using System;
using System.Collections.Generic;
using System.Linq;
using StackExchange.Redis;

namespace Redis.Workflow.Common
{
    public class WorkflowManagement
    {
        public WorkflowManagement(ITaskHandler taskHandler, WorkflowHandler workflowHandler, Behaviours behaviours = Behaviours.Processor | Behaviours.Submitter)
        {
            _taskHandler = taskHandler;

            _workflowHandler = workflowHandler;

            var mux = ConnectionMultiplexer.Connect("localhost");

            _db = mux.GetDatabase();

            var sub = mux.GetSubscriber();

            sub.Subscribe("submittedTask", (c, v) =>
            {
                ProcessNextTask();
            });

            sub.Subscribe("workflowFailed", (c, v) =>
            {
                Console.WriteLine("Workflow failed: " + v);

                // this should be a structure we pass back through OnWorkflowFailed -- that is we should be passing back
                // some actual event args
                var taskIds = ((string)_db.HashGet("workflow-" + v, "tasks")).Split(',');
                foreach (var taskId in taskIds)
                {
                    var submitted = _db.HashGet("task-" + taskId, "submitted");
                    var running = _db.HashGet("task-" + taskId, "running");
                    var complete = _db.HashGet("task-" + taskId, "complete");
                    var failed = _db.HashGet("task-" + taskId, "failed");
                    var parents = _db.HashGet("task-" + taskId, "parents");
                    var children = _db.HashGet("task-" + taskId, "children");

                    Console.WriteLine("Task " + taskId + " s: " + submitted + " r: " + running + " c: " + complete + " pa: " + parents + " ch: " + children);
                }

                _workflowHandler.OnWorkflowFailed(v);
            });


            sub.Subscribe("workflowComplete", (c, v) =>
            {
                Console.WriteLine("Workflow complete: " + v);

                // this should be a structure we pass back through OnWorkflowComplete -- that is we should be passing back
                // some actual event args
                var taskIds = ((string)_db.HashGet("workflow-" + v, "tasks")).Split(',');
                foreach (var taskId in taskIds)
                {
                    var submitted = _db.HashGet("task-" + taskId, "submitted");
                    var running = _db.HashGet("task-" + taskId, "running");
                    var complete = _db.HashGet("task-" + taskId, "complete");
                    var failed = _db.HashGet("task-" + taskId, "failed");
                    var parents = _db.HashGet("task-" + taskId, "parents");
                    var children = _db.HashGet("task-" + taskId, "children");

                    Console.WriteLine("Task " + taskId + " s: " + submitted + " r: " + running + " c: " + complete + " pa: " + parents + " ch: " + children);
                }

                _workflowHandler.OnWorkflowComplete(v);
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

            var rh = new SimpleResultHandler(task, CompleteTask, FailTask);

            _taskHandler.Run(payload, rh);

            return task;
        }

        private string PopTask()
        {
            return Lua.PopTask(_db, Timestamp());
        }

        private static string Timestamp()
        {
            return DateTime.Now.ToString("dd/MM/yy HH:mm:ss.fff");
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
                    Lua.PushTask(_db, ids[task.Name].ToString(), Timestamp());
                }
            }

            _db.SetAdd("workflows", workflowId);

            Console.WriteLine("pushed: " + workflow.Name + " as workflow " + workflowId);

            return workflowId.Value;
        }

        private void PushTask(string task)
        {
            Lua.PushTask(_db, task, Timestamp());

            Console.WriteLine("pushed: " + task);
        }

        private void FailTask(string task)
        {
            // if this class is told that a task has failed, we take it to mean that the workflow
            // has failed
            Lua.FailTask(_db, task, Timestamp());

            Console.WriteLine("failed: " + task);
        }

        private void CompleteTask(string task)
        {
            Lua.CompleteTask(_db, task, Timestamp());

            Console.WriteLine("completed: " + task);
        }

        private readonly ITaskHandler _taskHandler;

        private readonly WorkflowHandler _workflowHandler;

        private readonly IDatabase _db;

        private readonly object _turnstile = new object();
    }
}
