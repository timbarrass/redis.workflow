using System;
using System.Collections.Generic;
using System.Linq;
using StackExchange.Redis;

namespace Redis.Workflow.Common
{
    public class TaskDetails
    {
        public string name { get; set; }
        public string running { get; set; }
        public string workflow { get; set; }
        public string payload { get; set; }
        public string id { get; set; }
        public string lastKnownResponsible { get; set; }
        public string submitted { get; set; }
        public string complete { get; set; }
    }

    public class WorkflowDetails
    {
        public string Id { get; set; }
        public List<TaskDetails> Tasks { get; set; }
    }

    public class WorkflowManagement : IDisposable
    {
        public void Dispose()
        {
            // Interesting behaviour in tests. If we don't Unsubscribe all at end of test then
            // we get odd varying fails, which I suspect are to do with pub/sub messages crossing
            // test boundaries.
            _sub.UnsubscribeAll();
        }

        public WorkflowManagement(ConnectionMultiplexer mux, ITaskHandler taskHandler, WorkflowHandler workflowHandler, string identifier, Behaviours behaviours = Behaviours.All)
        {
            _taskHandler = taskHandler;

            _workflowHandler = workflowHandler;

            _db = mux.GetDatabase();

            _sub = mux.GetSubscriber();

            _sub.Subscribe("submittedTask", (c, v) =>
            {
                ProcessNextTask();
            });

            _sub.Subscribe("workflowFailed", (c, v) =>
            {
                ProcessNextFailedWorkflow();
            });


            _sub.Subscribe("workflowComplete", (c, v) =>
            {
                ProcessNextCompleteWorkflow();
            });

            _lua = new Lua();
            _lua.LoadScripts(_db, mux.GetServer("localhost:6379"));

            _identifier = identifier;

            if (behaviours.HasFlag(Behaviours.AutoRestart))
            {
                var resubmittedTasks = ResubmitTasks();

                foreach (var item in resubmittedTasks)
                {
                    Console.WriteLine("Resubmitted {0}", item);
                }
            }
        }

        private string ProcessNextFailedWorkflow()
        {
            var workflow = PopFailedWorkflow();

            if (workflow == null) return null;

            Console.WriteLine("Workflow failed: " + workflow);

            // TODO: have this populate an eventargs structure that can be passed back to the subscriber
            ProcessWorkflow(workflow);

            _workflowHandler.OnWorkflowFailed(workflow);

            return workflow;
        }

        private string PopFailedWorkflow()
        {
            return _lua.PopFailedWorkflow(_db);
        }

        private string ProcessNextCompleteWorkflow()
        {
            var workflow = PopCompleteWorkflow();

            if (workflow == null) return null;

            Console.WriteLine("Workflow complete: " + workflow);

            // TODO: have this populate an eventargs structure that can be passed back to the subscriber
            ProcessWorkflow(workflow);

            _workflowHandler.OnWorkflowComplete(workflow);

            return workflow;
        }

        private string PopCompleteWorkflow()
        {
            return _lua.PopCompleteWorkflow(_db);
        }

        public WorkflowDetails FetchWorkflowInformation(string workflowId)
        {
            var json = _lua.FetchWorkflowInformation(_db, workflowId);

            var deserialized = Newtonsoft.Json.JsonConvert.DeserializeObject<WorkflowDetails>(json);

            return deserialized;
        }

        private void ProcessWorkflow(string workflowId)
        {
            // this should be a structure we pass back through OnWorkflowComplete -- that is we should be passing back
            // some actual event args
            var taskIds = ((string)_db.HashGet("workflow:" + workflowId, "tasks")).Split(',');
            foreach (var taskId in taskIds)
            {
                var submitted = _db.HashGet("task:" + taskId, "submitted");
                var running = _db.HashGet("task:" + taskId, "running");
                var complete = _db.HashGet("task:" + taskId, "complete");
                var failed = _db.HashGet("task:" + taskId, "failed");
                var parents = _db.HashGet("task:" + taskId, "parents");
                var children = _db.HashGet("task:" + taskId, "children");

                Console.WriteLine("Task " + taskId + " s: " + submitted + " r: " + running + " c: " + complete + " fa: " + failed + " pa: " + parents + " ch: " + children);
            }
        }

        public void ClearBacklog()
        {
            Console.WriteLine("Clearing backlog");

            while (ProcessNextFailedWorkflow() != null) { }

            while (ProcessNextCompleteWorkflow() != null) { }

            while (ProcessNextTask() != null) { }
        }

        private string Identifier
        {
            get { return _identifier; }
        }

        private string ProcessNextTask()
        {
            var taskData = PopTask();

            if (taskData == null) return null;

            var task = taskData[0];
            var payload = taskData[1];

            var rh = new SimpleResultHandler(task, CompleteTask, FailTask);

            _taskHandler.Run(payload, rh);

            return task;
        }

        private string[] PopTask()
        {
            return _lua.PopTask(_db, Timestamp(), Identifier);
        }

        private static string Timestamp()
        {
            return DateTime.Now.ToString("dd/MM/yy HH:mm:ss.fff");
        }

        public void CleanUp(string workflow)
        {
            _lua.CleanupWorkflow(_db, workflow);

            Console.WriteLine("cleaned: " + workflow);
        }

        public string[] FindTasks()
        {
            return _lua.FindTasksFor(_db, Identifier);
        }

        public string[] ResubmitTasks()
        {
            return _lua.ResubmitTasksFor(_db, Identifier, Timestamp());
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
        public long? PushWorkflow(Workflow workflow)
        {
            var json = workflow.ToJson();

            var workflowId = _lua.PushWorkflow(_db, json, Timestamp());

            Console.WriteLine("pushed: " + workflow.Name + " as workflow " + workflowId);

            return workflowId.Value;
        }

        public async System.Threading.Tasks.Task<long?> PushWorkflowAsync(Workflow workflow)
        {
            var json = workflow.ToJson();

            var workflowId = await _lua.PushWorkflowAsync(_db, json, Timestamp());

            Console.WriteLine("pushed: " + workflow.Name + " as workflow " + workflowId);

            return workflowId.Value;
        }

        private void PushTask(string task)
        {
            _lua.PushTask(_db, task, Timestamp());

            Console.WriteLine("pushed: " + task);
        }

        private void FailTask(string task)
        {
            // if this class is told that a task has failed, we take it to mean that the workflow
            // has failed
            _lua.FailTask(_db, task, Timestamp());

            Console.WriteLine("failed: " + task);
        }

        private void CompleteTask(string task)
        {
            _lua.CompleteTask(_db, task, Timestamp(), Identifier);

            Console.WriteLine("completed: " + task);
        }

        private readonly ITaskHandler _taskHandler;

        private readonly WorkflowHandler _workflowHandler;

        private readonly IDatabase _db;

        private readonly ISubscriber _sub;

        private readonly Lua _lua;

        private readonly string _identifier;

        private readonly object _turnstile = new object();
    }
}
