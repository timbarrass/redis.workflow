using System;
using StackExchange.Redis;
using System.Collections.Generic;
using System.Linq;

namespace Redis.Workflow.Common
{
    public class WorkflowManagement : IDisposable
    {
        public void Dispose()
        {
            // Interesting behaviour in tests. If we don't Unsubscribe all at end of test then
            // we get odd varying fails, which I suspect are to do with pub/sub messages crossing
            // test boundaries.
            _sub.UnsubscribeAll();
        }

        internal WorkflowManagement(ConnectionMultiplexer mux, ITaskHandler taskHandler, WorkflowHandler workflowHandler, string identifier, IEnumerable<string> typesProcessed, ILua lua, EventHandler<Exception> exceptionHandler = null, Behaviours behaviours = Behaviours.All)
        {
            _taskHandler = taskHandler;

            _workflowHandler = workflowHandler;

            if (exceptionHandler != null)
            {
                ExceptionThrown += exceptionHandler;
            }

            _typesProcessed = typesProcessed;

            _db = mux.GetDatabase();

            _sub = mux.GetSubscriber();

            if (_typesProcessed == null || _typesProcessed.Count() == 0)
            {
                _sub.Subscribe("submittedTask", (c, v) =>
                {
                    ProcessNextTask();
                });
            }
            else
            {
                foreach(var t in _typesProcessed)
                {
                    _sub.Subscribe("submittedTask:" + t, (c, v) =>
                    {
                        ProcessNextTask(t);
                    });
                }
            }

            _sub.Subscribe("workflowFailed", (c, v) =>
            {
                ProcessNextFailedWorkflow();
            });

            _sub.Subscribe("workflowComplete", (c, v) =>
            {
                ProcessNextCompleteWorkflow();
            });

            _lua = lua;
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

        /// <summary>
        /// Instantiate a new workflow manager, able to submit workflows and process tasks.
        /// </summary>
        /// <param name="mux"></param>
        /// <param name="taskHandler"></param>
        /// <param name="workflowHandler"></param>
        /// <param name="identifier"></param>
        /// <param name="exceptionHandler"></param>
        /// <param name="lowestPriority">
        /// The lowest priority task this instance will consider. High priority is given a score of 0; lowest priority should be given 
        /// a score > 0. Defaults to 100. Note that you should have at least one instance able to cover the whole priority range you
        /// submit at.
        /// </param>
        /// <param name="typesProcessed"></param>
        /// <param name="behaviours"></param>
        public WorkflowManagement(ConnectionMultiplexer mux, ITaskHandler taskHandler, WorkflowHandler workflowHandler, string identifier, EventHandler<Exception> exceptionHandler, int lowestPriority = 100, IEnumerable<string> typesProcessed = null, Behaviours behaviours = Behaviours.All)
                        : this(mux, taskHandler, workflowHandler, identifier, typesProcessed, new Lua(lowestPriority), exceptionHandler, behaviours)
        {
            
        }

        private string ProcessNextFailedWorkflow()
        {
            string workflow = null;

            try
            {
                workflow = PopFailedWorkflow();

                if (workflow == null) return null;

                var workflowDetails = FetchWorkflowInformation(workflow);

                _workflowHandler.OnWorkflowFailed(workflow, workflowDetails);

                return workflow;

            }
            catch (Exception ex)
            {
                OnException(ex);

                return null; // TODO: see ProcessNextCompleteWorkflow for discussion ..
            }
        }

        private string PopFailedWorkflow()
        {
            return _lua.PopFailedWorkflow(_db);
        }

        private string ProcessNextCompleteWorkflow()
        {
            string workflow = null;

            try
            {
                workflow = PopCompleteWorkflow();

                if (workflow == null) return null;

                var workflowDetails = FetchWorkflowInformation(workflow);

                _workflowHandler.OnWorkflowComplete(workflow, workflowDetails);

                return workflow;
            }
            catch (Exception ex)
            {
                OnException(ex);

                return null; // TODO: tricky one, need to take a look at the use case for the return value. We're already calling into the workflow handler ...
            }
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

        private string ProcessNextTask(string type)
        {
            string[] taskData = null;

            try
            {
                taskData = PopTask(type);

                if (taskData == null) return null;

                var task = taskData[0];
                var payload = taskData[1];

                var rh = new SimpleResultHandler(task, CompleteTask, FailTask);

                _taskHandler.Run(payload, rh);

                return task;
            }
            catch (Exception ex)
            {
                OnException(ex);

                return null; // might need examination, although null does indicate no task found
            }
        }


        private string ProcessNextTask()
        {
            string[] taskData = null;

            try
            {
                taskData = PopTask();

                if (taskData == null) return null;

                var task = taskData[0];
                var payload = taskData[1];

                var rh = new SimpleResultHandler(task, CompleteTask, FailTask);

                _taskHandler.Run(payload, rh);

                return task;

            }
            catch (Exception ex)
            {
                OnException(ex);

                return null; // might need examination, although null does indicate no task found
            }
        }

        private void OnException(Exception ex)
        {
            var handler = ExceptionThrown;

            if (handler != null) handler(this, ex);
        }

        public event EventHandler<Exception> ExceptionThrown;

        private string[] PopTask()
        {
            return _lua.PopTask(_db, Timestamp(), Identifier);
        }

        private string[] PopTask(string type)
        {
            return _lua.PopTask(_db, type, Timestamp(), Identifier);
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

        internal void PauseWorkflow(long? workflowId)
        {
            _lua.PauseWorkflow(_db, workflowId.ToString(), Timestamp());
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
            var rootParentCount = 0;
            foreach(var task in workflow.Tasks)
            {
                if(task.Parents.Length == 0)
                {
                    rootParentCount++;
                }
            }

            if(rootParentCount == 0)
            {
                throw new ArgumentException("A workflow must have at least one task with no parents.", "workflow");
            }

            var json = workflow.ToJson();

            var workflowId = _lua.PushWorkflow(_db, json, Timestamp());
            
            return workflowId.Value;
        }

        public async System.Threading.Tasks.Task<long?> PushWorkflowAsync(Workflow workflow)
        {
            var json = workflow.ToJson();

            var workflowId = await _lua.PushWorkflowAsync(_db, json, Timestamp());

            return workflowId.Value;
        }

        private void FailTask(string task)
        {
            // if this class is told that a task has failed, we take it to mean that the workflow
            // has failed
            _lua.FailTask(_db, task, Timestamp());
        }

        private void CompleteTask(string task)
        {
            try
            {
                _lua.CompleteTask(_db, task, Timestamp(), Identifier);
            }
            catch(Exception ex)
            {
                OnException(ex);
            }
        }

        public void ReleaseWorkflow(long? workflowId)
        {
            _lua.ReleaseWorkflow(_db, workflowId.ToString(), Timestamp());
        }

        private readonly ITaskHandler _taskHandler;

        private readonly WorkflowHandler _workflowHandler;

        private readonly IDatabase _db;

        private readonly ISubscriber _sub;

        private readonly ILua _lua;

        private readonly string _identifier;

        private readonly object _turnstile = new object();

        private readonly IEnumerable<string> _typesProcessed;

        private readonly int _lowestPriority = 100;
    }
}
