using Microsoft.VisualStudio.TestTools.UnitTesting;
using StackExchange.Redis;
using System;
using System.Collections.Generic;
using System.Threading;

namespace Redis.Workflow.Common
{
    public class BlockingTaskHandler : ITaskHandler
    {
        public void Run(string configuration, IResultHandler resultHandler)
        {
            ;
        }
    }

    public class TestTaskHandler : ITaskHandler
    {
        public int TaskRunCount = 0;

        public void Run(string configuration, IResultHandler resultHandler)
        {
            TaskRunCount++;

            resultHandler.OnSuccess();
        }
    }

    public class ErroringTestTaskHandler : ITaskHandler
    {
        public int TaskRunCount = 0;

        public ErroringTestTaskHandler(int failAtTask)
        {
            _failAtTask = failAtTask;
        }

        public void Run(string configuration, IResultHandler resultHandler)
        {
            TaskRunCount++;

            if (TaskRunCount.Equals(_failAtTask))
                resultHandler.OnFailure();
            else
                resultHandler.OnSuccess();
        }

        private int _failAtTask;
    }

    public class ErroringTestWithLongRunnerTaskHandler : ITaskHandler
    {
        public int TaskRunCount = 0;

        public ErroringTestWithLongRunnerTaskHandler(string failTask, string longRunner)
        {
            _failAtTask = failTask;

            _longRunner = longRunner;
        }

        public List<string> Events = new List<string>();

        private object _turnstile = new object();

        public void Run(string configuration, IResultHandler resultHandler)
        {
            lock(_turnstile)
            {
                TaskRunCount++;

                Events.Add("Considering task " + configuration);

                if (configuration.Equals(_failAtTask))
                {
                    Events.Add("Failing task " + _failAtTask);

                    resultHandler.OnFailure();
                }
                else if (configuration.Equals(_longRunner))
                {
                    Events.Add("Long runner task " + _longRunner);

                    return;
                }
                else
                {
                    Events.Add("Normal succeeding task " + configuration);

                    resultHandler.OnSuccess();
                }
            }
        }

        private string _failAtTask;

        private string _longRunner;
    }

    [TestClass]
    /// These are integration-level tests; they require a local instance of redis to
    /// be up and running.
    public class BasicWorkflowTests 
    {
        public BasicWorkflowTests()
        {
            _mux = ConnectionMultiplexer.Connect("localhost");
        }

        [TestMethod]
        public void CanCleanUpAfterAFailedWorkflow()
        {
            var db = _mux.GetDatabase();
            db.ScriptEvaluate("print(\"CanCleanUpAfterAFailedWorkflow\")");
            db.ScriptEvaluate("redis.call(\"flushdb\")");

            var th = new ErroringTestWithLongRunnerTaskHandler("Node4", "Node3");

            var complete = new ManualResetEvent(false);
            var failed = new ManualResetEvent(false);

            var events = new List<string>();

            var wh = new WorkflowHandler();
            wh.WorkflowComplete += (s, w) => { events.Add("complete"); complete.Set(); };
            wh.WorkflowFailed += (s, w) => { events.Add("failed"); failed.Set(); };

            using (var wm = new WorkflowManagement(_mux, th, wh))
            {
                var workflowName = "TestWorkflow";

                var tasks = new List<Task>();
                tasks.Add(new Task { Name = "TestNode1", Payload = "Node1", Parents = new string[] { }, Children = new string[] { "TestNode2" }, Workflow = workflowName });
                tasks.Add(new Task { Name = "TestNode2", Payload = "Node2", Parents = new string[] { "TestNode1" }, Children = new string[] { "TestNode3", "TestNode4" }, Workflow = workflowName });
                tasks.Add(new Task { Name = "TestNode3", Payload = "Node3", Parents = new string[] { "TestNode2" }, Children = new string[] { }, Workflow = workflowName });
                tasks.Add(new Task { Name = "TestNode4", Payload = "Node4", Parents = new string[] { "TestNode2" }, Children = new string[] { }, Workflow = workflowName });

                var workflow = new Workflow { Name = workflowName, Tasks = tasks };

                var workflowId = wm.PushWorkflow(workflow);

                var result = WaitHandle.WaitAny(new[] { failed, complete }, 2000);

                // In principle this is a failed workflow, as one of the tasks will fail
                // However, if all tasks get started, then the succeeding leaf node finishes first, this will be marked successful
                // Is a workflow considered complete when all tasks have started, or when all tasks have finished?
                // CHoose latter.
                Console.WriteLine("TaskHandler events"); foreach (var ev in th.Events) Console.WriteLine("TH event: " + ev);
                Console.WriteLine("WM events"); foreach (var ev in events) Console.WriteLine("Event: " + ev);
                Assert.AreEqual(0, result);

                wm.CleanUp(workflowId.ToString());
            }

            // Should leave highwatermark id keys alone
            db = _mux.GetDatabase();
            for (var t = 0; t < 4; t++)
            {
                Assert.IsFalse(db.KeyExists("task:" + t));
                Assert.IsFalse(db.KeyExists("parents-" + t));
                Assert.IsFalse(db.KeyExists("children-" + t));
                Assert.IsFalse(db.SetContains("tasks", t));
                Assert.IsFalse(db.SetContains("submitted", t));
                Assert.IsFalse(db.SetContains("complete", t));
                Assert.IsFalse(db.SetContains("failed", t));
                if (t == 3)
                {
                    Assert.IsTrue(db.SetContains("abandoned", t));
                    db.SetRemove("abandoned", t);
                }
                else
                    Assert.IsFalse(db.SetContains("abandoned", t));
                Assert.AreEqual(false, db.SetRemove("running", t));
            }
            Assert.AreEqual(0, db.ListRemove("workflowComplete", "1"));
            Assert.AreEqual(0, db.ListRemove("workflowFailed", "1"));
            Assert.IsFalse(db.KeyExists("workflow-tasks-1"));
            Assert.IsFalse(db.KeyExists("workflow-remaining-1"));
            Assert.IsFalse(db.SetContains("workflows", "1"));
        }


        [TestMethod]
        public void CanSubmitAndRunAWorkflow()
        {
            var db = _mux.GetDatabase();
            db.ScriptEvaluate("print(\"CanSubmitAndRunAWorkflow\")");
            db.ScriptEvaluate("redis.call(\"flushdb\")");

            var th = new TestTaskHandler();

            var complete = new ManualResetEvent(false);

            var events = new List<string>();

            var wh = new WorkflowHandler();
            wh.WorkflowComplete += (s, w) => { events.Add("complete"); complete.Set(); };

            using (var wm = new WorkflowManagement(_mux, th, wh))
            {
                var workflowName = "TestWorkflow";

                var tasks = new List<Task>();
                tasks.Add(new Task { Name = "TestNode1", Payload = "Node1", Parents = new string[] { }, Children = new string[] { "TestNode2" }, Workflow = workflowName });
                tasks.Add(new Task { Name = "TestNode2", Payload = "Node2", Parents = new string[] { "TestNode1" }, Children = new string[] { "TestNode3", "TestNode4", "TestNode5", "TestNode6" }, Workflow = workflowName });
                tasks.Add(new Task { Name = "TestNode3", Payload = "Node3", Parents = new string[] { "TestNode2" }, Children = new string[] { }, Workflow = workflowName });
                tasks.Add(new Task { Name = "TestNode4", Payload = "Node4", Parents = new string[] { "TestNode2" }, Children = new string[] { }, Workflow = workflowName });
                tasks.Add(new Task { Name = "TestNode5", Payload = "Node5", Parents = new string[] { "TestNode2" }, Children = new string[] { }, Workflow = workflowName });
                tasks.Add(new Task { Name = "TestNode6", Payload = "Node6", Parents = new string[] { "TestNode2" }, Children = new string[] { }, Workflow = workflowName });

                var workflow = new Workflow { Name = workflowName, Tasks = tasks };

                wm.PushWorkflow(workflow);

                var result = complete.WaitOne(2000);

                Console.WriteLine("WM events"); foreach (var ev in events) Console.WriteLine("Event: " + ev);

                Assert.IsTrue(result);
                Assert.AreEqual(6, th.TaskRunCount);
            }
        }

        [TestMethod]
        public void CanCleanUpAfterAWorkflow()
        {
            var db = _mux.GetDatabase();
            db.ScriptEvaluate("print(\"CanCleanUpAfterAWorkflow\")");
            db.ScriptEvaluate("redis.call(\"flushdb\")");

            var th = new TestTaskHandler();

            var complete = new ManualResetEvent(false);

            var wh = new WorkflowHandler();
            wh.WorkflowComplete += (s, w) => { complete.Set(); };

            using (var wm = new WorkflowManagement(_mux, th, wh))
            {
                var workflowName = "TestWorkflow";

                var tasks = new List<Task>();
                tasks.Add(new Task { Name = "TestNode1", Payload = "Node1", Parents = new string[] { }, Children = new string[] { "TestNode2" }, Workflow = workflowName });
                tasks.Add(new Task { Name = "TestNode2", Payload = "Node2", Parents = new string[] { "TestNode1" }, Children = new string[] { "TestNode3", "TestNode4", "TestNode5", "TestNode6" }, Workflow = workflowName });
                tasks.Add(new Task { Name = "TestNode3", Payload = "Node3", Parents = new string[] { "TestNode2" }, Children = new string[] { }, Workflow = workflowName });
                tasks.Add(new Task { Name = "TestNode4", Payload = "Node4", Parents = new string[] { "TestNode2" }, Children = new string[] { }, Workflow = workflowName });
                tasks.Add(new Task { Name = "TestNode5", Payload = "Node5", Parents = new string[] { "TestNode2" }, Children = new string[] { }, Workflow = workflowName });
                tasks.Add(new Task { Name = "TestNode6", Payload = "Node6", Parents = new string[] { "TestNode2" }, Children = new string[] { }, Workflow = workflowName });

                var workflow = new Workflow { Name = workflowName, Tasks = tasks };

                var workflowId = wm.PushWorkflow(workflow);

                var result = complete.WaitOne(2000); // machine-performance dependent, but 2 seconds is a long time

                wm.CleanUp(workflowId.ToString());

                db = _mux.GetDatabase();
                for (var t = 0; t < 6; t++)
                {
                    Assert.IsFalse(db.KeyExists("task:" + t));
                    Assert.IsFalse(db.KeyExists("parents-" + t));
                    Assert.IsFalse(db.KeyExists("children-" + t));
                    Assert.IsFalse(db.SetContains("tasks", t));
                    Assert.IsFalse(db.SetContains("submitted", t));
                    Assert.IsFalse(db.SetContains("complete", t));
                    Assert.IsFalse(db.SetContains("failed", t));
                    Assert.IsFalse(db.SetContains("abandoned", t));
                    Assert.AreEqual(0, db.ListRemove("running", t));
                }
                Assert.AreEqual(0, db.ListRemove("workflowComplete", "1"));
                Assert.AreEqual(0, db.ListRemove("workflowFailed", "1"));
                Assert.IsFalse(db.KeyExists("workflow-tasks-1"));
                Assert.IsFalse(db.KeyExists("workflow-remaining-1"));
                Assert.IsFalse(db.SetContains("workflows", "1"));
            }
        }




        [TestMethod]
        public void IfATaskIsMarkedFailed_WorfklowFailsAndNoMoreTasksSubmitted()
        {
            var db = _mux.GetDatabase();
            db.ScriptEvaluate("print(\"IfATaskIsMarkedFailed_WorfklowFailsAndNoMoreTasksSubmitted\")");
            db.ScriptEvaluate("redis.call(\"flushdb\")");

            var th = new ErroringTestTaskHandler(2);

            var complete = new ManualResetEvent(false);
            var failed = new ManualResetEvent(false);

            var wh = new WorkflowHandler();
            wh.WorkflowComplete += (s, w) => { complete.Set(); };
            wh.WorkflowFailed += (s, w) => { failed.Set(); };

            using (var wm = new WorkflowManagement(_mux, th, wh))
            {
                var workflowName = "TestWorkflow";

                var tasks = new List<Task>();
                tasks.Add(new Task { Name = "TestNode1", Payload = "Node1", Parents = new string[] { }, Children = new string[] { "TestNode2" }, Workflow = workflowName });
                tasks.Add(new Task { Name = "TestNode2", Payload = "Node2", Parents = new string[] { "TestNode1" }, Children = new string[] { "TestNode3", "TestNode4" }, Workflow = workflowName });
                tasks.Add(new Task { Name = "TestNode3", Payload = "Node3", Parents = new string[] { "TestNode2" }, Children = new string[] { }, Workflow = workflowName });
                tasks.Add(new Task { Name = "TestNode4", Payload = "Node4", Parents = new string[] { "TestNode2" }, Children = new string[] { }, Workflow = workflowName });

                var workflow = new Workflow { Name = workflowName, Tasks = tasks };

                wm.PushWorkflow(workflow);

                var result = WaitHandle.WaitAny(new[] { failed, complete }, 2000);

                Assert.AreEqual(0, result);
                Assert.AreEqual(2, th.TaskRunCount); // only 2 get sent through the task handler; second fails, so no children are executed
            }
        }

        [TestMethod]
        public void CanSubmitAWorkflow()
        {
            var db = _mux.GetDatabase();
            db.ScriptEvaluate("print(\"CanSubmitAndRunAWorkflow\")");
            db.ScriptEvaluate("redis.call(\"flushdb\")");

            var th = new BlockingTaskHandler();

            var complete = new ManualResetEvent(false);

            var events = new List<string>();

            var wh = new WorkflowHandler();
            wh.WorkflowComplete += (s, w) => { events.Add("complete"); complete.Set(); };

            using (var wm = new WorkflowManagement(_mux, th, wh))
            {
                var workflowName = "TestWorkflow";

                var tasks = new List<Task>();
                tasks.Add(new Task { Name = "TestNode1", Payload = "Node1", Parents = new string[] { }, Children = new string[] { "TestNode2" }, Workflow = workflowName });
                tasks.Add(new Task { Name = "TestNode2", Payload = "Node2", Parents = new string[] { "TestNode1" }, Children = new string[] { "TestNode3", "TestNode4", "TestNode5", "TestNode6" }, Workflow = workflowName });
                tasks.Add(new Task { Name = "TestNode3", Payload = "Node3", Parents = new string[] { "TestNode2" }, Children = new string[] { }, Workflow = workflowName });
                tasks.Add(new Task { Name = "TestNode4", Payload = "Node4", Parents = new string[] { "TestNode2" }, Children = new string[] { }, Workflow = workflowName });
                tasks.Add(new Task { Name = "TestNode5", Payload = "Node5", Parents = new string[] { "TestNode2" }, Children = new string[] { }, Workflow = workflowName });
                tasks.Add(new Task { Name = "TestNode6", Payload = "Node6", Parents = new string[] { "TestNode2" }, Children = new string[] { }, Workflow = workflowName });

                var workflow = new Workflow { Name = workflowName, Tasks = tasks };

                wm.PushWorkflow(workflow);
            }

            // assert
            db.ScriptEvaluate("redis.call(\"flushdb\")");
        }


        private readonly ConnectionMultiplexer _mux;
    }
}
