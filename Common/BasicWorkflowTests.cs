using Microsoft.VisualStudio.TestTools.UnitTesting;
using StackExchange.Redis;
using System.Collections.Generic;
using System.Threading;

namespace Redis.Workflow.Common
{
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

        public void Run(string configuration, IResultHandler resultHandler)
        {
            TaskRunCount++;

            if (configuration.Equals(_failAtTask))
                resultHandler.OnFailure();
            else if (configuration.Equals(_longRunner))
                return;
            else
                resultHandler.OnSuccess();
        }

        private string _failAtTask;

        private string _longRunner;
    }

    [TestClass]
    /// These are integration-level tests; they require a local instance of redis to
    /// be up and running.
    public class BasicWorkflowTests
    {
        [TestMethod]
        public void CanSubmitAndRunAWorkflow()
        {
            var mux = ConnectionMultiplexer.Connect("localhost");

            var db = mux.GetDatabase();

            db.ScriptEvaluate("redis.call(\"flushdb\")");

            var th = new TestTaskHandler();

            var complete = new ManualResetEvent(false);

            var wh = new WorkflowHandler();
            wh.WorkflowComplete += (s, w) => { complete.Set(); };

            var wm = new WorkflowManagement(th, wh);

            var workflow = new Workflow { Name = "TestWorkflow" };

            var tasks = new List<Task>();
            tasks.Add(new Task { Name = "TestNode1", Payload = "Node1", Parents = new string[] { }, Children = new string[] { "TestNode2" }, Workflow = workflow.Name });
            tasks.Add(new Task { Name = "TestNode2", Payload = "Node2", Parents = new string[] { "TestNode1" }, Children = new string[] { "TestNode3", "TestNode4", "TestNode5", "TestNode6" }, Workflow = workflow.Name });
            tasks.Add(new Task { Name = "TestNode3", Payload = "Node3", Parents = new string[] { "TestNode2" }, Children = new string[] { }, Workflow = workflow.Name });
            tasks.Add(new Task { Name = "TestNode4", Payload = "Node4", Parents = new string[] { "TestNode2" }, Children = new string[] { }, Workflow = workflow.Name });
            tasks.Add(new Task { Name = "TestNode5", Payload = "Node5", Parents = new string[] { "TestNode2" }, Children = new string[] { }, Workflow = workflow.Name });
            tasks.Add(new Task { Name = "TestNode6", Payload = "Node6", Parents = new string[] { "TestNode2" }, Children = new string[] { }, Workflow = workflow.Name });

            wm.PushWorkflow(workflow, tasks);

            var result = complete.WaitOne(2000); // machine-performance dependent, but 2 seconds is a long time

            Assert.IsTrue(result);
            Assert.AreEqual(6, th.TaskRunCount);
        }

        [TestMethod]
        public void CanCleanUpAfterAWorkflow()
        {
            var mux = ConnectionMultiplexer.Connect("localhost");

            var db = mux.GetDatabase();

            db.ScriptEvaluate("redis.call(\"flushdb\")");

            var th = new TestTaskHandler();

            var complete = new ManualResetEvent(false);

            var wh = new WorkflowHandler();
            wh.WorkflowComplete += (s, w) => { complete.Set(); };

            var wm = new WorkflowManagement(th, wh);

            var workflow = new Workflow { Name = "TestWorkflow" };

            var tasks = new List<Task>();
            tasks.Add(new Task { Name = "TestNode1", Payload = "Node1", Parents = new string[] { }, Children = new string[] { "TestNode2" }, Workflow = workflow.Name });
            tasks.Add(new Task { Name = "TestNode2", Payload = "Node2", Parents = new string[] { "TestNode1" }, Children = new string[] { "TestNode3", "TestNode4", "TestNode5", "TestNode6" }, Workflow = workflow.Name });
            tasks.Add(new Task { Name = "TestNode3", Payload = "Node3", Parents = new string[] { "TestNode2" }, Children = new string[] { }, Workflow = workflow.Name });
            tasks.Add(new Task { Name = "TestNode4", Payload = "Node4", Parents = new string[] { "TestNode2" }, Children = new string[] { }, Workflow = workflow.Name });
            tasks.Add(new Task { Name = "TestNode5", Payload = "Node5", Parents = new string[] { "TestNode2" }, Children = new string[] { }, Workflow = workflow.Name });
            tasks.Add(new Task { Name = "TestNode6", Payload = "Node6", Parents = new string[] { "TestNode2" }, Children = new string[] { }, Workflow = workflow.Name });

            var workflowId = wm.PushWorkflow(workflow, tasks);

            var result = complete.WaitOne(2000); // machine-performance dependent, but 2 seconds is a long time

            wm.CleanUp(workflowId.ToString());

            for (var t = 0; t < 4; t++)
            {
                Assert.IsFalse(db.KeyExists("task-" + t));
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

        [TestMethod]
        public void CanCleanUpAfterAFailedWorkflow()
        {
            // TODO: test not structured entirely usefully. Does set up a failed workflow,
            // but doesn't exercise all possible parts of the cleanup. Need to actually poke
            // in the state representing a failed workflow and test.
            var mux = ConnectionMultiplexer.Connect("localhost");

            var db = mux.GetDatabase();

            db.ScriptEvaluate("redis.call(\"flushdb\")");

            var th = new ErroringTestWithLongRunnerTaskHandler("Node4", "Node3");

            var complete = new ManualResetEvent(false);
            var failed = new ManualResetEvent(false);

            var wh = new WorkflowHandler();
            wh.WorkflowComplete += (s, w) => { complete.Set(); };
            wh.WorkflowFailed += (s, w) => { failed.Set(); };

            var wm = new WorkflowManagement(th, wh);

            var workflow = new Workflow { Name = "TestWorkflow" };

            var tasks = new List<Task>();
            tasks.Add(new Task { Name = "TestNode1", Payload = "Node1", Parents = new string[] { }, Children = new string[] { "TestNode2" }, Workflow = workflow.Name });
            tasks.Add(new Task { Name = "TestNode2", Payload = "Node2", Parents = new string[] { "TestNode1" }, Children = new string[] { "TestNode3", "TestNode4" }, Workflow = workflow.Name });
            tasks.Add(new Task { Name = "TestNode3", Payload = "Node3", Parents = new string[] { "TestNode2" }, Children = new string[] { }, Workflow = workflow.Name });
            tasks.Add(new Task { Name = "TestNode4", Payload = "Node4", Parents = new string[] { "TestNode2" }, Children = new string[] { }, Workflow = workflow.Name });

            var workflowId = wm.PushWorkflow(workflow, tasks);

            var result = WaitHandle.WaitAny(new[] { failed, complete }, 2000);

            wm.CleanUp(workflowId.ToString());

            // Should leave highwatermark id keys alone
            for (var t = 0; t < 4; t++)
            {
                Assert.IsFalse(db.KeyExists("task-" + t));
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
                Assert.AreEqual(0, db.ListRemove("running", t));
            }
            Assert.AreEqual(0, db.ListRemove("workflowComplete", "1"));
            Assert.AreEqual(0, db.ListRemove("workflowFailed", "1"));
            Assert.IsFalse(db.KeyExists("workflow-tasks-1"));
            Assert.IsFalse(db.KeyExists("workflow-remaining-1"));
            Assert.IsFalse(db.SetContains("workflows", "1"));

            
        }



        [TestMethod]
        public void IfATaskIsMarkedFailed_WorfklowFailsAndNoMoreTasksSubmitted()
        {
            var mux = ConnectionMultiplexer.Connect("localhost");

            var db = mux.GetDatabase();

            db.ScriptEvaluate("redis.call(\"flushdb\")");

            var th = new ErroringTestTaskHandler(2);

            var complete = new ManualResetEvent(false);
            var failed = new ManualResetEvent(false);

            var wh = new WorkflowHandler();
            wh.WorkflowComplete += (s, w) => { complete.Set(); };
            wh.WorkflowFailed += (s, w) => { failed.Set(); };

            var wm = new WorkflowManagement(th, wh);

            var workflow = new Workflow { Name = "TestWorkflow" };

            var tasks = new List<Task>();
            tasks.Add(new Task { Name = "TestNode1", Payload = "Node1", Parents = new string[] { }, Children = new string[] { "TestNode2" }, Workflow = workflow.Name });
            tasks.Add(new Task { Name = "TestNode2", Payload = "Node2", Parents = new string[] { "TestNode1" }, Children = new string[] { "TestNode3", "TestNode4" }, Workflow = workflow.Name });
            tasks.Add(new Task { Name = "TestNode3", Payload = "Node3", Parents = new string[] { "TestNode2" }, Children = new string[] { }, Workflow = workflow.Name });
            tasks.Add(new Task { Name = "TestNode4", Payload = "Node4", Parents = new string[] { "TestNode2" }, Children = new string[] { }, Workflow = workflow.Name });

            wm.PushWorkflow(workflow, tasks);

            var result = WaitHandle.WaitAny(new[] { failed, complete }, 2000);

            Assert.AreEqual(0, result);
            Assert.AreEqual(2, th.TaskRunCount); // only 2 get sent through the task handler; second fails, so no children are executed
        }
    }
}
