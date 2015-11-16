using Microsoft.VisualStudio.TestTools.UnitTesting;
using StackExchange.Redis;
using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;

namespace Redis.Workflow.Common
{
    public class BlockingTaskHandler : ITaskHandler
    {
        public string ID;

        public ManualResetEvent LetRun = new ManualResetEvent(false);

        public AutoResetEvent Gate = new AutoResetEvent(false);

        public ManualResetEvent Abort = new ManualResetEvent(false);

        public bool LetComplete = false;

        public BlockingTaskHandler(string id = "1")
        {
            ID = id;
        }

        public void Run(string configuration, IResultHandler resultHandler)
        {
            Gate.Set();

            var result = WaitHandle.WaitAny(new WaitHandle[] { Abort, LetRun });

            switch(result)
            {
                case 0:
                    return;
                case 1:
                    resultHandler.OnSuccess();
                    break;
            };
        }
    }

    public class TestTaskHandler : ITaskHandler
    {
        public List<string> Payloads = new List<string>();

        public int TaskRunCount = 0;

        public void Run(string configuration, IResultHandler resultHandler)
        {
            TaskRunCount++;

            Payloads.Add(configuration);

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
        private static readonly TaskName[] EmptyTaskList = new TaskName[0];

        private static readonly TaskType NoType = new TaskType("");

        private static readonly TaskPriority SimplePriority = new TaskPriority(1);

        public BasicWorkflowTests()
        {
            _mux = ConnectionMultiplexer.Connect("localhost");
        }

        [TestMethod]
        public void HandlesExceptionsFromBadLuaInASucceedingWorkflow()
        {
            var cases = new[] { TestCase.PopTask, TestCase.CompleteTask, TestCase.PopCompleteWorkflow };

            foreach (var testCase in cases)
            {
                var lua = new BadLua(testCase);

                var message = "HandlesExceptionsFromBadLuaInASucceedingWorkflow:" + testCase;

                var db = _mux.GetDatabase();
                db.ScriptEvaluate("print(\"" + message + "\")");
                db.ScriptEvaluate("redis.call(\"flushdb\")");

                var th = new TestTaskHandler();

                var complete = new ManualResetEvent(false);
                var failed = new ManualResetEvent(false);
                var exception = new ManualResetEvent(false);

                var events = new List<string>();

                var wh = new WorkflowHandler();
                wh.WorkflowComplete += (s, w) => { events.Add("complete"); complete.Set(); };
                wh.WorkflowFailed += (s, w) => { events.Add("failed"); failed.Set(); };

                EventHandler<Exception> eh = (s, e) => { events.Add(e.Message); exception.Set(); };

                using (var wm = new WorkflowManagement(_mux, th, wh, new WorkflowManagementId("test"), null, lua, eh, Behaviours.Processor | Behaviours.Submitter))
                {
                    var workflow = new Workflow(new WorkflowName("TestWorkflow"));
                    workflow.AddTask(new TaskName("TestNode1"), new Payload("Node1"), new TaskType(""), new TaskPriority(1), new TaskName[0], new TaskName[0]);

                    wm.PushWorkflow(workflow);

                    var waitResult = WaitHandle.WaitAny(new[] { exception, failed, complete }, 2000);

                    Assert.AreEqual(0, waitResult);
                }
            }
        }

        [TestMethod]
        public void HandlesExceptionsFromBadLuaInAFailingWorkflow()
        {
            var cases = new[] { TestCase.FailTask, TestCase.PopFailedWorkflow };

            foreach (var testCase in cases)
            {
                var lua = new BadLua(testCase);

                var message = "HandlesExceptionsFromBadLuaInAFailingWorkflow:" + testCase;

                var db = _mux.GetDatabase();
                db.ScriptEvaluate("print(\"" + message + "\")");
                db.ScriptEvaluate("redis.call(\"flushdb\")");

                var th = new ErroringTestWithLongRunnerTaskHandler("Node4", "Node3");

                var complete = new ManualResetEvent(false);
                var failed = new ManualResetEvent(false);
                var exception = new ManualResetEvent(false);

                var events = new List<string>();

                var wh = new WorkflowHandler();
                wh.WorkflowComplete += (s, w) => { events.Add("complete"); complete.Set(); };
                wh.WorkflowFailed += (s, w) => { events.Add("failed"); failed.Set(); };

                EventHandler<Exception> eh = (s, e) => { events.Add(e.Message); exception.Set(); };

                using (var wm = new WorkflowManagement(_mux, th, wh, new WorkflowManagementId("test"), null, lua, eh, Behaviours.Processor | Behaviours.Submitter))
                {
                    var t1 = new TaskName("TestNode1");
                    var t2 = new TaskName("TestNode2");
                    var t3 = new TaskName("TestNode3");
                    var t4 = new TaskName("TestNode4");

                    var workflow = new Workflow(new WorkflowName("TestWorkflow"));
                    workflow.AddTask(t1, new Payload("Node1"), NoType, SimplePriority, EmptyTaskList, new [] { t2 });
                    workflow.AddTask(t2, new Payload("Node2"), NoType, SimplePriority, new [] { t1 }, new [] { t3, t4 });
                    workflow.AddTask(t3, new Payload("Node3"), NoType, SimplePriority, new [] { t2 }, EmptyTaskList);
                    workflow.AddTask(t4, new Payload("Node4"), NoType, SimplePriority, new [] { t2 }, EmptyTaskList);
                    
                    wm.PushWorkflow(workflow);

                    var waitResult = WaitHandle.WaitAny(new[] { exception, failed, complete }, 2000);

                    Assert.AreEqual(0, waitResult);
                }
            }
        }

        [TestMethod]
        public void HandlesDanglingChildReferences()
        {
            var db = _mux.GetDatabase();
            db.ScriptEvaluate("print(\"HandlesDanglingChildReferences\")");
            db.ScriptEvaluate("redis.call(\"flushdb\")");

            var th = new BlockingTaskHandler();
            th.LetRun.Set();

            var complete = new ManualResetEvent(false);

            var events = new List<string>();

            var wh = new WorkflowHandler();
            wh.WorkflowComplete += (s, w) => { events.Add("complete"); complete.Set(); };

            using (var wm = new WorkflowManagement(_mux, th, wh, new WorkflowManagementId("test"), null, new Lua()))
            {
                var workflow = new Workflow(new WorkflowName("TestWorkflow"));
                workflow.AddTask(new TaskName("TestNode1"), new Payload("Node1"), new TaskType(""), new TaskPriority(1), EmptyTaskList, new [] { new TaskName("TestNode2") });

                wm.PushWorkflow(workflow);

                var result = WaitHandle.WaitAny(new[] { complete }, 2000);

                Assert.AreEqual(0, result);
            }

            db.ScriptEvaluate("redis.call(\"flushdb\")");
        }

        [TestMethod, ExpectedException(typeof(System.ArgumentException))]
        public void ThrowsIfAWorkflowHasNoRootParents()
        {
            var db = _mux.GetDatabase();
            db.ScriptEvaluate("print(\"ThrowsIfAWorkflowHasNoRootParents\")");
            db.ScriptEvaluate("redis.call(\"flushdb\")");

            var th = new BlockingTaskHandler();
            th.LetRun.Set();

            var complete = new ManualResetEvent(false);

            var events = new List<string>();

            var wh = new WorkflowHandler();
            wh.WorkflowComplete += (s, w) => { events.Add("complete"); complete.Set(); };

            using (var wm = new WorkflowManagement(_mux, th, wh, new WorkflowManagementId("test"), null, new Lua()))
            {
                var workflow = new Workflow(new WorkflowName("TestWorkflow"));
                workflow.AddTask(new TaskName("TestNode1"), new Payload("Node1"), NoType, SimplePriority, new [] { new TaskName("SomeParent") }, EmptyTaskList);
                
                wm.PushWorkflow(workflow);

                var result = WaitHandle.WaitAny(new[] { complete }, 2000);

                Assert.AreEqual(0, result);
            }

            db.ScriptEvaluate("redis.call(\"flushdb\")");
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
            var exception = new ManualResetEvent(false);

            var events = new List<string>();

            var wh = new WorkflowHandler();
            wh.WorkflowComplete += (s, w) => { events.Add("complete"); complete.Set(); };
            wh.WorkflowFailed += (s, w) => { events.Add("failed"); failed.Set(); };

            EventHandler<Exception> eh = (s, e) => { events.Add(e.Message); exception.Set(); };

            using (var wm = new WorkflowManagement(_mux, th, wh, new WorkflowManagementId("test"), eh))
            {
                var t1 = new TaskName("TestNode1");
                var t2 = new TaskName("TestNode2");
                var t3 = new TaskName("TestNode3");
                var t4 = new TaskName("TestNode4");

                var workflow = new Workflow(new WorkflowName("TestWorkflow"));
                workflow.AddTask(t1, new Payload("Node1"), NoType, SimplePriority, EmptyTaskList, new [] { t2 });
                workflow.AddTask(t2, new Payload("Node2"), NoType, SimplePriority, new [] { t1 }, new [] { t3, t4 });
                workflow.AddTask(t3, new Payload("Node3"), NoType, SimplePriority, new [] { t2 }, EmptyTaskList);
                workflow.AddTask(t4, new Payload("Node4"), NoType, SimplePriority, new [] { t2 }, EmptyTaskList);

                var workflowId = wm.PushWorkflow(workflow);

                var result = WaitHandle.WaitAny(new[] { exception, failed, complete }, 2000);

                if(result == 0) throw new Exception("Failed in workflow operation"); // in a real app, handler could add exception to a list to process

                // In principle this is a failed workflow, as one of the tasks will fail
                // However, if all tasks get started, then the succeeding leaf node finishes first, this will be marked successful
                // Is a workflow considered complete when all tasks have started, or when all tasks have finished?
                // CHoose latter.
                Console.WriteLine("TaskHandler events"); foreach (var ev in th.Events) Console.WriteLine("TH event: " + ev);
                Console.WriteLine("WM events"); foreach (var ev in events) Console.WriteLine("Event: " + ev);
                Assert.AreEqual(1, result);

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

            using (var wm = new WorkflowManagement(_mux, th, wh, new WorkflowManagementId("test"), null, new Lua()))
            {
                var t1 = new TaskName("TestNode1");
                var t2 = new TaskName("TestNode2");
                var t3 = new TaskName("TestNode3");
                var t4 = new TaskName("TestNode4");
                var t5 = new TaskName("TestNode5");
                var t6 = new TaskName("TestNode6");

                var workflow = new Workflow(new WorkflowName("TestWorkflow"));
                workflow.AddTask(t1, new Payload("Node1"), NoType, SimplePriority, EmptyTaskList, new [] { t2 });
                workflow.AddTask(t2, new Payload("Node2"), NoType, SimplePriority, new [] { t1 }, new [] { t3, t4, t5, t6 });
                workflow.AddTask(t3, new Payload("Node3"), NoType, SimplePriority, new [] { t2 }, EmptyTaskList);
                workflow.AddTask(t4, new Payload("Node4"), NoType, SimplePriority, new [] { t2 }, EmptyTaskList);
                workflow.AddTask(t5, new Payload("Node5"), NoType, SimplePriority, new [] { t2 }, EmptyTaskList);
                workflow.AddTask(t6, new Payload("Node6"), NoType, SimplePriority, new [] { t2 }, EmptyTaskList);

                wm.PushWorkflow(workflow);

                var workflowWasCompleted = complete.WaitOne(2000);

                Assert.IsTrue(workflowWasCompleted);

                Console.WriteLine("WM events"); foreach (var ev in events) Console.WriteLine("Event: " + ev);

                Assert.AreEqual(6, th.TaskRunCount);
            }
        }

        [TestMethod]
        public void RunsTasksInPriorityOrder()
        {
            var db = _mux.GetDatabase();
            db.ScriptEvaluate("print(\"CanSubmitAndRunAWorkflow\")");
            db.ScriptEvaluate("redis.call(\"flushdb\")");

            var th = new TestTaskHandler();

            var complete = new ManualResetEvent(false);

            var events = new List<string>();

            var wh = new WorkflowHandler();
            wh.WorkflowComplete += (s, w) => { events.Add("complete"); complete.Set(); };

            using (var wm = new WorkflowManagement(_mux, th, wh, new WorkflowManagementId("test"), null, new Lua()))
            {
                var workflow = new Workflow(new WorkflowName("TestWorkflow"));
                workflow.AddTask(new TaskName("TestNode1"), new Payload("Node1"), NoType, new TaskPriority(3), EmptyTaskList, EmptyTaskList);
                workflow.AddTask(new TaskName("TestNode2"), new Payload("Node2"), NoType, new TaskPriority(2), EmptyTaskList, EmptyTaskList);
                workflow.AddTask(new TaskName("TestNode3"), new Payload("Node3"), NoType, new TaskPriority(1), EmptyTaskList, EmptyTaskList);
                workflow.AddTask(new TaskName("TestNode4"), new Payload("Node4"), NoType, new TaskPriority(4), EmptyTaskList, EmptyTaskList);
                workflow.AddTask(new TaskName("TestNode5"), new Payload("Node5"), NoType, new TaskPriority(6), EmptyTaskList, EmptyTaskList);
                workflow.AddTask(new TaskName("TestNode6"), new Payload("Node6"), NoType, new TaskPriority(5), EmptyTaskList, EmptyTaskList);

                wm.PushWorkflow(workflow);

                var workflowWasCompleted = complete.WaitOne(2000);

                Assert.IsTrue(workflowWasCompleted);

                Console.WriteLine("WM events"); foreach (var ev in events) Console.WriteLine("Event: " + ev);

                Assert.AreEqual(6, th.TaskRunCount);
                Assert.AreEqual("Node3", th.Payloads[0]);
                Assert.AreEqual("Node2", th.Payloads[1]);
                Assert.AreEqual("Node1", th.Payloads[2]);
                Assert.AreEqual("Node4", th.Payloads[3]);
                Assert.AreEqual("Node6", th.Payloads[4]);
                Assert.AreEqual("Node5", th.Payloads[5]);
            }
        }


        [TestMethod]
        public void CanSubmitAndRunAWorkflowWithTypedTasks()
        {
            var db = _mux.GetDatabase();
            db.ScriptEvaluate("print(\"CanSubmitAndRunAWorkflow\")");
            db.ScriptEvaluate("redis.call(\"flushdb\")");

            var th = new TestTaskHandler();

            var complete = new ManualResetEvent(false);

            var events = new List<string>();

            var wh = new WorkflowHandler();
            wh.WorkflowComplete += (s, w) => { events.Add("complete"); complete.Set(); };

            var type1 = new TaskType("testTaskType");
            var type2 = new TaskType("testTaskType2");

            using (var wm = new WorkflowManagement(_mux, th, wh, new WorkflowManagementId("test"), new[] { type1 }, new Lua()))
            using (var wm2 = new WorkflowManagement(_mux, th, wh, new WorkflowManagementId("test2"), new[] { type2 }, new Lua()))
            {
                var t1 = new TaskName("TestNode1");
                var t2 = new TaskName("TestNode2");
                var t3 = new TaskName("TestNode3");
                var t4 = new TaskName("TestNode4");
                var t5 = new TaskName("TestNode5");
                var t6 = new TaskName("TestNode6");
                
                var workflow = new Workflow(new WorkflowName("TestWorkflow"));
                workflow.AddTask(t1, new Payload("Node1"), type1, SimplePriority, EmptyTaskList, new[] { t2 });
                workflow.AddTask(t2, new Payload("Node2"), type1, SimplePriority, new[] { t1 }, new[] { t3, t4, t5, t6 });
                workflow.AddTask(t3, new Payload("Node3"), type2, SimplePriority, new[] { t2 }, EmptyTaskList);
                workflow.AddTask(t4, new Payload("Node4"), type1, SimplePriority, new[] { t2 }, EmptyTaskList);
                workflow.AddTask(t5, new Payload("Node5"), type2, SimplePriority, new[] { t2 }, EmptyTaskList);
                workflow.AddTask(t6, new Payload("Node6"), type1, SimplePriority, new[] { t2 }, EmptyTaskList);

                wm.PushWorkflow(workflow);

                var workflowWasCompleted = complete.WaitOne(2000);

                Assert.IsTrue(workflowWasCompleted);

                Console.WriteLine("WM events"); foreach (var ev in events) Console.WriteLine("Event: " + ev);

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

            using (var wm = new WorkflowManagement(_mux, th, wh, new WorkflowManagementId("test"), null, new Lua()))
            {
                var t1 = new TaskName("TestNode1");
                var t2 = new TaskName("TestNode2");
                var t3 = new TaskName("TestNode3");
                var t4 = new TaskName("TestNode4");
                var t5 = new TaskName("TestNode5");
                var t6 = new TaskName("TestNode6");

                var workflow = new Workflow(new WorkflowName("TestWorkflow"));
                workflow.AddTask(t1, new Payload("Node1"), NoType, SimplePriority, EmptyTaskList, new[] { t2 });
                workflow.AddTask(t2, new Payload("Node2"), NoType, SimplePriority, new[] { t1 }, new[] { t3, t4, t5, t6 });
                workflow.AddTask(t3, new Payload("Node3"), NoType, SimplePriority, new[] { t2 }, EmptyTaskList);
                workflow.AddTask(t4, new Payload("Node4"), NoType, SimplePriority, new[] { t2 }, EmptyTaskList);
                workflow.AddTask(t5, new Payload("Node5"), NoType, SimplePriority, new[] { t2 }, EmptyTaskList);
                workflow.AddTask(t6, new Payload("Node6"), NoType, SimplePriority, new[] { t2 }, EmptyTaskList);

                var workflowId = wm.PushWorkflow(workflow);

                var workflowCompleted = complete.WaitOne(2000); // machine-performance dependent, but 2 seconds is a long time

                Assert.IsTrue(workflowCompleted);

                var info = wm.FetchWorkflowInformation(workflowId.ToString());

                Assert.AreEqual("1", info.Id);
                Assert.AreEqual(6, info.Tasks.Count);

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
                Assert.IsFalse(db.KeyExists("submitted:1"));
                Assert.IsFalse(db.KeyExists("running:1"));
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

            using (var wm = new WorkflowManagement(_mux, th, wh, new WorkflowManagementId("test"), null, new Lua()))
            {
                var t1 = new TaskName("TestNode1");
                var t2 = new TaskName("TestNode2");
                var t3 = new TaskName("TestNode3");
                var t4 = new TaskName("TestNode4");

                var workflow = new Workflow(new WorkflowName("TestWorkflow"));
                workflow.AddTask(t1, new Payload("Node1"), NoType, SimplePriority, EmptyTaskList, new[] { t2 });
                workflow.AddTask(t2, new Payload("Node2"), NoType, SimplePriority, new[] { t1 }, new[] { t3, t4 });
                workflow.AddTask(t3, new Payload("Node3"), NoType, SimplePriority, new[] { t2 }, EmptyTaskList);
                workflow.AddTask(t4, new Payload("Node4"), NoType, SimplePriority, new[] { t2 }, EmptyTaskList);

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
            th.LetRun.Set();

            var complete = new ManualResetEvent(false);

            var events = new List<string>();

            var wh = new WorkflowHandler();
            wh.WorkflowComplete += (s, w) => { events.Add("complete"); complete.Set(); };

            using (var wm = new WorkflowManagement(_mux, th, wh, new WorkflowManagementId("test"), null, new Lua()))
            {
                var t1 = new TaskName("TestNode1");
                var t2 = new TaskName("TestNode2");
                var t3 = new TaskName("TestNode3");
                var t4 = new TaskName("TestNode4");
                var t5 = new TaskName("TestNode5");
                var t6 = new TaskName("TestNode6");

                var workflow = new Workflow(new WorkflowName("TestWorkflow"));
                workflow.AddTask(t1, new Payload("Node1"), NoType, SimplePriority, EmptyTaskList, new[] { t2 });
                workflow.AddTask(t2, new Payload("Node2"), NoType, SimplePriority, new[] { t1 }, new[] { t3, t4, t5, t6 });
                workflow.AddTask(t3, new Payload("Node3"), NoType, SimplePriority, new[] { t2 }, EmptyTaskList);
                workflow.AddTask(t4, new Payload("Node4"), NoType, SimplePriority, new[] { t2 }, EmptyTaskList);
                workflow.AddTask(t5, new Payload("Node5"), NoType, SimplePriority, new[] { t2 }, EmptyTaskList);
                workflow.AddTask(t6, new Payload("Node6"), NoType, SimplePriority, new[] { t2 }, EmptyTaskList);

                wm.PushWorkflow(workflow);

                var result = WaitHandle.WaitAny(new[] { complete }, 2000);

                Assert.AreEqual(0, result);
            }

            db.ScriptEvaluate("redis.call(\"flushdb\")");
        }

        [TestMethod]
        public void CanSubmitAWorkflowAsync()
        {
            var db = _mux.GetDatabase();
            db.ScriptEvaluate("print(\"CanSubmitAndRunAWorkflow\")");
            db.ScriptEvaluate("redis.call(\"flushdb\")");

            var th = new BlockingTaskHandler();
            th.LetRun.Set();

            var complete = new ManualResetEvent(false);

            var events = new List<string>();

            var wh = new WorkflowHandler();
            wh.WorkflowComplete += (s, w) => { events.Add("complete"); complete.Set(); };

            using (var wm = new WorkflowManagement(_mux, th, wh, new WorkflowManagementId("test"), null, new Lua()))
            {
                var t1 = new TaskName("TestNode1");
                var t2 = new TaskName("TestNode2");
                var t3 = new TaskName("TestNode3");
                var t4 = new TaskName("TestNode4");
                var t5 = new TaskName("TestNode5");
                var t6 = new TaskName("TestNode6");

                var workflow = new Workflow(new WorkflowName("TestWorkflow"));
                workflow.AddTask(t1, new Payload("Node1"), NoType, SimplePriority, EmptyTaskList, new[] { t2 });
                workflow.AddTask(t2, new Payload("Node2"), NoType, SimplePriority, new[] { t1 }, new[] { t3, t4, t5, t6 });
                workflow.AddTask(t3, new Payload("Node3"), NoType, SimplePriority, new[] { t2 }, EmptyTaskList);
                workflow.AddTask(t4, new Payload("Node4"), NoType, SimplePriority, new[] { t2 }, EmptyTaskList);
                workflow.AddTask(t5, new Payload("Node5"), NoType, SimplePriority, new[] { t2 }, EmptyTaskList);
                workflow.AddTask(t6, new Payload("Node6"), NoType, SimplePriority, new[] { t2 }, EmptyTaskList);

                var id = wm.PushWorkflowAsync(workflow);

                id.Wait();
            }

            db.ScriptEvaluate("redis.call(\"flushdb\")");
        }

        [TestMethod]
        public void CanQueryForOwnTasks()
        {
            var db = _mux.GetDatabase();
            db.ScriptEvaluate("print(\"CanSubmitAndRunAWorkflow\")");
            db.ScriptEvaluate("redis.call(\"flushdb\")");

            var th = new BlockingTaskHandler();

            var complete = new ManualResetEvent(false);

            var events = new List<string>();

            var wh = new WorkflowHandler();
            wh.WorkflowComplete += (s, w) => { events.Add("complete"); complete.Set(); };

            using (var wm = new WorkflowManagement(_mux, th, wh, new WorkflowManagementId("test"), null, new Lua()))
            {
                var workflow = new Workflow(new WorkflowName("TestWorkflow"));
                workflow.AddTask(new TaskName("TestNode1"), new Payload("Node1"), NoType, SimplePriority, EmptyTaskList, new [] { new TaskName("TestNode2") });

                wm.PushWorkflow(workflow);

                var taskWasStarted = th.Gate.WaitOne(2000);

                Assert.IsTrue(taskWasStarted);

                var myTasks = wm.FindTasks();

                Assert.AreEqual(1, myTasks.Length);
                Assert.AreEqual("1", myTasks[0]);
            }

            db.ScriptEvaluate("redis.call(\"flushdb\")");
        }

        [TestMethod]
        public void CanResetOwnTypedTasks()
        {
            var db = _mux.GetDatabase();
            db.ScriptEvaluate("print(\"CanSubmitAndRunAWorkflow\")");
            db.ScriptEvaluate("redis.call(\"flushdb\")");

            var th = new BlockingTaskHandler("1");
            var th2 = new BlockingTaskHandler("2");

            var complete = new ManualResetEvent(false);

            var events = new List<string>();

            var wh = new WorkflowHandler();
            wh.WorkflowComplete += (s, w) => { events.Add("complete"); complete.Set(); };

            var t1 = new TaskName("TestNode1");
            var t2 = new TaskName("TestNode2");
            var t3 = new TaskName("TestNode3");
            var t4 = new TaskName("TestNode4");
            var t5 = new TaskName("TestNode5");
            var t6 = new TaskName("TestNode6");

            var type1 = new TaskType("testTaskType");

            var workflow = new Workflow(new WorkflowName("TestWorkflow"));
            workflow.AddTask(t1, new Payload("Node1"), type1, SimplePriority, EmptyTaskList, new[] { t2 });
            workflow.AddTask(t2, new Payload("Node2"), type1, SimplePriority, new[] { t1 }, new[] { t3, t4, t5, t6 });
            workflow.AddTask(t3, new Payload("Node3"), type1, SimplePriority, new[] { t2 }, EmptyTaskList);
            workflow.AddTask(t4, new Payload("Node4"), type1, SimplePriority, new[] { t2 }, EmptyTaskList);
            workflow.AddTask(t5, new Payload("Node5"), type1, SimplePriority, new[] { t2 }, EmptyTaskList);
            workflow.AddTask(t6, new Payload("Node6"), type1, SimplePriority, new[] { t2 }, EmptyTaskList);

            using (var wm = new WorkflowManagement(_mux, th, wh, new WorkflowManagementId("test"), new[] { type1 }, new Lua()))
            {
                wm.PushWorkflow(workflow);

                // we've picked up the first task, and don't want to pick up any more
                th.Gate.WaitOne();

                Assert.AreEqual(0, db.SortedSetLength("submitted:testTaskType"));
            }

            // create a new wm to simulate a restart of a component
            using (var wm2 = new WorkflowManagement(_mux, th2, wh, new WorkflowManagementId("test"), new[] { type1 }, new Lua()))
            {
                Assert.AreEqual(1, db.SortedSetLength("submitted:testTaskType"));

                th.Abort.Set();
                th2.LetRun.Set();

                var result = complete.WaitOne();
            }

            db.ScriptEvaluate("redis.call(\"flushdb\")");
        }

        [TestMethod]
        public void CanResetOwnTasks()
        {
            var db = _mux.GetDatabase();
            db.ScriptEvaluate("print(\"CanSubmitAndRunAWorkflow\")");
            db.ScriptEvaluate("redis.call(\"flushdb\")");

            var th = new BlockingTaskHandler("1");
            var th2 = new BlockingTaskHandler("2");

            var complete = new ManualResetEvent(false);

            var events = new List<string>();

            var wh = new WorkflowHandler();
            wh.WorkflowComplete += (s, w) => { events.Add("complete"); complete.Set(); };

            var t1 = new TaskName("TestNode1");
            var t2 = new TaskName("TestNode2");
            var t3 = new TaskName("TestNode3");
            var t4 = new TaskName("TestNode4");
            var t5 = new TaskName("TestNode5");
            var t6 = new TaskName("TestNode6");

            var workflow = new Workflow(new WorkflowName("TestWorkflow"));
            workflow.AddTask(t1, new Payload("Node1"), NoType, SimplePriority, EmptyTaskList, new[] { t2 });
            workflow.AddTask(t2, new Payload("Node2"), NoType, SimplePriority, new[] { t1 }, new[] { t3, t4, t5, t6 });
            workflow.AddTask(t3, new Payload("Node3"), NoType, SimplePriority, new[] { t2 }, EmptyTaskList);
            workflow.AddTask(t4, new Payload("Node4"), NoType, SimplePriority, new[] { t2 }, EmptyTaskList);
            workflow.AddTask(t5, new Payload("Node5"), NoType, SimplePriority, new[] { t2 }, EmptyTaskList);
            workflow.AddTask(t6, new Payload("Node6"), NoType, SimplePriority, new[] { t2 }, EmptyTaskList);

            using (var wm = new WorkflowManagement(_mux, th, wh, new WorkflowManagementId("test"), null, new Lua()))
            {
                wm.PushWorkflow(workflow);

                // we've picked up the first task, and don't want to pick up any more
                th.Gate.WaitOne();

                Assert.AreEqual(0, db.ListLength("submitted"));
            }

            // create a new wm to simulate a restart of a component
            using (var wm2 = new WorkflowManagement(_mux, th2, wh, new WorkflowManagementId("test"), null, new Lua()))
            {
                Assert.AreEqual(1, db.SortedSetLength("submitted"));

                th.Abort.Set();
                th2.LetRun.Set();

                complete.WaitOne();
            }

            db.ScriptEvaluate("redis.call(\"flushdb\")");
        }

        [TestMethod]
        public void CanPauseAWorkflowWithTypedTasks()
        {
            var db = _mux.GetDatabase();
            db.ScriptEvaluate("print(\"CanPauseAWorkflow\")");
            db.ScriptEvaluate("redis.call(\"flushdb\")");

            var th = new BlockingTaskHandler("1");

            var complete = new ManualResetEvent(false);

            var events = new List<string>();

            var wh = new WorkflowHandler();
            wh.WorkflowComplete += (s, w) => { events.Add("complete"); complete.Set(); };

            var t1 = new TaskName("TestNode1");
            var t2 = new TaskName("TestNode2");
            var t3 = new TaskName("TestNode3");
            var t4 = new TaskName("TestNode4");
            var t5 = new TaskName("TestNode5");
            var t6 = new TaskName("TestNode6");

            var type1 = new TaskType("testTaskType");

            var workflow = new Workflow(new WorkflowName("TestWorkflow"));
            workflow.AddTask(t1, new Payload("Node1"), type1, SimplePriority, EmptyTaskList, new[] { t2 });
            workflow.AddTask(t2, new Payload("Node2"), type1, SimplePriority, EmptyTaskList, new[] { t3, t4, t5, t6 });
            workflow.AddTask(t3, new Payload("Node3"), type1, SimplePriority, new[] { t2 }, EmptyTaskList);
            workflow.AddTask(t4, new Payload("Node4"), type1, SimplePriority, new[] { t2 }, EmptyTaskList);
            workflow.AddTask(t5, new Payload("Node5"), type1, SimplePriority, new[] { t2 }, EmptyTaskList);
            workflow.AddTask(t6, new Payload("Node6"), type1, SimplePriority, new[] { t2 }, EmptyTaskList);

            using (var wm = new WorkflowManagement(_mux, th, wh, new WorkflowManagementId("test"), new[] { type1 }, new Lua()))
            {
                var workflowId = wm.PushWorkflow(workflow);

                th.Gate.WaitOne();

                Assert.AreEqual(1, db.SortedSetLength("submitted:testTaskType"));
                Assert.AreEqual(1, db.SetLength("running"));

                wm.PauseWorkflow(workflowId);

                var expected = new List<string> { "1", "2" };
                var state = "paused";

                CheckSetContent(db, expected, state);

                // The running task will run bang into the new state
                th.LetRun.Set();

                Thread.Sleep(500); // gah, honestly. I won't get a signal because no new tasks should be submitted

                expected = new List<string> { "2" };
                state = "paused";

                CheckSetContent(db, expected, state);

                expected = new List<string> { "2" };
                state = "running";

                CheckSetContent(db, expected, state, false);

                // If this is checked before the task has completed then there'll be two paused tasks
                // We've got no hook into the completion event as yet
                Assert.AreEqual(1, db.SetLength("paused"));
                Assert.AreEqual(1, db.SetLength("paused:1"));
                Assert.AreEqual(0, db.ListLength("submitted:testTaskType"));
                Assert.AreEqual(0, db.SetLength("running"));
            }

            db.ScriptEvaluate("redis.call(\"flushdb\")");
        }

        [TestMethod]
        public void CanReleaseAWorkflowWithTypedTasks()
        {
            var db = _mux.GetDatabase();
            db.ScriptEvaluate("print(\"CanPauseAWorkflow\")");
            db.ScriptEvaluate("redis.call(\"flushdb\")");

            var th = new BlockingTaskHandler("1");

            var complete = new ManualResetEvent(false);

            var events = new List<string>();

            var wh = new WorkflowHandler();
            wh.WorkflowComplete += (s, w) => { events.Add("complete"); complete.Set(); };

            var t1 = new TaskName("TestNode1");
            var t2 = new TaskName("TestNode2");
            var t3 = new TaskName("TestNode3");
            var t4 = new TaskName("TestNode4");
            var t5 = new TaskName("TestNode5");
            var t6 = new TaskName("TestNode6");

            var type1 = new TaskType("testTaskType");

            var workflow = new Workflow(new WorkflowName("TestWorkflow"));
            workflow.AddTask(t1, new Payload("Node1"), type1, SimplePriority, EmptyTaskList, new[] { t2 });
            workflow.AddTask(t2, new Payload("Node2"), type1, SimplePriority, EmptyTaskList, new[] { t3, t4, t5, t6 });
            workflow.AddTask(t3, new Payload("Node3"), type1, SimplePriority, new[] { t2 }, EmptyTaskList);
            workflow.AddTask(t4, new Payload("Node4"), type1, SimplePriority, new[] { t2 }, EmptyTaskList);
            workflow.AddTask(t5, new Payload("Node5"), type1, SimplePriority, new[] { t2 }, EmptyTaskList);
            workflow.AddTask(t6, new Payload("Node6"), type1, SimplePriority, new[] { t2 }, EmptyTaskList);

            using (var wm = new WorkflowManagement(_mux, th, wh, new WorkflowManagementId("test"), new[] { type1 }, new Lua()))
            {
                var workflowId = wm.PushWorkflow(workflow);

                th.Gate.WaitOne();

                wm.PauseWorkflow(workflowId);

                wm.ReleaseWorkflow(workflowId);

                Assert.AreEqual(1, db.ListLength("submitted:testTaskType"));
                Assert.AreEqual(1, db.SetLength("running"));
            }

            db.ScriptEvaluate("redis.call(\"flushdb\")");
        }

        [TestMethod]
        public void CanPauseAWorkflow()
        {
            var db = _mux.GetDatabase();
            db.ScriptEvaluate("print(\"CanPauseAWorkflow\")");
            db.ScriptEvaluate("redis.call(\"flushdb\")");

            var th = new BlockingTaskHandler("1");

            var complete = new ManualResetEvent(false);

            var events = new List<string>();

            var wh = new WorkflowHandler();
            wh.WorkflowComplete += (s, w) => { events.Add("complete"); complete.Set(); };

            var t1 = new TaskName("TestNode1");
            var t2 = new TaskName("TestNode2");
            var t3 = new TaskName("TestNode3");
            var t4 = new TaskName("TestNode4");
            var t5 = new TaskName("TestNode5");
            var t6 = new TaskName("TestNode6");

            var type1 = new TaskType("testTaskType");

            var workflow = new Workflow(new WorkflowName("TestWorkflow"));
            workflow.AddTask(t1, new Payload("Node1"), NoType, SimplePriority, EmptyTaskList, new[] { t2 });
            workflow.AddTask(t2, new Payload("Node2"), NoType, SimplePriority, EmptyTaskList, new[] { t3, t4, t5, t6 });
            workflow.AddTask(t3, new Payload("Node3"), NoType, SimplePriority, new[] { t2 }, EmptyTaskList);
            workflow.AddTask(t4, new Payload("Node4"), NoType, SimplePriority, new[] { t2 }, EmptyTaskList);
            workflow.AddTask(t5, new Payload("Node5"), NoType, SimplePriority, new[] { t2 }, EmptyTaskList);
            workflow.AddTask(t6, new Payload("Node6"), NoType, SimplePriority, new[] { t2 }, EmptyTaskList);

            using (var wm = new WorkflowManagement(_mux, th, wh, new WorkflowManagementId("test"), null, new Lua()))
            {
                var workflowId = wm.PushWorkflow(workflow);

                var waitResult = th.Gate.WaitOne(/*2000*/);

                Assert.IsTrue(waitResult);
                Assert.AreEqual(1, db.SortedSetLength("submitted"));
                Assert.AreEqual(1, db.SetLength("running"));

                wm.PauseWorkflow(workflowId);

                var expected = new List<string> { "1", "2" };
                var state = "paused";

                CheckSetContent(db, expected, state);

                // The running task will run bang into the new state
                th.LetRun.Set();

                Thread.Sleep(500); // gah, honestly. I won't get a signal because no new tasks should be submitted

                expected = new List<string> { "2" };
                state = "paused";

                CheckSetContent(db, expected, state);

                expected = new List<string> { "2" };
                state = "running";

                CheckSetContent(db, expected, state, false);

                // If this is checked before the task has completed then there'll be two paused tasks
                // We've got no hook into the completion event as yet
                Assert.AreEqual(1, db.SetLength("paused")); 
                Assert.AreEqual(1, db.SetLength("paused:1"));
                Assert.AreEqual(0, db.SortedSetLength("submitted"));
                Assert.AreEqual(0, db.SetLength("running"));
            }

            db.ScriptEvaluate("redis.call(\"flushdb\")");
        }

        [TestMethod]
        public void CanReleaseAWorkflow()
        {
            var db = _mux.GetDatabase();
            db.ScriptEvaluate("print(\"CanPauseAWorkflow\")");
            db.ScriptEvaluate("redis.call(\"flushdb\")");

            var th = new BlockingTaskHandler("1");

            var complete = new ManualResetEvent(false);

            var events = new List<string>();

            var wh = new WorkflowHandler();
            wh.WorkflowComplete += (s, w) => { events.Add("complete"); complete.Set(); };

            var t1 = new TaskName("TestNode1");
            var t2 = new TaskName("TestNode2");
            var t3 = new TaskName("TestNode3");
            var t4 = new TaskName("TestNode4");
            var t5 = new TaskName("TestNode5");
            var t6 = new TaskName("TestNode6");

            var type1 = new TaskType("testTaskType");

            var workflow = new Workflow(new WorkflowName("TestWorkflow"));
            workflow.AddTask(t1, new Payload("Node1"), NoType, SimplePriority, EmptyTaskList, new[] { t2 });
            workflow.AddTask(t2, new Payload("Node2"), NoType, SimplePriority, EmptyTaskList, new[] { t3, t4, t5, t6 });
            workflow.AddTask(t3, new Payload("Node3"), NoType, SimplePriority, new[] { t2 }, EmptyTaskList);
            workflow.AddTask(t4, new Payload("Node4"), NoType, SimplePriority, new[] { t2 }, EmptyTaskList);
            workflow.AddTask(t5, new Payload("Node5"), NoType, SimplePriority, new[] { t2 }, EmptyTaskList);
            workflow.AddTask(t6, new Payload("Node6"), NoType, SimplePriority, new[] { t2 }, EmptyTaskList);

            using (var wm = new WorkflowManagement(_mux, th, wh, new WorkflowManagementId("test"), null, new Lua()))
            {
                var workflowId = wm.PushWorkflow(workflow);

                th.Gate.WaitOne();

                wm.PauseWorkflow(workflowId);

                wm.ReleaseWorkflow(workflowId);

                Assert.AreEqual(1, db.ListLength("submitted"));
                Assert.AreEqual(1, db.SetLength("running"));
            }

            db.ScriptEvaluate("redis.call(\"flushdb\")");
        }

        private static void CheckSetContent(IDatabase db, List<string> expected, string state, bool condition = true)
        {
            var toRequeue = new List<string>();
            while (db.SetLength(state) != 0)
            {
                toRequeue.Add(db.SetPop(state));
            }
            foreach (var item in expected)
            {
                if(condition)
                    Assert.IsTrue(toRequeue.Contains(item));
                else
                    Assert.IsFalse(toRequeue.Contains(item));
            }
            foreach (var temp in toRequeue)
            {
                db.SetAdd(state, temp);
            }
        }

        private readonly ConnectionMultiplexer _mux;
    }
}
