﻿using Microsoft.VisualStudio.TestTools.UnitTesting;
using System;
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

    [TestClass]
    /// These are integration-level tests; they require a local instance of redis to
    /// be up and running.
    public class BasicWorkflowTests
    {
        [TestMethod]
        public void CanSubmitAndRunAWorkflow()
        {
            var th = new TestTaskHandler();

            var complete = new ManualResetEvent(false);

            var wh = new WorkflowHandler();
            wh.WorkflowComplete += w => { complete.Set(); };

            var wm = new WorkflowManagement(th, wh);

            var workflow = new Workflow { Name = "TestWorkflow" };

            var tasks = new List<Task>();
            tasks.Add(new Task { Name = "TestNode1", Payload = "Node1", Parents = new string[] { }, Children = new string[] { "TestNode2" }, Workflow = workflow.Name });
            tasks.Add(new Task { Name = "TestNode2", Payload = "Node2", Parents = new string[] { "TestNode1" }, Children = new string[] { "TestNode3", "TestNode4" }, Workflow = workflow.Name });
            tasks.Add(new Task { Name = "TestNode3", Payload = "Node3", Parents = new string[] { "TestNode2" }, Children = new string[] { }, Workflow = workflow.Name });
            tasks.Add(new Task { Name = "TestNode4", Payload = "Node4", Parents = new string[] { "TestNode2" }, Children = new string[] { }, Workflow = workflow.Name });

            wm.PushWorkflow(workflow, tasks);

            var result = complete.WaitOne(2000); // machine-performance dependent, but 2 seconds is a long time

            Assert.IsTrue(result);
            Assert.AreEqual(4, th.TaskRunCount);
        }
    }
}
