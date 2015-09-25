using Redis.Workflow.Common;
using StackExchange.Redis;
using System;
using System.Collections.Generic;
using System.Threading;

namespace Redis.Workflow.BenchmarkApp
{
    class TaskHandler : ITaskHandler
    {
        public void Run(string configuration, IResultHandler resultHandler)
        {
            resultHandler.OnSuccess();
        }
    }

    class BenchmarkProgram
    {
        static void Main(string[] args)
        {
            var app = new BenchmarkProgram();

            app.Run(args);
        }

        internal void Run(string[] args)
        {
            int workflows = 10000;

            Common.Workflow workflow = CreateTestWorkflow();

            var th = new TaskHandler();

            var wh = new WorkflowHandler();

            int total = 0;
            int completed = 0;
            var countLock = new object();
            var allDone = new ManualResetEvent(false);
            // TODO: condition variable ...
            wh.WorkflowComplete += (s, w) => { lock(countLock) { completed++; Console.Write("done: " + completed + "\r"); if (completed == workflows) allDone.Set(); } };
            wh.WorkflowFailed += (s, w) => { lock (countLock) { completed++; Console.Write("done: " + completed + "\r"); if (completed == workflows) allDone.Set(); } };

            EventHandler<Exception> eh = (s, e) => { Console.WriteLine("EXCEPTION"); allDone.Set(); };

            var wm = new WorkflowManagement(ConnectionMultiplexer.Connect("localhost"), th, wh, "sampleApp", eh);

            var startTime = DateTime.Now;

            for (int i = 0; i < workflows; i++)
            {
                var id = wm.PushWorkflow(workflow);
            }

            var pushDuration = new TimeSpan(DateTime.Now.Ticks - startTime.Ticks);

            WaitHandle.WaitAny(new[] { allDone });

            var duration = new TimeSpan(DateTime.Now.Ticks - startTime.Ticks);
            Console.WriteLine("Done, {0} in {1} ms (push took {2} ms) tasks: {3} churn: {4} tasks per second", workflows, duration.TotalMilliseconds, pushDuration.TotalMilliseconds, workflows * 4, workflows * 4 / duration.TotalSeconds);

            Console.ReadLine();
        }

        private static Common.Workflow CreateTestWorkflow()
        {
            var workflowName = "TestWorkflow";

            var tasks = new List<Task>();
            tasks.Add(new Task { Name = "TestNode1", Payload = "Node1", Parents = new string[] { }, Children = new string[] { "TestNode2" }, Workflow = workflowName });
            tasks.Add(new Task { Name = "TestNode2", Payload = "Node2", Parents = new string[] { "TestNode1" }, Children = new string[] { "TestNode3", "TestNode4" }, Workflow = workflowName });
            tasks.Add(new Task { Name = "TestNode3", Payload = "Node3", Parents = new string[] { "TestNode2" }, Children = new string[] { }, Workflow = workflowName });
            tasks.Add(new Task { Name = "TestNode4", Payload = "Node4", Parents = new string[] { "TestNode2" }, Children = new string[] { }, Workflow = workflowName });

            var workflow = new Common.Workflow { Name = workflowName, Tasks = tasks };
            return workflow;
        }
    }
}
