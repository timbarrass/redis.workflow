using Redis.Workflow.Common;
using System;
using System.Threading;
using StackExchange.Redis;

namespace Redis.Workflow.ExampleApp
{
    public class Program
    {
        static void Main(string[] args)
        {
            var app = new Program();

            app.Run(args);
        }

        internal void Run(string[] args)
        {
            // Should you want to decompose to running single tasks you can just create a workflow containing nodes with no parents or
            // children. The system works out where to start by identifying nodes with no parents, and assuming they're immediately
            // runnable. The hosting workflow then gives you a handle by which you can treat the individual tasks as a related set.
            var t1 = new TaskName("TestNode1");
            var t2 = new TaskName("TestNode2");
            var t3 = new TaskName("TestNode3");
            var t4 = new TaskName("TestNode4");
            var t5 = new TaskName("TestNode5");
            var t6 = new TaskName("TestNode6");

            var type1 = new TaskType("testTaskType");

            var workflow = new Common.Workflow(new WorkflowName("TestWorkflow"));
            workflow.AddTask(t1, new Payload("Node1"), type1, SimplePriority, EmptyTaskList, new[] { t2 });
            workflow.AddTask(t2, new Payload("Node2"), type1, SimplePriority, EmptyTaskList, new[] { t3, t4, t5, t6 });
            workflow.AddTask(t3, new Payload("Node3"), type1, SimplePriority, new[] { t2 }, EmptyTaskList);
            workflow.AddTask(t4, new Payload("Node4"), type1, SimplePriority, new[] { t2 }, EmptyTaskList);
            workflow.AddTask(t5, new Payload("Node5"), type1, SimplePriority, new[] { t2 }, EmptyTaskList);
            workflow.AddTask(t6, new Payload("Node6"), type1, SimplePriority, new[] { t2 }, EmptyTaskList);

            // A client will start acting on tasks immediately. May not want that to
            // be behaviour -- so might want to have an independent submit-only client. TaskHandler only needed for
            // submission. TaskHandler needs to be stateless, taking configuration for a specific task on a run call? No, that's
            // true per-task, but agiven ITaskHandler instance might be configured once wth e.g. compute resource connection details
            var th = new TaskHandler();
            var wh = new WorkflowHandler();
            var complete = new ManualResetEvent(false);
            var failed = new ManualResetEvent(false);
            wh.WorkflowComplete += (s, w) => { Console.WriteLine("workflow complete: " + w); complete.Set(); };
            wh.WorkflowFailed += (s, w) => { Console.WriteLine("workflow failed: " + w); failed.Set(); };

            EventHandler<Exception> eh = (s, e) => { };

            var wm = new WorkflowManagement(ConnectionMultiplexer.Connect("localhost"), th, wh, new WorkflowManagementId("sampleApp"), eh);

            wm.ClearBacklog();

            // Pushing a workflow causes it to be executed
            var id = wm.PushWorkflow(workflow);

            Console.WriteLine("Workflow pushed");

            WaitHandle.WaitAny(new[] { failed, complete });

            wm.CleanUp(id.ToString());

            Console.ReadLine();
        }

        private static readonly TaskName[] EmptyTaskList = new TaskName[0];

        private static readonly TaskType NoType = new TaskType("");

        private static readonly TaskPriority SimplePriority = new TaskPriority(1);
    }
}
