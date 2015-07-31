﻿using Redis.Workflow.Common;
using System;
using System.Collections.Generic;

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
            var workflow = new Redis.Workflow.Common.Workflow { Name = "TestWorkflow" };

            var nodes = new List<Task>();
            nodes.Add(new Task { Name = "TestNode1", Payload = "Node1", Parents = new string[] { }, Children = new string[] { "TestNode2" }, Workflow = workflow.Name });
            nodes.Add(new Task { Name = "TestNode2", Payload = "Node2", Parents = new string[] { "TestNode1" }, Children = new string[] { "TestNode3", "TestNode4" }, Workflow = workflow.Name });
            nodes.Add(new Task { Name = "TestNode3", Payload = "Node3", Parents = new string[] { "TestNode2" }, Children = new string[] { }, Workflow = workflow.Name });
            nodes.Add(new Task { Name = "TestNode4", Payload = "Node4", Parents = new string[] { "TestNode2" }, Children = new string[] { }, Workflow = workflow.Name });

            // A client will start acting on tasks immediately. May not want that to
            // be behaviour -- so might want to have an independent submit-only client. TaskHandler only needed for
            // submission. TaskHandler needs to be stateless, taking configuration for a specific task on a run call? No, that's
            // true per-task, but agiven ITaskHandler instance might be configured once wth e.g. compute resource connection details
            var th = new ThreadPoolTaskHandler();
            var wm = new WorkflowManagement(th);

            wm.ClearBacklog();

            // Pushing a workflow causes it to be executed
            wm.PushWorkflow(workflow, nodes);

            Console.WriteLine("Workflow pushed");
            // but how do we know when a workflow has finished?

            Console.ReadLine();
        }
    }
}
