# redis.workflow
A simple system that helps manage workflows at enterprise scale using redis. Its first design goal is to scale to O(1M) tasks over O(100k) workflows, and occupy no more than 0.5GB memory on a 64bit Redis host, with some minimal task payload. It should be able to process this level of work, with no-op tasks, in 2 minutes.

Fundamentally redis.workflow holds workflow state -- relationships between tasks, their content, and their current processing state -- and provides a standard set of reliable processes to help clients progress those workflows to completion. It also provides more sophisticated functions, including workflow pause and release.

Tasks can be differentiated by (arbitrary string) type, and clients can be configured to pick up work of specific types, allowing the creation of quite a rich ecosystem of agents or services with a functional split by task.

A fundamental design principle of redis.workflow is that either tasks, or the workflow system, or both, will fail. State is persisted in redis, and process that act on that state are transactional. Clients self-identify with an ID that should be unique for a logical entity in the client system; if that logical entity reconnects it can automatically reconnect to the tasks that it had running before stopping.

Early benchmarking in a low spec environment show that redis.workflow can process 0.9M no-op tasks across 100k workflows in about 3 minutes without improvements. In the same simple tests it reached a task churn of 5k tasks per second; it pushed workflows at a rate of around several hundred per second.

Discussion of some design principles, and tests, can be found at http//timbarrass.co.uk/posts .

## Getting started with a sample client application
This is a simple example application that configures a workflow containing 4 tasks, submits and handles the task turnover. You'll need an instance of Redis running on localhost.

    public class Program
    {
        static void Main(string[] args)
        {
            var app = new Program();

            app.Run(args);
        }

        internal void Run(string[] args)
        {
            // Should you want to decompose to running single tasks you can just create a 
            // workflow containing nodes with no parents or children. The system works out 
            // where to start by identifying nodes with no parents, and assuming they're 
            // immediately runnable. The hosting workflow then gives you a handle by which 
            // you can treat the individual tasks as a related set.
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

            var th = new TaskHandler();
            var wh = new WorkflowHandler();
            
            var complete = new ManualResetEvent(false);
            var failed = new ManualResetEvent(false);
            wh.WorkflowComplete += (s, w) => { Console.WriteLine("workflow complete: " + w); complete.Set(); };
            wh.WorkflowFailed += (s, w) => { Console.WriteLine("workflow failed: " + w); failed.Set(); };
            
            // WorkflowManagement gives us our entry point for injecting workflows
            var wm = new WorkflowManagement(ConnectionMultiplexer.Connect("localhost"), th, wh, "sampleApp", null);

            // Pushing a workflow causes it to be executed
            // You'll need to hold on to this id if you want to issue specific operations against it
            var id = wm.PushWorkflow(workflow);

            Console.WriteLine("Workflow pushed");

            WaitHandle.WaitAny(new[] { failed, complete });

            // When a workflow is complete its down to the client to trigger a cleanup (until backstop
            // cleanup is implemented). This is to make sure you have time to fetch any details stored.
            wm.CleanUp(id.ToString());

            Console.ReadLine();
        }
    }
	
    private static readonly TaskName[] EmptyTaskList = new TaskName[0];

    private static readonly TaskType NoType = new TaskType("");

    private static readonly TaskPriority SimplePriority = new TaskPriority(1);

	
    // An ITaskHandler is the point at which control passes back to the client, giving 
    // them a chance to act on the task's payload. It needs to call resultHandler.OnSuccess()
    // or OnFail() as is appropriate. Here, it's a simple no-op that keeps the workflow
    // churning.
    class TaskHandler : ITaskHandler
    {
        public void Run(string payload, IResultHandler resultHandler)
        {
            resultHandler.OnSuccess();
        }
    }

## A quick note on scaling
You can scale out by spinning up multiple instances of WorkfloadManagement, each of which points at the same Redis instance. However, each of them will only process sequentially if you don't set

    var mux = ConnectionMultiplexer("localhost");
    mux.PreserveAsyncOrder = false;

on the multiplexer you create. (I'm intending to wrap this in a builder method that gives you a scaling/non-scaling but trivially safe WorkloadManagment instance choice).

## Workflow consistency
It's possible to misconfigure tasks because you're asked to explicitly declare parents and children of given tasks as a collection of task names. By default when you call `PushWorkflow` an internal consistency check will be run that will determine whether there are any root tasks defined that can be run immediately, and whether there are any parent or child task lists that refer to undeclared children. A `WorkflowException` will be thrown if these checks fail.

If you want to disable this consistency checking you can pass `false` as the second parameter to `Workflow.PushWorkflow`:

    wm.PushWorkflow(someWorkflow, false);

## Pausing and releasing, and abandonment
You can pause a workflow using

    using(var wm = new WorkloadManagement(ConnectionMultiplexer.Connect("localhost"), th, wh, "sampleApp", null))
    {
       wm.PauseWorkflow(id);
    }
    
Pausing a workflow prevents any unstarted tasks from starting. There's no pre-emptive cancellation; currently running tasks are allowed to run down, but on completion no dependent tasks will get started if the workflow is still paused.

You can release a workflow using

    using(var wm = new WorkloadManagement(ConnectionMultiplexer.Connect("localhost"), th, wh, "sampleApp", null))
    {
       wm.ReleaseWorkflow(id);
    }
    
All tasks that were submitted before being paused will be returned to the submitted state. If any running tasks completed while paused then their dependent tasks will get moved to the submitted state.

There is not any way of explicitly abandoning a task currently. Instead, a workflow could either be paused indefinitely, or just cleaned up.

## Task and workflow priority
Tasks can be prioritised by giving them a score between 0 and some arbitrary integer up to Int.MaxValue, with 0 being high priority.

    tasks.Add(new Task { 
                Priority = 10,
                Type = "",
                Name = "TestNode3", 
                Payload = "Node3", 
                Parents = new string[] { "TestNode2" }, 
                Children = new string[] { }, 
                Workflow = workflowName });
                
If you want to set priority at workflow level, simply set all tasks within the workflow with the same priority.

You can configure WorkflowManager s to only fetch tasks to some minimum priority; this allows you to create instances that will only process higher priority tasks, if you wish. You might, for example, only want to process tasks down to priority 50

    using(var wm = new WorkloadManagement(ConnectionMultiplexer.Connect("localhost"), th, wh, "sampleApp", 50))
    {
        ...
    }
    
By default WorkflowManagers are instantiated with a lowest priority setting of 100.
