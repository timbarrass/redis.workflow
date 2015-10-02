# redis.workflow
A simple system that helps manage workflows at enterprise scale using redis. Its first design goal is to scale to O(1M) tasks over O(100k) workflows, and occupy no more than 0.5GB memory on a 64bit Redis host, with some minimal task payload. It should be able to process this level of work, with no-op tasks, in 2 minutes.

Fundamentally redis.workflow holds workflow state -- relationships between tasks, their content, and their current processing state -- and provides a standard set of reliable processes to help clients progress those workflows to completion. It also provides more sophisticated functions, including workflow pause and release.

Tasks can be differentiated by (arbitrary string) type, and clients can be configured to pick up work of specific types, allowing the creation of quite a rich ecosystem of agents or services with a functional split by task.

A fundamental design principle of redis.workflow is that either tasks, or the workflow system, or both, will fail. State is persisted in redis, and process that act on that state are transactional. Clients self-identify with an ID that should be unique for a logical entity in the client system; if that logical entity reconnects it can automatically reconnect to the tasks that it had running before stopping.

Early benchmarking in a low spec environment show that redis.workflow can process 0.9M no-op tasks across 100k workflows in about 3 minutes without improvements. In the same simple tests it reached a task churn of 5k tasks per second; it pushed workflows at a rate of around several hundred per second.

Discussion of some design principles, and tests, can be found at http://timbar.blogspot.com .

## Getting started -- a sample client application
This is a simple example application that configures a workflow containing 4 tasks, submits and handles the task turnover:

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
            var workflowName = "TestWorkflow";

            var tasks = new List<Task>();
            tasks.Add(new Task { 
                Type = "",
                Name = "TestNode1", 
                Payload = "Node1", 
                Parents = new string[] { }, 
                Children = new string[] { "TestNode2" }, 
                Workflow = workflowName });
            tasks.Add(new Task { 
                Type = "",
                Name = "TestNode2", 
                Payload = "Node2", 
                Parents = new string[] { "TestNode1" }, 
                Children = new string[] { "TestNode3", "TestNode4" }, 
                Workflow = workflowName });
            tasks.Add(new Task { 
                Type = "",
                Name = "TestNode3", 
                Payload = "Node3", 
                Parents = new string[] { "TestNode2" }, 
                Children = new string[] { }, 
                Workflow = workflowName });
            tasks.Add(new Task { 
                Type = "",
                Name = "TestNode4", 
                Payload = "Node4", 
                Parents = new string[] { "TestNode2" }, 
                Children = new string[] { }, 
                Workflow = workflowName });

            var workflow = new Common.Workflow { Name = workflowName, Tasks = tasks };

            var th = new TaskHandler();
            var wh = new WorkflowHandler();
            
            var complete = new ManualResetEvent(false);
            var failed = new ManualResetEvent(false);
            wh.WorkflowComplete += (s, w) => { Console.WriteLine("workflow complete: " + w); complete.Set(); };
            wh.WorkflowFailed += (s, w) => { Console.WriteLine("workflow failed: " + w); failed.Set(); };
            
            // WorkflowManagement gives us our entry point for injecting workflows
            var wm = new WorkflowManagement(ConnectionMultiplexer.Connect("localhost"), th, wh, "sampleApp", null);

            // Pushing a workflow causes it to be executed
            var id = wm.PushWorkflow(workflow);

            Console.WriteLine("Workflow pushed");

            WaitHandle.WaitAny(new[] { failed, complete });

            // When a workflow is complete its down to the client to trigger a cleanup (until backstop
            // cleanup is implemented). This is to make sure you have time to fetch any details stored.
            wm.CleanUp(id.ToString());

            Console.ReadLine();
        }
    }
	
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

