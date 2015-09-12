# redis.workflow
A simple structure for managing workflows in redis. It should scale to O(1M) tasks over O(100k) workflows, and occupy no more than 0.5GB memory on a 64bit Redis host, with some minimal task payload.


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
                Name = "TestNode1", 
                Payload = "Node1", 
                Parents = new string[] { }, 
                Children = new string[] { "TestNode2" }, 
                Workflow = workflowName });
            tasks.Add(new Task { 
                Name = "TestNode2", 
                Payload = "Node2", 
                Parents = new string[] { "TestNode1" }, 
                Children = new string[] { "TestNode3", "TestNode4" }, 
                Workflow = workflowName });
            tasks.Add(new Task { 
                Name = "TestNode3", 
                Payload = "Node3", 
                Parents = new string[] { "TestNode2" }, 
                Children = new string[] { }, 
                Workflow = workflowName });
            tasks.Add(new Task { 
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
            var wm = new WorkflowManagement(ConnectionMultiplexer.Connect("localhost"), th, wh, "sampleApp");

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
