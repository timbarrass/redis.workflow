using Redis.Workflow.Common;
using System;

namespace Redis.Workflow.ExampleApp
{
    /// <summary>
    /// A simple example task handler. It spins up a new task that does nothing but 
    /// state it's doing work, and call the workflow result handler to keep things 
    /// ticking over.
    /// </summary>
    public class ThreadPoolTaskHandler : ITaskHandler
    {
        public void Run(string configuration, IResultHandler resultHandler)
        {
            Console.WriteLine("Doing work for: " + configuration);

            var t = new System.Threading.Tasks.Task(() => { resultHandler.OnSuccess(); });

            t.Start();
        }
    }
}
