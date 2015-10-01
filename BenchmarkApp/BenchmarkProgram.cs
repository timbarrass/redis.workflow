using Redis.Workflow.Common;
using StackExchange.Redis;
using System;
using System.Collections.Generic;
using System.IO;
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
            int workflows = 100000;

            var mux = ConnectionMultiplexer.Connect("localhost");
            mux.PreserveAsyncOrder = false;
            var db = mux.GetDatabase();
            db.ScriptEvaluate("redis.call(\"flushdb\")");
            
            Common.Workflow workflow = CreateTestWorkflow();

            var th = new TaskHandler();

            var wh = new WorkflowHandler();

            var completionEvents = new List<DateTime>();

            bool pushDone = false;
            int total = 0;
            int completed = 0;
            var countLock = new object();
            var allDone = new ManualResetEvent(false);
            wh.WorkflowComplete += (s, id) => { lock (countLock) { completed++; Console.Write("pushDone: " + pushDone + " done: " + completed + "\r"); completionEvents.Add(DateTime.Now); if (completed == workflows) allDone.Set(); } };
            wh.WorkflowFailed += (s, id) => { lock (countLock) { completed++; Console.Write("pushDone: " + pushDone + " done: " + completed + "\r"); completionEvents.Add(DateTime.Now); if (completed == workflows) allDone.Set(); } };

            EventHandler<Exception> eh = (s, e) => { Console.WriteLine("EXCEPTION: " + e.Message + ": " + e.StackTrace); allDone.Set(); };

            var wm = new WorkflowManagement(mux, th, wh, "sampleApp", eh);

            var startTime = DateTime.Now;

            var submittedQueueLength = new List<Tuple<DateTime, string>>();
            var runningSetLength     = new List<Tuple<DateTime, string>>();
            var queueSampler = new System.Timers.Timer() { Interval = 10000, AutoReset = true, Enabled = true };
            queueSampler.Elapsed += (s, e) => {
                var val = db.ListLength("submitted");
                submittedQueueLength.Add(new Tuple<DateTime, string>(DateTime.Now, val.ToString()));
                val = db.SetLength("running");
                runningSetLength.Add(new Tuple<DateTime, string>(DateTime.Now, val.ToString()));
            };

            for (int i = 0; i < workflows; i++)
            {
                var id = wm.PushWorkflow(workflow);
            }

            var pushDuration = new TimeSpan(DateTime.Now.Ticks - startTime.Ticks);
            pushDone = true;

            WaitHandle.WaitAny(new[] { allDone });

            var duration = new TimeSpan(DateTime.Now.Ticks - startTime.Ticks);
            Console.WriteLine("Done, {0} in {1} ms (push took {2} ms, {3} per second) tasks: {4} churn: {5} tasks per second", workflows, duration.TotalMilliseconds, pushDuration.TotalMilliseconds, workflows / pushDuration.TotalSeconds, workflows * 9, workflows * 9 / duration.TotalSeconds);

            Console.Write("Saving data .. ");

            using (var w = new StreamWriter("queueLengths.txt"))
            {
                for(int i = 0; i < submittedQueueLength.Count; i++)
                {
                    w.WriteLine(new TimeSpan(submittedQueueLength[i].Item1.Ticks - startTime.Ticks).TotalSeconds.ToString() + "\t" + submittedQueueLength[i].Item2 + "\t" + runningSetLength[i].Item2);
                }
            }

            using (var w = new StreamWriter("finalWorkflowCompletion.txt"))
            {
                foreach (var item in completionEvents)
                {
                    w.WriteLine(new TimeSpan(item.Ticks - startTime.Ticks).TotalSeconds);
                }
            }

            using (var w = new StreamWriter("redisWorkflowCompletion.txt"))
            {
                for (int i = 1; i <= workflows; i++)
                {
                    var details = wm.FetchWorkflowInformation(i.ToString());

                    w.WriteLine(new TimeSpan(DateTime.Parse(details.Complete).Ticks - startTime.Ticks).TotalSeconds);
                }
            }

            using (var w = new StreamWriter("redisTaskData.txt"))
            {
                for (int i = 1; i <= workflows; i++)
                {
                    var details = wm.FetchWorkflowInformation(i.ToString());

                    foreach (var task in details.Tasks)
                    {
                        w.WriteLine(
                            new TimeSpan(DateTime.Parse(task.submitted).Ticks - startTime.Ticks).TotalSeconds + "\t" +
                            new TimeSpan(DateTime.Parse(task.complete).Ticks - startTime.Ticks).TotalSeconds);
                    }
                }
            }

            Console.WriteLine("done");

            Console.ReadLine();

            db.ScriptEvaluate("redis.call(\"flushdb\")"); // TODO need to look at timeouts
        }

        private static Common.Workflow CreateTestWorkflow()
        {
            var workflowName = "TestWorkflow";

            var tasks = new List<Task>();
            tasks.Add(new Task { Name = "1", Payload = "payload", Parents = new string[] { }, Children = new string[] { "2" }, Workflow = workflowName });

            for(int i = 2; i < 10; i++)
            {
                tasks.Add(new Task { Name = i.ToString(), Payload = "payload", Parents = new string[] { (i - 1).ToString() }, Children = new string[] { (i + 1).ToString() }, Workflow = workflowName });
            }

            var workflow = new Common.Workflow { Name = workflowName, Tasks = tasks };

            return workflow;
        }
    }
}
