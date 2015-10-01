using Redis.Workflow.Common;
using StackExchange.Redis;
using System;
using System.Collections.Generic;
using System.IO;
using System.Net;
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
            bool pushDone = false;
            int completed = 0;
            var countLock = new object();
            var allDone = new ManualResetEvent(false);

            var completionEvents = new List<DateTime>();
            var submittedQueueLength = new List<Tuple<DateTime, string>>();
            var runningSetLength = new List<Tuple<DateTime, string>>();

            var mux = ConnectMultiplexer();

            var db = ConnectAndFlushDatabase(mux);

            var eh = CreateEventHandler(allDone);

            var th = CreateTaskHandler();

            var wh = new WorkflowHandler();
            wh.WorkflowComplete += (s, id) => { lock (countLock) { completed++; Console.Write("pushDone: " + pushDone + " done: " + completed + "\r"); completionEvents.Add(DateTime.Now); if (completed == workflows) allDone.Set(); } };
            wh.WorkflowFailed += (s, id) => { lock (countLock) { completed++; Console.Write("pushDone: " + pushDone + " done: " + completed + "\r"); completionEvents.Add(DateTime.Now); if (completed == workflows) allDone.Set(); } };

            CreateQueueLengthSampler(submittedQueueLength, runningSetLength, db);

            var wm = BuildWorkflowManagers(mux, th, wh, eh);

            var workflow = CreateTestWorkflow();

            var startTime = DateTime.Now;

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

            ExportQueueLengths(submittedQueueLength, runningSetLength, startTime);

            ExportFinalWorkflowCompletion(completionEvents, startTime);

            ExportWorkflowCompletion(workflows, wm, startTime);

            ExportTaskData(workflows, wm, startTime);

            Console.WriteLine("done");

            Console.ReadLine();

            db.ScriptEvaluate("redis.call(\"flushdb\")"); // TODO need to look at timeouts
        }

        private static void ExportTaskData(int workflows, WorkflowManagement wm, DateTime startTime)
        {
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
        }

        private static void ExportWorkflowCompletion(int workflows, WorkflowManagement wm, DateTime startTime)
        {
            using (var w = new StreamWriter("redisWorkflowCompletion.txt"))
            {
                for (int i = 1; i <= workflows; i++)
                {
                    var details = wm.FetchWorkflowInformation(i.ToString());

                    w.WriteLine(new TimeSpan(DateTime.Parse(details.Complete).Ticks - startTime.Ticks).TotalSeconds);
                }
            }
        }

        private static void ExportFinalWorkflowCompletion(List<DateTime> completionEvents, DateTime startTime)
        {
            using (var w = new StreamWriter("finalWorkflowCompletion.txt"))
            {
                foreach (var item in completionEvents)
                {
                    w.WriteLine(new TimeSpan(item.Ticks - startTime.Ticks).TotalSeconds);
                }
            }
        }

        private static void ExportQueueLengths(List<Tuple<DateTime, string>> submittedQueueLength, List<Tuple<DateTime, string>> runningSetLength, DateTime startTime)
        {
            using (var w = new StreamWriter("queueLengths.txt"))
            {
                for (int i = 0; i < submittedQueueLength.Count; i++)
                {
                    w.WriteLine(new TimeSpan(submittedQueueLength[i].Item1.Ticks - startTime.Ticks).TotalSeconds.ToString() + "\t" + submittedQueueLength[i].Item2 + "\t" + runningSetLength[i].Item2);
                }
            }
        }

        private static void CreateQueueLengthSampler(List<Tuple<DateTime, string>> submittedQueueLength, List<Tuple<DateTime, string>> runningSetLength, IDatabase db)
        {
            var queueSampler = new System.Timers.Timer() { Interval = 10000, AutoReset = true, Enabled = true };
            queueSampler.Elapsed += (s, e) =>
            {
                var val = db.ListLength("submitted");
                submittedQueueLength.Add(new Tuple<DateTime, string>(DateTime.Now, val.ToString()));
                val = db.SetLength("running");
                runningSetLength.Add(new Tuple<DateTime, string>(DateTime.Now, val.ToString()));
            };
        }

        private static EventHandler<Exception> CreateEventHandler(ManualResetEvent allDone)
        {
            return (s, e) => { Console.WriteLine("EXCEPTION: " + e.Message + ": " + e.StackTrace); allDone.Set(); };
        }

        private static TaskHandler CreateTaskHandler()
        {
            return new TaskHandler();
        }

        private static WorkflowManagement BuildWorkflowManagers(ConnectionMultiplexer mux, TaskHandler th, WorkflowHandler wh, EventHandler<Exception> eh)
        {
            var wm = new WorkflowManagement(mux, th, wh, "sampleApp", eh);

            var extraProcessors = new List<WorkflowManagement>();
            for (int i = 0; i < 10; i++)
            {
                extraProcessors.Add(new WorkflowManagement(mux, th, wh, "processor-" + i, eh));
            }

            return wm;
        }

        private static IDatabase ConnectAndFlushDatabase(ConnectionMultiplexer mux)
        {
            var db = mux.GetDatabase();
            db.ScriptEvaluate("redis.call(\"flushdb\")");
            return db;
        }

        private static ConnectionMultiplexer ConnectMultiplexer()
        {
            var mux = ConnectionMultiplexer.Connect("localhost,syncTimeout=3000,responseTimeout=2000");
            //var mux = ConnectionMultiplexer.Connect("localhost");
            mux.PreserveAsyncOrder = false;
            return mux;
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
