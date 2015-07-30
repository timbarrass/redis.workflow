using System;
using StackExchange.Redis;

namespace Redis.Workflow.Common
{
    /// <summary>
    /// Provides direct access to redis workflow ops without using Lua. As a result, not thread-
    /// or multi-process safe -- use with caution.
    /// </summary>
    public static class Direct
    {
        public static string PopTask(IDatabase db)
        {
            var task = db.ListRightPop("submitted");

            if (task.Equals(RedisValue.Null)) return null;

            db.SetAdd("running", task);
            db.HashSet("task-" + task, new[] { new HashEntry("running", DateTime.Now.ToString("dd/MM/yy HH:mm:ss")) });

            Console.WriteLine("popped: " + task);

            return task;
        }

        public static long GetTaskId(IDatabase db)
        {
            if (db.SetLength("taskIds").Equals(0))
            {
                var tempId = db.HashIncrement("ids", "currentTaskId");
                db.SetAdd("taskIds", tempId);
            }

            var taskId = (long)db.SetPop("taskIds");

            return taskId;
        }

        public static long GetWorkflowId(IDatabase db)
        {
            if (db.SetLength("workflowIds").Equals(0))
            {
                var tempId = db.HashIncrement("ids", "currentWorkflowId");
                db.SetAdd("workflowIds", tempId);
            }

            var workflowId = (long)db.SetPop("workflowIds");

            return workflowId;
        }

        public static void PushTask(IDatabase db, string task)
        {
            db.HashSet("task-" + task, new[] { new HashEntry("submitted", DateTime.Now.ToString("dd/MM/yy HH:mm:ss")) });

            db.ListLeftPush("submitted", task);

            db.Publish("submittedTask", "");

            Console.WriteLine("pushed: " + task);
        }

        public static void CompleteTask(IDatabase db, string task)
        {
            db.SetRemove("running", task);
            db.HashSet("task-" + task, new[] { new HashEntry("complete", DateTime.Now.ToString("dd/MM/yy HH:mm:ss")) });
            db.SetAdd("complete", task);

            var workflow = db.HashGet("task-" + task, "workflow");
            var remaining = db.HashDecrement("workflow-" + workflow, "tasksRemaining");
            if (remaining.Equals(0))
            {
                db.Publish("workflowComplete", workflow);

                return;
            }

            var children = db.SetMembers("children-" + task);
            foreach (var child in children)
            {
                db.SetRemove("parents-" + child, task);
                if (db.SetLength("parents-" + child).Equals(0))
                {
                    Direct.PushTask(db, child);
                }
            }

            Console.WriteLine("completed: " + task);
        }

    }
}
