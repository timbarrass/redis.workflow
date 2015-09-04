using StackExchange.Redis;

namespace Redis.Workflow.Common
{
    internal static class Lua
    {
        public static long? PushWorkflow(IDatabase db, string workflowJson, string timestamp)
        {
            var script =
                  "local taskCount = 0\r\n"
                + "local taskList = \"\"\r\n"
                + "local workflowId = redis.call(\"incr\", \"currentWorkflowId\")\r\n"
                + "local tasks = cjson.decode(ARGV[1])[\"Tasks\"]\r\n"
                + "for key, value in next,tasks,nil do\r\n"
                + "taskCount = taskCount + 1\r\n"
                + "local taskId = redis.call(\"incr\", \"currentTaskId\")\r\n"                  
                + "local name = value[\"Name\"]\r\n"
                + "redis.call(\"hset\", \"temp-workflow-tasklookup-\"..workflowId, name, taskId)\r\n"
                + "taskList = taskList..\",\"..taskId\r\n"
                + "end\r\n"
                + "taskList = string.sub(taskList, 2, string.len(taskList))\r\n"
                + "for key, value in next,tasks,nil do\r\n"
                + "local name = value[\"Name\"]\r\n"
                + "local payload = value[\"Payload\"]\r\n"
                + "local taskId = redis.call(\"hget\", \"temp-workflow-tasklookup-\"..workflowId, name)\r\n"
                + "redis.call(\"hset\", \"task-\"..taskId, \"name\", name)\r\n"
                + "redis.call(\"hset\", \"task-\"..taskId, \"workflow\", workflowId)\r\n"
                + "redis.call(\"hset\", \"task-\"..taskId, \"payload\", payload)\r\n"
                + "local parentCount = 0\r\n"
                + "for key2, parentName in next,value[\"Parents\"],nil do\r\n"
                + "local parentTaskId = redis.call(\"hget\", \"temp-workflow-tasklookup-\"..workflowId, parentName)\r\n"
                + "redis.call(\"sadd\", \"parents-\"..taskId, parentTaskId)\r\n"
                + "parentCount = parentCount + 1\r\n"
                + "end\r\n"
                + "if parentCount == 0 then\r\n"
                + "redis.call(\"hset\", \"task-\" .. taskId, \"submitted\", \"" + timestamp + "\")\r\n"
                + "redis.call(\"lpush\", \"submitted\", taskId)\r\n"
                + "redis.call(\"publish\", \"submittedTask\", \"\")"
                + "end\r\n"
                + "for key2, childName in next,value[\"Children\"],nil do\r\n"
                + "local childTaskId = redis.call(\"hget\", \"temp-workflow-tasklookup-\"..workflowId, childName)\r\n"
                + "redis.call(\"lpush\", \"children-\"..taskId, childTaskId)\r\n"
                + "end\r\n"
                + "redis.call(\"sadd\", \"tasks\", taskId)\r\n"
                + "redis.call(\"lpush\", \"workflow-tasks-\"..workflowId, taskId)\r\n"
                + "end\r\n"
                + "redis.call(\"set\", \"workflow-remaining-\"..workflowId, taskCount)\r\n"
                + "redis.call(\"hset\", \"workflow-\"..workflowId, \"name\", workflowId)\r\n" // is this right?
                + "redis.call(\"hset\", \"workflow-\"..workflowId, \"tasks\", taskList)\r\n"
                + "redis.call(\"sadd\", \"workflows\", workflowId)\r\n"
                + "redis.call(\"del\", \"temp-workflow-tasklookup-\"..workflowId)\r\n"
                + "return workflowId"
                ;

            var result = db.ScriptEvaluate(script, new RedisKey[] { }, new RedisValue[] { workflowJson });

            return (long?)result;
        }

        public static void PushTask(IDatabase DB, string task, string timestamp)
        {
            var script =
                  "redis.call(\"hset\", \"task-\" .. ARGV[1], \"submitted\", \"" + timestamp + "\")\r\n"
                + "redis.call(\"lpush\", \"submitted\", ARGV[1])\r\n"
                + "redis.call(\"publish\", \"submittedTask\", \"\")";

            DB.ScriptEvaluate(script, new RedisKey[] { }, new RedisValue[] { task });
        }

        public static void FailTask(IDatabase DB, string task, string timestamp)
        {
            var script =
                  "local runningCount = redis.call(\"srem\", \"running\", ARGV[1])\r\n"
                + "if runningCount == 0 then\r\n"
                + "local abandonedCount = redis.call(\"srem\", \"abandoned\", ARGV[1])\r\n"
                + "if abandonedCount ~= 0 then\r\n"
                + "return\n"
                + "else\r\n"
                + "error(\"Completed task '\"..ARGV[1]..\" but it doesn't seem to be in expected state (running, or abandoned)\")\r\n"
                + "end\r\n"
                + "end\r\n"
                + "redis.call(\"hset\", \"task-\" .. ARGV[1], \"failed\", \"" + timestamp + "\")\r\n"
                + "redis.call(\"sadd\", \"failed\", ARGV[1])\r\n"
                + "local workflow = redis.call(\"hget\", \"task-\"..ARGV[1], \"workflow\")\r\n"
                + "local remaining = redis.call(\"decr\", \"workflow-remaining-\" .. workflow)\r\n"
                + "redis.call(\"lpush\", \"workflowFailed\", workflow)\r\n"
                + "redis.call(\"publish\", \"workflowFailed\", \"\")\r\n"
                ;

            RedisResult result = DB.ScriptEvaluate(script, new RedisKey[] { }, new RedisValue[] { task });
        }

        /// <summary>
        /// </summary>
        /// <param name="DB"></param>
        /// <param name="task"></param>
        /// <param name="timestamp"></param>
        public static void CompleteTask(IDatabase DB, string task, string timestamp)
        {
            var script =
                  "local runningCount = redis.call(\"srem\", \"running\", ARGV[1])\r\n"
                + "if runningCount == 0 then\r\n"
                + "local abandonedCount = redis.call(\"srem\", \"abandoned\", ARGV[1])\r\n"
                + "if abandonedCount ~= 0 then\r\n"
                + "return\r\n"
                + "else\r\n"
                + "error(\"Completed task '\"..ARGV[1]..\" but it doesn't seem to be in expected state (running, or abandoned)\")\r\n"
                + "end\r\n"
                + "end\r\n"
                + "redis.call(\"hset\", \"task-\" .. ARGV[1], \"complete\", \"" + timestamp + "\")\r\n"
                + "redis.call(\"sadd\", \"complete\", ARGV[1])\r\n"
                + "local workflow = redis.call(\"hget\", \"task-\"..ARGV[1], \"workflow\")\r\n"
                + "local remaining = redis.call(\"decr\", \"workflow-remaining-\" .. workflow)\r\n"
                + "if remaining == 0 then\r\n"
                + "redis.call(\"lpush\", \"workflowComplete\", workflow)\r\n"
                + "redis.call(\"publish\", \"workflowComplete\", \"\")\r\n"
                + "return\r\n"
                + "end\r\n"
                + "local child = redis.call(\"rpop\", \"children-\"..ARGV[1])\r\n"
                + "while child do\r\n"
                + "redis.call(\"srem\", \"parents-\" .. child, ARGV[1])\r\n"
                + "local parentCount = redis.call(\"scard\", \"parents-\"..child)\r\n"
                + "if parentCount == 0 then\r\n"
                + "redis.call(\"hset\", \"task-\"..child, \"submitted\", \"" + timestamp + "\")\r\n"
                + "redis.call(\"lpush\", \"submitted\", child)\r\n"
                + "redis.call(\"publish\", \"submittedTask\", \"\")\r\n"
                + "end\r\n"
                + "child = redis.call(\"rpop\", \"children-\"..ARGV[1])\r\n"
                + "end"
                ;

            RedisResult result = DB.ScriptEvaluate(script, new RedisKey[] { }, new RedisValue[] { task });
        }

        public static string PopCompleteWorkflow(IDatabase db)
        {
            var script =
                  "local workflow = redis.call(\"rpop\", \"workflowComplete\")\r\n"
                + "if workflow then\r\n"
                + "return workflow\r\n"
                + "else\r\n"
                + "return ''\r\n"
                + "end"
                ;

            var result = db.ScriptEvaluate(script);

            return (string.IsNullOrEmpty(result.ToString())) ? null : result.ToString();
        }

        public static string PopFailedWorkflow(IDatabase db)
        {
            var script =
                  "local workflow = redis.call(\"rpop\", \"workflowFailed\")\r\n"
                + "if workflow then\r\n"
                + "return workflow\r\n"
                + "else\r\n"
                + "return ''\r\n"
                + "end"
                ;

            var result = db.ScriptEvaluate(script);

            return (string.IsNullOrEmpty(result.ToString())) ? null : result.ToString();
        }

        public static string PopTask(IDatabase db, string timestamp)
        {
            var script =
                  "local task = redis.call(\"rpop\", \"submitted\")\r\n"
                + "if task then\r\n"
                + "redis.call(\"sadd\", \"running\", task)\r\n"
                + "redis.call(\"hset\", \"task-\" .. task, \"running\", \"" + timestamp + "\")"
                + "return task\r\n"
                + "else\r\n"
                + "return ''\r\n"
                + "end";

            var result = db.ScriptEvaluate(script);

            return (string.IsNullOrEmpty(result.ToString())) ? null : result.ToString();
        }

        /// <summary>
        /// Build a pool of ids from which to take. If there's not one spare in the pool, create a new one
        /// At some point, if we clean up after ourselves, we can return ids to pool...
        /// </summary>
        /// <param name="db">Redis db</param>
        /// <returns>Next workflow id</returns>
        public static long? GetWorkflowId(IDatabase db)
        {
            var script =
                  "local count = redis.call(\"llen\", \"workflowIds\")\r\n"
                + "if tonumber(count) == 0 then\r\n"
                + "local nextId = redis.call(\"incr\", \"currentWorkflowId\")\r\n"
                + "redis.call(\"lpush\", \"workflowIds\", nextId)\r\n"
                + "end\r\n"
                + "local id = redis.call(\"rpop\", \"workflowIds\")\r\n"
                + "return id"
                ;

            var result = db.ScriptEvaluate(script);

            return (long?)result;
        }

        /// <summary>
        /// Build a pool of ids from which to take. If there's not one spare in the pool, create a new one
        /// At some point, if we clean up after ourselves, we can return ids to pool...
        /// </summary>
        /// <param name="db">Redis db</param>
        /// <returns>Next task id</returns>
        public static long? GetTaskId(IDatabase db)
        {
            var script =
                  "local count = redis.call(\"llen\", \"taskIds\")\r\n"
                + "if tonumber(count) == 0 then\r\n"
                + "local nextId = redis.call(\"incr\", \"currentTaskId\")\r\n"
                + "redis.call(\"lpush\", \"taskIds\", nextId)\r\n"
                + "end\r\n"
                + "local id = redis.call(\"rpop\", \"taskIds\")\r\n"
                + "return id"
                ;

            var result = db.ScriptEvaluate(script);

            return (long?)result;
        }

        /// <summary>
        /// Cleans up all entries in a workflow. Attempts to do this slightly gracefully -- any
        /// tasks in a running state will be moved to abandoned, so that when they complete it won't
        /// bring down a process.
        /// </summary>
        /// <param name="db"></param>
        /// <param name="workflow"></param>
        public static void CleanupWorkflow(IDatabase db, string workflow)
        {
            var script =
                  "local task = redis.call(\"rpop\", \"workflow-tasks-\"..ARGV[1])\r\n"
                + "while task do\r\n"
                + "redis.call(\"srem\", \"tasks\", task)\r\n"
                + "redis.call(\"del\", \"task-\"..task)\r\n"
                + "redis.call(\"lrem\", \"submitted\", 1, task)\r\n"
                + "redis.call(\"srem\", \"abandoned\", task)\r\n"
                + "redis.call(\"smove\", \"running\", \"abandoned\", task)\r\n"
                + "redis.call(\"srem\", \"complete\", task)\r\n"
                + "redis.call(\"srem\", \"failed\", task)\r\n"
                + "redis.call(\"del\", \"parents-\"..task)\r\n"
                + "redis.call(\"del\", \"children-\"..task)\r\n"
                + "task = redis.call(\"rpop\", \"workflow-tasks-\"..ARGV[1])\r\n"
                + "end\r\n"
                + "redis.call(\"lrem\", \"workflowComplete\", 1, ARGV[1])\r\n"
                + "redis.call(\"lrem\", \"workflowFailed\", 1, ARGV[1])\r\n"
                + "redis.call(\"del\", \"workflow-tasks-\"..ARGV[1])\r\n"
                + "redis.call(\"del\", \"workflow-remaining-\"..ARGV[1])\r\n"
                + "redis.call(\"srem\", \"workflows\", ARGV[1])\r\n"
                + "redis.call(\"del\", \"workflow-\"..ARGV[1])"
                ;

            db.ScriptEvaluate(script, new RedisKey[] { }, new RedisValue[] { workflow });
        }
    }
}
