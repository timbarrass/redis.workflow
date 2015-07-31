using System;
using StackExchange.Redis;

namespace Redis.Workflow.Common
{
    public static class Lua
    {
        public static void HelloWorld(IDatabase db)
        {
            var script = "local msg = \"Hello World\" " + "return msg";

            var result = db.ScriptEvaluate(script);

            Console.WriteLine(result);
        }

        public static void PushTask(IDatabase DB, string task, string timestamp)
        {
            var script =
                  "redis.call(\"hset\", \"task-\" .. ARGV[1], \"submitted\", \"" + timestamp + "\")\r\n"
                + "redis.call(\"lpush\", \"submitted\", ARGV[1])\r\n"
                + "redis.call(\"publish\", \"submittedTask\", \"\")";

            DB.ScriptEvaluate(script, new RedisKey[] { }, new RedisValue[] { task });
        }

        public static void CompleteTask(IDatabase DB, string task, string timestamp)
        {
            // crossed a thread boundary here .. handle with Task exceptions, better

            try
            {
                var script =
                      "redis.call(\"srem\", \"running\", ARGV[1])\r\n"
                    + "redis.call(\"hset\", \"task-\" .. ARGV[1], \"complete\", \"" + timestamp + "\")\r\n"
                    + "redis.call(\"sadd\", \"complete\", ARGV[1])\r\n"
                    + "local workflow = redis.call(\"hget\", \"task-\"..ARGV[1], \"workflow\")\r\n"
                    + "local remaining = redis.call(\"decr\", \"workflow-remaining-\" .. workflow)\r\n"
                    + "if remaining == 0 then\r\n"
                    + "redis.call(\"publish\", \"workflowComplete\", workflow)\r\n"
                    + "return\r\n"
                    + "end\r\n"
                    + "print(\"children-\"..ARGV[1])\r\n"
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
            catch (Exception ex)
            {
                Console.WriteLine(ex);

                throw;
            }
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
                + "print(\"ids: \"..count)\r\n"
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
                + "print(\"task ids: \"..count)\r\n"
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
    }
}
