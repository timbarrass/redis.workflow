﻿using StackExchange.Redis;
using System.Collections.Generic;

namespace Redis.Workflow.Common
{
    internal class Lua
    {
        private static readonly string _pushWorkflowScript =
              "local taskCount = 0\r\n"
            + "local taskList = \"\"\r\n"
            + "local workflowId = redis.call(\"incr\", \"currentWorkflowId\")\r\n"
            + "local tasks = cjson.decode(@workflowJson)[\"Tasks\"]\r\n"
            + "for key, value in next,tasks,nil do\r\n"
            + "taskCount = taskCount + 1\r\n"
            + "local taskId = redis.call(\"incr\", \"currentTaskId\")\r\n"
            + "local name = value[\"Name\"]\r\n"
            + "redis.call(\"hset\", \"workflow:\"..workflowId..\":tasklookup\", name, taskId)\r\n"
            + "taskList = taskList..\",\"..taskId\r\n"
            + "end\r\n"
            + "taskList = string.sub(taskList, 2, string.len(taskList))\r\n"
            + "for key, value in next,tasks,nil do\r\n"
            + "local name = value[\"Name\"]\r\n"
            + "local payload = value[\"Payload\"]\r\n"
            + "local taskId = redis.call(\"hget\", \"workflow:\"..workflowId..\":tasklookup\", name)\r\n"
            + "redis.call(\"hset\", \"task:\"..taskId, \"name\", name)\r\n"
            + "redis.call(\"hset\", \"task:\"..taskId, \"workflow\", workflowId)\r\n"
            + "redis.call(\"hset\", \"task:\"..taskId, \"payload\", payload)\r\n"
            + "local parentCount = 0\r\n"
            + "for key2, parentName in next,value[\"Parents\"],nil do\r\n"
            + "local parentTaskId = redis.call(\"hget\", \"workflow:\"..workflowId..\":tasklookup\", parentName)\r\n"
            + "redis.call(\"sadd\", \"parents:\"..taskId, parentTaskId)\r\n"
            + "parentCount = parentCount + 1\r\n"
            + "end\r\n"
            + "if parentCount == 0 then\r\n"
            + "redis.call(\"hset\", \"task:\" .. taskId, \"submitted\", @timestamp)\r\n"
            + "redis.call(\"lpush\", \"submitted\", taskId)\r\n"
            + "redis.call(\"publish\", \"submittedTask\", \"\")"
            + "end\r\n"
            + "for key2, childName in next,value[\"Children\"],nil do\r\n"
            + "local childTaskId = redis.call(\"hget\", \"workflow:\"..workflowId..\":tasklookup\", childName)\r\n"
            + "redis.call(\"lpush\", \"children:\"..taskId, childTaskId)\r\n"
            + "end\r\n"
            + "redis.call(\"sadd\", \"tasks\", taskId)\r\n"
            + "redis.call(\"lpush\", \"tasks:\"..workflowId, taskId)\r\n"
            + "end\r\n"
            + "redis.call(\"set\", \"remaining:\"..workflowId, taskCount)\r\n"
            + "redis.call(\"hset\", \"workflow:\"..workflowId, \"name\", workflowId)\r\n" // is this right?
            + "redis.call(\"hset\", \"workflow:\"..workflowId, \"tasks\", taskList)\r\n"
            + "redis.call(\"sadd\", \"workflows\", workflowId)\r\n"
            + "redis.call(\"del\", \"workflow:\"..workflowId..\":tasklookup\")\r\n"
            + "return workflowId"
            ;

        private static readonly string _pushTaskScript =
              "redis.call(\"hset\", \"task:\" .. @taskId, \"submitted\", @timestamp)\r\n"
            + "redis.call(\"lpush\", \"submitted\", @taskId)\r\n"
            + "redis.call(\"publish\", \"submittedTask\", \"\")"
            ;

        private static readonly string _failTaskScript =
                  "local runningCount = redis.call(\"srem\", \"running\", @taskId)\r\n"
                + "if runningCount == 0 then\r\n"
                + "local abandonedCount = redis.call(\"srem\", \"abandoned\", @taskId)\r\n"
                + "if abandonedCount ~= 0 then\r\n"
                + "return\n"
                + "else\r\n"
                + "error(\"Completed task '\"..@taskId..\" but it doesn't seem to be in expected state (running, or abandoned)\")\r\n"
                + "end\r\n"
                + "end\r\n"
                + "redis.call(\"hset\", \"task:\" .. @taskId, \"failed\", \"@timestamp\")\r\n"
                + "redis.call(\"sadd\", \"failed\", @taskId)\r\n"
                + "local workflow = redis.call(\"hget\", \"task:\"..@taskId, \"workflow\")\r\n"
                + "local remaining = redis.call(\"decr\", \"remaining:\"..workflow)\r\n"
                + "redis.call(\"lpush\", \"workflowFailed\", workflow)\r\n"
                + "redis.call(\"publish\", \"workflowFailed\", \"\")\r\n"
                ;

        private static readonly string _completeTaskScript =
                  "local runningCount = redis.call(\"srem\", \"running\", @taskId)\r\n"
                + "if runningCount == 0 then\r\n"
                + "local abandonedCount = redis.call(\"srem\", \"abandoned\", @taskId)\r\n"
                + "if abandonedCount ~= 0 then\r\n"
                + "return\r\n"
                + "else\r\n"
                + "error(\"Completed task '\"..@taskId..\" but it doesn't seem to be in expected state (running, or abandoned)\")\r\n"
                + "end\r\n"
                + "end\r\n"
                + "redis.call(\"hset\", \"task:\" .. @taskId, \"complete\", @timestamp)\r\n"
                + "redis.call(\"srem\", \"responsible:\"..@responsible, @taskId)\r\n"
                + "redis.call(\"sadd\", \"complete\", @taskId)\r\n"
                + "local workflow = redis.call(\"hget\", \"task:\"..@taskId, \"workflow\")\r\n"
                + "local remaining = redis.call(\"decr\", \"remaining:\"..workflow)\r\n"
                + "if remaining == 0 then\r\n"
                + "redis.call(\"lpush\", \"workflowComplete\", workflow)\r\n"
                + "redis.call(\"publish\", \"workflowComplete\", \"\")\r\n"
                + "return\r\n"
                + "end\r\n"
                + "local child = redis.call(\"rpop\", \"children:\"..@taskId)\r\n"
                + "while child do\r\n"
                + "redis.call(\"srem\", \"parents:\" .. child, @taskId)\r\n"
                + "local parentCount = redis.call(\"scard\", \"parents:\"..child)\r\n"
                + "if parentCount == 0 then\r\n"
                + "redis.call(\"hset\", \"task:\"..child, \"submitted\", @timestamp)\r\n"
                + "redis.call(\"lpush\", \"submitted\", child)\r\n"
                + "redis.call(\"publish\", \"submittedTask\", \"\")\r\n"
                + "end\r\n"
                + "child = redis.call(\"rpop\", \"children:\"..@taskId)\r\n"
                + "end"
                ;

        private static readonly string _popCompleteWorkflowScript =
                  "local workflow = redis.call(\"rpop\", \"workflowComplete\")\r\n"
                + "if workflow then\r\n"
                + "return workflow\r\n"
                + "else\r\n"
                + "return ''\r\n"
                + "end"
                ;

        private static readonly string _popFailedWorkflowScript =
                  "local workflow = redis.call(\"rpop\", \"workflowFailed\")\r\n"
                + "if workflow then\r\n"
                + "return workflow\r\n"
                + "else\r\n"
                + "return ''\r\n"
                + "end"
                ;

        private static readonly string _popTaskScript =
                  "local task = redis.call(\"rpop\", \"submitted\")\r\n"
                + "if task then\r\n"
                + "redis.call(\"sadd\", \"running\", task)\r\n"
                + "redis.call(\"hset\", \"task:\" .. task, \"running\", @timestamp)"
                + "redis.call(\"hset\", \"task:\" .. task, \"lastKnownResponsible\", @responsible)"
                + "redis.call(\"sadd\", \"responsible:\"..@responsible, task)\r\n"
                + "return task\r\n"
                + "else\r\n"
                + "return ''\r\n"
                + "end";

        private static readonly string _getWorkflowIdScript =
                  "local count = redis.call(\"llen\", \"workflowIds\")\r\n"
                + "if tonumber(count) == 0 then\r\n"
                + "local nextId = redis.call(\"incr\", \"currentWorkflowId\")\r\n"
                + "redis.call(\"lpush\", \"workflowIds\", nextId)\r\n"
                + "end\r\n"
                + "local id = redis.call(\"rpop\", \"workflowIds\")\r\n"
                + "return id"
                ;

        private static readonly string _getTaskIdScript =
                  "local count = redis.call(\"llen\", \"taskIds\")\r\n"
                + "if tonumber(count) == 0 then\r\n"
                + "local nextId = redis.call(\"incr\", \"currentTaskId\")\r\n"
                + "redis.call(\"lpush\", \"taskIds\", nextId)\r\n"
                + "end\r\n"
                + "local id = redis.call(\"rpop\", \"taskIds\")\r\n"
                + "return id"
                ;

        private static readonly string _cleanupWorkflowScript =
                  "local task = redis.call(\"rpop\", \"tasks:\"..@workflowId)\r\n"
                + "while task do\r\n"
                + "redis.call(\"srem\", \"tasks\", task)\r\n"
                + "redis.call(\"del\", \"task:\"..task)\r\n"
                + "redis.call(\"lrem\", \"submitted\", 1, task)\r\n"
                + "redis.call(\"srem\", \"abandoned\", task)\r\n"
                + "redis.call(\"smove\", \"running\", \"abandoned\", task)\r\n"
                + "redis.call(\"srem\", \"complete\", task)\r\n"
                + "redis.call(\"srem\", \"failed\", task)\r\n"
                + "redis.call(\"del\", \"parents:\"..task)\r\n"
                + "redis.call(\"del\", \"children:\"..task)\r\n"
                + "task = redis.call(\"rpop\", \"tasks:\"..@workflowId)\r\n"
                + "end\r\n"
                + "redis.call(\"lrem\", \"workflowComplete\", 1, @workflowId)\r\n"
                + "redis.call(\"lrem\", \"workflowFailed\", 1, @workflowId)\r\n"
                + "redis.call(\"del\", \"tasks:\"..@workflowId)\r\n"
                + "redis.call(\"del\", \"remaining:\"..@workflowId)\r\n"
                + "redis.call(\"srem\", \"workflows\", @workflowId)\r\n"
                + "redis.call(\"del\", \"workflow:\"..@workflowId)"
                ;

        private readonly Dictionary<string, LoadedLuaScript> _scripts = new Dictionary<string, LoadedLuaScript>();

        public void LoadScripts(IDatabase db, IServer srv)
        {
            var scripts = new Dictionary<string, string>
            {
                { "pushWorkflow",        _pushWorkflowScript },
                { "pushTask",            _pushTaskScript },
                { "popTask",             _popTaskScript },
                { "completeTask",        _completeTaskScript },
                { "failTask",            _failTaskScript },
                { "popFailedWorkflow",   _popFailedWorkflowScript },
                { "popCompleteWorkflow", _popCompleteWorkflowScript },
                { "getWorkflowId",       _getWorkflowIdScript },
                { "getTaskId",           _getTaskIdScript },
                { "cleanupWorkflow",     _cleanupWorkflowScript },
            };

            foreach (var scriptName in scripts.Keys)
            {
                var prepped = LuaScript.Prepare(scripts[scriptName]);

                _scripts.Add(scriptName, prepped.Load(srv));
            }
        }

        public long? PushWorkflow(IDatabase db, string workflowJson, string timestamp)
        {
            var arguments = new { workflowJson = workflowJson, timestamp = timestamp };

            var result = _scripts["pushWorkflow"].Evaluate(db, arguments);

            return (long?)result;
        }

        public void PushTask(IDatabase db, string task, string timestamp)
        {
            var arguments = new { taskId = task, timestamp = timestamp };

            _scripts["pushTask"].Evaluate(db, arguments);
        }

        public void FailTask(IDatabase db, string task, string timestamp)
        {
            var arguments = new { taskId = task, timestamp = timestamp };

            _scripts["failTask"].Evaluate(db, arguments);
        }

        /// <summary>
        /// </summary>
        /// <param name="db"></param>
        /// <param name="task"></param>
        /// <param name="timestamp"></param>
        public void CompleteTask(IDatabase db, string task, string timestamp, string responsible)
        {
            var arguments = new { taskId = task, timestamp = timestamp, responsible = responsible };

            _scripts["completeTask"].Evaluate(db, arguments);
        }

        public string PopCompleteWorkflow(IDatabase db)
        {
            var result = _scripts["popCompleteWorkflow"].Evaluate(db);

            return (string.IsNullOrEmpty(result.ToString())) ? null : result.ToString();
        }

        public string PopFailedWorkflow(IDatabase db)
        {
            var result = _scripts["popFailedWorkflow"].Evaluate(db);

            return (string.IsNullOrEmpty(result.ToString())) ? null : result.ToString();
        }

        public string PopTask(IDatabase db, string timestamp, string responsible)
        {
            var arguments = new { timestamp = timestamp, responsible = responsible };

            var result = _scripts["popTask"].Evaluate(db, arguments);

            return (string.IsNullOrEmpty(result.ToString())) ? null : result.ToString();
        }

        /// <summary>
        /// Build a pool of ids from which to take. If there's not one spare in the pool, create a new one
        /// At some point, if we clean up after ourselves, we can return ids to pool...
        /// </summary>
        /// <param name="db">Redis db</param>
        /// <returns>Next workflow id</returns>
        public long? GetWorkflowId(IDatabase db)
        {
            var result = _scripts["getWorkflowId"].Evaluate(db);

            return (long?)result;
        }

        /// <summary>
        /// Build a pool of ids from which to take. If there's not one spare in the pool, create a new one
        /// At some point, if we clean up after ourselves, we can return ids to pool...
        /// </summary>
        /// <param name="db">Redis db</param>
        /// <returns>Next task id</returns>
        public long? GetTaskId(IDatabase db)
        {
            var result = _scripts["getTaskId"].Evaluate(db);

            return (long?)result;
        }

        /// <summary>
        /// Cleans up all entries in a workflow. Attempts to do this slightly gracefully -- any
        /// tasks in a running state will be moved to abandoned, so that when they complete it won't
        /// bring down a process.
        /// </summary>
        /// <param name="db"></param>
        /// <param name="workflow"></param>
        public void CleanupWorkflow(IDatabase db, string workflow)
        {
            var arguments = new { workflowId = workflow };

            _scripts["cleanupWorkflow"].Evaluate(db, arguments);
        }
    }
}
