using StackExchange.Redis;
using System.Collections.Generic;

namespace Redis.Workflow.Common
{
    internal class Lua : ILua
    {
        private static readonly string _pushWorkflowScript =
              "local taskCount = 0\r\n"
            + "local taskList = \"\"\r\n"
            + "local workflowId = redis.call(\"incr\", \"currentWorkflowId\")\r\n"
            + "local tasks = cjson.decode(@workflowJson)[\"Tasks\"]\r\n"
            + "for key, value in next,tasks,nil do\r\n"
            + "  taskCount = taskCount + 1\r\n"
            + "  local taskId = redis.call(\"incr\", \"currentTaskId\")\r\n"
            + "  local name = value[\"Name\"]\r\n"
            + "  redis.call(\"hset\", \"workflow:\"..workflowId..\":tasklookup\", name, taskId)\r\n"
            + "  taskList = taskList..\",\"..taskId\r\n"
            + "end\r\n"
            + "taskList = string.sub(taskList, 2, string.len(taskList))\r\n"
            + "for key, value in next,tasks,nil do\r\n"
            + "  local name = value[\"Name\"]\r\n"
            + "  local payload = value[\"Payload\"]\r\n"
            + "  local taskType = value[\"Type\"]\r\n"
            + "  local priority = value[\"Priority\"]\r\n"
            + "  local taskId = redis.call(\"hget\", \"workflow:\"..workflowId..\":tasklookup\", name)\r\n"
            + "  redis.call(\"hset\", \"task:\"..taskId, \"name\", name)\r\n"
            + "  redis.call(\"hset\", \"task:\"..taskId, \"workflow\", workflowId)\r\n"
            + "  redis.call(\"hset\", \"task:\"..taskId, \"payload\", payload)\r\n"
            + "  redis.call(\"hset\", \"task:\"..taskId, \"type\", taskType)\r\n"
            + "  redis.call(\"hset\", \"task:\"..taskId, \"priority\", priority)\r\n"
            + "  local parentCount = 0\r\n"
            + "  for key2, parentName in next,value[\"Parents\"],nil do\r\n"
            + "    local parentTaskId = redis.call(\"hget\", \"workflow:\"..workflowId..\":tasklookup\", parentName)\r\n"
            + "    redis.call(\"sadd\", \"parents:\"..taskId, parentTaskId)\r\n"
            + "    parentCount = parentCount + 1\r\n"
            + "  end\r\n"
            + "  if parentCount == 0 then\r\n"
            + "    redis.call(\"hset\", \"task:\" .. taskId, \"previousState\", \"none\")\r\n"
            + "    redis.call(\"hset\", \"task:\" .. taskId, \"submitted\", @timestamp)\r\n"
            + "    redis.call(\"sadd\", \"submitted:\"..workflowId, taskId)\r\n"
            + "    if taskType ~= \"\" then\r\n"
            + "      redis.call(\"zadd\", \"submitted:\"..taskType, priority, taskId)\r\n"
            + "      redis.call(\"publish\", \"submittedTask:\"..taskType, \"\")\r\n"
            + "    else\r\n"
            + "      redis.call(\"zadd\", \"submitted\", priority, taskId)\r\n"
            + "      redis.call(\"publish\", \"submittedTask\", \"\")\r\n"
            + "    end\r\n"
            + "  end\r\n"
            + "  for key2, childName in next,value[\"Children\"],nil do\r\n"
            + "    local childTaskId = redis.call(\"hget\", \"workflow:\"..workflowId..\":tasklookup\", childName)\r\n"
            + "    redis.call(\"lpush\", \"children:\"..taskId, childTaskId)\r\n"
            + "  end\r\n"
            + "  redis.call(\"sadd\", \"tasks\", taskId)\r\n"
            + "  redis.call(\"lpush\", \"tasks:\"..workflowId, taskId)\r\n"
            + "end\r\n"
            + "redis.call(\"set\", \"remaining:\"..workflowId, taskCount)\r\n"
            + "redis.call(\"hset\", \"workflow:\"..workflowId, \"name\", workflowId)\r\n" // is this right?
            + "redis.call(\"hset\", \"workflow:\"..workflowId, \"tasks\", taskList)\r\n"
            + "redis.call(\"sadd\", \"workflows\", workflowId)\r\n"
            + "redis.call(\"del\", \"workflow:\"..workflowId..\":tasklookup\")\r\n"
            + "return workflowId"
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
                + "redis.call(\"hset\", \"task:\" .. @taskId, \"previousState\", \"running\")\r\n"
                + "redis.call(\"sadd\", \"failed\", @taskId)\r\n"
                + "local workflow = redis.call(\"hget\", \"task:\"..@taskId, \"workflow\")\r\n"
                + "redis.call(\"srem\", \"running:\"..workflow, @taskId)\r\n"
                + "local remaining = redis.call(\"decr\", \"remaining:\"..workflow)\r\n"
                + "redis.call(\"lpush\", \"workflowFailed\", workflow)\r\n"
                + "redis.call(\"publish\", \"workflowFailed\", \"\")\r\n"
                ;

        private static readonly string _completeTaskScript =
                  "local paused = ''\r\n"
                + "print(\"Completing task \"..@taskId)\r\n"
                + "local runningCount = redis.call(\"srem\", \"running\", @taskId)\r\n"
                + "if runningCount == 0 then\r\n"
                + "  local abandonedCount = redis.call(\"srem\", \"abandoned\", @taskId)\r\n"
                + "  if abandonedCount ~= 0 then\r\n"
                + "    return\r\n"
                + "  else\r\n"
                + "    paused = redis.call(\"srem\", \"paused\", @taskId)\r\n"
                + "    if paused ~= 1 then\r\n"
                + "      error(\"Completed task '\"..@taskId..\" but it doesn't seem to be in expected state (running, or abandoned, or paused)\")\r\n"
                + "    end\r\n"
                + "  end\r\n"
                + "end\r\n"                
                + "redis.call(\"hset\", \"task:\" .. @taskId, \"complete\", @timestamp)\r\n"
                + "redis.call(\"hset\", \"task:\" .. @taskId, \"previousState\", \"running\")\r\n"
                + "redis.call(\"srem\", \"responsible:\"..@responsible, @taskId)\r\n"
                + "redis.call(\"sadd\", \"complete\", @taskId)\r\n"
                + "local workflow = redis.call(\"hget\", \"task:\"..@taskId, \"workflow\")\r\n"
                + "redis.call(\"srem\", \"paused:\"..workflow, @taskId)\r\n"
                + "redis.call(\"srem\", \"running:\"..workflow, @taskId)\r\n"
                + "local remaining = redis.call(\"decr\", \"remaining:\"..workflow)\r\n"
                + "if remaining == 0 then\r\n"
                + "  redis.call(\"lpush\", \"workflowComplete\", workflow)\r\n"
                + "  redis.call(\"hset\", \"workflow:\"..workflow, \"complete\", @timestamp)\r\n"
                + "  redis.call(\"publish\", \"workflowComplete\", \"\")\r\n"
                + "  return\r\n"
                + "end\r\n"
                + "local child = redis.call(\"rpop\", \"children:\"..@taskId)\r\n"
                + "while child do\r\n"
                + "  redis.call(\"srem\", \"parents:\" .. child, @taskId)\r\n"
                + "  local parentCount = redis.call(\"scard\", \"parents:\"..child)\r\n"
                + "  if parentCount == 0 then\r\n"
                + "    if paused == 1 then\r\n"
                + "      redis.call(\"hset\", \"task:\"..child, \"paused\", @timestamp)\r\n"
                + "      redis.call(\"sadd\", \"paused\", child)\r\n"
                + "      redis.call(\"sadd\", \"paused:\"..workflow, child)\r\n"
                + "    else\r\n"
                + "      redis.call(\"hset\", \"task:\"..child, \"submitted\", @timestamp)\r\n"
                + "      redis.call(\"hset\", \"task:\" .. @taskId, \"previousState\", \"none\")\r\n"
                + "      redis.call(\"sadd\", \"submitted:\"..workflow, child)\r\n"
                + "      local taskType = redis.call(\"hget\", \"task:\"..child, \"type\")\r\n"
                + "      local priority = redis.call(\"hget\", \"task:\"..child, \"priority\")\r\n"
                + "      if taskType ~= \"\" then\r\n"
                + "        print(\"Submit on completion: \"..@taskId..\" \"..child)\r\n"
                + "        redis.call(\"zadd\", \"submitted:\"..taskType, priority, child)\r\n"
                + "        redis.call(\"publish\", \"submittedTask:\"..taskType, \"\")\r\n"
                + "      else\r\n"
                + "        redis.call(\"zadd\", \"submitted\", priority, child)\r\n"
                + "        redis.call(\"publish\", \"submittedTask\", \"\")\r\n"
                + "      end\r\n"
                + "    end\r\n"
                + "    child = redis.call(\"rpop\", \"children:\"..@taskId)\r\n"
                + "  end\r\n"
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
                  "local ret = { }\r\n"
                + "local typeSuffix = ''\r\n"
                + "if @taskType ~= '' then\r\n"
                + "typeSuffix = \":\"..@taskType\r\n"
                + "end\r\n"
                + "local tasks = redis.call(\"zrange\", \"submitted\"..typeSuffix, \"0\", \"100\")\r\n"
                + "local task = tasks[1]\r\n"
                + "redis.call(\"zrem\", \"submitted\"..typeSuffix, tasks[1])\r\n"
                + "if task then\r\n"
                + "redis.call(\"sadd\", \"running\", task)\r\n"
                + "redis.call(\"hset\", \"task:\" .. task, \"running\", @timestamp)"
                + "redis.call(\"hset\", \"task:\" .. task, \"previousState\", \"submitted\"..typeSuffix)\r\n"
                + "redis.call(\"hset\", \"task:\" .. task, \"lastKnownResponsible\", @responsible)"
                + "redis.call(\"sadd\", \"responsible:\"..@responsible, task)\r\n"
                + "local workflow = redis.call(\"hget\", \"task:\"..task, \"workflow\")\r\n"
                + "redis.call(\"srem\", \"submitted:\"..workflow, task)\r\n"
                + "redis.call(\"sadd\", \"running:\"..workflow, task)\r\n"
                + "ret[1] = task\r\n"
                + "ret[2] = redis.call(\"hget\", \"task:\"..task, \"payload\")\r\n"
                + "else\r\n"
                + "ret[1] = ''\r\n"
                + "ret[2] = ''\r\n"
                + "end\r\n"
                + "return ret"
                ;

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

        // TODO: this not working quite right -- need to check detail
        private static readonly string _cleanupWorkflowScript =
                  "local task = redis.call(\"rpop\", \"tasks:\"..@workflowId)\r\n"
                + "while task do\r\n"
                + "redis.call(\"srem\", \"tasks\", task)\r\n"
                + "redis.call(\"del\", \"task:\"..task)\r\n"
                + "redis.call(\"zrem\", \"submitted\", 1, task)\r\n"
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
                + "redis.call(\"del\", \"submitted:\"..@workflowId)\r\n"
                + "redis.call(\"srem\", \"workflows\", @workflowId)\r\n"
                + "redis.call(\"del\", \"workflow:\"..@workflowId)"
                ;

        // Here it'll just remove any running tasks from the running set. Might think that they actually
        // need to get transitioned to abandoned, so that whichever component that's running them won't
        // hit an exception when it completes the task. However -- core assumption is that this is only
        // called by the component that started running the tasks, and that that component has died and
        // calls this on restart.
        private static readonly string _resetTasksForResponsibleComponentScript =
              "local tasks = redis.call(\"smembers\", \"responsible:\"..@responsible)\r\n"
            + "for key, task in next,tasks,nil do\r\n"
            + "  print(\"key \"..key..\" value \"..task)"
            + "  redis.call(\"hset\", \"task:\" .. task, \"submitted\", @timestamp)\r\n"
            + "  local taskType = redis.call(\"hget\", \"task:\"..task, \"type\")\r\n"
            + "  local priority = redis.call(\"hget\", \"task:\"..task, \"priority\")\r\n"
            + "  if taskType == \"\" then\r\n"
            + "    redis.call(\"zadd\", \"submitted\", priority, task)\r\n"
            + "    redis.call(\"publish\", \"submittedTask\", \"\")"
            + "  else\r\n"
            + "    redis.call(\"zadd\", \"submitted:\"..taskType, priority, task)\r\n"
            + "    redis.call(\"publish\", \"submittedTask:\"..taskType, \"\")"
            + "  end\r\n"
            + "  local workflow = redis.call(\"hget\", \"task:\"..task, \"workflow\")\r\n"
            + "  redis.call(\"sadd\", \"submitted:\"..workflow, task)\r\n"
            + "  redis.call(\"srem\", \"running:\"..workflow, task)\r\n"
            + "  redis.call(\"srem\", \"running\", task)\r\n"
            + "  redis.call(\"hset\", \"task:\" .. task, \"previousState\", \"none\")\r\n"
            + "end\r\n"
            + "redis.call(\"del\", \"responsible:\"..@responsible)\r\n"
            + "return tasks\r\n"
            ;

        private static readonly string _listTasksForResponsibleComponentScript =
              "local tasks = redis.call(\"smembers\", \"responsible:\"..@responsible)\r\n"
            + "return tasks\r\n"
            ;

        private static readonly string _fetchWorkflowInformationScript =
              "local allResults = {}\r\n"
            + "local taskCSV = redis.call(\"hget\", \"workflow:\"..@workflowId, \"tasks\")\r\n"
            + "local tokens = {}\r\n"
            + "for w in string.gmatch(taskCSV, \"(%d+)\" ) do\r\n"
            + "tokens[#tokens+1] = w\r\n"
            + "end\r\n"
            + "for i = 1, #tokens do\r\n"
            + "local task = tokens[i]\r\n"
            + "local taskDetails = redis.call(\"hgetall\", \"task:\"..task)\r\n"
            + "local result = { }\r\n"
            + "for idx = 1, #taskDetails, 2 do\r\n"
            + "result[taskDetails[idx]] = taskDetails[idx + 1]\r\n"
            + "end\r\n"
            + "result[\"id\"] = task\r\n"
            + "allResults[#allResults + 1] = result\r\n"
            + "end\r\n"
            + "local finalResult = {}\r\n"
            + "finalResult[\"complete\"] = redis.call(\"hget\", \"workflow:\"..@workflowId, \"complete\")\r\n"
            + "finalResult[\"id\"] = @workflowId\r\n"
            + "finalResult[\"tasks\"] = allResults\r\n"
            + "return cjson.encode(finalResult)"
            ;

        private string _pauseWorkflowScript =
              // anything in the submitted state to paused
              // anything in submitted:workflow to paused:workflow
              // anything in running state to paused
              "local tasks = redis.call(\"smembers\", \"submitted:\"..@workflowId)\r\n"
            + "print(\"pausing submitted tasks for \"..@workflowId)\r\n"
            + "for key, task in next,tasks,nil do\r\n"
            + "  local taskType = redis.call(\"hget\", \"task:\"..task, \"type\")\r\n"
            + "  if taskType ~= \"\" then\r\n"
            + "    redis.call(\"zrem\", \"submitted:\"..taskType, 1, task)\r\n"
            + "    redis.call(\"hset\", \"task:\" .. task, \"previousState\", \"submitted:\"..taskType)\r\n"
            + "  else\r\n"
            + "    redis.call(\"zrem\", \"submitted\", 1, task)\r\n"
            + "    redis.call(\"hset\", \"task:\" .. task, \"previousState\", \"submitted\")\r\n"
            + "  end\r\n"
            + "  redis.call(\"srem\", \"submitted:\"..@workflowId, task)\r\n"
            + "  redis.call(\"sadd\", \"paused\", task)\r\n"
            + "  redis.call(\"sadd\", \"paused:\"..@workflowId, task)\r\n"
            + "  redis.call(\"hset\", \"task:\"..task, \"paused\", @timestamp)\r\n"
            + "end\r\n"
            + "local runningTasks = redis.call(\"smembers\", \"running:\"..@workflowId)\r\n"
            + "print(\"pausing running tasks for \"..@workflowId)\r\n"
            + "for key, task in next,runningTasks,nil do\r\n"
            + "  redis.call(\"srem\", \"running\", 1, task)\r\n"
            + "  redis.call(\"srem\", \"running:\"..@workflowId, task)\r\n"
            + "  redis.call(\"sadd\", \"paused\", task)\r\n"
            + "  redis.call(\"sadd\", \"paused:\"..@workflowId, task)\r\n"
            + "  redis.call(\"hset\", \"task:\"..task, \"paused\", @timestamp)\r\n"
            + "  redis.call(\"hset\", \"task:\" .. task, \"previousState\", \"running\")\r\n"
            + "end\r\n"
            ;

        private static readonly string _releaseWorkflowScript =
            // find tasks in paused state for this workflow
            // determine their last state
            // return to that state
              "local pausedTasks = redis.call(\"smembers\", \"paused:\"..@workflowId)\r\n"
            + "for key, task in next,pausedTasks,nil do\r\n"
            + "  local previousState = redis.call(\"hget\", \"task:\"..task, \"previousState\")\r\n"
            + "  if previousState:match(\"submitted\") then\r\n"
            + "    print(\"submitting \"..task)\r\n"
            + "    redis.call(\"hset\", \"task:\"..task, \"submitted\", @timestamp)\r\n"
            + "    redis.call(\"hset\", \"task:\" .. task, \"previousState\", \"paused\")\r\n"
            + "    redis.call(\"lpush\", previousState, task)\r\n"
            + "    redis.call(\"sadd\", \"submitted:\"..@workflowId, task)\r\n"
            + "    redis.call(\"publish\", \"submittedTask\", \"\")\r\n"
            + "  elseif previousState == \"running\" then\r\n"
            + "    redis.call(\"hset\", \"task:\"..task, \"running\", @timestamp)\r\n"
            + "    redis.call(\"hset\", \"task:\"..task, \"previousState\", \"paused\")\r\n"
            + "    redis.call(\"sadd\", \"running\", task)\r\n"
            + "    redis.call(\"sadd\", \"running:\"..@workflowId, task)\r\n"
            + "  else\r\n"
            + "    error(\"Attempted to release task '\"..task..\" but it's previous state was unexpected\")\r\n"
            + "  end\r\n"
            + "end\r\n"
            + "redis.call(\"del\", \"paused:\"..@workflowId)\r\n"
            ;

        private readonly Dictionary<string, LoadedLuaScript> _scripts = new Dictionary<string, LoadedLuaScript>();

        public void LoadScripts(IDatabase db, IServer srv)
        {
            var scripts = new Dictionary<string, string>
            {
                { "pushWorkflow",                      _pushWorkflowScript },
                { "popTask",                           _popTaskScript },
                { "completeTask",                      _completeTaskScript },
                { "failTask",                          _failTaskScript },
                { "popFailedWorkflow",                 _popFailedWorkflowScript },
                { "popCompleteWorkflow",               _popCompleteWorkflowScript },
                { "getWorkflowId",                     _getWorkflowIdScript },
                { "getTaskId",                         _getTaskIdScript },
                { "cleanupWorkflow",                   _cleanupWorkflowScript },
                { "listTasksForResponsibleComponent",  _listTasksForResponsibleComponentScript },
                { "resetTasksForResponsibleComponent", _resetTasksForResponsibleComponentScript },
                { "fetchWorkflowInformation",          _fetchWorkflowInformationScript },
                { "pauseWorkflow",                     _pauseWorkflowScript },
                { "releaseWorkflow",                   _releaseWorkflowScript },
            };

            foreach (var scriptName in scripts.Keys)
            {
                var prepped = LuaScript.Prepare(scripts[scriptName]);

                _scripts.Add(scriptName, prepped.Load(srv));
            }
        }

        public void ReleaseWorkflow(IDatabase db, string workflowId, string timestamp)
        {
            var arguments = new { workflowId = workflowId, timestamp = timestamp };

            _scripts["releaseWorkflow"].Evaluate(db, arguments);
        }

        public void PauseWorkflow(IDatabase db, string workflowId, string timestamp)
        {
            var arguments = new { workflowId = workflowId, timestamp = timestamp };

            _scripts["pauseWorkflow"].Evaluate(db, arguments);
        }

        public string FetchWorkflowInformation(IDatabase db, string workflowId)
        {
            var arguments = new { workflowId = workflowId };

            string result = (string)_scripts["fetchWorkflowInformation"].Evaluate(db, arguments);

            return result;
        }

        public string[] ResubmitTasksFor(IDatabase db, string responsible, string timestamp)
        {
            var arguments = new { responsible = responsible, timestamp = timestamp };

            string[] result = (string[])_scripts["resetTasksForResponsibleComponent"].Evaluate(db, arguments);

            return result;
        }


        public string[] FindTasksFor(IDatabase db, string responsible)
        {
            var arguments = new { responsible = responsible };

            string[] result = (string[])_scripts["listTasksForResponsibleComponent"].Evaluate(db, arguments);

            return result;
        }

        public async System.Threading.Tasks.Task<long?> PushWorkflowAsync(IDatabase db, string workflowJson, string timestamp)
        {
            var arguments = new { workflowJson = workflowJson, timestamp = timestamp };

            var result = await _scripts["pushWorkflow"].EvaluateAsync(db, arguments);

            return (long?)result;
        }


        public long? PushWorkflow(IDatabase db, string workflowJson, string timestamp)
        {
            var arguments = new { workflowJson = workflowJson, timestamp = timestamp };

            var result = _scripts["pushWorkflow"].Evaluate(db, arguments);

            return (long?)result;
        }

        public void FailTask(IDatabase db, string task, string timestamp)
        {
            var arguments = new { taskId = task, timestamp = timestamp };

            _scripts["failTask"].Evaluate(db, arguments);
        }

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

        public string[] PopTask(IDatabase db, string timestamp, string responsible)
        {
            return PopTask(db, "", timestamp, responsible);
        }

        public string[] PopTask(IDatabase db, string type, string timestamp, string responsible)
        {
            var arguments = new { timestamp = timestamp, responsible = responsible, taskType = type };

            var result = (string[])_scripts["popTask"].Evaluate(db, arguments);

            return (string.IsNullOrEmpty(result[0].ToString())) ? null : result;
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
