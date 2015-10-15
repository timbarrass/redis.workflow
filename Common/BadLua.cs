using StackExchange.Redis;
using System;
using System.Threading.Tasks;

namespace Redis.Workflow.Common
{

    internal class BadLua : ILua
    {
        private readonly TestCase _case;

        private readonly ILua _lua;

        public BadLua(TestCase testCase)
        {
            _case = testCase;

            _lua = new Lua(100);
        }

        public void CleanupWorkflow(IDatabase db, string workflow)
        {
            throw new NotImplementedException();
        }

        public void CompleteTask(IDatabase db, string task, string timestamp, string responsible)
        {
            if (_case == TestCase.CompleteTask)
            {
                throw new NotImplementedException();
            }

            _lua.CompleteTask(db, task, timestamp, responsible);
        }

        public void FailTask(IDatabase db, string task, string timestamp)
        {
            if (_case == TestCase.FailTask)
            {
                throw new NotImplementedException();
            }

            _lua.FailTask(db, task, timestamp);
        }

        public string FetchWorkflowInformation(IDatabase db, string workflowId)
        {
            throw new NotImplementedException();
        }

        public string[] FindTasksFor(IDatabase db, string responsible)
        {
            throw new NotImplementedException();
        }

        public long? GetTaskId(IDatabase db)
        {
            throw new NotImplementedException();
        }

        public long? GetWorkflowId(IDatabase db)
        {
            throw new NotImplementedException();
        }

        public void LoadScripts(IDatabase db, IServer srv)
        {
            _lua.LoadScripts(db, srv);
        }

        public void PauseWorkflow(IDatabase db, string workflowId, string timestamp)
        {
            throw new NotImplementedException();
        }

        public string PopCompleteWorkflow(IDatabase db)
        {
            if (_case == TestCase.PopCompleteWorkflow)
                throw new NotImplementedException();
            else
                return _lua.PopCompleteWorkflow(db);
        }

        public string PopFailedWorkflow(IDatabase db)
        {
            if (_case == TestCase.PopFailedWorkflow)
                throw new NotImplementedException();
            else
                return _lua.PopFailedWorkflow(db);
        }

        public string[] PopTask(IDatabase db, string timestamp, string responsible)
        {
            if (_case == TestCase.PopTask)
                throw new NotImplementedException();
            else
                return PopTask(db, "", timestamp, responsible);
        }

        public string[] PopTask(IDatabase db, string type, string timestamp, string responsible)
        {
            if (_case == TestCase.PopTask)
                throw new NotImplementedException();
            else
                return _lua.PopTask(db, timestamp, responsible);
        }

        public long? PushWorkflow(IDatabase db, string workflowJson, string timestamp)
        {
            return _lua.PushWorkflow(db, workflowJson, timestamp);
        }

        public Task<long?> PushWorkflowAsync(IDatabase db, string workflowJson, string timestamp)
        {
            throw new NotImplementedException();
        }

        public void ReleaseWorkflow(IDatabase db, string workflowId, string timestamp)
        {
            throw new NotImplementedException();
        }

        public string[] ResubmitTasksFor(IDatabase db, string responsible, string timestamp)
        {
            throw new NotImplementedException();
        }
    }

}