using System.Threading.Tasks;
using StackExchange.Redis;

namespace Redis.Workflow.Common
{
    internal interface ILua
    {
        void CleanupWorkflow(IDatabase db, string workflow);
        void CompleteTask(IDatabase db, string task, string timestamp, string responsible);
        void FailTask(IDatabase db, string task, string timestamp);
        string FetchWorkflowInformation(IDatabase db, string workflowId);
        string[] FindTasksFor(IDatabase db, string responsible);
        long? GetTaskId(IDatabase db);
        long? GetWorkflowId(IDatabase db);
        void LoadScripts(IDatabase db, IServer srv);
        void PauseWorkflow(IDatabase db, string workflowId, string timestamp);
        string PopCompleteWorkflow(IDatabase db);
        string PopFailedWorkflow(IDatabase db);
        string[] PopTask(IDatabase db, string timestamp, string responsible);
        void PushTask(IDatabase db, string task, string timestamp);
        long? PushWorkflow(IDatabase db, string workflowJson, string timestamp);
        Task<long?> PushWorkflowAsync(IDatabase db, string workflowJson, string timestamp);
        void ReleaseWorkflow(IDatabase db, string workflowId, string timestamp);
        string[] ResubmitTasksFor(IDatabase db, string responsible, string timestamp);
    }
}