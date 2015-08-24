using Redis.Workflow.Common;

namespace Redis.Workflow.ExampleApp
{
    class TaskHandler : ITaskHandler
    {
        public void Run(string configuration, IResultHandler resultHandler)
        {
            resultHandler.OnSuccess();
        }
    }
}
