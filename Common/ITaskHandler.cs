namespace Redis.Workflow.Common
{
    public interface ITaskHandler
    {
        void Run(string configuration, IResultHandler resultHandler);
    }
}
