namespace Redis.Workflow.Common
{
    public interface IResultHandler
    {
        void OnSuccess();

        // void OnFailure();
    }
}
