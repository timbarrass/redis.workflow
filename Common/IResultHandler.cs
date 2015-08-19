namespace Redis.Workflow.Common
{
    public interface IResultHandler
    {
        /// <summary>
        /// Should be called on successful task completion. Will cause
        /// child tasks, if any, to be submitted.
        /// </summary>
        void OnSuccess();

        /// <summary>
        /// Should be called on task failure. Will not cause child tasks
        /// to be submitted, and will cause workflow to end abruptly.
        /// </summary>
        void OnFailure();
    }
}
