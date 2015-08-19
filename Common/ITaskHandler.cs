namespace Redis.Workflow.Common
{
    public interface ITaskHandler
    {
        /// <summary>
        /// Callback that allows client to run the task they want to run.
        /// 
        /// The resultHandler must be used to keep the workflow ticking over; on success
        /// resultHandler.OnSuccess should be called. On a failure that should bring the
        /// workflow to a halt OnFailure should be called.
        /// 
        /// No failure reason is passed back, just as no result is passed back. It's the
        /// client's responsibility to manage results and failures/exceptions separately.
        /// </summary>
        /// <param name="configuration">The configuration string associated with this task.</param>
        /// <param name="resultHandler">SHould be used to signal task completion, whether a success or fail.</param>
        void Run(string configuration, IResultHandler resultHandler);
    }
}
