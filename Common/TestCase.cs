namespace Redis.Workflow.Common
{
    internal enum TestCase
    {
        None = 0,

        PopTask = 1,

        PopFailedWorkflow = 2,

        PopCompleteWorkflow = 3,

        CompleteTask = 4,

        FailTask = 5,
    }
}