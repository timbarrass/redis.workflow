using System;

namespace Redis.Workflow.Common
{
    internal class SimpleResultHandler : IResultHandler
    {
        public void OnSuccess()
        {
            _completeTask(_task);
        }

        public void OnFailure()
        {
            _failTask(_task);
        }

        internal SimpleResultHandler(string task, Action<string> CompleteTask, Action<string> FailTask)
        {
            _task = task;

            _completeTask = CompleteTask;

            _failTask = FailTask;
        }

        private string _task;

        private Action<string> _completeTask;

        private Action<string> _failTask;
    }
}
