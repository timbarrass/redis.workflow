using System;

namespace Redis.Workflow.Common
{
    internal class SimpleResultHandler : IResultHandler
    {
        public void OnSuccess()
        {
            _completeTask(_task);
        }

        internal SimpleResultHandler(string task, Action<string> CompleteTask)
        {
            _task = task;

            _completeTask = CompleteTask;
        }

        private string _task;

        private Action<string> _completeTask;
    }
}
