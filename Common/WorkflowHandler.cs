using System;

namespace Redis.Workflow.Common
{
    public class WorkflowHandler
    {
        internal void OnWorkflowComplete(string workflow, WorkflowDetails details)
        {
            var handler = WorkflowComplete;

            if (handler != null) handler(this, details);
        }

        internal void OnWorkflowFailed(string workflow, WorkflowDetails details)
        {
            var handler = WorkflowFailed;

            if (handler != null) handler(this, details);
        }

        public event EventHandler<WorkflowDetails> WorkflowComplete;

        public event EventHandler<WorkflowDetails> WorkflowFailed;
    }
}