using System;

namespace Redis.Workflow.Common
{
    public class WorkflowHandler
    {
        internal void OnWorkflowComplete(string workflow)
        {
            var handler = WorkflowComplete;

            if(WorkflowComplete != null) WorkflowComplete(this, workflow);
        }

        internal void OnWorkflowFailed(string workflow)
        {
            var handler = WorkflowFailed;

            if (handler != null) handler(this, workflow);
        }

        public event EventHandler<string> WorkflowComplete;

        public event EventHandler<string> WorkflowFailed;
    }
}