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

        public event EventHandler<string> WorkflowComplete;
    }
}