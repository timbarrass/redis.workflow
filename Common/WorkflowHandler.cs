using System;

namespace Redis.Workflow.Common
{
    public class WorkflowHandler
    {
        internal void OnWorkflowComplete(string workflow)
        {
            if(WorkflowComplete != null)
            {
                WorkflowComplete(workflow);
            }
        }

        public event Action<string> WorkflowComplete;
    }
}