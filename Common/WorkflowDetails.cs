using System.Collections.Generic;

namespace Redis.Workflow.Common
{
    public class WorkflowDetails
    {
        public string Id { get; set; }
        public List<TaskDetails> Tasks { get; set; }
    }
}
