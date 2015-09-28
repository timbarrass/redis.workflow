using System.Collections.Generic;

namespace Redis.Workflow.Common
{
    public class WorkflowDetails
    {
        public string Complete { get; set; }
        public string Id { get; set; }
        public List<TaskDetails> Tasks { get; set; }
    }
}
