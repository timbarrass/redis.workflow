namespace Redis.Workflow.Common
{
    public class WorkflowManagementId
    {
        public WorkflowManagementId(string id)
        {
            _id = id;
        }

        public override string ToString()
        {
            return _id;
        }

        private WorkflowManagementId()
        { }

        private readonly string _id;
    }
}