namespace Redis.Workflow.Common
{
    public class WorkflowName
    {
        public WorkflowName(string name)
        {
            _name = name;
        }

        private WorkflowName()
        { }

        public override string ToString()
        {
            return _name;
        }

        private string _name;
    }
}