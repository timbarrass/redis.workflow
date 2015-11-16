namespace Redis.Workflow.Common
{
    public class TaskName
    {
        public TaskName(string name)
        {
            _name = name;
        }

        public override string ToString()
        {
            return _name;
        }

        private readonly string _name;
    }
}