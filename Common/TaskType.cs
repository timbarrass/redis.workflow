namespace Redis.Workflow.Common
{
    public class TaskType
    {
        public TaskType(string type)
        {
            _type = type;
        }

        protected TaskType()
        { }

        public override string ToString()
        {
            return _type;
        }

        private readonly string _type;
    }
}