namespace Redis.Workflow.Common
{
    public class TaskPriority
    {
        public TaskPriority(int priority)
        {
            _priority = priority;
        }

        public override string ToString()
        {
            return _priority.ToString();
        }

        private TaskPriority()
        { }

        private readonly int _priority;
    }
}