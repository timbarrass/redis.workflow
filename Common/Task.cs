namespace Redis.Workflow.Common
{
    public class Task
    {
        public string Name { get; set; }

        public string[] Parents { get; set; }

        public string[] Children { get; set; }

        public string Payload { get; set; }

        public string Workflow { get; set; }

        public string Type { get; set; }
    }
}
