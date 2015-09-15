namespace Redis.Workflow.Common
{
    public class TaskDetails
    {
        public string name { get; set; }
        public string running { get; set; }
        public string workflow { get; set; }
        public string payload { get; set; }
        public string id { get; set; }
        public string lastKnownResponsible { get; set; }
        public string submitted { get; set; }
        public string complete { get; set; }
    }
}
