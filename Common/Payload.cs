namespace Redis.Workflow.Common
{
    public class Payload
    {
        public Payload(string payload)
        {
            _payload = payload;
        }

        private Payload()
        { }

        public override string ToString()
        {
            return _payload;
        }

        private readonly string _payload;
    }
}