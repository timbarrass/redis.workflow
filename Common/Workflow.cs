using Newtonsoft.Json;
using System.Collections.Generic;

namespace Redis.Workflow.Common
{
    public class Workflow
    {
        public string Name { get; set; }

        public IEnumerable<Task> Tasks {get; set; }

        public string ToJson()
        {
            return JsonConvert.SerializeObject(this);
        }
    }
}
