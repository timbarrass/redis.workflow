using Newtonsoft.Json;
using System.Collections.Generic;
using System;
using System.IO;
using System.Text;
using System.Linq;

namespace Redis.Workflow.Common
{
    public class Workflow
    {
        public Workflow(WorkflowName name)
        {
            Name = name;
        }

        public WorkflowName Name { get; private set; }

        public IEnumerable<Task> Tasks
        {
            get { return _tasks; }
        }

        public void AddTask(TaskName name, Payload payload, TaskType type, TaskPriority priority, IEnumerable<TaskName> parents, IEnumerable<TaskName> children)
        {
            var task = new Task(Name, name, parents, children, payload, type, priority);

            _tasks.Add(task);
        }

        public string ToJson()
        {
            return JsonConvert.SerializeObject(this, new TaskConverter(), new WorkflowConverter());
        }

        private readonly HashSet<Task> _tasks = new HashSet<Task>();
    }
}

namespace Redis.Workflow.Common
{
    internal class WorkflowConverter : JsonConverter
    {
        public WorkflowConverter()
        {
        }

        public override bool CanConvert(Type objectType)
        {
            return objectType == typeof(Workflow);
        }

        public override object ReadJson(JsonReader reader, Type objectType, object existingValue, JsonSerializer serializer)
        {
            throw new NotImplementedException();
        }

        public override void WriteJson(JsonWriter writer, object value, JsonSerializer serializer)
        {
            var workflow = value as Workflow;

            var obj = new { Name = workflow.Name.ToString(), Tasks = workflow.Tasks };

            serializer.Serialize(writer, obj);
        }
    }
}

namespace Redis.Workflow.Common
{
    internal class TaskConverter : JsonConverter
    {
        public override bool CanConvert(Type objectType)
        {
            return objectType == typeof(Task);
        }

        public override object ReadJson(JsonReader reader, Type objectType, object existingValue, JsonSerializer serializer)
        {
            throw new NotImplementedException();
        }

        public override void WriteJson(JsonWriter writer, object value, JsonSerializer serializer)
        {
            var task = value as Task;

            var obj = new {
                WorkflowId = task.Workflow.ToString(),
                Name = task.Name.ToString(),
                Payload = task.Payload.ToString(),
                Priority = task.Priority.ToString(),
                Type = task.Type.ToString(),
                Parents = task.Parents.Select(t => t.ToString()),
                Children = task.Children.Select(t => t.ToString())
            };

            serializer.Serialize(writer, obj);
        }
    }
}

