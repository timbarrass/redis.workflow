using Newtonsoft.Json;
using System.Collections.Generic;
using System;
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

        internal IEnumerable<Task> Tasks
        {
            get { return _tasks; }
        }

        public void AddTask(TaskName name, Payload payload, TaskType type, TaskPriority priority, IEnumerable<TaskName> parents, IEnumerable<TaskName> children)
        {
            if (!_defined.Add(name))
            {
                throw new WorkflowException(string.Format("Task '{0}' has already been declared.", name));
            }

            foreach (var parent in parents) _parents.Add(parent);

            foreach (var child in children) _children.Add(child);

            var task = new Task(Name, name, parents, children, payload, type, priority);

            _tasks.Add(task);
        }

        internal void VerifyInternalConsistency()
        {
            var undeclaredChildren = _children.Except(_defined).Select(t => t.ToString()).ToArray();

            if(undeclaredChildren.Length > 0)
            {
                throw new WorkflowException(string.Format("Some tasks defined undeclared tasks as their children. Undeclared child(ren): '{0}'", string.Join(",", undeclaredChildren)));
            }

            var undeclaredParents = _children.Except(_defined).Select(t => t.ToString()).ToArray();

            if (undeclaredParents.Length > 0)
            {
                throw new WorkflowException(string.Format("Some tasks defined undeclared tasks as their parents. Undeclared parent(s): '{0}'", string.Join(",", undeclaredParents)));
            }

            var rootParentCount = 0;
            foreach (var task in Tasks)
            {
                if (task.Parents.Count() == 0)
                {
                    rootParentCount++;
                }
            }

            if (rootParentCount == 0)
            {
                throw new WorkflowException("A workflow must have at least one task with no parents.");
            }
        }

        internal string ToJson()
        {
            return JsonConvert.SerializeObject(this, new TaskConverter(), new WorkflowConverter());
        }

        private readonly HashSet<TaskName> _defined = new HashSet<TaskName>();

        private readonly HashSet<TaskName> _parents = new HashSet<TaskName>();

        private readonly HashSet<TaskName> _children = new HashSet<TaskName>();

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

