using System.Collections.Generic;
using System.Linq;

namespace Redis.Workflow.Common
{
    internal class Task
    {
        public TaskName Name { get; private set; }

        public IEnumerable<TaskName> Parents { get  { return _parents; } }

        public IEnumerable<TaskName> Children { get { return _children; } }

        public Payload Payload { get; private set; }

        public WorkflowName Workflow { get; private set; }

        public TaskType Type { get; private set; }

        public TaskPriority Priority { get; private set; }

        internal Task(WorkflowName workflowId, TaskName name, IEnumerable<TaskName> parents, IEnumerable<TaskName> children, Payload payload, TaskType type, TaskPriority priority)
        {
            Workflow = workflowId;
            Name = name;
            Payload = payload;
            Type = type;
            Priority = priority;

            _parents = parents.ToArray();

            _children = children.ToArray();
        }

        private Task()
        { }

        private readonly TaskName[] _parents;

        private readonly TaskName[] _children;
    }
}
