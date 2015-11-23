using System;
using System.Runtime.Serialization;

namespace Redis.Workflow.Common
{
    [Serializable]
    internal class WorkflowException : Exception
    {
        public WorkflowException()
        {
        }

        public WorkflowException(string message) : base(message)
        {
        }

        public WorkflowException(string message, Exception innerException) : base(message, innerException)
        {
        }

        protected WorkflowException(SerializationInfo info, StreamingContext context) : base(info, context)
        {
        }
    }
}