using System;

namespace Redis.Workflow.Common
{
    [Flags]
    public enum Behaviours
    {
        None = 0,

        Submitter = 1 << 0,

        Processor = 1 << 1,

        AutoRestart = 1 << 2,

        All = Submitter | Processor | AutoRestart,
    }
}
