using System;
using System.Collections.Generic;
using System.Text;

namespace WorkerLogic
{
    public class WorkItem
    {
        public string PartitionKey { get; set; }

        public string Data { get; set; }
    }
}