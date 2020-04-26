using System;
using System.Collections.Generic;
using System.Text;

namespace MessageBrokerReceiver
{
    public class WorkItem
    {
        public string PartitionKey { get; set; }

        public string Data { get; set; }
    }
}