using System;
using System.Collections.Generic;
using System.Text;

namespace MessageBrokerSender
{
    public class WorkItem
    {
        public string PartitionKey { get; set; }

        public string Data { get; set; }
    }
}