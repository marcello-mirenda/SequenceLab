namespace MessageBrokerReceiver
{
    public class WorkItem
    {
        public string PartitionKey { get; set; }

        public string Data { get; set; }
    }
}