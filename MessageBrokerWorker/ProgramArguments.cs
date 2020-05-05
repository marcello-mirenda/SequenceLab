namespace MessageBrokerWorker
{
    public class ProgramArguments
    {
        public string FilePath { get; set; }
        public string GroupId { get; set; }
        public string Topic { get; internal set; }
    }
}