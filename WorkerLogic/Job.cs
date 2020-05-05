using System;
using System.Threading.Tasks;

namespace WorkerLogic
{
    public class Job
    {
        private readonly ILogger _logger;

        public Job(ILogger logger)
        {
            _logger = logger;
        }

        public async Task ExecuteAsync(
            WorkItem wi,
            string filePath)
        {
            Func<string, int, string> stringMax = (string s, int max) =>
            {
                if (s.Length > max)
                {
                    return s.Substring(0, max);
                }
                else
                {
                    return s;
                }
            };
            _logger.Information("Workitem:{Data}, Length:{Length}", stringMax(wi.Data, 10), wi.Data.Length);

            var currentData = await JsonStorage.LoadAsync<CountData>(filePath);
            var originalData = currentData.Clone();
            currentData.Count++;
            currentData.Message = wi.Data;
            await Task.Delay(TimeSpan.FromMilliseconds(2000));
            //Thread.Sleep(TimeSpan.FromSeconds(2));
            var lastData = await JsonStorage.LoadAsync<CountData>(filePath);
            var conflict = false;
            if (lastData == originalData)
            {
                await JsonStorage.SaveAsync(currentData, filePath);
                _logger.Information("Content updated. Conflict:{Conflict}, LastContent:[{LastContent}], NewContent:[{NewContent}]", conflict, lastData, currentData);
            }
            else
            {
                conflict = true;
                _logger.Warning("Another process modified the file. Conflict:{Conflict}, LastContent:[{LastContent}], OriginalContent:[{OriginalContent}]", conflict, lastData, originalData);
            }
        }
    }
}