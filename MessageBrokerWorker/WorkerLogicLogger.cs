using System;
using System.Collections.Generic;
using System.Text;
using Serilog;

namespace MessageBrokerAggregator
{
    internal class WorkerLogicLogger : WorkerLogic.ILogger
    {
        private readonly ILogger _logger;

        public WorkerLogicLogger(ILogger logger)
        {
            _logger = logger;
        }

        public void Information<T0, T1>(string messageTemplate, T0 propertyValue0, T1 propertyValue1)
        {
            _logger.Information(messageTemplate, propertyValue0, propertyValue1);
        }

        public void Information<T0, T1, T2>(string messageTemplate, T0 propertyValue0, T1 propertyValue1, T2 propertyValue2)
        {
            _logger.Information(messageTemplate, propertyValue0, propertyValue1, propertyValue2);
        }

        public void Warning<T0, T1, T2>(string messageTemplate, T0 propertyValue0, T1 propertyValue1, T2 propertyValue2)
        {
            _logger.Warning(messageTemplate, propertyValue0, propertyValue1, propertyValue2);
        }
    }
}