using System;
using System.Diagnostics;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using Confluent.Kafka;
using Fclp;
using MessageBrokerWorker;
using Microsoft.ApplicationInsights;
using Microsoft.ApplicationInsights.Extensibility;
using Microsoft.Extensions.Configuration;
using Newtonsoft.Json;
using Serilog;
using Serilog.Context;
using Serilog.Exceptions;
using Serilog.Sinks.SystemConsole.Themes;

namespace MessageBrokerAggregator
{
    internal class ProgramWorker
    {
        private static void Main(string[] args)
        {
            var parser = BuildParser(args);
            if (parser == null)
            {
                Environment.Exit(0);
            }

            var settings = new ConfigurationBuilder()
                .AddEnvironmentVariables()
                .Build();

            var loggerConf = BuildLoggerConfiguration(settings);

            using var loggerMain = loggerConf.CreateLogger();
            var logger = loggerMain.ForContext<ProgramWorker>();

            var config = new ConsumerConfig
            {
                BootstrapServers = settings["KAFKA_BOOTSTRAPSERVERS"],
                GroupId = parser.Object.GroupId,
                EnableAutoCommit = false,
                StatisticsIntervalMs = 5000,
                SessionTimeoutMs = 6000,
                AutoOffsetReset = AutoOffsetReset.Earliest,
                EnablePartitionEof = true
            };

            int commitPeriod = int.Parse(settings["KAFKA_COMMITPERIOD"]);

            var cts = new CancellationTokenSource();
            var cancellationToken = cts.Token;

            //using var topic = LogContext.PushProperty("Topic", parser.Object.Topic);
            using var groupId = LogContext.PushProperty("GroupId", parser.Object.GroupId);
            using var processId = LogContext.PushProperty("ProcessId", Process.GetCurrentProcess().Id);
            using var threadId = LogContext.PushProperty("ThreadId", Thread.CurrentThread.ManagedThreadId);

            using (var consumer = new ConsumerBuilder<Ignore, string>(config)
               // Note: All handlers are called on the main .Consume thread.
               .SetErrorHandler((_, e) => logger.Error("Error:{Error}", e))
               //.SetStatisticsHandler((_, json) => Console.WriteLine($"Statistics: {json}"))
               .SetPartitionsAssignedHandler((c, partitions) =>
               {
                   logger.Information("Assigned partitions: [{Partitions}]", string.Join(", ", partitions));
                   // possibly manually specify start offsets or override the partition assignment provided by
                   // the consumer group by returning a list of topic/partition/offsets to assign to, e.g.:
                   //
                   // return partitions.Select(tp => new TopicPartitionOffset(tp, externalOffsets[tp]));
               })
               .SetPartitionsRevokedHandler((c, partitions) =>
               {
                   logger.Information("Revoking assignment: [{Partitions}]", string.Join(", ", partitions));
               })
               .Build())
            {
                consumer.Subscribe(parser.Object.Topic);
                using var topic = LogContext.PushProperty("Topic", parser.Object.Topic);
                logger.Information("Subscribed to topic");

                try
                {
                    while (true)
                    {
                        try
                        {
                            var consumeResult = consumer.Consume(cancellationToken);

                            if (consumeResult.IsPartitionEOF)
                            {
                                using var pk = LogContext.PushProperty("Partition", consumeResult.Partition.Value);
                                logger.Information(
                                    "Reached end of partition {Partition}, offset {Offset}."
                                    , consumeResult.Partition
                                    , consumeResult.Offset);
                                continue;
                            }

                            var task = ProcessComsumeResultAsync(consumeResult, parser, logger);

                            Task.WaitAny(task);
                            if (consumeResult.Offset % commitPeriod == 0)
                            {
                                // The Commit method sends a "commit offsets" request to the Kafka
                                // cluster and synchronously waits for the response. This is very
                                // slow compared to the rate at which the consumer is capable of
                                // consuming messages. A high performance application will typically
                                // commit offsets relatively infrequently and be designed handle
                                // duplicate messages in the event of
                                try
                                {
                                    consumer.Commit(consumeResult);
                                }
                                catch (KafkaException e)
                                {
                                    logger.Error(e, e.Error.Reason);
                                }
                            }
                            if (task.IsFaulted)
                            {
                                logger.Error(task.Exception, "Error during processing.");
                            }
                            else
                            {
                                logger.Information("Msg processed.");
                            }
                        }
                        catch (ConsumeException e)
                        {
                            logger.Error(e, e.Error.Reason);
                        }
                    }
                }
                catch (OperationCanceledException)
                {
                    logger.Information("Closing consumer.");
                    consumer.Close();
                }
            }

            Console.TreatControlCAsInput = true;
            Console.CancelKeyPress += (_, e) =>
            {
                e.Cancel = true; // prevent the process from terminating.
                cts.Cancel();
            };
        }

        private static async Task ProcessComsumeResultAsync(ConsumeResult<Ignore, string> consumeResult, FluentCommandLineParser<ProgramArguments> parser, ILogger logger)
        {
            var wi = JsonConvert.DeserializeObject<WorkerLogic.WorkItem>(consumeResult.Message.Value);
            using var pk = LogContext.PushProperty("Partition", consumeResult.Partition.Value);
            using var wipk = LogContext.PushProperty("WorkItemPartitionKey", wi.PartitionKey);

            logger.Information("Received message at {TopicPartitionOffset}: {Message}"
                , consumeResult.TopicPartitionOffset
                , consumeResult.Message.Value);

            var fileName = $"{Path.GetFileNameWithoutExtension(parser.Object.FilePath)}_{wi.PartitionKey}.json";
            var fileDirectory = Path.GetDirectoryName(parser.Object.FilePath);
            var filePath = Path.Combine(fileDirectory, fileName);

            var job = new WorkerLogic.Job(new WorkerLogicLogger(logger));
            await job.ExecuteAsync(wi, filePath);
            logger.Information("Processed message at {TopicPartitionOffset}: {Message}"
                , consumeResult.TopicPartitionOffset
                , consumeResult.Message.Value);
        }

        private static LoggerConfiguration BuildLoggerConfiguration(IConfigurationRoot settings)
        {
            TelemetryClient telemetryClient = null;
            if (settings["APPINSIGHTS_INSTRUMENTATIONKEY"] != null)
            {
                var telemetryConf = new TelemetryConfiguration(settings["APPINSIGHTS_INSTRUMENTATIONKEY"]);
                telemetryClient = new TelemetryClient(telemetryConf);
            }
            var loggerConf = new LoggerConfiguration();
            loggerConf
                .ReadFrom.Configuration(settings)
                .Enrich.FromLogContext()
                .Enrich.WithExceptionDetails()
                .WriteTo.Console(theme: AnsiConsoleTheme.Literate);
            if (telemetryClient != null)
            {
                loggerConf.WriteTo.ApplicationInsights(telemetryClient, TelemetryConverter.Traces);
            }
            if (settings["SERILOG_WRITETOSEQSERVERURL"] != null)
            {
                loggerConf.WriteTo.Seq(settings["SERILOG_WRITETOSEQSERVERURL"]);
            }

            return loggerConf;
        }

        private static FluentCommandLineParser<ProgramArguments> BuildParser(string[] args)
        {
            var parser = new FluentCommandLineParser<ProgramArguments>();
            parser.Setup(arg => arg.Topic)
                .As('t', "topic")
                .Required();
            parser.Setup(arg => arg.GroupId)
                .As('g', "group-id")
                .Required();
            parser.Setup(arg => arg.FilePath)
                .As('f', "file")
                .Required();
            var result = parser.Parse(args);
            if (result.HelpCalled)
            {
                return null;
            }
            else if (result.HasErrors)
            {
                Console.WriteLine(result.ErrorText);
                return null;
            }
            return parser;
        }
    }
}