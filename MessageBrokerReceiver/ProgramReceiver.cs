using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Confluent.Kafka;
using Fclp;
using Microsoft.ApplicationInsights;
using Microsoft.ApplicationInsights.Extensibility;
using Microsoft.Extensions.Configuration;
using Newtonsoft.Json;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;
using Serilog;
using Serilog.Context;
using Serilog.Exceptions;
using Serilog.Sinks.SystemConsole.Themes;

namespace MessageBrokerReceiver
{
    internal class ProgramReceiver
    {
        private const int EXPIRES = 259200000;
        private const string NAMESPACE = "mmi";
        private const string CHANNELNAME = "reviso-mailbox-example";

        private static void Main(string[] args)
        {
            var settings = new ConfigurationBuilder()
                .AddEnvironmentVariables()
                .Build();

            // PROGRAM ARGUMENTS CONFIG
            var parser = new FluentCommandLineParser<ProgramArguments>();
            parser.Setup(arg => arg.Topic)
                .As('t', "topic")
                .Required();
            parser.Setup(arg => arg.FilePath)
                .As('f', "file")
                .Required();
            var result = parser.Parse(args);
            if (result.HelpCalled)
            {
                return;
            }
            else if (result.HasErrors)
            {
                Console.WriteLine(result.ErrorText);
                return;
            }
            // PROGRAM ARGUMENTS CONFIG

            // LOGGER CONFIG
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

            var factory = new ConnectionFactory
            {
                HostName = settings["RABBITMQ_HOSTNAME"],
                UserName = "user",
                Password = "cbclUU9lcErVcLX7"
            };

            using var loggerMain = loggerConf.CreateLogger();
            var logger = loggerMain.ForContext<ProgramReceiver>();
            // LOGGER CONFIG

            // KAFKA CONFIG
            var config = new ProducerConfig
            {
                BootstrapServers = settings["KAFKA_BOOTSTRAPSERVERS"],
            };
            using var kafkaProducer = new ProducerBuilder<string, string>(config).Build();
            // KAFKA CONFIG

            // RABBITMQ CONFIG
            using var connection = factory.CreateConnection($"{NAMESPACE}-{CHANNELNAME}-pull");
            using var channel = connection.CreateModel();
            channel.BasicQos(
                prefetchSize: 0,
                prefetchCount: 10,
                global: false);
            logger.Information("RabbitMQ connection {ClientProvidedName}", connection.ClientProvidedName);
            //var mailboxOptions = new RevisoMailbox.MailboxOptions(
            //// rabbitUri: "amqps://gqgdsils:CO8-9sELWuDgkja3KWWdOdDuPax1azme@peppy-bear.rmq.cloudamqp.com/gqgdsils",
            //rabbitUri: "amqp://guest:guest@marcello-g3-3590",
            //channelName: "reviso-mailbox-example",
            //serviceNameSpace: "mmi");
            channel.ExchangeDeclare(
                exchange: "xglobalfanout",
                type: ExchangeType.Fanout,
                durable: true);

            channel.ExchangeDeclare(
                exchange: $"x{CHANNELNAME}",
                type: ExchangeType.Topic,
                durable: true);

            channel.ExchangeBind(
                source: "xglobalfanout",
                destination: $"x{CHANNELNAME}",
                routingKey: "#");

            var queue = channel.QueueDeclare(
                queue: $"{NAMESPACE}-{CHANNELNAME}-{parser.Object.Topic}",
                durable: true,
                exclusive: false,
                autoDelete: false,
                arguments: new Dictionary<string, object>
                {
                    {"x-expires", Convert.ToInt64(EXPIRES)},
                });

            channel.QueueBind(
                queue: queue.QueueName,
                exchange: $"x{CHANNELNAME}",
                routingKey: $"{NAMESPACE}.{parser.Object.Topic}");

            var consumer = new EventingBasicConsumer(channel);
            // RABBITMQ CONFIG

            var rnd = new Random();
            consumer.Received += (model, payload) =>
            {
                try
                {
                    if (payload is null)
                    {
                        return;
                    }

                    byte[] body = payload.Body.ToArray();
                    string data = Encoding.UTF8.GetString(body);
                    var wi = JsonConvert.DeserializeObject<WorkerLogic.WorkItem>(data);
                    var headers = payload.BasicProperties?.Headers;
                    using var processId = LogContext.PushProperty("ProcessId", Process.GetCurrentProcess().Id);
                    using var threadId = LogContext.PushProperty("ThreadId", Thread.CurrentThread.ManagedThreadId);
                    using var topic = LogContext.PushProperty("Topic", payload.RoutingKey);
                    using var pk = LogContext.PushProperty("PartitionKey", wi.PartitionKey);
                    logger.Information("Msg received.");

                    var fileName = $"{Path.GetFileNameWithoutExtension(parser.Object.FilePath)}_{wi.PartitionKey}.json";
                    var fileDirectory = Path.GetDirectoryName(parser.Object.FilePath);
                    var filePath = Path.Combine(fileDirectory, fileName);
                    Task task = MessageHandlerAsync(wi, filePath, headers, logger, payload, kafkaProducer, parser, rnd);

                    Task.WaitAny(task);
                    channel.BasicAck(
                        deliveryTag: payload.DeliveryTag,
                        multiple: false);
                    if (task.IsFaulted)
                    {
                        logger.Error(task.Exception, "Error during processing.");
                    }
                    else
                    {
                        logger.Information("Msg processed.");
                    }
                }
                catch (Exception ex)
                {
                    channel.BasicReject(
                        deliveryTag: payload.DeliveryTag,
                        requeue: false);
                    logger.Error(ex, ex.Message);
                }
            };

            channel.BasicConsume(queue.QueueName, false, consumer);
            Console.ReadLine();
        }

        private static async Task MessageHandlerAsync(
            WorkerLogic.WorkItem wi,
            string filePath,
            IDictionary<string, object> headers,
            ILogger logger,
            BasicDeliverEventArgs args,
            IProducer<string, string> kafkaProducer,
            FluentCommandLineParser<ProgramArguments> parser,
            Random rnd)
        {
#if KAFKA
            var content = JsonConvert.SerializeObject(wi);
#if KAFKA_DEFAULT_PARTITIONER
            string pk = wi.PartitionKey;
            var deliveryReport = await kafkaProducer.ProduceAsync(
                parser.Object.Topic,
                new Message<string, string> { Key = pk, Value = content });
#else
            string pk = string.Empty;
            TopicPartition topicPartition = null;
            if (wi.PartitionKey == "era")
            {
                topicPartition = new TopicPartition(parser.Object.Topic, new Partition(0));
            }
            else if (wi.PartitionKey == "mmi")
            {
                topicPartition = new TopicPartition(parser.Object.Topic, new Partition(1));
            }
            else
            {
                throw new InvalidOperationException("Unknown partition key.");
            }
            var deliveryReport = await kafkaProducer.ProduceAsync(
                topicPartition,
                new Message<string, string> { Key = pk, Value = content });

#endif
            logger.Information("Delivered to kafka. Topic:{Topic}, Partition Key:{PartitionKey}", parser.Object.Topic, wi.PartitionKey);
#else
            var job = new WorkerLogic.Job(new WorkerLogicLogger(logger));
            await job.ExecuteAsync(wi, filePath);
#endif
        }

        //private static async Task MessageHandlerAsync(
        //    WorkItem wi,
        //    string filePath,
        //    IDictionary<string, object> headers,
        //    ILogger logger,
        //    BasicDeliverEventArgs args)
        //{
        //    Func<string, int, string> stringMax = (string s, int max) =>
        //      {
        //          if (s.Length > max)
        //          {
        //              return s.Substring(0, max);
        //          }
        //          else
        //          {
        //              return s;
        //          }
        //      };
        //    logger.Information("Workitem:pk:{PartitionKey}, {Data}, Length:{Length}", wi.PartitionKey, stringMax(wi.Data, 10), wi.Data.Length);

        //    var currentData = await JsonStorage.LoadAsync<CountData>(filePath);
        //    var originalData = currentData.Clone();
        //    currentData.Count++;
        //    currentData.Message = wi.Data;
        //    await Task.Delay(TimeSpan.FromSeconds(2));
        //    //Thread.Sleep(TimeSpan.FromSeconds(2));
        //    var lastData = await JsonStorage.LoadAsync<CountData>(filePath);
        //    var conflict = false;
        //    if (lastData == originalData)
        //    {
        //        await JsonStorage.SaveAsync(currentData, filePath);
        //        logger.Information("Content updated. Conflict:{Conflict}, LastContent:[{LastContent}], NewContent:[{NewContent}]", conflict, lastData, currentData);
        //    }
        //    else
        //    {
        //        conflict = true;
        //        logger.Warning("Another process modified the file. Conflict:{Conflict}, LastContent:[{LastContent}], OriginalContent:[{OriginalContent}]", conflict, lastData, originalData);
        //    }
        //}
    }
}