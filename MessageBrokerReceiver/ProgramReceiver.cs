using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Fclp;
using Microsoft.ApplicationInsights;
using Microsoft.ApplicationInsights.Extensibility;
using Microsoft.Extensions.Configuration;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;
using Serilog;
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

            if (!File.Exists(parser.Object.FilePath))
            {
                JsonStorage.Save(new CountData { Count = 1000 }, parser.Object.FilePath);
            }

            var settings = new ConfigurationBuilder()
                .AddEnvironmentVariables()
                .Build();
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
                HostName = settings["RABBITMQ_HOSTNAME"]
            };

            using var loggerMain = loggerConf.CreateLogger();
            var logger = loggerMain.ForContext<ProgramReceiver>();
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
            consumer.Received += (model, payload) =>
            {
                try
                {
                    if (payload is null)
                    {
                        return;
                    }

                    byte[] body = payload.Body;
                    string message = Encoding.UTF8.GetString(body);
                    var headers = payload.BasicProperties?.Headers;
                    logger.Information(
                        "Msg received on {Topic}, Process: {Process}, Thread:{Thread}", payload.RoutingKey,
                        Process.GetCurrentProcess().Id, Thread.CurrentThread.ManagedThreadId);
                    Task task = MessageHandlerAsync(message, parser.Object.FilePath, headers, logger, payload);
                    Task.WaitAny(task);
                    channel.BasicAck(
                        deliveryTag: payload.DeliveryTag,
                        multiple: false);
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
            string message,
            string filePath,
            IDictionary<string, object> headers,
            ILogger logger,
            BasicDeliverEventArgs args)
        {
            logger.Information("{Message}", message);

            var currentData = JsonStorage.Load<CountData>(filePath);
            var originalData = currentData.Clone();
            currentData.Count++;
            currentData.Message = message;
            await Task.Delay(1500);
            var lastData = await JsonStorage.LoadAsync<CountData>(filePath);
            if (lastData == originalData)
            {
                await JsonStorage.SaveAsync(currentData, filePath);
                logger.Information("{Topic} Content updated. [{LastContent}] [{NewContent}]", args.RoutingKey, lastData, currentData);
            }
            else
            {
                logger.Warning("{Topic} Another process modified the file. [{OriginalContent}] [{LastContent}]", args.RoutingKey, originalData, lastData);
            }
        }
    }
}