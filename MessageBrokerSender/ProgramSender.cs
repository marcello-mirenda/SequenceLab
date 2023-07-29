using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Fclp;
using Microsoft.ApplicationInsights;
using Microsoft.ApplicationInsights.Extensibility;
using Microsoft.Extensions.Configuration;
using Newtonsoft.Json;
using RabbitMQ.Client;
using Serilog;
using Serilog.Context;
using Serilog.Exceptions;
using Serilog.Sinks.SystemConsole.Themes;

namespace MessageBrokerSender
{
    internal class ProgramSender
    {
        private const string CHANNELNAME = "reviso-mailbox-example";
        private const string NAMESPACE = "mmi";

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
            using var loggerMain = loggerConf.CreateLogger();
            var logger = loggerMain.ForContext<ProgramSender>();
            // LOGGER CONFIG

            // RABBITMQ CONFIG
            var factory = new ConnectionFactory
            {
                HostName = settings["RABBITMQ_HOSTNAME"],
                UserName = "user",
                Password = "cbclUU9lcErVcLX7"
            };
            using var connection = factory.CreateConnection("mmi-reviso-mailbox-example-push");
            using var channel = connection.CreateModel();

            logger.Information("RabbitMQ connection {ClientProvidedName}", connection.ClientProvidedName);

            channel.ExchangeDeclare(
                exchange: "xglobalfanout",
                type: ExchangeType.Fanout,
                durable: true);
            var properties = channel.CreateBasicProperties();
            // RABBITMQ CONFIG

            // MAIN LOOP
            var maxTimeout = TimeSpan.Parse(settings["MESSAGEBROKERSENDER_MAXTIMEOUT"]);
            var topic = $"{NAMESPACE}.{parser.Object.Topic}";
            var cts = new CancellationTokenSource();
            var task = Task.Run(async () =>
            {
                using var processId = LogContext.PushProperty("ProcessId", Process.GetCurrentProcess().Id);
                using var threadId = LogContext.PushProperty("ThreadId", Thread.CurrentThread.ManagedThreadId);
                using var rk = LogContext.PushProperty("Topic", topic);
                var rnd = new Random();
                var partitionKeys = new List<string>
                {
                    "mmi",
                    "era",
                };
                while (!cts.IsCancellationRequested)
                {
                    var pk = partitionKeys[rnd.Next(0, 2)];
                    var wi = new WorkItem
                    {
                        PartitionKey = pk,
                        Data = "hello",
                    };
                    var json = JsonConvert.SerializeObject(wi);
                    byte[] payload = Encoding.UTF8.GetBytes(json);
                    // Routing key is ignored in exchange type "fanout"
                    channel.BasicPublish(
                        exchange: "xglobalfanout",
                        routingKey: topic,
                        basicProperties: properties,
                        body: payload);
                    var timeout = TimeSpan.FromSeconds(rnd.Next(1, maxTimeout.Seconds));
                    logger.Information("Message sent. {PartitionKey}  Waiting for {MaxTimeOut}.", wi.PartitionKey, timeout);
                    await Task.Delay(timeout, cts.Token);
                }
            }, cts.Token);
            Console.ReadLine();
            cts.Cancel();
            Task.WaitAny(task);
            // MAIN LOOP
        }
    }
}