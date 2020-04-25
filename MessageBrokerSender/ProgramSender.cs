using System;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Fclp;
using Microsoft.ApplicationInsights;
using Microsoft.ApplicationInsights.Extensibility;
using Microsoft.Extensions.Configuration;
using RabbitMQ.Client;
using Serilog;
using Serilog.Exceptions;
using Serilog.Sinks.SystemConsole.Themes;

namespace MessageBrokerSender
{
    internal class ProgramSender
    {
        private const string NAMESPACE = "mmi";
        private const string CHANNELNAME = "reviso-mailbox-example";

        private static void Main(string[] args)
        {
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
            var logger = loggerMain.ForContext<ProgramSender>();

            using var connection = factory.CreateConnection("mmi-reviso-mailbox-example-push");
            using var channel = connection.CreateModel();

            logger.Information("RabbitMQ connection {ClientProvidedName}", connection.ClientProvidedName);

            channel.ExchangeDeclare(
                exchange: "xglobalfanout",
                type: ExchangeType.Fanout,
                durable: true);
            var properties = channel.CreateBasicProperties();

            var maxTimeout = TimeSpan.Parse(settings["MESSAGEBROKERSENDER_MAXTIMEOUT"]);
            var cts = new CancellationTokenSource();
            var task = Task.Run(async () =>
            {
                var rnd = new Random();
                while (!cts.IsCancellationRequested)
                {
                    byte[] payload = Encoding.UTF8.GetBytes($"hello");
                    // Routing key is ignored in exchange type "fanout"
                    var topic = $"{NAMESPACE}.{parser.Object.Topic}";
                    channel.BasicPublish(
                        exchange: "xglobalfanout",
                        routingKey: topic,
                        basicProperties: properties,
                        body: payload);
                    var timeout = TimeSpan.FromSeconds(rnd.Next(1, maxTimeout.Seconds));
                    logger.Information("Message sent on {Topic}. Waiting for {MaxTimeOut}.", topic, timeout);
                    await Task.Delay(timeout, cts.Token);
                }
            }, cts.Token);
            Console.ReadLine();
            cts.Cancel();
            Task.WaitAny(task);
        }
    }
}