using System;
using System.Collections.Generic;
using System.Text;
using System.Threading.Tasks;
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
    internal class Program
    {
        private const int EXPIRES = 259200000;

        private static void Main(string[] args)
        {
            var settings = new ConfigurationBuilder()
                .Build();

            var telemetryConf = new TelemetryConfiguration(settings["APPINSIGHTS_INSTRUMENTATIONKEY"]);
            var telemetryClient = new TelemetryClient(telemetryConf);
            var loggerConf = new LoggerConfiguration();
            loggerConf
                .ReadFrom.Configuration(settings)
                .Enrich.FromLogContext()
                .Enrich.WithExceptionDetails()
                .WriteTo.Console(theme: AnsiConsoleTheme.Literate)
                .WriteTo.ApplicationInsights(telemetryClient, TelemetryConverter.Traces);
            if (settings["Serilog_WriteTo_Seq_ServerUrl"] != null)
            {
                loggerConf.WriteTo.Seq(settings["Serilog_WriteTo_Seq_ServerUrl"]);
            }

            var factory = new ConnectionFactory
            {
                HostName = "marcello-g3-3590"
            };

            using (var connection = factory.CreateConnection("mmi-reviso-mailbox-example-pull"))
            using (var channel = connection.CreateModel())
            {
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
                    exchange: "xreviso-mailbox-example",
                    type: ExchangeType.Topic,
                    durable: true);

                channel.ExchangeBind(
                    source: "xglobalfanout",
                    destination: "xreviso-mailbox-example",
                    routingKey: "#");

                var queue = channel.QueueDeclare(
                    queue: "reviso-mailbox-example",
                    durable: true,
                    exclusive: false,
                    autoDelete: false,
                    arguments: new Dictionary<string, object>
                    {
                        {"x-expires", Convert.ToInt64(EXPIRES)},
                    });

                channel.QueueBind(
                    queue: queue.QueueName,
                    exchange: "xreviso-mailbox-example",
                    routingKey: "mmi.hello.world");

                var consumer = new EventingBasicConsumer(channel);
                consumer.Received += async (model, payload) =>
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
                        await MessageHandlerAsync(message, headers);
                    }
                    catch (Exception e)
                    {
                        Console.Error(e);
                    }
                };

                channel.BasicConsume(queue.QueueName, false, consumer);
                Console.ReadLine();
            }
        }

        private static async Task MessageHandlerAsync(string message, IDictionary<string, object> headers)
        {
        }
    }
}