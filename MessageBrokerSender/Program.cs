using System.Text;
using RabbitMQ.Client;

namespace MessageBrokerSender
{
    internal class Program
    {
        private static void Main(string[] args)
        {
            var factory = new ConnectionFactory
            {
                HostName = "marcello-g3-3590"
            };

            using (var connection = factory.CreateConnection("mmi-reviso-mailbox-example-push"))
            using (var channel = connection.CreateModel())
            {
                channel.ExchangeDeclare(
                    exchange: "xglobalfanout",
                    type: ExchangeType.Fanout,
                    durable: true);
                var properties = channel.CreateBasicProperties();

                byte[] payload = Encoding.UTF8.GetBytes("hello");
                // Routing key is ignored in exchange type "fanout"
                channel.BasicPublish(
                    exchange: "xglobalfanout",
                    routingKey: "mmi.hello.world",
                    basicProperties: properties,
                    body: payload);
            }
        }
    }
}