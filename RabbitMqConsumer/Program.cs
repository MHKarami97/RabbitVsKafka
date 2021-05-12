using System;
using System.Text;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;

namespace RabbitMqConsumer
{
    class Program
    {
        private static IModel _channel;
        private static IBasicProperties _properties;

        private const string Queue = "";
        private const string Exchange = "";
        private const string RoutingKey = "";

        static void Main(string[] args)
        {
            Console.WriteLine("Start");

            var factory = new ConnectionFactory
            {
                AutomaticRecoveryEnabled = true
            };

            var connection = factory.CreateConnection();

            _channel = connection.CreateModel();

            _properties = _channel.CreateBasicProperties();

            _channel.QueueDeclare(
                queue: "hello",
                durable: false,
                exclusive: false,
                autoDelete: false,
                arguments: null);

            _channel.ExchangeDeclare(
                exchange: Exchange,
                type: ExchangeType.Fanout,
                durable: true,
                autoDelete: false);

            _channel.QueueBind(
                queue: Queue,
                exchange: Exchange,
                routingKey: RoutingKey);

            var consumer = new EventingBasicConsumer(_channel);

            consumer.Received += (model, ea) =>
            {
                var body = ea.Body.ToArray();
                var message = Encoding.UTF8.GetString(body);

                Console.WriteLine(" [x] Received {0}", message);
            };

            _channel.BasicConsume(
                queue: Queue,
                autoAck: true,
                consumer: consumer);

            Console.WriteLine(" Press [enter] to exit.");
            Console.ReadLine();
        }
    }
}