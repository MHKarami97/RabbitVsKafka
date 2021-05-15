using System;
using System.Collections.Generic;
using System.Text;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;

namespace RabbitMqConsumer
{
    class Program
    {
        private static IModel _channel;
        private static IBasicProperties _properties;

        private const bool AutoAck = false;
        private const string Queue = "myQueue";
        private const string Exchange = "myExchange";
        private const string RoutingKey = "myRouting";
        private const string ExchangeTypeName = ExchangeType.Fanout;

        private static List<string> Hosts = new()
        {
            "192.168.99.100:30000" +
            "192.168.99.100:30002" +
            "192.168.99.100:30004"
        };

        static void Main(string[] args)
        {
            Console.WriteLine("Start");

            var factory = new ConnectionFactory
            {
                AutomaticRecoveryEnabled = true
            };

            var connection = factory.CreateConnection();
            //var connection = factory.CreateConnection(Hosts);

            _channel = connection.CreateModel();

            //with the prefetchCount = 1 setting.
            //This tells RabbitMQ not to give more than one message to a worker at a time.
            //Or, in other words, don't dispatch a new message to a worker until it has processed and acknowledged the previous one.
            //Instead, it will dispatch it to the next worker that is not still busy.
            _channel.BasicQos(0, 1, false);

            _properties = _channel.CreateBasicProperties();

            _channel.QueueDeclare(
                queue: Queue,
                durable: false,
                exclusive: false,
                autoDelete: false,
                arguments: null);

            _channel.ExchangeDeclare(
                exchange: Exchange,
                type: ExchangeTypeName,
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

                //Manual acknowledgements can be batched to reduce network traffic.
                //This is done by setting the multiple field of acknowledgement methods to true
                //When the multiple field is set to true,
                //RabbitMQ will acknowledge all outstanding delivery tags up to and including the tag specified in the acknowledgement.
                //Like everything else related to acknowledgements, this is scoped per channel.
                _channel.BasicAck(
                    deliveryTag: ea.DeliveryTag,
                    multiple: false);

                // _channel.BasicReject(
                //     deliveryTag: ea.DeliveryTag,
                //     requeue: false);

                //It is possible to reject or requeue multiple messages at once using
                // _channel.BasicNack(
                //     deliveryTag: ea.DeliveryTag,
                //     multiple: true,
                //     requeue: true);
            };

            _channel.BasicConsume(
                queue: Queue,
                autoAck: AutoAck,
                consumer: consumer);

            Console.WriteLine(" Press [enter] to exit.");
            Console.ReadLine();
        }
    }
}