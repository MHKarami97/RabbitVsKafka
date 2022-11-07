using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;
using Util;

namespace RabbitMqConsumer
{
    class Program
    {
        private static IModel _channel;
        private static RabbitMqConfig _config;

        static void Main(string[] args)
        {
            Console.WriteLine("Start");

            _config = Configuration.GetConfiguration();

            var factory = new ConnectionFactory
            {
                AutomaticRecoveryEnabled = _config.AutomaticRecoveryEnabled
            };

            var endpoints = _config.HostNames
                .Select(item => new AmqpTcpEndpoint(item.Name, item.Port))
                .ToList();

            //var connection = factory.CreateConnection();
            var connection = factory.CreateConnection(endpoints);

            _channel = connection.CreateModel();

            //with the prefetchCount = 1 setting.
            //This tells RabbitMQ not to give more than one message to a worker at a time.
            //Or, in other words, don't dispatch a new message to a worker until it has processed and acknowledged the previous one.
            //Instead, it will dispatch it to the next worker that is not still busy.
            _channel.BasicQos(0, 1, false);

            _ = _channel.CreateBasicProperties();

            //if queue has 'x-dead-letter-routing-key' it can be 'direct', if not it must be 'fanout'
            _channel.ExchangeDeclare(
                exchange: _config.DeadLetter.Exchange,
                type: "direct",
                durable: true,
                autoDelete: false);

            _channel.ExchangeDeclare(
                exchange: _config.Exchange,
                type: _config.ExchangeType,
                durable: true,
                autoDelete: false);

            _channel.QueueDeclare(
                queue: _config.Queue,
                durable: false,
                exclusive: false,
                autoDelete: false,
                arguments: new Dictionary<string, object>
                {
                    { "x-dead-letter-exchange", _config.DeadLetter.Exchange },
                    { "x-dead-letter-routing-key", _config.DeadLetter.RoutingKey }
                });

            _channel.QueueBind(
                queue: _config.Queue,
                exchange: _config.Exchange,
                routingKey: _config.RouteKey);

            var consumer = new EventingBasicConsumer(_channel);

            // For some Encoding. not necessary
            Encoding.RegisterProvider(CodePagesEncodingProvider.Instance);

            var counter = 0;

            consumer.Received += (model, ea) =>
            {
                try
                {
                    var body = ea.Body.ToArray();

                    var message = Encoding.UTF8.GetString(body);

                    Console.WriteLine(" [x] Received {0}", message);

                    //var isSuccessful = Received.Invoke(message);

                    if (HandleIsSuccess())
                    {
                        counter = 0;

                        //Manual acknowledgements can be batched to reduce network traffic.
                        //This is done by setting the multiple field of acknowledgement methods to true
                        //When the multiple field is set to true,
                        //RabbitMQ will acknowledge all outstanding delivery tags up to and including the tag specified in the acknowledgement.
                        //Like everything else related to acknowledgements, this is scoped per channel.
                        _channel.BasicAck(
                            deliveryTag: ea.DeliveryTag,
                            multiple: false);

                        return;
                    }

                    if (counter > _config.RequeueMessageRetryCount)
                    {
                        counter = 0;

                        _channel.BasicReject(
                            deliveryTag: ea.DeliveryTag,
                            requeue: false);

                        return;
                    }

                    counter++;

                    Thread.SpinWait(100);

                    //It is possible to reject or requeue multiple messages at once using
                    _channel.BasicNack(
                        deliveryTag: ea.DeliveryTag,
                        multiple: false,
                        requeue: true);
                }
                catch (Exception e)
                {
                    Console.WriteLine(e);
                    throw;
                }
            };

            _channel.BasicConsume(
                queue: _config.Queue,
                autoAck: _config.AutoAck,
                consumer: consumer);

            Console.WriteLine(" Press [enter] to exit.");
            Console.ReadLine();
        }

        private static bool HandleIsSuccess()
        {
            var rnd = new Random().Next(1, 1000);

            return rnd <= 500;
        }
    }
}