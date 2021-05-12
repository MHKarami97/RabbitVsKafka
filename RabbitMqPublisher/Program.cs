using System;
using System.Text;
using System.Threading;
using RabbitMQ.Client;
using RabbitMQ.Client.Exceptions;

namespace RabbitMqPublisher
{
    class Program
    {
        private static IModel _channel;
        private static IBasicProperties _properties;
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
        }

        static void Send()
        {
            try
            {
                _channel.BasicPublish(
                    exchange: Exchange,
                    routingKey: RoutingKey,
                    basicProperties: _properties,
                    body: Encoding.UTF8.GetBytes("my message"));
            }
            catch (BrokerUnreachableException)
            {
                Thread.Sleep(TimeSpan.FromSeconds(1));
            }
            catch (Exception e)
            {
                Console.WriteLine(e);
            }
        }
    }
}