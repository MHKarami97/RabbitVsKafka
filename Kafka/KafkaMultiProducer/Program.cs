using System;
using System.Collections.Generic;
using Confluent.Kafka;

//An example showing how to construct a Producer which re-uses the 
//underlying librdkafka client instance (and Kafka broker connections)
//of another producer instance. This allows you to produce messages
//with different types efficiently.
namespace KafkaMultiProducer
{
    class Program
    {
        private static ProducerConfig _config;
        private const string BootstrapServers = "localhost:9092";
        private static readonly List<string> Topics = new()
        {
            "first-topic",
            "second-topic"
        };

        static void Main(string[] args)
        {
            _config = new ProducerConfig
            {
                BootstrapServers = BootstrapServers
            };

            using var producer = new ProducerBuilder<string, string>(_config).Build();
            using var producer2 = new DependentProducerBuilder<Null, int>(producer.Handle).Build();

            // write (string, string) data to topic "first-topic".
            producer.ProduceAsync(Topics[0],
                new Message<string, string>
                {
                    Key = "my-key-value",
                    Value = "my-value"
                });

            // write (null, int) data to topic "second-data" using the same underlying broker connections.
            producer2.ProduceAsync(Topics[1], new Message<Null, int>
            {
                Value = 42
            });

            // producers are not tied to topics. Although it's unusual that you might want to
            // do so, you can use different producers to write to the same topic.
            producer2.ProduceAsync(Topics[0], new Message<Null, int>
            {
                Value = 107
            });

            // As the Tasks returned by ProduceAsync are not waited on there will still be messages in flight.
            producer.Flush(TimeSpan.FromSeconds(10));
        }
    }
}