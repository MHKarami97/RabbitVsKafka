using System;
using System.Collections.Generic;
using System.Linq;
using Confluent.Kafka;

namespace KafkaTopicPartitionOffset
{
    class Program
    {
        private static ConsumerConfig _config;
        private const string GroupId = "test-consumer-group";
        private const string BootstrapServers = "localhost:9092";
        private static readonly List<string> Topics = new()
        {
            "my-topic"
        };

        static void Main(string[] args)
        {
            Console.WriteLine("Start");

            _config = new ConsumerConfig
            {
                // the group.id property must be specified when creating a consumer, even 
                // if you do not intend to use any consumer group functionality.
                GroupId = GroupId,

                BootstrapServers = BootstrapServers,

                // partition offsets can be committed to a group even by consumers not
                // subscribed to the group. in this example, auto commit is disabled
                // to prevent this from occurring.
                EnableAutoCommit = true
            };

            Consume();
        }


        //Consumer group functionality (i.e. .Subscribe + offset commits) is not used.
        //The consumer is manually assigned to a partition and always starts consumption
        //From a specific offset (0).
        private static void Consume()
        {
            using var consumer =
                new ConsumerBuilder<Ignore, string>(_config)
                    .SetErrorHandler((_, e) => Console.WriteLine($"Error: {e.Reason}"))
                    .Build();

            //From a specific offset (0)
            consumer.Assign(
                Topics.Select(topic =>
                        new TopicPartitionOffset(topic, 0, Offset.Beginning))
                    .ToList());

            try
            {
                while (true)
                {
                    try
                    {
                        var consumeResult = consumer.Consume();

                        // Note: End of partition notification has not been enabled, so
                        // it is guaranteed that the ConsumeResult instance corresponds
                        // to a Message, and not a PartitionEOF event.
                        Console.WriteLine(
                            $"Received message at {consumeResult.TopicPartitionOffset}: ${consumeResult.Message.Value}");
                    }
                    catch (ConsumeException e)
                    {
                        Console.WriteLine($"Consume error: {e.Error.Reason}");
                    }
                }
            }
            catch (OperationCanceledException)
            {
                Console.WriteLine("Closing consumer.");

                consumer.Close();
            }
        }
    }
}