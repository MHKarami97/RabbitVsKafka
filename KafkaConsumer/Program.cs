using System;
using System.Threading;
using System.Threading.Tasks;
using Confluent.Kafka;

namespace KafkaConsumer
{
    class Program
    {
        private static ConsumerConfig _config;
        private const string TopicName = "my-topic";
        private const string GroupId = "test-consumer-group";
        private const string BootstrapServers = "localhost:9092";

        static async Task Main(string[] args)
        {
            Console.WriteLine("Start");

            _config = new ConsumerConfig
            {
                BootstrapServers = BootstrapServers,
                GroupId = GroupId,

                // Note: The AutoOffsetReset property determines the start offset in the event
                // there are not yet any committed offsets for the consumer group for the
                // topic/partitions of interest. By default, offsets are committed
                // automatically, so in this example, consumption will only start from the
                // earliest message in the topic 'my-topic' the first time you run the program.
                AutoOffsetReset = AutoOffsetReset.Earliest
            };

            await Consume();
        }

        static async Task Consume()
        {
            using var consumer = new ConsumerBuilder<Ignore, string>(_config).Build();

            consumer.Subscribe(TopicName);

            var cts = new CancellationTokenSource();

            Console.CancelKeyPress += (_, e) =>
            {
                e.Cancel = true; // prevent the process from terminating.
                cts.Cancel();
            };

            try
            {
                while (true)
                {
                    try
                    {
                        var cr = consumer.Consume(cts.Token);
                        Console.WriteLine($"Consumed message '{cr.Value}' at: '{cr.TopicPartitionOffset}'.");
                    }
                    catch (ConsumeException e)
                    {
                        Console.WriteLine($"Error occured: {e.Error.Reason}");
                    }
                }
            }
            catch (OperationCanceledException)
            {
                // Ensure the consumer leaves the group cleanly and final offsets are committed.
                consumer.Close();
            }
        }
    }
}