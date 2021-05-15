using System;
using System.Threading;
using System.Threading.Tasks;
using Confluent.Kafka;

namespace KafkaConsumer
{
    class Program
    {
        private const int CommitPeriod = 5;
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

            Consume();
        }

        private static void Consume()
        {
            // Note: All handlers are called on the main .Consume thread.
            using var consumer = new ConsumerBuilder<Ignore, string>(_config)
                .SetErrorHandler((_, e) => Console.WriteLine($"Error: {e.Reason}"))
                .SetStatisticsHandler((_, json) => Console.WriteLine($"Statistics: {json}"))
                .SetPartitionsAssignedHandler((c, partitions) =>
                {
                    // Since a cooperative assignor (CooperativeSticky) has been configured, the
                    // partition assignment is incremental (adds partitions to any existing assignment).
                    Console.WriteLine($"Incremental partition assignment: [{string.Join(", ", partitions)}]");

                    // Possibly manually specify start offsets by returning a list of topic/partition/offsets
                    // to assign to, e.g.:
                    // return partitions.Select(tp => new TopicPartitionOffset(tp, externalOffsets[tp]));
                })
                .SetPartitionsRevokedHandler((c, partitions) =>
                {
                    // Since a cooperative assignor (CooperativeSticky) has been configured, the revoked
                    // assignment is incremental (may remove only some partitions of the current assignment).
                    Console.WriteLine($"Incremental partition revokation: [{string.Join(", ", partitions)}]");
                })
                .SetPartitionsLostHandler((c, partitions) =>
                {
                    // The lost partitions handler is called when the consumer detects that it has lost ownership
                    // of its assignment (fallen out of the group).
                    Console.WriteLine($"Partitions were lost: [{string.Join(", ", partitions)}]");
                }).Build();

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
                        var consumeResult = consumer.Consume(cts.Token);

                        if (consumeResult.IsPartitionEOF)
                        {
                            Console.WriteLine(
                                $"Reached end of topic {consumeResult.Topic}, partition {consumeResult.Partition}, offset {consumeResult.Offset}.");

                            continue;
                        }

                        Console.WriteLine(
                            $"Received message at {consumeResult.TopicPartitionOffset}: {consumeResult.Message.Value}");
                        
                        if (consumeResult.Offset % CommitPeriod == 0)
                        {
                            // The Commit method sends a "commit offsets" request to the Kafka
                            // cluster and synchronously waits for the response. This is very
                            // slow compared to the rate at which the consumer is capable of
                            // consuming messages. A high performance application will typically
                            // commit offsets relatively infrequently and be designed handle
                            // duplicate messages in the event of failure.
                            try
                            {
                                consumer.Commit(consumeResult);
                            }
                            catch (KafkaException e)
                            {
                                Console.WriteLine($"Commit error: {e.Error.Reason}");
                            }
                        }
                    }
                    catch (ConsumeException e)
                    {
                        Console.WriteLine($"Error occured: {e.Error.Reason}");
                    }
                }
            }
            catch (OperationCanceledException)
            {
                Console.WriteLine("Closing consumer.");
                
                // Ensure the consumer leaves the group cleanly and final offsets are committed.
                consumer.Close();
            }
        }
    }
}