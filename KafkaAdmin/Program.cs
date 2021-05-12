using System;
using System.Threading.Tasks;
using Confluent.Kafka;
using Confluent.Kafka.Admin;

namespace KafkaAdmin
{
    public class Program
    {
        private const string TopicName = "my-topic";
        private const string BootstrapServers = "localhost:9092";

        public static async Task Main(string[] args)
        {
            Console.WriteLine(
                "usage: \n list-groups \n metadata \n library-version \n create-topic");

            var input = Console.ReadLine();

            switch (input)
            {
                case "library-version":
                    Console.WriteLine($"librdkafka Version: {Library.VersionString} ({Library.Version:X})");
                    Console.WriteLine($"Debug Contexts: {string.Join(", ", Library.DebugContexts)}");
                    break;
                case "list-groups":
                    ListGroups(BootstrapServers);
                    break;
                case "metadata":
                    PrintMetadata(BootstrapServers);
                    break;
                case "create-topic":
                    await CreateTopicAsync(BootstrapServers, TopicName);
                    break;
                default:
                    Console.WriteLine($"unknown command: {input}");
                    break;
            }
        }

        static void ListGroups(string bootstrapServers)
        {
            using var adminClient = new AdminClientBuilder(new AdminClientConfig {BootstrapServers = bootstrapServers})
                .Build();

            // Warning: The API for this functionality is subject to change.
            var groups = adminClient.ListGroups(TimeSpan.FromSeconds(10));

            Console.WriteLine("Consumer Groups:");

            foreach (var g in groups)
            {
                Console.WriteLine($"  Group: {g.Group} {g.Error} {g.State}");
                Console.WriteLine($"  Broker: {g.Broker.BrokerId} {g.Broker.Host}:{g.Broker.Port}");
                Console.WriteLine($"  Protocol: {g.ProtocolType} {g.Protocol}");
                Console.WriteLine($"  Members:");

                foreach (var m in g.Members)
                {
                    Console.WriteLine($"    {m.MemberId} {m.ClientId} {m.ClientHost}");
                    Console.WriteLine($"    Metadata: {m.MemberMetadata.Length} bytes");
                    Console.WriteLine($"    Assignment: {m.MemberAssignment.Length} bytes");
                }
            }
        }

        static void PrintMetadata(string bootstrapServers)
        {
            using var adminClient = new AdminClientBuilder(new AdminClientConfig {BootstrapServers = bootstrapServers})
                .Build();

            // Warning: The API for this functionality is subject to change.
            var meta = adminClient.GetMetadata(TimeSpan.FromSeconds(20));

            Console.WriteLine($"{meta.OriginatingBrokerId} {meta.OriginatingBrokerName}");

            meta.Brokers.ForEach(broker =>
                Console.WriteLine($"Broker: {broker.BrokerId} {broker.Host}:{broker.Port}"));

            meta.Topics.ForEach(topic =>
            {
                Console.WriteLine($"Topic: {topic.Topic} {topic.Error}");
                topic.Partitions.ForEach(partition =>
                {
                    Console.WriteLine($"  Partition: {partition.PartitionId}");
                    Console.WriteLine($"    Replicas: {ToString(partition.Replicas)}");
                    Console.WriteLine($"    InSyncReplicas: {ToString(partition.InSyncReplicas)}");
                });
            });
        }

        static async Task CreateTopicAsync(string bootstrapServers, string topicName)
        {
            using var adminClient = new AdminClientBuilder(new AdminClientConfig {BootstrapServers = bootstrapServers})
                .Build();

            try
            {
                await adminClient.CreateTopicsAsync(new[]
                {
                    new TopicSpecification {Name = topicName, ReplicationFactor = 1, NumPartitions = 1}
                });
            }
            catch (CreateTopicsException e)
            {
                Console.WriteLine(
                    $"An error occured creating topic {e.Results[0].Topic}: {e.Results[0].Error.Reason}");
            }
        }

        private static string ToString(int[] array) => $"[{string.Join(", ", array)}]";
    }
}