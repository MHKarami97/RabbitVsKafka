using Avro;
using Avro.Generic;
using System;
using System.Threading;
using System.Threading.Tasks;
using Confluent.Kafka.SyncOverAsync;
using Confluent.SchemaRegistry.Serdes;
using Confluent.SchemaRegistry;
using Confluent.Kafka;

//Kafka producers and consumers are already decoupled in the sense that they do not communicate with one another directly;
//instead, information transfer happens via Kafka topics.
//But they are coupled in the sense that consumers need to know what the data they are reading represents in order
//to make sense of it—but this is something that is controlled by the producer!
//In the same way that Kafka acts as an intermediary for your data,
//Schema Registry can act as an intermediary for the types of data that you publish to Kafka.
namespace KafkaAvroGeneric
{
    class Program
    {
        private const string TopicName = "my-topic";
        private const string GroupId = "test-consumer-group";
        private const string BootstrapServers = "localhost:9092";
        private const string SchemaRegistryUrl = "localhost:9092";

        static async Task Main(string[] args)
        {
            // var s = (RecordSchema)RecordSchema.Parse(File.ReadAllText("my-schema.json"));
            var schema = (RecordSchema) RecordSchema.Parse(
                @"{
                    ""namespace"": ""Confluent.Kafka.Examples.AvroSpecific"",
                    ""type"": ""record"",
                    ""name"": ""User"",
                    ""fields"": [
                        {""name"": ""name"", ""type"": ""string""},
                        {""name"": ""favorite_number"",  ""type"": [""int"", ""null""]},
                        {""name"": ""favorite_color"", ""type"": [""string"", ""null""]}
                    ]
                  }"
            );

            var cts = new CancellationTokenSource();

            await Task.Run(() =>
            {
                using var schemaRegistry =
                    new CachedSchemaRegistryClient(
                        new SchemaRegistryConfig
                        {
                            Url = SchemaRegistryUrl
                        });
                
                using var consumer =
                    new ConsumerBuilder<string, GenericRecord>(new ConsumerConfig
                            {BootstrapServers = BootstrapServers, GroupId = GroupId})
                        .SetKeyDeserializer(new AvroDeserializer<string>(schemaRegistry).AsSyncOverAsync())
                        .SetValueDeserializer(new AvroDeserializer<GenericRecord>(schemaRegistry).AsSyncOverAsync())
                        .SetErrorHandler((_, e) => Console.WriteLine($"Error: {e.Reason}"))
                        .Build();
                
                consumer.Subscribe(TopicName);

                try
                {
                    while (true)
                    {
                        try
                        {
                            var consumeResult = consumer.Consume(cts.Token);

                            Console.WriteLine(
                                $"Key: {consumeResult.Message.Key}\nValue: {consumeResult.Message.Value}");
                        }
                        catch (ConsumeException e)
                        {
                            Console.WriteLine($"Consume error: {e.Error.Reason}");
                        }
                    }
                }
                catch (OperationCanceledException)
                {
                    // commit final offsets and leave the group.
                    consumer.Close();
                }
            }, cts.Token);

            using (var schemaRegistry =
                new CachedSchemaRegistryClient(
                    new SchemaRegistryConfig {Url = SchemaRegistryUrl}))
                
            using (var producer =
                new ProducerBuilder<string, GenericRecord>(new ProducerConfig {BootstrapServers = BootstrapServers})
                    .SetKeySerializer(new AvroSerializer<string>(schemaRegistry))
                    .SetValueSerializer(new AvroSerializer<GenericRecord>(schemaRegistry))
                    .Build())
            {
                Console.WriteLine($"{producer.Name} producing on {TopicName}. Enter user names, q to exit.");

                var i = 0;
                string text;
                
                while ((text = Console.ReadLine()) != "q")
                {
                    var record = new GenericRecord(schema);
                    record.Add("name", text);
                    record.Add("favorite_number", i++);
                    record.Add("favorite_color", "blue");

                    try
                    {
                        var dr = await producer.ProduceAsync(TopicName,
                            new Message<string, GenericRecord> {Key = text, Value = record}, cts.Token);
                        
                        Console.WriteLine($"produced to: {dr.TopicPartitionOffset}");
                    }
                    catch (ProduceException<string, GenericRecord> ex)
                    {
                        // In some cases (notably Schema Registry connectivity issues), the InnerException
                        // of the ProduceException contains additional information pertaining to the root
                        // cause of the problem. This information is automatically included in the output
                        // of the ToString() method of the ProduceException, called implicitly in the below.
                        Console.WriteLine($"error producing message: {ex}");
                    }
                }
            }

            cts.Cancel();
        }
    }
}