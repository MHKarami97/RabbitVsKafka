using Confluent.Kafka;
using Confluent.SchemaRegistry;
using System;
using System.ComponentModel.DataAnnotations;
using System.Threading;
using System.Threading.Tasks;
using Confluent.Kafka.SyncOverAsync;
using Confluent.SchemaRegistry.Serdes;
using Newtonsoft.Json;

namespace KafkaJsonSerialization
{
    class Program
    {
        private static ProducerConfig _producerConfig;
        private static ConsumerConfig _configConsumerConfig;
        private const string TopicName = "my-topic";
        private const string BootstrapServers = "localhost:9092";
        private const string SchemaRegistryUrl = "localhost:9092";

        static async Task Main(string[] args)
        {
            _producerConfig = new ProducerConfig
            {
                BootstrapServers = BootstrapServers
            };

            var schemaRegistryConfig = new SchemaRegistryConfig
            {
                // Note: you can specify more than one schema registry url using the
                // schema.registry.url property for redundancy (comma separated list). 
                // The property name is not plural to follow the convention set by
                // the Java implementation.
                Url = SchemaRegistryUrl
            };

            _configConsumerConfig = new ConsumerConfig
            {
                BootstrapServers = BootstrapServers,
                GroupId = "json-example-consumer-group"
            };

            // Note: Specifying json serializer configuration is optional.
            var avroSerializerConfig = new AvroSerializerConfig
            {
                BufferBytes = 100
            };

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
                    new ConsumerBuilder<Null, Person>(_configConsumerConfig)
                        .SetValueDeserializer(new AvroDeserializer<Person>(schemaRegistry).AsSyncOverAsync())
                        .SetErrorHandler((_, e) => Console.WriteLine($"Error: {e.Reason}"))
                        .Build();

                consumer.Subscribe(TopicName);

                try
                {
                    while (true)
                    {
                        try
                        {
                            var cr = consumer.Consume(cts.Token);
                            Console.WriteLine(
                                $"Name: {cr.Message.Value.FirstName} {cr.Message.Value.LastName}, age: {cr.Message.Value.Age}");
                        }
                        catch (ConsumeException e)
                        {
                            Console.WriteLine($"Consume error: {e.Error.Reason}");
                        }
                    }
                }
                catch (OperationCanceledException)
                {
                    consumer.Close();
                }
            }, cts.Token);

            using (var schemaRegistry = new CachedSchemaRegistryClient(schemaRegistryConfig))
            using (var producer =
                new ProducerBuilder<Null, Person>(_producerConfig)
                    .SetValueSerializer(new AvroSerializer<Person>(schemaRegistry, avroSerializerConfig))
                    .Build())
            {
                Console.WriteLine($"{producer.Name} producing on {TopicName}. Enter first names, q to exit.");

                var i = 0;
                string text;

                while ((text = Console.ReadLine()) != "q")
                {
                    var person = new Person
                    {
                        FirstName = text,
                        LastName = "lastname",
                        Age = i++ % 150
                    };

                    await producer
                        .ProduceAsync(TopicName, new Message<Null, Person> {Value = person}, cts.Token)
                        .ContinueWith(task => task.IsFaulted
                            ? $"error producing message: {task.Exception?.Message}"
                            : $"produced to: {task.Result.TopicPartitionOffset}", cts.Token);
                }
            }

            cts.Cancel();

            using (var schemaRegistry = new CachedSchemaRegistryClient(schemaRegistryConfig))
            {
                // Note: a subject name strategy was not configured, so the default "Topic" was used.
                var schema =
                    await schemaRegistry.GetLatestSchemaAsync(
                        SubjectNameStrategy.Topic.ConstructValueSubjectName(TopicName));

                Console.WriteLine("\nThe JSON schema corresponding to the written data:");

                Console.WriteLine(schema.SchemaString);
            }
        }
    }

    //     A POCO class corresponding to the JSON data written
    //     to Kafka, where the schema is implicitly defined through 
    //     the class properties and their attributes.
    //
    //     Internally, the JSON serializer uses Newtonsoft.Json for
    //     serialization and NJsonSchema for schema creation and
    ///     validation. You can use any property annotations recognised
    ///     by these libraries.
    //
    //     Note: Off-the-shelf libraries do not yet exist to enable
    //     integration of System.Text.Json and JSON Schema, so this
    //     is not yet supported by the Confluent serializers.
    internal class Person
    {
        [JsonRequired] // use Newtonsoft.Json annotations
        [JsonProperty("firstName")]
        public string FirstName { get; set; }

        [JsonRequired]
        [JsonProperty("lastName")]
        public string LastName { get; set; }

        [Range(0, 150)] // or System.ComponentModel.DataAnnotations annotations
        [JsonProperty("age")]
        public int Age { get; set; }
    }
}