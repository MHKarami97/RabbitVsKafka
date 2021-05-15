using System;
using System.Threading;
using System.Threading.Tasks;
using Confluent.Kafka;

namespace kafkaProducer
{
    class Program
    {
        private static ProducerConfig _config;
        private const string TopicName = "my-topic";
        private const string BootstrapServers = "localhost:9092";

        static async Task Main(string[] args)
        {
            Console.WriteLine("Start");

            _config = new ProducerConfig
            {
                BootstrapServers = BootstrapServers,

                //the minimum time between batches of messages being sent to the cluster.
                //Larger values allow for more batching, which increases throughput. Smaller values may reduce latency.
                //The tradeoff is complicated somewhat as throughput increases though because a smaller LingerMs will
                //result in more broker requests, putting greater load on the server, which in turn may reduce throughput.
                //A good general purpose setting is 5 (the default is 0.5).
                LingerMs = 5,

                //If your throughput is very low, and you really care about latency, you should probably set to true.
                SocketNagleDisable = false,

                //The default value for the Acks configuration property is All.
                //This means that if a delivery report returns without error,
                //the message has been replicated to all replicas in the in-sync replica set.
                //If you have EnableIdempotence set to true, Acks must be all.
                //You should generally prefer having acks set to all. There's no real benefit to setting it lower.
                //Note that this won't improve end-to-end latency because messages must be replicated to all in-sync replicas
                //before they are available for consumption - you will just get to know whether the message has been successfully
                //written to the leader replica a little earlier.
                Acks = Acks.All,

                //This has very little overhead - there's not much downside to having this on by default
                //Before the idempotent producer was available,
                //you could achieve this by setting MaxInFlight to 1 (at the expense of reduced throughput).
                //when idempotence is not enabled, in case of failure (temporary network failure for example),
                //Confluent.Kafka will try to resend the data which can cause reordering
                EnableIdempotence = null
            };

            await Produce();
        }

        static async Task Produce()
        {
            var counter = 1;

            //The --Null-- deserializer will throw an exception if the corresponding message key or value is not null,
            //whereas the --Ignore-- deserializer will return null regardless of whether the data is null or something else.
            using var producer = new ProducerBuilder<Null, string>(_config).Build();

            while (true)
            {
                producer.Produce(TopicName, new Message<Null, string>
                {
                    Value = (counter++).ToString()
                }, Handler);

                // Note: Awaiting the asynchronous produce request below prevents flow of execution
                // from proceeding until the acknowledgement from the broker is received (at the 
                // expense of low throughput).

                // var deliveryReport = await producer
                //     .ProduceAsync(TopicName, new Message<Null, string>
                //     {
                //         Value = (counter++).ToString()
                //     });
                //
                // Console.WriteLine($"delivered to: {deliveryReport.TopicPartitionOffset}");

                //The Produce method is more efficient,
                //and you should care about that if your throughput is high (>~ 20k msgs/s).
                //Even if your throughput is low,
                //the difference between Produce and ProduceAsync
                //will be negligible compared to whatever else you application is doing.
                //As a general rule, Produce is recommended

                Thread.SpinWait(500);
            }

            // Since we are producing synchronously, at this point there will be no messages
            // in-flight and no delivery reports waiting to be acknowledged, so there is no
            // need to call producer.Flush before disposing the producer.

            // wait for up to 10 seconds for any inflight messages to be delivered.
            //producer.Flush(TimeSpan.FromSeconds(10));
        }

        static void Handler(DeliveryReport<Null, string> report)
        {
            //Most importantly, you should be checking the result of each produce call
            Console.WriteLine(!report.Error.IsError
                ? $"Delivered message to {report.TopicPartitionOffset}"
                : $"Delivery Error: {report.Error.Reason}");
        }
    }
}