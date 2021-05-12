using System;
using System.Collections.Concurrent;
using System.Linq;
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

        private const string Queue = "myQueue";
        private const string Exchange = "myExchange";
        private const string RoutingKey = "myRouting";
        private static ConcurrentDictionary<ulong, string> _outstandingConfirms = new();

        static void Main(string[] args)
        {
            Console.WriteLine("Start");

            var factory = new ConnectionFactory
            {
                AutomaticRecoveryEnabled = true
            };

            var connection = factory.CreateConnection();

            _channel = connection.CreateModel();

            //Publisher confirms are a RabbitMQ extension to the AMQP 0.9.1 protocol, so they are not enabled by default.
            //This method must be called on every channel that you expect to use publisher confirms.
            //Confirms should be enabled just once, not for every message published.
            _channel.ConfirmSelect();

            //it possible to limit the number of unacknowledged messages on a channel (or connection) when consuming
            //_channel.BasicQos(10, 10, true);

            //delivery tag: the sequence number identifying the confirmed or nack-ed message.
            //We will see shortly how to correlate it with the published message.

            //multiple: this is a boolean value. If false, only one message is confirmed/nack-ed, if true,
            //all messages with a lower or equal sequence number are confirmed/nack-ed.
            _channel.BasicAcks += (sender, ea) =>
            {
                // code when message is confirmed

                CleanOutstandingConfirms(ea.DeliveryTag, ea.Multiple);
            };
            _channel.BasicNacks += (sender, ea) =>
            {
                //code when message is nack-ed

                _outstandingConfirms.TryGetValue(ea.DeliveryTag, out var body);

                Console.WriteLine(
                    $"Message with body {body} has been nack-ed. Sequence number: {ea.DeliveryTag}, multiple: {ea.Multiple}");

                CleanOutstandingConfirms(ea.DeliveryTag, ea.Multiple);
            };

            _properties = _channel.CreateBasicProperties();

            //Marking messages as persistent doesn't fully guarantee that a message won't be lost.
            //Although it tells RabbitMQ to save the message to disk,
            //there is still a short time window when RabbitMQ has accepted a message and hasn't saved it yet. Also,
            //RabbitMQ doesn't do fsync(2) for every message -- it may be just saved to cache and not really written to the disk.
            //The persistence guarantees aren't strong, but it's more than enough for our simple task queue.
            //If you need a stronger guarantee then you can use publisher confirms
            _properties.Persistent = true;

            Send();
        }

        static void Send()
        {
            try
            {
                var counter = 1;
                var batchSize = 100;
                var body = string.Empty;
                var outstandingMessageCount = 0;

                while (true)
                {
                    body = $"my message {counter++}";

                    _outstandingConfirms.TryAdd(_channel.NextPublishSeqNo, body);

                    _channel.BasicPublish(
                        exchange: Exchange,
                        routingKey: RoutingKey,
                        basicProperties: _properties,
                        body: Encoding.UTF8.GetBytes(body));

                    //publishing a message and waiting synchronously for its confirmation
                    //The method returns as soon as the message has been confirmed.
                    //If the message is not confirmed within the timeout or if it is nack-ed
                    //(meaning the broker could not take care of it for some reason),
                    //the method will throw an exception
                    //The handling of the exception usually consists in logging an error message and/or retrying to send the message.
                    //it significantly slows down publishing,
                    //as the confirmation of a message blocks the publishing of all subsequent messages.
                    //This approach is not going to deliver throughput of more than a few hundreds of published messages per second

                    //_channel.WaitForConfirmsOrDie(new TimeSpan(0, 0, 5));


                    //Waiting for a batch of messages to be confirmed improves throughput drastically over waiting for a confirm
                    //for individual message (up to 20-30 times with a remote RabbitMQ node).
                    //One drawback is that we do not know exactly what went wrong in case of failure,
                    //so we may have to keep a whole batch in memory to log something meaningful or to re-publish the messages.
                    //And this solution is still synchronous, so it blocks the publishing of messages.

                    // outstandingMessageCount++;
                    // if (outstandingMessageCount == batchSize)
                    // {
                    //     _channel.WaitForConfirmsOrDie(new TimeSpan(0, 0, 5));
                    //
                    //     outstandingMessageCount = 0;
                    // }

                    Thread.SpinWait(500);
                }
            }
            catch (OperationInterruptedException e)
            {
                Console.WriteLine(e);
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

        static void CleanOutstandingConfirms(ulong sequenceNumber, bool multiple)
        {
            if (multiple)
            {
                var confirmed = _outstandingConfirms
                    .Where(k => k.Key <= sequenceNumber);

                foreach (var entry in confirmed)
                {
                    _outstandingConfirms.TryRemove(entry.Key, out _);
                }
            }
            else
            {
                _outstandingConfirms.TryRemove(sequenceNumber, out _);
            }
        }
    }
}