namespace Util
{
    public static class Configuration
    {
        private const bool AutoAck = false;
        private const bool AutoDelete = false;
        private const bool Exclusive = false;
        private const bool Durable = true;
        private const bool Persistent = true;
        private const bool ExchangeAutoDelete = false;
        private const bool AutomaticRecoveryEnabled = true;
        private const int RetryCount = 3;
        private const string UserName = "guest";
        private const string Password = "guest";
        private const string Exchange = "amq.direct";
        private const string ExchangeType = "direct";
        private const string RoutingKey = "MyRouting";
        private const string Queue = "myQueue";
        private const string QueueDeadLetter = "QueueDeadLetter";
        private const string ExchangeDeadLetter = "DeadLetterExchange";
        private const string RoutingKeyDeadLetter = "DeadLetterRoute";
        private const string Hosts = "localhost:5672";

        public static RabbitMqConfig GetConfiguration()
        {
            var hostnamesString = Hosts.Split(',').ToList();

            var hostnames = hostnamesString
                .Select(hostname => hostname.Split(':'))
                .Select(tmp => new RabbitEndpoint
                {
                    Name = tmp[0],
                    Port = Convert.ToInt32(tmp[1])
                }).ToList();

            var configuration = new RabbitMqConfig
            {
                UserName = UserName,
                Password = Password,
                Exchange = Exchange,
                AutomaticRecoveryEnabled = AutomaticRecoveryEnabled,
                RequeueMessageRetryCount = RetryCount,
                ExchangeAutoDelete = ExchangeAutoDelete,
                Durable = Durable,
                AutoAck = AutoAck,
                Exclusive = Exclusive,
                AutoDelete = AutoDelete,
                Persistent = Persistent,
                ExchangeType = ExchangeType,
                HostNames = hostnames,
                Queue = Queue,
                QueueDeadLetter = QueueDeadLetter,
                RouteKey = RoutingKey,
                DeadLetter = new DeadLetter
                {
                    Exchange = ExchangeDeadLetter,
                    RoutingKey = RoutingKeyDeadLetter
                }
            };

            return configuration;
        }
    }
}