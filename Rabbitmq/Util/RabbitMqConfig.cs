namespace Util
{
    public class RabbitMqConfig
    {
        public List<RabbitEndpoint> HostNames { get; set; }
        public DeadLetter DeadLetter { get; set; }
        public string Exchange { get; set; }
        public string ExchangeType { get; set; }
        public bool ExchangeAutoDelete { get; set; }
        public bool Durable { get; set; }
        public bool AutoAck { get; set; }
        public bool AutoDelete { get; set; }
        public bool Exclusive { get; set; }
        public bool Persistent { get; set; }
        public string RouteKey { get; set; }
        public string UserName { get; set; }
        public string Password { get; set; }
        public string Queue { get; set; }
        public string QueueDeadLetter { get; set; }
        public int RequeueMessageRetryCount { get; set; }
        public bool AutomaticRecoveryEnabled { get; set; }
    }

    public class RabbitEndpoint
    {
        public string Name { get; set; }
        public int Port { get; set; }
    }

    public class DeadLetter
    {
        public string Exchange { get; set; }
        public string RoutingKey { get; set; }
    }
}