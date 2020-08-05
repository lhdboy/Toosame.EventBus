namespace Toosame.EventBus.RabbitMQ
{
    public class RabbitMQOption
    {
        /// <summary>
        /// Connection Host And Port
        /// </summary>
        public string EventBusConnection { get; set; }

        /// <summary>
        /// Username
        /// </summary>
        public string EventBusUserName { get; set; }

        /// <summary>
        /// Password
        /// </summary>
        public string EventBusPassword { get; set; }

        /// <summary>
        /// Fail Retry Count
        /// </summary>
        public int EventBusRetryCount { get; set; }

        /// <summary>
        /// ExchangeName
        /// </summary>
        public string EventBusBrokeName { get; set; }

        /// <summary>
        /// QueueName
        /// </summary>
        public string SubscriptionClientName { get; set; }

        /// <summary>
        /// Client Provided Name
        /// </summary>
        public string ClientProvidedName { get; set; }
    }
}
