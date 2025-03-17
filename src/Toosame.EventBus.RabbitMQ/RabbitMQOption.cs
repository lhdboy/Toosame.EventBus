namespace Toosame.EventBus.RabbitMQ
{
    public class RabbitMQOption
    {
        /// <summary>
        /// Fail Retry Count
        /// </summary>
        public int EventBusRetryCount { get; set; }

        /// <summary>
        /// Queue Retry Count
        /// </summary>
        public int EventBusConsumerRetryCount { get; set; }

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
