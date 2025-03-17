namespace Toosame.EventBus.Events
{
    public record class DeadletterEvent : IntegrationEvent
    {
        public string? BrokerName { get; set; }

        public string? QueueName { get; set; }

        public string ConsumerName { get; set; }

        public string? DeathReason { get; set; }

        public string? EventName { get; set; }

        public string Payload { get; set; }
    }
}
