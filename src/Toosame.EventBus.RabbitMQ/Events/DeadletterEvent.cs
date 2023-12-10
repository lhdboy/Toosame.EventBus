using Toosame.EventBus.Events;

namespace Toosame.EventBus.RabbitMQ.Events
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
