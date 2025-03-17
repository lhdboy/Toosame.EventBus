using System.Collections.Generic;
using System.Threading.Tasks;

using Toosame.EventBus.Events;

namespace Toosame.EventBus.Abstractions
{
    public interface IEventBus
    {
        Task PublishAsync(IntegrationEvent @event);

        Task PublishAsync(params IntegrationEvent[] @event);

        Task PublishAsync(IEnumerable<IntegrationEvent> @event);

        Task StartSubscribeAsync();

        Task SubscribeAsync<T, TH>()
            where T : IntegrationEvent
            where TH : IIntegrationEventHandler<T>;

        Task SubscribeDynamicAsync<TH>(string eventName)
            where TH : IDynamicIntegrationEventHandler;

        void UnsubscribeDynamic<TH>(string eventName)
            where TH : IDynamicIntegrationEventHandler;

        void Unsubscribe<T, TH>()
            where TH : IIntegrationEventHandler<T>
            where T : IntegrationEvent;
    }
}
