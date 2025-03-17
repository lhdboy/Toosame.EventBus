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
    }
}
