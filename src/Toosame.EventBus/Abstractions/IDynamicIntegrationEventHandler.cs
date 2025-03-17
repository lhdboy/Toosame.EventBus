using System.Threading.Tasks;

namespace Toosame.EventBus.Abstractions
{
    public interface IDynamicIntegrationEventHandler
    {
        Task HandleAsync(dynamic eventData);
    }
}
