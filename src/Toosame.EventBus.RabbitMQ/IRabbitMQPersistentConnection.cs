using RabbitMQ.Client;

using System;
using System.Threading.Tasks;

namespace Toosame.EventBus.RabbitMQ
{
    public interface IRabbitMQPersistentConnection : IAsyncDisposable
    {
        bool IsConnected { get; }

        string ClientProvidedName { get; }

        IConnection GetConnectionAsync();

        Task<bool> TryConnectAsync();

        Task<IChannel> CreateModelAsync();
    }
}
