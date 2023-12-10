using RabbitMQ.Client;
using System;

namespace Toosame.EventBus.RabbitMQ
{
    public interface IRabbitMQPersistentConnection
        : IDisposable
    {
        bool IsConnected { get; }

        string ClientProvidedName { get; }

        bool TryConnect();

        IModel CreateModel();
    }
}
