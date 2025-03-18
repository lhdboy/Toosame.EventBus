using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;

using Polly;
using Polly.Retry;

using RabbitMQ.Client;
using RabbitMQ.Client.Events;
using RabbitMQ.Client.Exceptions;

using System;
using System.IO;
using System.Net.Sockets;
using System.Threading;
using System.Threading.Tasks;

namespace Toosame.EventBus.RabbitMQ
{
    public class DefaultRabbitMQPersistentConnection : IRabbitMQPersistentConnection
    {
        readonly IConnectionFactory _connectionFactory;
        readonly ILogger<DefaultRabbitMQPersistentConnection> _logger;
        readonly RabbitMQOption _option;

        readonly SemaphoreSlim _semaphoreSlim = new(1);

        IConnection _connection;
        bool _disposed;

        public DefaultRabbitMQPersistentConnection(
            IConnectionFactory connectionFactory,
            ILogger<DefaultRabbitMQPersistentConnection> logger,
            IOptions<RabbitMQOption> options)
        {
            _connectionFactory = connectionFactory ?? throw new ArgumentNullException(nameof(connectionFactory));
            _logger = logger ?? throw new ArgumentNullException(nameof(logger));
            _option = options.Value;
        }

        public bool IsConnected
        {
            get
            {
                return _connection != null && _connection.IsOpen && !_disposed;
            }
        }

        public string ClientProvidedName => _option.ClientProvidedName;

        public IConnection Connection => _connection;

        public Task<IChannel> CreateModelAsync()
        {
            if (!IsConnected)
            {
                throw new InvalidOperationException("No RabbitMQ connections are available to perform this action");
            }

            return _connection.CreateChannelAsync();
        }

        public async ValueTask DisposeAsync()
        {
            if (_disposed) return;

            _disposed = true;

            try
            {
                await _connection.DisposeAsync();
            }
            catch (IOException ex)
            {
                _logger.LogCritical(ex, "Rabbit MQ Connection dispose failed");
            }
        }

        public async Task<bool> TryConnectAsync()
        {
            _logger.LogInformation("RabbitMQ Client is trying to connect");

            await _semaphoreSlim.WaitAsync();

            var policy = RetryPolicy.Handle<SocketException>()
                .Or<BrokerUnreachableException>()
                .WaitAndRetry(_option.EventBusRetryCount, retryAttempt => TimeSpan.FromSeconds(Math.Pow(2, retryAttempt)), (ex, time) =>
                {
                    _logger.LogWarning(ex, "RabbitMQ Client could not connect after {TimeOut}s ({ExceptionMessage})", $"{time.TotalSeconds:n1}", ex.Message);
                }
            );

            if (_connection != null)
            {
                await _connection.DisposeAsync();
                _connection = null;
            }

            _connection = await policy.Execute(
                () => _connectionFactory.CreateConnectionAsync(_option.ClientProvidedName));

            if (IsConnected)
            {
                _connection.ConnectionShutdownAsync += OnConnectionShutdown;
                _connection.CallbackExceptionAsync += OnCallbackException;
                _connection.ConnectionBlockedAsync += OnConnectionBlocked;

                _logger.LogInformation("RabbitMQ Client acquired a persistent connection to '{HostName}' and is subscribed to failure events", _connection.Endpoint.HostName);

                _semaphoreSlim.Release();

                return true;
            }
            else
            {
                _logger.LogCritical("FATAL ERROR: RabbitMQ connections could not be created and opened");

                _semaphoreSlim.Release();

                return false;
            }
        }

        async Task OnConnectionBlocked(object sender, ConnectionBlockedEventArgs e)
        {
            if (_disposed) return;

            _logger.LogWarning("A RabbitMQ connection is shutdown. Trying to re-connect...");

            await TryConnectAsync();
        }

        async Task OnCallbackException(object sender, CallbackExceptionEventArgs e)
        {
            if (_disposed) return;

            _logger.LogWarning("A RabbitMQ connection throw exception. Trying to re-connect...");

            await TryConnectAsync();
        }

        async Task OnConnectionShutdown(object sender, ShutdownEventArgs reason)
        {
            if (_disposed) return;

            _logger.LogWarning("A RabbitMQ connection is on shutdown. Trying to re-connect...");

            await TryConnectAsync();
        }
    }
}
