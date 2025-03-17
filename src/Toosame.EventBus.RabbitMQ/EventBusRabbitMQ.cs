using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;

using Polly;
using Polly.Retry;

using RabbitMQ.Client;
using RabbitMQ.Client.Events;
using RabbitMQ.Client.Exceptions;

using System;
using System.Collections.Generic;
using System.Linq;
using System.Net.Sockets;
using System.Text;
using System.Text.Json;
using System.Threading.Tasks;

using Toosame.EventBus.Abstractions;
using Toosame.EventBus.Events;
using Toosame.EventBus.Extensions;
using Toosame.EventBus.RabbitMQ.Events;

namespace Toosame.EventBus.RabbitMQ
{
    public class EventBusRabbitMQ : IEventBus, IAsyncDisposable
    {
        const string DeadletterExchangeFormat = "{0}.deadletter.exchange";
        const string DeadletterQueueFormat = "{0}.deadletter.queue";
        const string DeadletterRoutingKeyFormat = "{0}.deadletter.routingkey";

        readonly string _brokerName;
        readonly string _deadletterBrokerName;
        readonly string _deadletterQueueName;
        readonly string _deadletterRoutingKey;

        readonly IRabbitMQPersistentConnection _persistentConnection;
        readonly ILogger<EventBusRabbitMQ> _logger;
        readonly IEventBusSubscriptionsManager _subsManager;
        readonly IServiceProvider _serviceProvider;
        readonly int _connRetryCount;
        readonly int _consumerRetryCount;

        IChannel _consumerChannel;
        IChannel _deadletterChannel;
        string _queueName;

        public EventBusRabbitMQ(
            IRabbitMQPersistentConnection persistentConnection,
            ILogger<EventBusRabbitMQ> logger,
            IServiceProvider serviceProvider,
            IEventBusSubscriptionsManager subsManager,
            string brokerName,
            string queueName,
            int connRetryCount,
            int consumerRetryCount)
        {
            _persistentConnection = persistentConnection ?? throw new ArgumentNullException(nameof(persistentConnection));
            _logger = logger ?? throw new ArgumentNullException(nameof(logger));
            _subsManager = subsManager ?? new InMemoryEventBusSubscriptionsManager();
            _deadletterRoutingKey = string.Format(DeadletterRoutingKeyFormat, queueName);
            _brokerName = brokerName;
            _deadletterBrokerName = string.Format(DeadletterExchangeFormat, brokerName);
            _queueName = queueName;
            _deadletterQueueName = string.Format(DeadletterQueueFormat, queueName);
            _serviceProvider = serviceProvider;
            _connRetryCount = connRetryCount;
            _consumerRetryCount = consumerRetryCount;

            _subsManager.OnEventRemoved += SubsManager_OnEventRemoved;
        }

        /// <summary>
        /// 当事件移除的时候取消绑定队列
        /// </summary>
        /// <param name="sender"></param>
        /// <param name="eventName"></param>
        private async void SubsManager_OnEventRemoved(object sender, string eventName)
        {
            if (!_persistentConnection.IsConnected)
            {
                await _persistentConnection.TryConnectAsync();
            }

            using var channel = await _persistentConnection.CreateModelAsync();
            await channel.QueueUnbindAsync(queue: _queueName,
                exchange: _brokerName,
                routingKey: eventName);

            if (_subsManager.IsEmpty)
            {
                _queueName = string.Empty;

                await _consumerChannel?.CloseAsync();
                await _deadletterChannel?.CloseAsync();
            }
        }

        public async Task PublishAsync(IntegrationEvent @event)
        {
            await PublishAsync([@event]);
        }

        public async Task PublishAsync(params IntegrationEvent[] @event)
        {
            await PublishAsync(@event.AsEnumerable());
        }

        public async Task PublishAsync(IEnumerable<IntegrationEvent> @event)
        {
            if (@event == null || !@event.Any()) return;

            if (!_persistentConnection.IsConnected)
                await _persistentConnection.TryConnectAsync();

            var policy = RetryPolicy.Handle<BrokerUnreachableException>()
                .Or<SocketException>()
                .WaitAndRetry(_connRetryCount, retryAttempt => TimeSpan.FromSeconds(Math.Pow(2, retryAttempt)), (ex, time) =>
                {
                    _logger.LogWarning(ex, "Could not publish events: after {Timeout}s ({ExceptionMessage})", $"{time.TotalSeconds:n1}", ex.Message);
                });

            using var channel = await _persistentConnection.CreateModelAsync();
            _logger.LogTrace("Declaring RabbitMQ exchange to publish event");

            await channel.ExchangeDeclareAsync(exchange: _brokerName, type: "direct");

            foreach (IntegrationEvent item in @event)
            {
                if (item == default) return;

                var message = JsonSerializer.Serialize(item, item.GetType());
                var body = Encoding.UTF8.GetBytes(message);

                await policy.Execute<Task>(async () =>
                {
                    var properties = new BasicProperties();
                    properties.DeliveryMode = DeliveryModes.Persistent; // persistent

                    _logger.LogTrace("Publishing event to RabbitMQ: {EventId}", item.Id);

                    await channel.BasicPublishAsync(
                        exchange: _brokerName,
                        routingKey: item.GetType().Name,
                        mandatory: true,
                        basicProperties: properties,
                        body: body);
                });
            }
        }

        public async Task SubscribeDynamicAsync<TH>(string eventName)
            where TH : IDynamicIntegrationEventHandler
        {
            _logger.LogInformation("Subscribing to dynamic event {EventName} with {EventHandler}", eventName, typeof(TH).GetGenericTypeName());

            await DoInternalSubscriptionAsync(eventName);
            _subsManager.AddDynamicSubscription<TH>(eventName);
        }

        public async Task SubscribeAsync<T, TH>()
            where T : IntegrationEvent
            where TH : IIntegrationEventHandler<T>
        {
            var eventName = _subsManager.GetEventKey<T>();
            await DoInternalSubscriptionAsync(eventName);

            _logger.LogInformation("Subscribing to event {EventName} with {EventHandler}", eventName, typeof(TH).GetGenericTypeName());

            _subsManager.AddSubscription<T, TH>();
        }

        private async Task DoInternalSubscriptionAsync(string eventName)
        {
            var containsKey = _subsManager.HasSubscriptionsForEvent(eventName);
            if (!containsKey)
            {
                if (!_persistentConnection.IsConnected)
                {
                    await _persistentConnection.TryConnectAsync();
                }

                using var channel = await _persistentConnection.CreateModelAsync();
                await channel.QueueBindAsync(queue: _queueName,
                    exchange: _brokerName,
                    routingKey: eventName);
            }
        }

        public void Unsubscribe<T, TH>()
            where T : IntegrationEvent
            where TH : IIntegrationEventHandler<T>
        {
            var eventName = _subsManager.GetEventKey<T>();

            _logger.LogInformation("Unsubscribing from event {EventName}", eventName);

            _subsManager.RemoveSubscription<T, TH>();
        }

        public void UnsubscribeDynamic<TH>(string eventName)
            where TH : IDynamicIntegrationEventHandler
        {
            _subsManager.RemoveDynamicSubscription<TH>(eventName);
        }

        private async Task StartBasicConsumeAsync()
        {
            _consumerChannel ??= await CreateConsumerChannelAsync();

            _logger.LogTrace("Starting RabbitMQ basic consume");

            var consumer = new AsyncEventingBasicConsumer(_consumerChannel);

            consumer.ReceivedAsync += AsyncConsumer_Received;

            await _consumerChannel.BasicConsumeAsync(
                queue: _queueName,
                autoAck: false,
                consumer: consumer);
        }

        async Task StartDeadletterConsumeAsync()
        {
            _deadletterChannel ??= await CreateDeadletterConsumerChannelAsync();

            _logger.LogTrace("Starting RabbitMQ deadletter consume");

            var deadletterConsumer = new AsyncEventingBasicConsumer(_deadletterChannel);

            deadletterConsumer.ReceivedAsync += AsyncDeadletterConsumer_Received;

            //绑定死信队列
            await _consumerChannel.QueueBindAsync(_deadletterQueueName,
                _deadletterBrokerName,
                _deadletterRoutingKey);

            await _consumerChannel.BasicConsumeAsync(
                queue: _deadletterQueueName,
                autoAck: false,
                consumer: deadletterConsumer);
        }

        async Task AsyncDeadletterConsumer_Received(object sender, BasicDeliverEventArgs eventArgs)
        {
            var message = Encoding.UTF8.GetString(eventArgs.Body.ToArray());

            try
            {
                DeadletterEvent deadletterEvent = new()
                {
                    Payload = message,
                    ConsumerName = _persistentConnection.ClientProvidedName,
                };

                if (eventArgs.BasicProperties.Headers != null &&
                    eventArgs.BasicProperties.Headers is Dictionary<string, object> messageHeaders &&
                    messageHeaders.TryGetValue("x-death", out object deathInfos) &&
                    deathInfos is List<object> deathInfoList &&
                    deathInfoList.Count > 0 &&
                    deathInfoList[0] is Dictionary<string, object> deathDict &&
                    deathDict.TryGetValue("routing-keys", out object routingKeys) &&
                    routingKeys is List<object> routingKeyInfo &&
                    routingKeyInfo.Count > 0 &&
                    routingKeyInfo[0] is byte[] routingKeysData)
                {
                    if (deathDict.TryGetValue("queue", out object deathQueueName))
                        deadletterEvent.QueueName = Encoding.UTF8.GetString((byte[])deathQueueName);

                    if (deathDict.TryGetValue("exchange", out object deathExchangeName))
                        deadletterEvent.BrokerName = Encoding.UTF8.GetString((byte[])deathExchangeName);

                    if (deathDict.TryGetValue("reason", out object deathReason))
                        deadletterEvent.DeathReason = Encoding.UTF8.GetString((byte[])deathReason);

                    deadletterEvent.EventName = Encoding.UTF8.GetString(routingKeysData);
                }

                await ProcessEvent(
                    _subsManager.GetEventKey<DeadletterEvent>(),
                    JsonSerializer.Serialize(deadletterEvent, deadletterEvent.GetType()));

                _logger.LogInformation("----- SUCCESS Processing Deadletter message \"{Message}\"", message);

                await _consumerChannel.BasicAckAsync(eventArgs.DeliveryTag, multiple: false);
            }
            catch (Exception ex)
            {
                _logger.LogWarning(ex, "----- ERROR Processing Deadletter message \"{Message}\", exception message: {ErrorMessage}",
                    message, ex.Message);

                await _consumerChannel.BasicRejectAsync(eventArgs.DeliveryTag, true);
            }
        }

        async Task AsyncConsumer_Received(object sender, BasicDeliverEventArgs eventArgs)
        {
            var eventName = eventArgs.RoutingKey;
            var message = Encoding.UTF8.GetString(eventArgs.Body.ToArray());

            try
            {
                if (message.ToLowerInvariant().Contains("throw-fake-exception"))
                {
                    throw new InvalidOperationException($"Fake exception requested: \"{message}\"");
                }

                bool processed = await ProcessEvent(eventName, message);

                if (processed)
                {
                    _logger.LogInformation("----- SUCCESS Processing message \"{Message}\"", message);

                    // Even on exception we take the message off the queue.
                    // in a REAL WORLD app this should be handled with a Dead Letter Exchange (DLX). 
                    // For more information see: https://www.rabbitmq.com/dlx.html
                    await _consumerChannel.BasicAckAsync(eventArgs.DeliveryTag, multiple: false);

                    return;
                }
                else
                {
                    _logger.LogWarning("----- ERROR Processing message \"{Message}\"", message);

                    await _consumerChannel.BasicRejectAsync(eventArgs.DeliveryTag, true);
                }
            }
            catch (Exception ex)
            {
                _logger.LogWarning(ex, "----- ERROR Processing message \"{Message}\", exception message: {ErrorMessage}",
                    message, ex.Message);
            }

            if (_consumerRetryCount > 0)
            {
                int retriedCount = 0;

                if (eventArgs.BasicProperties.Headers != null
                    && eventArgs.BasicProperties.Headers.TryGetValue("x-retried-count", out object value))
                {
                    retriedCount = (int)value + 1;
                }

                if (retriedCount < _consumerRetryCount)
                {
                    BasicProperties properties = new BasicProperties
                    {
                        DeliveryMode = DeliveryModes.Persistent,
                        Headers = new Dictionary<string, object>
                        {
                            { "x-retried-count", retriedCount }
                        }
                    };

                    await _consumerChannel.BasicPublishAsync(
                        exchange: eventArgs.Exchange,
                        routingKey: eventArgs.RoutingKey,
                        mandatory: true,
                        basicProperties: properties,
                        body: eventArgs.Body);
                    await _consumerChannel.BasicAckAsync(eventArgs.DeliveryTag, multiple: false);

                    _logger.LogTrace("Rebublish event to RabbitMQ: {EventName}", eventName);

                    return;
                }
            }

            //to deadlatter
            await _consumerChannel.BasicRejectAsync(eventArgs.DeliveryTag, false);
        }

        async Task<IChannel> CreateConsumerChannelAsync()
        {
            if (!_persistentConnection.IsConnected)
                await _persistentConnection.TryConnectAsync();

            _logger.LogTrace("Creating RabbitMQ consumer channel");

            var channel = await _persistentConnection.CreateModelAsync();

            await channel.ExchangeDeclareAsync(exchange: _brokerName,
                type: "direct");

            await channel.QueueDeclareAsync(queue: _queueName,
                durable: true,
                exclusive: false,
                autoDelete: false,
                arguments: new Dictionary<string, object>()
                {
                    ["x-dead-letter-exchange"] = _deadletterBrokerName,
                    ["x-dead-letter-routing-key"] = _deadletterRoutingKey
                });

            channel.CallbackExceptionAsync += async (sender, ea) =>
            {
                _logger.LogWarning(ea.Exception, "Recreating RabbitMQ consumer channel");

                _consumerChannel.Dispose();
                _consumerChannel = await CreateConsumerChannelAsync();
                await StartBasicConsumeAsync();
            };

            return channel;
        }

        async Task<IChannel> CreateDeadletterConsumerChannelAsync()
        {
            if (!_persistentConnection.IsConnected)
                await _persistentConnection.TryConnectAsync();

            _logger.LogTrace("Creating RabbitMQ deadletter consumer channel");

            var channel = await _persistentConnection.CreateModelAsync();
            await channel.ExchangeDeclareAsync(
                exchange: _deadletterBrokerName,
                type: "direct");

            await channel.QueueDeclareAsync(queue: _deadletterQueueName,
                durable: true,
                exclusive: false,
                autoDelete: false,
                arguments: null);

            channel.CallbackExceptionAsync += async (sender, ea) =>
            {
                _logger.LogWarning(ea.Exception, "Recreating RabbitMQ deadletter consumer channel");

                _deadletterChannel.Dispose();
                _deadletterChannel = await CreateDeadletterConsumerChannelAsync();

                await StartDeadletterConsumeAsync();
            };

            return channel;
        }

        async Task<bool> ProcessEvent(string eventName, string message)
        {
            _logger.LogTrace("Processing RabbitMQ event: {EventName}", eventName);

            if (_subsManager.HasSubscriptionsForEvent(eventName))
            {
                using var scope = _serviceProvider.CreateScope();
                var subscriptions = _subsManager.GetHandlersForEvent(eventName);
                foreach (var subscription in subscriptions)
                {
                    if (subscription.IsDynamic)
                    {
                        if (!(scope.ServiceProvider.GetService(subscription.HandlerType) is IDynamicIntegrationEventHandler handler)) continue;
                        dynamic eventData = JsonDocument.Parse(message);
                        await handler.HandleAsync(eventData);
                    }
                    else
                    {
                        var handler = scope.ServiceProvider.GetService(subscription.HandlerType);
                        if (handler == null) continue;
                        var eventType = _subsManager.GetEventTypeByName(eventName);
                        var integrationEvent = JsonSerializer.Deserialize(message, eventType);
                        var concreteType = typeof(IIntegrationEventHandler<>).MakeGenericType(eventType);
                        await (Task)concreteType.GetMethod("HandleAsync").Invoke(handler, [integrationEvent]);
                    }
                }

                return true;
            }
            else
            {
                _logger.LogWarning("No subscription for RabbitMQ event: {EventName}", eventName);

                return false;
            }
        }

        public async Task StartSubscribeAsync()
        {
            if (_subsManager.HasSubscriptionsForEvent(nameof(DeadletterEvent)))
            {
                _deadletterChannel ??= await CreateDeadletterConsumerChannelAsync();

                await StartDeadletterConsumeAsync();
            }

            await StartBasicConsumeAsync();
        }

        public async ValueTask DisposeAsync()
        {
            if (_consumerChannel != null)
                await _consumerChannel.DisposeAsync();

            if (_deadletterChannel != null)
                await _deadletterChannel.DisposeAsync();

            _subsManager.Clear();
        }
    }
}
