using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;

using Polly;
using Polly.Retry;

using RabbitMQ.Client;
using RabbitMQ.Client.Events;
using RabbitMQ.Client.Exceptions;

using System;
using System.Collections.Generic;
using System.Diagnostics.CodeAnalysis;
using System.Linq;
using System.Net.Sockets;
using System.Text;
using System.Text.Json;
using System.Threading.Tasks;

using Toosame.EventBus.Abstractions;
using Toosame.EventBus.Events;

namespace Toosame.EventBus.RabbitMQ
{
    public class EventBusRabbitMQ : IEventBus, IAsyncDisposable
    {
        const string DeadletterExchangeFormat = "{0}.deadletter.exchange";
        const string DeadletterQueueFormat = "{0}.deadletter.queue";
        const string DeadletterRoutingKeyFormat = "{0}.deadletter.routingkey";

        readonly string _deadletterBrokerName;
        readonly string _deadletterQueueName;
        readonly string _deadletterRoutingKey;

        readonly IRabbitMQPersistentConnection _persistentConnection;
        readonly ILogger<EventBusRabbitMQ> _logger;
        readonly RabbitMQOption _option;
        readonly EventBusSubscriptionInfo _subscriptionInfo;
        readonly IServiceProvider _serviceProvider;

        IChannel _consumerChannel;
        IChannel _deadletterChannel;

        public EventBusRabbitMQ(
            IRabbitMQPersistentConnection persistentConnection,
            ILogger<EventBusRabbitMQ> logger,
            IServiceProvider serviceProvider,
            IOptions<RabbitMQOption> options,
            IOptions<EventBusSubscriptionInfo> subscriptionOptions)
        {
            _persistentConnection = persistentConnection ?? throw new ArgumentNullException(nameof(persistentConnection));
            _logger = logger ?? throw new ArgumentNullException(nameof(logger));
            _serviceProvider = serviceProvider;
            _option = options.Value;
            _subscriptionInfo = subscriptionOptions.Value;

            _deadletterRoutingKey = string.Format(DeadletterRoutingKeyFormat, _option.SubscriptionClientName);
            _deadletterBrokerName = string.Format(DeadletterExchangeFormat, _option.EventBusBrokeName);
            _deadletterQueueName = string.Format(DeadletterQueueFormat, _option.SubscriptionClientName);

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
                .WaitAndRetry(_option.EventBusRetryCount, retryAttempt => TimeSpan.FromSeconds(Math.Pow(2, retryAttempt)), (ex, time) =>
                {
                    _logger.LogWarning(ex, "Could not publish events: after {Timeout}s ({ExceptionMessage})", $"{time.TotalSeconds:n1}", ex.Message);
                });

            using var channel = await _persistentConnection.CreateModelAsync();

            if (_logger.IsEnabled(LogLevel.Trace))
            {
                _logger.LogTrace("Declaring RabbitMQ exchange to publish event");
            }

            await channel.ExchangeDeclareAsync(
                exchange: _option.EventBusBrokeName, type: "direct");

            foreach (IntegrationEvent item in @event)
            {
                if (item == default) return;

                var routingKey = @event.GetType().Name;
                var body = SerializeMessage(item);

                if (_logger.IsEnabled(LogLevel.Trace))
                {
                    _logger.LogTrace("Creating RabbitMQ channel to publish event: {EventId} ({EventName})", item.Id, routingKey);
                }


                await policy.Execute<Task>(async () =>
                {
                    var properties = new BasicProperties();
                    properties.DeliveryMode = DeliveryModes.Persistent; // persistent

                    if (_logger.IsEnabled(LogLevel.Trace))
                    {
                        _logger.LogTrace("Publishing event to RabbitMQ: {EventId}", item.Id);
                    }

                    await channel.BasicPublishAsync(
                        exchange: _option.EventBusBrokeName,
                        routingKey: routingKey,
                        mandatory: true,
                        basicProperties: properties,
                        body: body);
                });
            }
        }

        async Task StartBasicConsumeAsync()
        {
            _consumerChannel ??= await CreateConsumerChannelAsync();

            _logger.LogTrace("Starting RabbitMQ basic consume");

            var consumer = new AsyncEventingBasicConsumer(_consumerChannel);

            consumer.ReceivedAsync += AsyncConsumer_Received;

            await _consumerChannel.BasicConsumeAsync(
                queue: _option.SubscriptionClientName,
                autoAck: false,
                consumer: consumer);

            foreach (var (eventName, _) in _subscriptionInfo.EventTypes)
            {
                await _consumerChannel.QueueBindAsync(
                    queue: _option.SubscriptionClientName,
                    exchange: _option.EventBusBrokeName,
                    routingKey: eventName);
            }
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
            DeadletterEvent deadletterEvent = new()
            {
                Payload = Encoding.UTF8.GetString(eventArgs.Body.Span),
                ConsumerName = _persistentConnection.ClientProvidedName,
            };

            var body = SerializeMessage(deadletterEvent);
            string message = Encoding.UTF8.GetString(body);

            try
            {
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

                await ProcessEvent(eventArgs.RoutingKey, message);

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
            var message = Encoding.UTF8.GetString(eventArgs.Body.Span);

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

            if (_option.EventBusConsumerRetryCount > 0)
            {
                int retriedCount = 0;

                if (eventArgs.BasicProperties.Headers != null
                    && eventArgs.BasicProperties.Headers.TryGetValue("x-retried-count", out object value))
                {
                    retriedCount = (int)value + 1;
                }

                if (retriedCount < _option.EventBusConsumerRetryCount)
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

            await channel.ExchangeDeclareAsync(
                exchange: _option.EventBusBrokeName, type: "direct");

            await channel.QueueDeclareAsync(queue: _option.SubscriptionClientName,
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
            if (_logger.IsEnabled(LogLevel.Trace))
            {
                _logger.LogTrace("Processing RabbitMQ event: {EventName}", eventName);
            }

            await using var scope = _serviceProvider.CreateAsyncScope();

            if (!_subscriptionInfo.EventTypes.TryGetValue(eventName, out var eventType))
            {
                _logger.LogWarning("Unable to resolve event type for event name {EventName}", eventName);

                return false;
            }

            // Deserialize the event
            var integrationEvent = DeserializeMessage(message, eventType);

            // REVIEW: This could be done in parallel

            // Get all the handlers using the event type as the key
            foreach (var handler in scope.ServiceProvider.GetKeyedServices<IIntegrationEventHandler>(eventType))
            {
                await handler.Handle(integrationEvent);
            }

            return true;
        }

        public async Task StartDeadletterAsync()
        {
            await StartDeadletterConsumeAsync();
        }

        public async Task StartAsync()
        {
            await StartBasicConsumeAsync();
        }

        public async ValueTask DisposeAsync()
        {
            if (_consumerChannel != null)
                await _consumerChannel.DisposeAsync();

            if (_deadletterChannel != null)
                await _deadletterChannel.DisposeAsync();
        }

        [UnconditionalSuppressMessage("Trimming", "IL2026:RequiresUnreferencedCode",
    Justification = "The 'JsonSerializer.IsReflectionEnabledByDefault' feature switch, which is set to false by default for trimmed .NET apps, ensures the JsonSerializer doesn't use Reflection.")]
        [UnconditionalSuppressMessage("AOT", "IL3050:RequiresDynamicCode", Justification = "See above.")]
        private IntegrationEvent DeserializeMessage(string message, Type eventType)
        {
            return JsonSerializer.Deserialize(message, eventType, _subscriptionInfo.JsonSerializerOptions) as IntegrationEvent;
        }

        [UnconditionalSuppressMessage("Trimming", "IL2026:RequiresUnreferencedCode",
            Justification = "The 'JsonSerializer.IsReflectionEnabledByDefault' feature switch, which is set to false by default for trimmed .NET apps, ensures the JsonSerializer doesn't use Reflection.")]
        [UnconditionalSuppressMessage("AOT", "IL3050:RequiresDynamicCode", Justification = "See above.")]
        private byte[] SerializeMessage(IntegrationEvent @event)
        {
            return JsonSerializer.SerializeToUtf8Bytes(@event, @event.GetType(), _subscriptionInfo.JsonSerializerOptions);
        }
    }
}
