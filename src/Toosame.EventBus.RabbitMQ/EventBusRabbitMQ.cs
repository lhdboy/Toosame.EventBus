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
    public class EventBusRabbitMQ : IEventBus, IDisposable
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

        IModel _consumerChannel;
        IModel _deadletterChannel;
        string _queueName;

        public EventBusRabbitMQ(IRabbitMQPersistentConnection persistentConnection,
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

            _consumerChannel = CreateConsumerChannel();
            _subsManager.OnEventRemoved += SubsManager_OnEventRemoved;
        }

        /// <summary>
        /// 当事件移除的时候取消绑定队列
        /// </summary>
        /// <param name="sender"></param>
        /// <param name="eventName"></param>
        private void SubsManager_OnEventRemoved(object sender, string eventName)
        {
            if (!_persistentConnection.IsConnected)
            {
                _persistentConnection.TryConnect();
            }

            using var channel = _persistentConnection.CreateModel();
            channel.QueueUnbind(queue: _queueName,
                exchange: _brokerName,
                routingKey: eventName);

            if (_subsManager.IsEmpty)
            {
                _queueName = string.Empty;
                _consumerChannel?.Close();
                _deadletterChannel?.Close();
            }
        }

        public void Publish(IntegrationEvent @event)
        {
            Publish(new IntegrationEvent[] { @event });
        }

        public void Publish(params IntegrationEvent[] @event)
        {
            Publish(@event.AsEnumerable());
        }

        public void Publish(IEnumerable<IntegrationEvent> @event)
        {
            if (@event == null || !@event.Any()) return;

            if (!_persistentConnection.IsConnected)
                _persistentConnection.TryConnect();

            var policy = RetryPolicy.Handle<BrokerUnreachableException>()
                .Or<SocketException>()
                .WaitAndRetry(_connRetryCount, retryAttempt => TimeSpan.FromSeconds(Math.Pow(2, retryAttempt)), (ex, time) =>
                {
                    _logger.LogWarning(ex, "Could not publish events: after {Timeout}s ({ExceptionMessage})", $"{time.TotalSeconds:n1}", ex.Message);
                });

            using (var channel = _persistentConnection.CreateModel())
            {
                _logger.LogTrace("Declaring RabbitMQ exchange to publish event");

                channel.ExchangeDeclare(exchange: _brokerName, type: "direct");

                foreach (IntegrationEvent item in @event)
                {
                    if (item == default) return;

                    var message = JsonSerializer.Serialize(item, item.GetType());
                    var body = Encoding.UTF8.GetBytes(message);

                    policy.Execute(() =>
                    {
                        var properties = channel.CreateBasicProperties();
                        properties.DeliveryMode = 2; // persistent

                        _logger.LogTrace("Publishing event to RabbitMQ: {EventId}", item.Id);

                        channel.BasicPublish(
                            exchange: _brokerName,
                            routingKey: item.GetType().Name,
                            mandatory: true,
                            basicProperties: properties,
                            body: body);
                    });
                }
            }
        }

        public void SubscribeDynamic<TH>(string eventName)
            where TH : IDynamicIntegrationEventHandler
        {
            _logger.LogInformation("Subscribing to dynamic event {EventName} with {EventHandler}", eventName, typeof(TH).GetGenericTypeName());

            DoInternalSubscription(eventName);
            _subsManager.AddDynamicSubscription<TH>(eventName);
        }

        public void Subscribe<T, TH>()
            where T : IntegrationEvent
            where TH : IIntegrationEventHandler<T>
        {
            var eventName = _subsManager.GetEventKey<T>();
            DoInternalSubscription(eventName);

            _logger.LogInformation("Subscribing to event {EventName} with {EventHandler}", eventName, typeof(TH).GetGenericTypeName());

            _subsManager.AddSubscription<T, TH>();
        }

        private void DoInternalSubscription(string eventName)
        {
            var containsKey = _subsManager.HasSubscriptionsForEvent(eventName);
            if (!containsKey)
            {
                if (!_persistentConnection.IsConnected)
                {
                    _persistentConnection.TryConnect();
                }

                using var channel = _persistentConnection.CreateModel();
                channel.QueueBind(queue: _queueName,
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

        public void Dispose()
        {
            _consumerChannel?.Dispose();
            _deadletterChannel?.Dispose();

            _subsManager.Clear();
        }

        private void StartBasicConsume()
        {
            _logger.LogTrace("Starting RabbitMQ basic consume");

            if (_consumerChannel != null)
            {
                var consumer = new EventingBasicConsumer(_consumerChannel);

                consumer.Received += Consumer_Received;

                _consumerChannel.BasicConsume(
                    queue: _queueName,
                    autoAck: false,
                    consumer: consumer);
            }
            else
            {
                _logger.LogError("StartBasicConsume can't call on _consumerChannel == null");
            }
        }

        void StartDeadletterConsume()
        {
            if (_deadletterChannel != null)
            {
                var deadletterConsumer = new EventingBasicConsumer(_deadletterChannel);

                deadletterConsumer.Received += DeadletterConsumer_Received; ;

                //绑定死信队列
                _consumerChannel.QueueBind(_deadletterQueueName,
                    _deadletterBrokerName,
                    _deadletterRoutingKey);

                _consumerChannel.BasicConsume(
                    queue: _deadletterQueueName,
                    autoAck: false,
                    consumer: deadletterConsumer);
            }
            else
            {
                _logger.LogError("StartDeadletterConsume can't call on _deadletterChannel == null");
            }
        }

        async void DeadletterConsumer_Received(object sender, BasicDeliverEventArgs eventArgs)
        {
            var message = Encoding.UTF8.GetString(eventArgs.Body.ToArray());

            try
            {
                DeadletterEvent deadletterEvent = new DeadletterEvent()
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

                _consumerChannel.BasicAck(eventArgs.DeliveryTag, multiple: false);
            }
            catch (Exception ex)
            {
                _logger.LogWarning(ex, "----- ERROR Processing Deadletter message \"{Message}\", exception message: {ErrorMessage}",
                    message, ex.Message);

                _consumerChannel.BasicReject(eventArgs.DeliveryTag, true);
            }
        }

        async void Consumer_Received(object sender, BasicDeliverEventArgs eventArgs)
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
                    _consumerChannel.BasicAck(eventArgs.DeliveryTag, multiple: false);

                    return;
                }
                else
                {
                    _logger.LogWarning("----- ERROR Processing message \"{Message}\"", message);

                    _consumerChannel.BasicReject(eventArgs.DeliveryTag, true);
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
                    var properties = _consumerChannel.CreateBasicProperties();
                    properties.DeliveryMode = 2; // persistent
                    properties.Headers = new Dictionary<string, object>
                    {
                        { "x-retried-count", retriedCount }
                    };

                    _consumerChannel.BasicPublish(
                        exchange: eventArgs.Exchange,
                        routingKey: eventArgs.RoutingKey,
                        mandatory: true,
                        basicProperties: properties,
                        body: eventArgs.Body);
                    _consumerChannel.BasicAck(eventArgs.DeliveryTag, multiple: false);

                    _logger.LogTrace("Rebublish event to RabbitMQ: {EventName}", eventName);

                    return;
                }
            }

            //to deadlatter
            _consumerChannel.BasicReject(eventArgs.DeliveryTag, false);
        }

        IModel CreateConsumerChannel()
        {
            if (!_persistentConnection.IsConnected)
            {
                _persistentConnection.TryConnect();
            }

            _logger.LogTrace("Creating RabbitMQ consumer channel");

            var channel = _persistentConnection.CreateModel();

            channel.ExchangeDeclare(exchange: _brokerName,
                type: "direct");

            channel.QueueDeclare(queue: _queueName,
                                 durable: true,
                                 exclusive: false,
                                 autoDelete: false,
                                 arguments: new Dictionary<string, object>()
                                 {
                                     ["x-dead-letter-exchange"] = _deadletterBrokerName,
                                     ["x-dead-letter-routing-key"] = _deadletterRoutingKey
                                 });

            channel.CallbackException += (sender, ea) =>
            {
                _logger.LogWarning(ea.Exception, "Recreating RabbitMQ consumer channel");

                _consumerChannel.Dispose();
                _consumerChannel = CreateConsumerChannel();
                StartBasicConsume();
            };

            return channel;
        }

        IModel CreateDeadletterConsumerChannel()
        {
            if (!_persistentConnection.IsConnected)
            {
                _persistentConnection.TryConnect();
            }

            _logger.LogTrace("Creating RabbitMQ deadletter consumer channel");

            var channel = _persistentConnection.CreateModel();
            channel.ExchangeDeclare(
                exchange: _deadletterBrokerName,
                type: "direct");

            //声明队列
            channel.QueueDeclare(queue: _deadletterQueueName,
                                 durable: true,
                                 exclusive: false,
                                 autoDelete: false,
                                 arguments: null);

            channel.CallbackException += (sender, ea) =>
            {
                _logger.LogWarning(ea.Exception, "Recreating RabbitMQ deadletter consumer channel");

                _deadletterChannel.Dispose();
                _deadletterChannel = CreateDeadletterConsumerChannel();
                StartDeadletterConsume();
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
                        await handler.Handle(eventData);
                    }
                    else
                    {
                        var handler = scope.ServiceProvider.GetService(subscription.HandlerType);
                        if (handler == null) continue;
                        var eventType = _subsManager.GetEventTypeByName(eventName);
                        var integrationEvent = JsonSerializer.Deserialize(message, eventType);
                        var concreteType = typeof(IIntegrationEventHandler<>).MakeGenericType(eventType);
                        await (Task)concreteType.GetMethod("Handle").Invoke(handler, new object[] { integrationEvent });
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

        public void StartSubscribe()
        {
            if (_subsManager.HasSubscriptionsForEvent(nameof(DeadletterEvent)))
            {
                _deadletterChannel ??= CreateDeadletterConsumerChannel();

                StartDeadletterConsume();
            }

            StartBasicConsume();
        }
    }
}
