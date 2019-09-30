using Autofac;
using Toosame.EventBus;
using Toosame.EventBus.Abstractions;
using Toosame.EventBus.Events;
using Toosame.EventBus.Extensions;
using Microsoft.Extensions.Logging;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;
using Polly;
using Polly.Retry;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;
using RabbitMQ.Client.Exceptions;
using System;
using System.Net.Sockets;
using System.Text;
using System.Threading.Tasks;

namespace Toosame.EventBus.RabbitMQ
{
    public class EventBusRabbitMQ : IEventBus, IDisposable
    {
        private const string AUTOFAC_SCOPE_NAME = "event_bus";

        private readonly string _brokerName;

        private readonly IRabbitMQPersistentConnection _persistentConnection;
        private readonly ILogger<EventBusRabbitMQ> _logger;
        private readonly IEventBusSubscriptionsManager _subsManager;
        private readonly ILifetimeScope _autofac;
        private readonly int _retryCount;

        private IModel _consumerChannel;
        private string _queueName;

        public EventBusRabbitMQ(IRabbitMQPersistentConnection persistentConnection, ILogger<EventBusRabbitMQ> logger,
            ILifetimeScope autofac, IEventBusSubscriptionsManager subsManager, string brokerName, string queueName, int retryCount)
        {
            _persistentConnection = persistentConnection ?? throw new ArgumentNullException(nameof(persistentConnection));
            _logger = logger ?? throw new ArgumentNullException(nameof(logger));
            _subsManager = subsManager ?? new InMemoryEventBusSubscriptionsManager();
            _brokerName = brokerName;
            _queueName = queueName;
            _consumerChannel = CreateConsumerChannel();
            _autofac = autofac;
            _retryCount = retryCount;
            _subsManager.OnEventRemoved += SubsManager_OnEventRemoved;
        }

        public EventBusRabbitMQ(IRabbitMQPersistentConnection persistentConnection,
            ILogger<EventBusRabbitMQ> logger,
            ILifetimeScope autofac,
            IEventBusSubscriptionsManager subsManager,
            string queueName,
            int retryCount = 5)
            : this(persistentConnection, logger, autofac, subsManager, "tp_event_bus", queueName, retryCount)
        {
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

            using (var channel = _persistentConnection.CreateModel())
            {
                channel.QueueUnbind(queue: _queueName,
                    exchange: _brokerName,
                    routingKey: eventName);

                if (_subsManager.IsEmpty)
                {
                    _queueName = string.Empty;
                    _consumerChannel.Close();
                }
            }
        }

        public void Publish(IntegrationEvent @event)
        {
            Publish(new IntegrationEvent[] { @event });
        }

        public void Publish(params IntegrationEvent[] @event)
        {
            if (@event == null || @event.Length == 0) return;

            if (!_persistentConnection.IsConnected)
            {
                _persistentConnection.TryConnect();
            }

            var policy = RetryPolicy.Handle<BrokerUnreachableException>()
                .Or<SocketException>()
                .WaitAndRetry(_retryCount, retryAttempt => TimeSpan.FromSeconds(Math.Pow(2, retryAttempt)), (ex, time) =>
                {
                    _logger.LogWarning(ex, "Could not publish events: after {Timeout}s ({ExceptionMessage})", $"{time.TotalSeconds:n1}", ex.Message);
                });

            using (var channel = _persistentConnection.CreateModel())
            {
                _logger.LogTrace("Declaring RabbitMQ exchange to publish event");

                channel.ExchangeDeclare(exchange: _brokerName, type: "direct");

                foreach (IntegrationEvent item in @event)
                {
                    var message = JsonConvert.SerializeObject(item);
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

                using (var channel = _persistentConnection.CreateModel())
                {
                    channel.QueueBind(queue: _queueName,
                                      exchange: _brokerName,
                                      routingKey: eventName);
                }
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
            if (_consumerChannel != null)
            {
                _consumerChannel.Dispose();
            }

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

        private async void Consumer_Received(object sender, BasicDeliverEventArgs eventArgs)
        {
            var eventName = eventArgs.RoutingKey;
            var message = Encoding.UTF8.GetString(eventArgs.Body);

            try
            {
                if (message.ToLowerInvariant().Contains("throw-fake-exception"))
                {
                    throw new InvalidOperationException($"Fake exception requested: \"{message}\"");
                }

                await ProcessEvent(eventName, message);

                _logger.LogInformation("----- SUCCESS Processing message \"{Message}\"", message);
            }
            catch (Exception ex)
            {
                _logger.LogWarning(ex, "----- ERROR Processing message \"{Message}\"", message);
            }

            // Even on exception we take the message off the queue.
            // in a REAL WORLD app this should be handled with a Dead Letter Exchange (DLX). 
            // For more information see: https://www.rabbitmq.com/dlx.html
            _consumerChannel.BasicAck(eventArgs.DeliveryTag, multiple: false);
        }

        private IModel CreateConsumerChannel()
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
                                 arguments: null);

            channel.CallbackException += (sender, ea) =>
            {
                _logger.LogWarning(ea.Exception, "Recreating RabbitMQ consumer channel");

                _consumerChannel.Dispose();
                _consumerChannel = CreateConsumerChannel();
                StartBasicConsume();
            };

            return channel;
        }

        private async Task ProcessEvent(string eventName, string message)
        {
            _logger.LogTrace("Processing RabbitMQ event: {EventName}", eventName);

            if (_subsManager.HasSubscriptionsForEvent(eventName))
            {
                using (var scope = _autofac.BeginLifetimeScope(AUTOFAC_SCOPE_NAME))
                {
                    var subscriptions = _subsManager.GetHandlersForEvent(eventName);
                    foreach (var subscription in subscriptions)
                    {
                        if (subscription.IsDynamic)
                        {
                            var handler = scope.ResolveOptional(subscription.HandlerType) as IDynamicIntegrationEventHandler;
                            if (handler == null) continue;
                            dynamic eventData = JObject.Parse(message);
                            await handler.Handle(eventData);
                        }
                        else
                        {
                            var handler = scope.ResolveOptional(subscription.HandlerType);
                            if (handler == null) continue;
                            var eventType = _subsManager.GetEventTypeByName(eventName);
                            var integrationEvent = JsonConvert.DeserializeObject(message, eventType);
                            var concreteType = typeof(IIntegrationEventHandler<>).MakeGenericType(eventType);
                            await (Task)concreteType.GetMethod("Handle").Invoke(handler, new object[] { integrationEvent });
                        }
                    }
                }
            }
            else
            {
                _logger.LogWarning("No subscription for RabbitMQ event: {EventName}", eventName);
            }
        }

        public void StartSubscribe()
        {
            StartBasicConsume();
        }
    }
}
