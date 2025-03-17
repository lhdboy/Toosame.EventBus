using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;

using RabbitMQ.Client;

using System;
using System.Collections.Generic;

using Toosame.EventBus.Abstractions;

namespace Toosame.EventBus.RabbitMQ.Extensions
{
    public static class StartupExtensions
    {
        public static void AddEventBus(this IServiceCollection services,
            string connectionString,
            RabbitMQOption rabbitMqOption,
            Action<ICollection<Type>> eventHandlerOption)
        {
            AddEventBus(services, connectionString, rabbitMqOption);

            ICollection<Type> eventHandlers = [];

            eventHandlerOption?.Invoke(eventHandlers);

            foreach (var handler in eventHandlers)
            {
                services.AddTransient(handler);
            }
        }

        /// <summary>
        /// Add EventBus Service
        /// </summary>
        /// <param name="services">IServiceCollection</param>
        /// <param name="connectionString">amqp://user:pass@hostName:port/vhost</param>
        /// <param name="rabbitMqOption">RabbitMQ Option</param>
        public static void AddEventBus(this IServiceCollection services, string connectionString, RabbitMQOption rabbitMqOption)
        {
            services.AddSingleton<IRabbitMQPersistentConnection, DefaultRabbitMQPersistentConnection>(sp
                => new DefaultRabbitMQPersistentConnection(
                    new ConnectionFactory()
                    {
                        Uri = new Uri(connectionString)
                    },
                    sp.GetRequiredService<ILogger<DefaultRabbitMQPersistentConnection>>(),
                    rabbitMqOption.EventBusRetryCount,
                    rabbitMqOption.ClientProvidedName));

            services.AddSingleton<IEventBus, EventBusRabbitMQ>(sp =>
            {
                var rabbitMQPersistentConnection = sp.GetRequiredService<IRabbitMQPersistentConnection>();
                var logger = sp.GetRequiredService<ILogger<EventBusRabbitMQ>>();
                var eventBusSubcriptionsManager = sp.GetRequiredService<IEventBusSubscriptionsManager>();

                var retryCount = 5;
                if (rabbitMqOption.EventBusRetryCount > 0)
                {
                    retryCount = rabbitMqOption.EventBusRetryCount;
                }

                return new EventBusRabbitMQ(rabbitMQPersistentConnection,
                    logger,
                    sp,
                    eventBusSubcriptionsManager,
                    rabbitMqOption.EventBusBrokeName,
                    rabbitMqOption.SubscriptionClientName,
                    retryCount,
                    rabbitMqOption.EventBusConsumerRetryCount);
            });

            services.AddSingleton<IEventBusSubscriptionsManager, InMemoryEventBusSubscriptionsManager>();
        }

        public static void AddEventHandler<EH>(this ICollection<Type> types)
            where EH : class, IIntegrationEventHandler
        {
            types.Add(typeof(EH));
        }
    }
}
