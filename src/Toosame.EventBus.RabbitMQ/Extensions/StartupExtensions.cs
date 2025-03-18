using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;

using RabbitMQ.Client;

using System;
using System.Diagnostics.CodeAnalysis;
using System.Text.Json;

using Toosame.EventBus.Abstractions;
using Toosame.EventBus.Events;
using Toosame.EventBus.RabbitMQ;

namespace Microsoft.Extensions.Hosting
{
    public static class StartupExtensions
    {
        public static IEventBusBuilder ConfigureJsonOptions(this IEventBusBuilder eventBusBuilder, Action<JsonSerializerOptions> configure)
        {
            eventBusBuilder.Services.Configure<EventBusSubscriptionInfo>(o =>
            {
                configure(o.JsonSerializerOptions);
            });

            return eventBusBuilder;
        }

        public static IEventBusBuilder AddSubscription<T, [DynamicallyAccessedMembers(DynamicallyAccessedMemberTypes.PublicConstructors)] TH>(this IEventBusBuilder eventBusBuilder)
            where T : IntegrationEvent
            where TH : class, IIntegrationEventHandler<T>
        {
            // Use keyed services to register multiple handlers for the same event type
            // the consumer can use IKeyedServiceProvider.GetKeyedService<IIntegrationEventHandler>(typeof(T)) to get all
            // handlers for the event type.
            eventBusBuilder.Services.AddKeyedTransient<IIntegrationEventHandler, TH>(typeof(T));

            eventBusBuilder.Services.Configure<EventBusSubscriptionInfo>(o =>
            {
                // Keep track of all registered event types and their name mapping. We send these event types over the message bus
                // and we don't want to do Type.GetType, so we keep track of the name mapping here.

                // This list will also be used to subscribe to events from the underlying message broker implementation.
                o.EventTypes[typeof(T).Name] = typeof(T);
            });

            return eventBusBuilder;
        }

        /// <summary>
        /// Add EventBus Service
        /// </summary>
        /// <param name="services">IHostApplicationBuilder</param>
        /// <param name="connectionString">amqp://user:pass@hostName:port/vhost</param>
        public static IEventBusBuilder AddEventBus(
            this IHostApplicationBuilder builder, string connectionString, IConfiguration config)
        {
            return AddEventBus(builder.Services, connectionString, config);
        }

        /// <summary>
        /// Add EventBus Service
        /// </summary>
        /// <param name="services">IServiceCollection</param>
        /// <param name="connectionString">amqp://user:pass@hostName:port/vhost</param>
        public static IEventBusBuilder AddEventBus(
            this IServiceCollection services, string connectionString, IConfiguration config)
        {
            // Options support
            services.Configure<RabbitMQOption>(config);

            services.AddSingleton<IRabbitMQPersistentConnection, DefaultRabbitMQPersistentConnection>(sp
                => new DefaultRabbitMQPersistentConnection(
                    new ConnectionFactory()
                    {
                        Uri = new Uri(connectionString)
                    },
                    sp.GetRequiredService<ILogger<DefaultRabbitMQPersistentConnection>>(),
                    sp.GetRequiredService<IOptions<RabbitMQOption>>()));

            services.AddSingleton<IEventBus, EventBusRabbitMQ>(sp =>
            {
                var logger = sp.GetRequiredService<ILogger<EventBusRabbitMQ>>();
                var rabbitMQPersistentConnection = sp.GetRequiredService<IRabbitMQPersistentConnection>();

                return new EventBusRabbitMQ(
                    rabbitMQPersistentConnection,
                    logger,
                    sp,
                    sp.GetRequiredService<IOptions<RabbitMQOption>>(),
                    sp.GetRequiredService<IOptions<EventBusSubscriptionInfo>>());
            });

            return new EventBusBuilder(services);
        }

        private class EventBusBuilder(IServiceCollection services) : IEventBusBuilder
        {
            public IServiceCollection Services => services;
        }
    }
}
