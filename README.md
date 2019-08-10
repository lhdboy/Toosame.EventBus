# Toosame.EventBus

An Event Bus Based on RabbitMQ, whose core code is from [eShopOnContainers](https://github.com/dotnet-architecture/eShopOnContainers), I just pulled it out and made some extensions, fixes and improvements.

I currently only use it for microservices. If your project has only one ASP. NET Core project, then it may not be suitable for you.

## Install from Nuget.org

```
PM> Install-Package Toosame.EventBus.RabbitMQ -Version 1.1.2
```

## Using (Publish Event)

1. Add Event
2. Add Event Handler
3. Publish Event in Controller

### 1. Add Event

Create `YourEvent.cs`

```
public class YourEvent : IntegrationEvent
{
    public string Name { get; set; }

    public int Age { get; set; }
}
```

### 2. Add Event Handler

Create `YourEventHandler.cs`

```
public class YourEventHandler : IIntegrationEventHandler<YourEvent>
{
    private readonly IConfiguration _configuration;

    public YourEventHandler(IConfiguration configuration){
        //I'm just telling you that you can also use Dependency Injection Services.
        _configuration = configuration;
    }

    public Task Handle(YourEvent @event)
    {
        //you can get @event.Name
        //you can get @event.Age

        //Do something...
    
        return Task.CompletedTask;
    }
}
```

### 3. Publish Event in Controller

```
public class HomeController : Controller
{
    private readonly IEventBus _eventBus;

    public YourEventHandler(IEventBus eventBus){
        _eventBus = eventBus;
    }

    [HttpGet]
    public IAcionResult Index(){
        _eventBus.Publish(new YourEvent(){
            Name: "my name",
            Age: 22
        })
    }
}
```

***

## Setup

You can subscribe to the event you just created here.

1. Configure appsettings.json
2. Setup on `Startup.cs`

### 1. Configure appsettings.json

```JSON
{
  "Logging": {
    "LogLevel": {
      "Default": "Warning"
    }
  },
  "RabbitMQ": {
    "EventBusConnection": "<yourRabbitMqHost>[:port(default 5672)]",
    "EventBusUserName": "<rabbitMqUserName>",
    "EventBusPassword": "<rabbitMqPassword>",
    "EventBusRetryCount": 5,
    "EventBusBrokeName": "<rabbitMqExchangeName>",
    "SubscriptionClientName": "<queueName>" //It's better to have different microservices with different names
  }
}
```

### 2.Setup on `Startup.cs`

Standard£º

```CSharp
public void ConfigureServices(IServiceCollection services)
{
    services.AddEventBus(Configuration.GetSection("RabbitMQ").Get<RabbitMQOption>(),
                    eventHandlers =>
                    {
                        eventHandlers.AddEventHandler<YourEventHandler1>();
                        eventHandlers.AddEventHandler<YourEventHandler2>();
                    });

    services.AddMvc()
        .SetCompatibilityVersion(CompatibilityVersion.Version_2_2);
}

public void Configure(IApplicationBuilder app, IHostingEnvironment env)
{
    app.UseEventBus(eventBus =>
    {
        eventBus.Subscribe<YourEvent1, YourEventHandler1>();
        eventBus.Subscribe<YourEvent2, YourEventHandler2>();
    });

    app.UseMvc();
}
```

Using `Autofac`:

1. Please install `Autofac.Extensions.DependencyInjection`  package.

2. As below:

```CSharp
public IServiceProvider ConfigureServices(IServiceCollection services)
{
    services.AddMvc()
        .SetCompatibilityVersion(CompatibilityVersion.Version_2_2)
        .AddControllersAsServices();

    return services.AddEventBusAsAutofacService(Configuration.GetSection("RabbitMQ").Get<RabbitMQOption>(),
                eventHandlers =>
            {
                eventHandlers.AddEventHandler<YourEventHandler1>();
                eventHandlers.AddEventHandler<YourEventHandler2>();
            });
}


public void Configure(IApplicationBuilder app, IHostingEnvironment env)
{
    app.UseEventBus(eventBus =>
    {
        eventBus.Subscribe<YourEvent1, YourEventHandler1>();
        eventBus.Subscribe<YourEvent2, YourEventHandler2>();
    });

    app.UseMvc();
}
```
