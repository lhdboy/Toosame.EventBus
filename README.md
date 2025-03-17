# Toosame.EventBus

An Event Bus Based on RabbitMQ, whose core code is from [eShopOnContainers](https://github.com/dotnet-architecture/eShopOnContainers), I just pulled it out and made some extensions, fixes and improvements.

I currently only use it for microservices. If your project has only one ASP. NET Core project, then it may not be suitable for you.

## Install from Nuget.org

```
PM> Install-Package Toosame.EventBus.RabbitMQ -Version 2.1.0
```

## Using (Publish Event)

1. Add Event
2. Add Event Handler
3. Publish Event in Controller

### 1. Add Event

Create `YourEvent.cs`

```
public record class YourEvent : IntegrationEvent
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
public class HomeController : ControllerBase
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

## Setup (ASP.NET Core 9.0)

You can subscribe to the event you just created here.

1. Configure appsettings.json
2. Setup on `Program.cs`

### 1. Configure appsettings.json

```JSON
{
  "Logging": {
    "LogLevel": {
      "Default": "Warning"
    }
  },
  "RabbitMQOption": {
    "EventBusRetryCount": 5,
    "EventBusBrokeName": "<rabbitMqExchangeName>",
    "SubscriptionClientName": "<queueName>" //It's better to have different microservices with different names
  }
}
```

### 2.Setup on `Program.cs`

```CSharp
    builder.Services.AddEventBus(Configuration.GetConnectionString("RabbitMQ"), Configuration.GetSection("RabbitMQOption"))
        .AddSubscription<YourEvent1, YourEventHandler1>();
```

### 3.Add `YourHostedService.cs`

```CSharp
    builder.Services.AddHostedService<YourHostedService>();
```
