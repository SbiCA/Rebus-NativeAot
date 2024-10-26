using System.Text.Json.Serialization;
using Rebus.Bus;
using Rebus.Config;
using Rebus.Handlers;
using Rebus.Retry.Simple;
using Rebus.Routing.TypeBased;
using Rebus.Serialization.Json;
using Rebus.Transport.InMem;

var builder = WebApplication.CreateSlimBuilder(args);


builder.Services.AddRebus(configure =>
{
    configure.Transport(t => t.UseInMemoryTransport(new InMemNetwork(),"my-queue"));
    configure.Options(o => o.RetryStrategy());
    configure.Serialization(c => c.UseSystemTextJson( AppJsonSerializerContext.Default.Options));
    configure.Routing(r => r.TypeBased().Map<SimpleCommand>("my-queue"));
    return configure;
});

builder.Services.AddRebusHandler<MyHandler>();

builder.Services.ConfigureHttpJsonOptions(options =>
{
    options.SerializerOptions.TypeInfoResolverChain.Insert(0, AppJsonSerializerContext.Default);
});

var app = builder.Build();

var sampleTodos = new Todo[]
{
    new(1, "Walk the dog"),
    new(2, "Do the dishes", DateOnly.FromDateTime(DateTime.Now)),
    new(3, "Do the laundry", DateOnly.FromDateTime(DateTime.Now.AddDays(1))),
    new(4, "Clean the bathroom"),
    new(5, "Clean the car", DateOnly.FromDateTime(DateTime.Now.AddDays(2)))
};

var todosApi = app.MapGroup("/todos");
todosApi.MapGet("/", () => sampleTodos);
todosApi.MapGet("/{id}", (int id) =>
    sampleTodos.FirstOrDefault(a => a.Id == id) is { } todo
        ? Results.Ok(todo)
        : Results.NotFound());

var cancellationToken = app.Services.GetRequiredService<IHostApplicationLifetime>().ApplicationStopping;
var bus = app.Services.GetRequiredService<IBus>();
_ = Sender(cancellationToken, bus, app.Services.GetRequiredService<ILogger<Program>>());
app.Run();


static async Task Sender(CancellationToken cancellationToken, IBus bus, ILogger<Program> logger)
{
    var timer = new PeriodicTimer(TimeSpan.FromMilliseconds(100));
    while (await timer.WaitForNextTickAsync(cancellationToken))
    {
        try
        {
            await bus.SendLocal(new SimpleCommand(Guid.NewGuid().ToString(), DateTime.Now));
        }
        catch (Exception e)
        {
           logger.LogError(e, "Failed to send command");
        }
    }
}

public record Todo(int Id, string? Title, DateOnly? DueBy = null, bool IsComplete = false);

[JsonSerializable(typeof(Todo[]))]
[JsonSerializable(typeof(SimpleCommand))]
internal partial class AppJsonSerializerContext : JsonSerializerContext
{
}

public record SimpleCommand(string Id, DateTime CreatedAt);
public class MyHandler  : IHandleMessages<SimpleCommand>
{
    private readonly ILogger<MyHandler> _logger;

    public MyHandler(ILogger<MyHandler> logger)
    {
        _logger = logger;
    }

    public Task Handle(SimpleCommand message)
    {
        _logger.LogInformation("Received command with ID {Id} created at {CreatedAt}", message.Id, message.CreatedAt);
        return Task.CompletedTask;
    }
}