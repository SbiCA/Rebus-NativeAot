using System.Text.Json.Serialization;
using Amazon;
using Amazon.Runtime;
using Amazon.Runtime.CredentialManagement;
using Amazon.SQS;
using Raven.Client.Documents;
using Rebus.Bus;
using Rebus.Config;
using Rebus.Handlers;
using Rebus.Retry.Simple;
using Rebus.Routing.TypeBased;
using Rebus.Serialization.Json;
using Scalar.AspNetCore;

var builder = WebApplication.CreateSlimBuilder(args);


builder.Services.AddRavenDb(builder.Configuration);

builder.Services.AddRebus(configure =>
{
    configure.Options(o => o.RetryStrategy());
    
    // 👇 Use System.Text.Json with source generators no reflection based serialization!
    configure.Serialization(c => c.UseSystemTextJson(AppJsonSerializerContext.Default.Options));
    configure.Routing(r => r.TypeBased().Map<SimpleCommand>("my-queue"));
    
    if (!new CredentialProfileStoreChain().TryGetAWSCredentials("ssyuser", out var credentials))
    {
        credentials = new EnvironmentVariablesAWSCredentials();
    }
    // 👇 Use InMemoryTransport for testing
    configure.Transport(t => 
        // t.UseInMemoryTransport(new InMemNetwork(),"my-queue")
        t.UseAmazonSQS( credentials,new AmazonSQSConfig {RegionEndpoint = RegionEndpoint.EUCentral1},  "my-demo-queue", new AmazonSQSTransportOptions { CreateQueues = true })
        );
    return configure;
});

builder.Services.AddRebusHandler<MyHandler>();

builder.Services.ConfigureHttpJsonOptions(options =>
{
    options.SerializerOptions.TypeInfoResolverChain.Insert(0, AppJsonSerializerContext.Default);
});

builder.Services.AddOpenApi(options =>
{
    options.AddDocumentTransformer((document, context, cancellationToken) =>
    {
        document.Info = new()
        {
            Title = "Rebus native AOT sample",
            Version = "v1",
            Description = """
                          ## API for processing checkouts from cart
                          
                          some other content
                          
                          ## another section
                          """
        };
        return Task.CompletedTask;
    });
});

var app = builder.Build();

// var sampleTodos = new Todo[]
// {
//     new(1, "Walk the dog"),
//     new(2, "Do the dishes", DateOnly.FromDateTime(DateTime.Now)),
//     new(3, "Do the laundry", DateOnly.FromDateTime(DateTime.Now.AddDays(1))),
//     new(4, "Clean the bathroom"),
//     new(5, "Clean the car", DateOnly.FromDateTime(DateTime.Now.AddDays(2)))
// };

var todosApi = app.MapGroup("/todos");
todosApi.MapGet("/", (IDocumentStore store) =>
    {
        using var session = store.OpenAsyncSession();
        return session.Query<Todo>().ToListAsync();
    })
    .WithDescription("""
                     # Get all todos
                     
                     some description
                     
                     here you go?
                     """)
    .Produces<Todo[]>(200)
    .WithTags("Todos");

todosApi.MapGet("/{id:int}", async (int id, IDocumentStore store) =>
    {
        using var session = store.OpenAsyncSession();

        var todo = await session.Query<Todo>().FirstOrDefaultAsync(a => a.Id == id);
        return todo != null
            ? Results.Ok(todo)
            : Results.NotFound();
    })
    .WithTags("Todos");

todosApi.MapPost("/", async (Todo todo, IDocumentStore store) =>
    {
        using var session = store.OpenAsyncSession();
        await session.StoreAsync(todo);
        await session.SaveChangesAsync();
        return Results.Created($"/todos/{todo.Id}", todo);
    })
    .Produces<Todo>(201)
    // 👇 exclude from API reference
    // .ExcludeFromDescription()
    .WithDescription("Create a new todo")
    .WithTags("Todos");

app.MapOpenApi();
app.MapScalarApiReference(options =>
{
    
    options.Title = """
                    My custom API reference
                    """;
    
    options.Theme = ScalarTheme.BluePlanet;
    options.ShowSidebar = true;
    options.DarkMode = true;
    options.DefaultHttpClient = new KeyValuePair<ScalarTarget, ScalarClient>(ScalarTarget.CSharp, ScalarClient.HttpClient);
    options.Authentication = new ScalarAuthenticationOptions
    {
        PreferredSecurityScheme = "ApiKey",
        ApiKey = new ApiKeyOptions
        {
            Token = "my-api-key"
        }
    };
});

var cancellationToken = app.Services.GetRequiredService<IHostApplicationLifetime>().ApplicationStopping;
var bus = app.Services.GetRequiredService<IBus>();

// 👇 generate some messages
_ = Sender(cancellationToken, bus, app.Services.GetRequiredService<ILogger<Program>>());
app.Run();


static async Task Sender(CancellationToken cancellationToken, IBus bus, ILogger<Program> logger)
{
    var timer = new PeriodicTimer(TimeSpan.FromSeconds(1));
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

public record Todo(int Id, string? Title, DateOnly? DueBy = null, bool IsComplete = false);

[JsonSerializable(typeof(Todo[]))]
// 👇 add type to serializer
[JsonSerializable(typeof(SimpleCommand))]
internal partial class AppJsonSerializerContext : JsonSerializerContext;
