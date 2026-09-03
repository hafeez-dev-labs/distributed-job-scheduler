using DistributedJobScheduler.Application;
using DistributedJobScheduler.Contracts;
using DistributedJobScheduler.Domain;
using DistributedJobScheduler.Infrastructure;

var builder = WebApplication.CreateBuilder(args);
builder.Services.AddControllers();
builder.Services.AddHealthChecks();
builder.Services.AddSingleton<IJobRepository, InMemoryJobRepository>();

var app = builder.Build();
app.MapControllers();
app.MapHealthChecks("/health");
app.Run();

public partial class Program;
