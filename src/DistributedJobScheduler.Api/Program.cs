using DistributedJobScheduler.Application;
using DistributedJobScheduler.Infrastructure;

var builder = WebApplication.CreateBuilder(args);
builder.Services.AddControllers();
builder.Services.AddHealthChecks();
builder.Services.AddSingleton<IJobRepository>(_ => new SqliteJobRepository(
    builder.Configuration.GetConnectionString("Scheduler") ?? "Data Source=distributed-job-scheduler.db"));
builder.Services.AddScoped<IJobManagementService, JobManagementService>();

var app = builder.Build();
app.MapControllers();
app.MapHealthChecks("/health");
app.Run();

public partial class Program;
