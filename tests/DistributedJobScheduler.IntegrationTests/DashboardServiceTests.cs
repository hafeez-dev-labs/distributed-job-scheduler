using Microsoft.Data.Sqlite;
using DistributedJobScheduler.Infrastructure;
using Xunit;

namespace DistributedJobScheduler.IntegrationTests;

public sealed class DashboardServiceTests
{
    [Fact]
    public async Task Snapshot_ReturnsJobsAndExecutions()
    {
        var database=$"Data Source=file:dashboard-{Guid.NewGuid():N}?mode=memory&cache=shared";
        await using var keepAlive=new SqliteConnection(database);
        await keepAlive.OpenAsync();
        await using (var command=keepAlive.CreateCommand())
        {
            command.CommandText="""CREATE TABLE Jobs (Id TEXT PRIMARY KEY,Name TEXT NOT NULL,CronExpression TEXT NULL,Priority INTEGER NOT NULL,MaxAttempts INTEGER NOT NULL,InitialDelaySeconds INTEGER NOT NULL,BackoffMultiplier REAL NOT NULL,Status INTEGER NOT NULL,NextExecutionAt TEXT NULL,TenantId TEXT NULL,ConcurrencyGroup TEXT NULL,MaxConcurrentExecutions INTEGER NULL); CREATE TABLE JobExecutions (Id TEXT PRIMARY KEY,JobId TEXT NOT NULL,Status INTEGER NOT NULL,Attempt INTEGER NOT NULL,StartedAt TEXT NULL,CompletedAt TEXT NULL,FailureReason TEXT NULL); INSERT INTO Jobs VALUES ('00000000-0000-0000-0000-000000000001','demo','*/5 * * * *',1,3,5,2,1,NULL,NULL,NULL,NULL); INSERT INTO JobExecutions VALUES ('00000000-0000-0000-0000-000000000002','00000000-0000-0000-0000-000000000001',3,1,NULL,NULL,'timeout');""";
            await command.ExecuteNonQueryAsync();
        }
        var service=new DashboardService(database);
        var snapshot=await service.GetSnapshotAsync(search:"demo");
        Assert.Single(snapshot.Jobs);
        Assert.Single(snapshot.RecentExecutions);
        Assert.Equal("Failed",snapshot.RecentExecutions[0].Status);
        Assert.Equal("timeout",snapshot.RecentExecutions[0].FailureReason);
    }
}