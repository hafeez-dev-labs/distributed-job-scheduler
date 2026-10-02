using Microsoft.Data.Sqlite;
using DistributedJobScheduler.Domain;

namespace DistributedJobScheduler.Infrastructure;

public sealed record DashboardSnapshot(IReadOnlyList<DashboardJob> Jobs, IReadOnlyList<DashboardExecution> RecentExecutions, DashboardWorkerStatus WorkerStatus);
public sealed record DashboardJob(Guid Id,string Name,string? Schedule,string Status,DateTimeOffset? NextExecutionAt,string? TenantId,string? ConcurrencyGroup,int? MaxConcurrentExecutions);
public sealed record DashboardExecution(Guid Id,Guid JobId,string JobName,string Status,int Attempt,DateTimeOffset? StartedAt,DateTimeOffset? CompletedAt,string? FailureReason);
public sealed record DashboardWorkerStatus(string Status,string Message);

public sealed class DashboardService
{
    private readonly string connectionString;
    public DashboardService(string connectionString="Data Source=distributed-job-scheduler.db") => this.connectionString=connectionString;

    public async Task<DashboardSnapshot> GetSnapshotAsync(string? search=null,string? status=null,CancellationToken cancellationToken=default)
    {
        await using var connection=new SqliteConnection(connectionString);
        await connection.OpenAsync(cancellationToken);
        var jobs=await ReadJobsAsync(connection,search,status,cancellationToken);
        var executions=await ReadExecutionsAsync(connection,search,status,cancellationToken);
        return new DashboardSnapshot(jobs,executions,new DashboardWorkerStatus("limited","Worker heartbeats are process-local in the current worker host and are not persisted or exposed through the API yet."));
    }

    private static async Task<IReadOnlyList<DashboardJob>> ReadJobsAsync(SqliteConnection connection,string? search,string? status,CancellationToken cancellationToken)
    {
        await using var command=connection.CreateCommand();
        command.CommandText="""SELECT Id,Name,CronExpression,Status,NextExecutionAt,TenantId,ConcurrencyGroup,MaxConcurrentExecutions FROM Jobs WHERE ($status IS NULL OR $status='' OR Status=$status) AND ($search IS NULL OR $search='' OR Name LIKE $search OR Id LIKE $search) ORDER BY COALESCE(NextExecutionAt,'9999-12-31T23:59:59.9999999+00:00'),Name LIMIT 100;""";
        command.Parameters.AddWithValue("$status",(object?)ParseJobStatus(status)??DBNull.Value);
        command.Parameters.AddWithValue("$search",string.IsNullOrWhiteSpace(search)?DBNull.Value:$"%{search.Trim()}%");
        await using var reader=await command.ExecuteReaderAsync(cancellationToken);
        var result=new List<DashboardJob>();
        while(await reader.ReadAsync(cancellationToken))
            result.Add(new DashboardJob(Guid.Parse(reader.GetString(0)),reader.GetString(1),reader.IsDBNull(2)?null:reader.GetString(2),((JobStatus)reader.GetInt32(3)).ToString(),reader.IsDBNull(4)?null:DateTimeOffset.Parse(reader.GetString(4)),reader.IsDBNull(5)?null:reader.GetString(5),reader.IsDBNull(6)?null:reader.GetString(6),reader.IsDBNull(7)?null:reader.GetInt32(7)));
        return result;
    }

    private static async Task<IReadOnlyList<DashboardExecution>> ReadExecutionsAsync(SqliteConnection connection,string? search,string? status,CancellationToken cancellationToken)
    {
        await using var command=connection.CreateCommand();
        command.CommandText="""SELECT e.Id,e.JobId,j.Name,e.Status,e.Attempt,e.StartedAt,e.CompletedAt,e.FailureReason FROM JobExecutions e INNER JOIN Jobs j ON j.Id=e.JobId WHERE ($status IS NULL OR $status='' OR e.Status=$status) AND ($search IS NULL OR $search='' OR j.Name LIKE $search OR e.Id LIKE $search OR e.FailureReason LIKE $search) ORDER BY COALESCE(e.StartedAt,'0000-01-01T00:00:00+00:00') DESC LIMIT 100;""";
        command.Parameters.AddWithValue("$status",(object?)ParseExecutionStatus(status)??DBNull.Value);
        command.Parameters.AddWithValue("$search",string.IsNullOrWhiteSpace(search)?DBNull.Value:$"%{search.Trim()}%");
        await using var reader=await command.ExecuteReaderAsync(cancellationToken);
        var result=new List<DashboardExecution>();
        while(await reader.ReadAsync(cancellationToken))
            result.Add(new DashboardExecution(Guid.Parse(reader.GetString(0)),Guid.Parse(reader.GetString(1)),reader.GetString(2),((JobExecutionStatus)reader.GetInt32(3)).ToString(),reader.GetInt32(4),reader.IsDBNull(5)?null:DateTimeOffset.Parse(reader.GetString(5)),reader.IsDBNull(6)?null:DateTimeOffset.Parse(reader.GetString(6)),reader.IsDBNull(7)?null:reader.GetString(7)));
        return result;
    }

    private static int? ParseJobStatus(string? value)=>Enum.TryParse<JobStatus>(value,true,out var parsed)?(int)parsed:null;
    private static int? ParseExecutionStatus(string? value)=>Enum.TryParse<JobExecutionStatus>(value,true,out var parsed)?(int)parsed:null;
}
