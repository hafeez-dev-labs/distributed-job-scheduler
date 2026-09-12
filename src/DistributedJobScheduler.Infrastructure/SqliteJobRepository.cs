using Microsoft.Data.Sqlite;
using DistributedJobScheduler.Application;
using DistributedJobScheduler.Domain;

namespace DistributedJobScheduler.Infrastructure;

public sealed class SqliteJobRepository : IJobRepository
{
    private readonly string connectionString;

    public SqliteJobRepository(string connectionString)
    {
        this.connectionString = connectionString;
        Initialize();
    }

    public async Task<JobDefinition?> GetAsync(Guid id, CancellationToken cancellationToken = default)
    {
        await using var connection = OpenConnection();
        await using var command = connection.CreateCommand();
        command.CommandText = "SELECT Id, Name, CronExpression, Priority, MaxAttempts, InitialDelaySeconds, BackoffMultiplier, Status FROM Jobs WHERE Id = $id";
        command.Parameters.AddWithValue("$id", id.ToString());
        await using var reader = await command.ExecuteReaderAsync(cancellationToken);
        return await reader.ReadAsync(cancellationToken) ? ReadJob(reader) : null;
    }

    public async Task<JobDefinition> CreateAsync(JobDefinition job, string idempotencyKey, CancellationToken cancellationToken = default)
    {
        await using var connection = OpenConnection();
        await using var transaction = await connection.BeginTransactionAsync(cancellationToken);

        var existingId = await FindIdempotentResourceAsync(connection, transaction, "create-job", idempotencyKey, cancellationToken);
        if (existingId is not null)
        {
            var existing = await GetAsync(Guid.Parse(existingId), cancellationToken);
            if (existing is not null)
                return existing;
        }

        await using (var command = connection.CreateCommand())
        {
            command.Transaction = transaction;
            command.CommandText = "INSERT INTO Jobs (Id, Name, CronExpression, Priority, MaxAttempts, InitialDelaySeconds, BackoffMultiplier, Status) VALUES ($id, $name, $cron, $priority, $maxAttempts, $delay, $multiplier, $status)";
            AddJobParameters(command, job);
            await command.ExecuteNonQueryAsync(cancellationToken);
        }

        await InsertIdempotencyAsync(connection, transaction, "create-job", idempotencyKey, job.Id, cancellationToken);
        await transaction.CommitAsync(cancellationToken);
        return job;
    }

    public async Task SaveAsync(JobDefinition job, CancellationToken cancellationToken = default)
    {
        await using var connection = OpenConnection();
        await using var command = connection.CreateCommand();
        command.CommandText = "UPDATE Jobs SET Name = $name, CronExpression = $cron, Priority = $priority, MaxAttempts = $maxAttempts, InitialDelaySeconds = $delay, BackoffMultiplier = $multiplier, Status = $status WHERE Id = $id";
        command.Parameters.AddWithValue("$id", job.Id.ToString());
        AddJobParameters(command, job, false);
        await command.ExecuteNonQueryAsync(cancellationToken);
    }

    public async Task<JobExecution?> GetExecutionAsync(Guid id, CancellationToken cancellationToken = default)
    {
        await using var connection = OpenConnection();
        await using var command = connection.CreateCommand();
        command.CommandText = "SELECT Id, JobId, Status, Attempt, StartedAt, CompletedAt, FailureReason FROM JobExecutions WHERE Id = $id";
        command.Parameters.AddWithValue("$id", id.ToString());
        await using var reader = await command.ExecuteReaderAsync(cancellationToken);
        return await reader.ReadAsync(cancellationToken) ? ReadExecution(reader) : null;
    }

    public async Task<JobExecution> CreateExecutionAsync(JobExecution execution, string idempotencyKey, CancellationToken cancellationToken = default)
    {
        await using var connection = OpenConnection();
        await using var transaction = await connection.BeginTransactionAsync(cancellationToken);

        var existingId = await FindIdempotentResourceAsync(connection, transaction, "trigger-job", idempotencyKey, cancellationToken);
        if (existingId is not null)
        {
            var existing = await GetExecutionAsync(Guid.Parse(existingId), cancellationToken);
            if (existing is not null)
                return existing;
        }

        await using (var command = connection.CreateCommand())
        {
            command.Transaction = transaction;
            command.CommandText = "INSERT INTO JobExecutions (Id, JobId, Status, Attempt, StartedAt, CompletedAt, FailureReason) VALUES ($id, $jobId, $status, $attempt, $startedAt, $completedAt, $failureReason)";
            command.Parameters.AddWithValue("$id", execution.Id.ToString());
            command.Parameters.AddWithValue("$jobId", execution.JobId.ToString());
            command.Parameters.AddWithValue("$status", (int)execution.Status);
            command.Parameters.AddWithValue("$attempt", execution.Attempt);
            command.Parameters.AddWithValue("$startedAt", (object?)execution.StartedAt?.ToString("O") ?? DBNull.Value);
            command.Parameters.AddWithValue("$completedAt", (object?)execution.CompletedAt?.ToString("O") ?? DBNull.Value);
            command.Parameters.AddWithValue("$failureReason", (object?)execution.FailureReason ?? DBNull.Value);
            await command.ExecuteNonQueryAsync(cancellationToken);
        }

        await InsertIdempotencyAsync(connection, transaction, "trigger-job", idempotencyKey, execution.Id, cancellationToken);
        await transaction.CommitAsync(cancellationToken);
        return execution;
    }

    private SqliteConnection OpenConnection()
    {
        var connection = new SqliteConnection(connectionString);
        connection.Open();
        return connection;
    }

    private void Initialize()
    {
        using var connection = OpenConnection();
        using var command = connection.CreateCommand();
        command.CommandText = """
            CREATE TABLE IF NOT EXISTS Jobs (
                Id TEXT PRIMARY KEY,
                Name TEXT NOT NULL,
                CronExpression TEXT NULL,
                Priority INTEGER NOT NULL,
                MaxAttempts INTEGER NOT NULL,
                InitialDelaySeconds INTEGER NOT NULL,
                BackoffMultiplier REAL NOT NULL,
                Status INTEGER NOT NULL
            );
            CREATE TABLE IF NOT EXISTS JobExecutions (
                Id TEXT PRIMARY KEY,
                JobId TEXT NOT NULL,
                Status INTEGER NOT NULL,
                Attempt INTEGER NOT NULL,
                StartedAt TEXT NULL,
                CompletedAt TEXT NULL,
                FailureReason TEXT NULL,
                FOREIGN KEY (JobId) REFERENCES Jobs(Id)
            );
            CREATE TABLE IF NOT EXISTS IdempotencyKeys (
                Operation TEXT NOT NULL,
                IdempotencyKey TEXT NOT NULL,
                ResourceId TEXT NOT NULL,
                PRIMARY KEY (Operation, IdempotencyKey)
            );
            CREATE INDEX IF NOT EXISTS IX_JobExecutions_JobId ON JobExecutions(JobId);
            """;
        command.ExecuteNonQuery();
    }

    private static async Task<string?> FindIdempotentResourceAsync(SqliteConnection connection, SqliteTransaction transaction, string operation, string key, CancellationToken cancellationToken)
    {
        await using var command = connection.CreateCommand();
        command.Transaction = transaction;
        command.CommandText = "SELECT ResourceId FROM IdempotencyKeys WHERE Operation = $operation AND IdempotencyKey = $key";
        command.Parameters.AddWithValue("$operation", operation);
        command.Parameters.AddWithValue("$key", key);
        return await command.ExecuteScalarAsync(cancellationToken) as string;
    }

    private static async Task InsertIdempotencyAsync(SqliteConnection connection, SqliteTransaction transaction, string operation, string key, Guid resourceId, CancellationToken cancellationToken)
    {
        await using var command = connection.CreateCommand();
        command.Transaction = transaction;
        command.CommandText = "INSERT INTO IdempotencyKeys (Operation, IdempotencyKey, ResourceId) VALUES ($operation, $key, $resourceId)";
        command.Parameters.AddWithValue("$operation", operation);
        command.Parameters.AddWithValue("$key", key);
        command.Parameters.AddWithValue("$resourceId", resourceId.ToString());
        await command.ExecuteNonQueryAsync(cancellationToken);
    }

    private static void AddJobParameters(SqliteCommand command, JobDefinition job, bool includeId = true)
    {
        if (includeId)
            command.Parameters.AddWithValue("$id", job.Id.ToString());
        command.Parameters.AddWithValue("$name", job.Name);
        command.Parameters.AddWithValue("$cron", (object?)job.CronExpression ?? DBNull.Value);
        command.Parameters.AddWithValue("$priority", (int)job.Priority);
        command.Parameters.AddWithValue("$maxAttempts", job.RetryPolicy.MaxAttempts);
        command.Parameters.AddWithValue("$delay", job.RetryPolicy.InitialDelaySeconds);
        command.Parameters.AddWithValue("$multiplier", job.RetryPolicy.BackoffMultiplier);
        command.Parameters.AddWithValue("$status", (int)job.Status);
    }

    private static JobDefinition ReadJob(SqliteDataReader reader) =>
        new(
            Guid.Parse(reader.GetString(0)),
            reader.GetString(1),
            reader.IsDBNull(2) ? null : reader.GetString(2),
            (JobPriority)reader.GetInt32(3),
            new RetryPolicy(reader.GetInt32(4), reader.GetInt32(5), reader.GetDouble(6)),
            (JobStatus)reader.GetInt32(7));

    private static JobExecution ReadExecution(SqliteDataReader reader) =>
        new(
            Guid.Parse(reader.GetString(0)),
            Guid.Parse(reader.GetString(1)),
            (JobExecutionStatus)reader.GetInt32(2),
            reader.GetInt32(3),
            reader.IsDBNull(4) ? null : DateTimeOffset.Parse(reader.GetString(4)),
            reader.IsDBNull(5) ? null : DateTimeOffset.Parse(reader.GetString(5)),
            reader.IsDBNull(6) ? null : reader.GetString(6));
}
