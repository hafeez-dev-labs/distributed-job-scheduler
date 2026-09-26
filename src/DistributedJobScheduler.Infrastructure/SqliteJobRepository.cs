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
        command.CommandText = "SELECT Id, Name, CronExpression, Priority, MaxAttempts, InitialDelaySeconds, BackoffMultiplier, Status, NextExecutionAt, TenantId, ConcurrencyGroup, MaxConcurrentExecutions FROM Jobs WHERE Id = $id";
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

        await using var command = connection.CreateCommand();
        command.Transaction = transaction;
        command.CommandText = "INSERT INTO Jobs (Id, Name, CronExpression, Priority, MaxAttempts, InitialDelaySeconds, BackoffMultiplier, Status, NextExecutionAt, TenantId, ConcurrencyGroup, MaxConcurrentExecutions) VALUES ($id, $name, $cron, $priority, $maxAttempts, $delay, $multiplier, $status, $nextExecutionAt, $tenantId, $concurrencyGroup, $maxConcurrentExecutions)";
        AddJobParameters(command, job);
        await command.ExecuteNonQueryAsync(cancellationToken);
        await InsertIdempotencyAsync(connection, transaction, "create-job", idempotencyKey, job.Id, cancellationToken);
        await transaction.CommitAsync(cancellationToken);
        return job;
    }

    public async Task SaveAsync(JobDefinition job, CancellationToken cancellationToken = default)
    {
        await using var connection = OpenConnection();
        await using var command = connection.CreateCommand();
        command.CommandText = "UPDATE Jobs SET Name = $name, CronExpression = $cron, Priority = $priority, MaxAttempts = $maxAttempts, InitialDelaySeconds = $delay, BackoffMultiplier = $multiplier, Status = $status, NextExecutionAt = $nextExecutionAt, TenantId = $tenantId, ConcurrencyGroup = $concurrencyGroup, MaxConcurrentExecutions = $maxConcurrentExecutions WHERE Id = $id";
        command.Parameters.AddWithValue("$id", job.Id.ToString());
        AddJobParameters(command, job, false);
        await command.ExecuteNonQueryAsync(cancellationToken);
    }

    public async Task<IReadOnlyList<JobDefinition>> GetDueJobsAsync(DateTimeOffset now, CancellationToken cancellationToken = default)
    {
        await using var connection = OpenConnection();
        await using var command = connection.CreateCommand();
        command.CommandText = "SELECT Id, Name, CronExpression, Priority, MaxAttempts, InitialDelaySeconds, BackoffMultiplier, Status, NextExecutionAt, TenantId, ConcurrencyGroup, MaxConcurrentExecutions FROM Jobs WHERE Status = $status AND CronExpression IS NOT NULL AND (NextExecutionAt IS NULL OR NextExecutionAt <= $now) ORDER BY COALESCE(NextExecutionAt, $minimum)";
        command.Parameters.AddWithValue("$status", (int)JobStatus.Active);
        command.Parameters.AddWithValue("$now", now.ToString("O"));
        command.Parameters.AddWithValue("$minimum", DateTimeOffset.MinValue.ToString("O"));
        await using var reader = await command.ExecuteReaderAsync(cancellationToken);
        var jobs = new List<JobDefinition>();
        while (await reader.ReadAsync(cancellationToken))
            jobs.Add(ReadJob(reader));
        return jobs;
    }

    public async Task<bool> TryAcquireSchedulerLeaseAsync(Guid jobId, string owner, DateTimeOffset now, TimeSpan duration, CancellationToken cancellationToken = default)
    {
        await using var connection = OpenConnection();
        await using var command = connection.CreateCommand();
        command.CommandText = "INSERT INTO SchedulerLeases (JobId, Owner, LeaseUntil) VALUES ($jobId, $owner, $leaseUntil) ON CONFLICT(JobId) DO UPDATE SET Owner = excluded.Owner, LeaseUntil = excluded.LeaseUntil WHERE SchedulerLeases.LeaseUntil <= $now OR SchedulerLeases.Owner = $owner; SELECT changes();";
        command.Parameters.AddWithValue("$jobId", jobId.ToString());
        command.Parameters.AddWithValue("$owner", owner);
        command.Parameters.AddWithValue("$leaseUntil", now.Add(duration).ToString("O"));
        command.Parameters.AddWithValue("$now", now.ToString("O"));
        return Convert.ToInt32(await command.ExecuteScalarAsync(cancellationToken)) == 1;
    }

    public async Task<bool> RenewSchedulerLeaseAsync(Guid jobId, string owner, DateTimeOffset now, TimeSpan duration, CancellationToken cancellationToken = default)
    {
        await using var connection = OpenConnection();
        await using var command = connection.CreateCommand();
        command.CommandText = "UPDATE SchedulerLeases SET LeaseUntil = $leaseUntil WHERE JobId = $jobId AND Owner = $owner AND LeaseUntil > $now";
        command.Parameters.AddWithValue("$jobId", jobId.ToString());
        command.Parameters.AddWithValue("$owner", owner);
        command.Parameters.AddWithValue("$leaseUntil", now.Add(duration).ToString("O"));
        command.Parameters.AddWithValue("$now", now.ToString("O"));
        return await command.ExecuteNonQueryAsync(cancellationToken) == 1;
    }

    public async Task<JobExecution?> GetExecutionAsync(Guid id, CancellationToken cancellationToken = default)
    {
        await using var connection = OpenConnection();
        await using var command = connection.CreateCommand();
        command.CommandText = "SELECT Id, JobId, Status, Attempt, StartedAt, CompletedAt, FailureReason, LeaseOwner, LeaseUntil FROM JobExecutions WHERE Id = $id";
        command.Parameters.AddWithValue("$id", id.ToString());
        await using var reader = await command.ExecuteReaderAsync(cancellationToken);
        return await reader.ReadAsync(cancellationToken) ? ReadExecution(reader) : null;
    }

    public Task<bool> TryAcquireExecutionLeaseAsync(Guid executionId, string owner, DateTimeOffset now, TimeSpan duration, CancellationToken cancellationToken = default) =>
        TryAcquireExecutionLeaseAsync(executionId, owner, now, duration, null, null, null, cancellationToken);

    public async Task<bool> TryAcquireExecutionLeaseAsync(
        Guid executionId,
        string owner,
        DateTimeOffset now,
        TimeSpan duration,
        int? maxConcurrentExecutions,
        string? tenantId,
        string? concurrencyGroup,
        CancellationToken cancellationToken = default)
    {
        if (string.IsNullOrWhiteSpace(owner))
            throw new ArgumentException("Execution lease owner is required.", nameof(owner));

        await using var connection = OpenConnection();
        await using var command = connection.CreateCommand();
        command.CommandText = """
            UPDATE JobExecutions
            SET LeaseOwner = $owner,
                LeaseUntil = $leaseUntil
            WHERE Id = $id
              AND Status NOT IN ($succeeded, $cancelled)
              AND (LeaseUntil IS NULL OR LeaseUntil <= $now OR LeaseOwner = $owner)
              AND (
                  $maxConcurrentExecutions IS NULL
                  OR (
                      SELECT COUNT(*)
                      FROM JobExecutions activeExecution
                      INNER JOIN Jobs activeJob ON activeJob.Id = activeExecution.JobId
                      WHERE activeExecution.Id <> $id
                        AND activeExecution.Status = $running
                        AND activeExecution.LeaseUntil > $now
                        AND (
                            ($concurrencyGroup IS NOT NULL AND $concurrencyGroup <> ''
                                AND activeJob.TenantId = $tenantId
                                AND activeJob.ConcurrencyGroup = $concurrencyGroup)
                            OR
                            (($concurrencyGroup IS NULL OR $concurrencyGroup = '')
                                AND $tenantId IS NOT NULL AND $tenantId <> ''
                                AND activeJob.TenantId = $tenantId)
                            OR
                            (($concurrencyGroup IS NULL OR $concurrencyGroup = '')
                                AND ($tenantId IS NULL OR $tenantId = '')
                                AND activeExecution.JobId = $jobId)
                        )
                  ) < $maxConcurrentExecutions
              );
            SELECT changes();
            """;
        command.Parameters.AddWithValue("$id", executionId.ToString());
        command.Parameters.AddWithValue("$jobId", GetExecutionJobId(connection, executionId));
        command.Parameters.AddWithValue("$owner", owner);
        command.Parameters.AddWithValue("$leaseUntil", now.Add(duration).ToString("O"));
        command.Parameters.AddWithValue("$now", now.ToString("O"));
        command.Parameters.AddWithValue("$succeeded", (int)JobExecutionStatus.Succeeded);
        command.Parameters.AddWithValue("$cancelled", (int)JobExecutionStatus.Cancelled);
        command.Parameters.AddWithValue("$running", (int)JobExecutionStatus.Running);
        command.Parameters.AddWithValue("$maxConcurrentExecutions", (object?)maxConcurrentExecutions ?? DBNull.Value);
        command.Parameters.AddWithValue("$tenantId", (object?)tenantId ?? DBNull.Value);
        command.Parameters.AddWithValue("$concurrencyGroup", (object?)concurrencyGroup ?? DBNull.Value);
        return Convert.ToInt32(await command.ExecuteScalarAsync(cancellationToken)) == 1;
    }

    public async Task<bool> RenewExecutionLeaseAsync(Guid executionId, string owner, DateTimeOffset now, TimeSpan duration, CancellationToken cancellationToken = default)
    {
        await using var connection = OpenConnection();
        await using var command = connection.CreateCommand();
        command.CommandText = "UPDATE JobExecutions SET LeaseUntil = $leaseUntil WHERE Id = $id AND LeaseOwner = $owner AND LeaseUntil > $now";
        command.Parameters.AddWithValue("$id", executionId.ToString());
        command.Parameters.AddWithValue("$owner", owner);
        command.Parameters.AddWithValue("$leaseUntil", now.Add(duration).ToString("O"));
        command.Parameters.AddWithValue("$now", now.ToString("O"));
        return await command.ExecuteNonQueryAsync(cancellationToken) == 1;
    }

    public async Task<bool> TryUpdateExecutionAsync(JobExecution execution, string owner, DateTimeOffset now, CancellationToken cancellationToken = default)
    {
        await using var connection = OpenConnection();
        await using var command = connection.CreateCommand();
        command.CommandText = """
            UPDATE JobExecutions
            SET Status = $status,
                Attempt = $attempt,
                StartedAt = $startedAt,
                CompletedAt = $completedAt,
                FailureReason = $failureReason
            WHERE Id = $id
              AND LeaseOwner = $owner
              AND LeaseUntil > $now
            """;
        command.Parameters.AddWithValue("$id", execution.Id.ToString());
        command.Parameters.AddWithValue("$owner", owner);
        command.Parameters.AddWithValue("$now", now.ToString("O"));
        command.Parameters.AddWithValue("$status", (int)execution.Status);
        command.Parameters.AddWithValue("$attempt", execution.Attempt);
        command.Parameters.AddWithValue("$startedAt", (object?)execution.StartedAt?.ToString("O") ?? DBNull.Value);
        command.Parameters.AddWithValue("$completedAt", (object?)execution.CompletedAt?.ToString("O") ?? DBNull.Value);
        command.Parameters.AddWithValue("$failureReason", (object?)execution.FailureReason ?? DBNull.Value);
        return await command.ExecuteNonQueryAsync(cancellationToken) == 1;
    }

    public async Task<bool> ReleaseExecutionLeaseAsync(Guid executionId, string owner, CancellationToken cancellationToken = default)
    {
        await using var connection = OpenConnection();
        await using var command = connection.CreateCommand();
        command.CommandText = "UPDATE JobExecutions SET LeaseOwner = NULL, LeaseUntil = NULL WHERE Id = $id AND LeaseOwner = $owner";
        command.Parameters.AddWithValue("$id", executionId.ToString());
        command.Parameters.AddWithValue("$owner", owner);
        return await command.ExecuteNonQueryAsync(cancellationToken) == 1;
    }

    public async Task UpdateExecutionAsync(JobExecution execution, CancellationToken cancellationToken = default)
    {
        await using var connection = OpenConnection();
        await using var command = connection.CreateCommand();
        command.CommandText = "UPDATE JobExecutions SET Status = $status, Attempt = $attempt, StartedAt = $startedAt, CompletedAt = $completedAt, FailureReason = $failureReason WHERE Id = $id";
        command.Parameters.AddWithValue("$id", execution.Id.ToString());
        command.Parameters.AddWithValue("$status", (int)execution.Status);
        command.Parameters.AddWithValue("$attempt", execution.Attempt);
        command.Parameters.AddWithValue("$startedAt", (object?)execution.StartedAt?.ToString("O") ?? DBNull.Value);
        command.Parameters.AddWithValue("$completedAt", (object?)execution.CompletedAt?.ToString("O") ?? DBNull.Value);
        command.Parameters.AddWithValue("$failureReason", (object?)execution.FailureReason ?? DBNull.Value);
        if (await command.ExecuteNonQueryAsync(cancellationToken) != 1)
            throw new InvalidOperationException($"Execution '{execution.Id}' was not found.");
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

        await using var command = connection.CreateCommand();
        command.Transaction = transaction;
        command.CommandText = "INSERT INTO JobExecutions (Id, JobId, Status, Attempt, StartedAt, CompletedAt, FailureReason, LeaseOwner, LeaseUntil) VALUES ($id, $jobId, $status, $attempt, $startedAt, $completedAt, $failureReason, NULL, NULL)";
        command.Parameters.AddWithValue("$id", execution.Id.ToString());
        command.Parameters.AddWithValue("$jobId", execution.JobId.ToString());
        command.Parameters.AddWithValue("$status", (int)execution.Status);
        command.Parameters.AddWithValue("$attempt", execution.Attempt);
        command.Parameters.AddWithValue("$startedAt", (object?)execution.StartedAt?.ToString("O") ?? DBNull.Value);
        command.Parameters.AddWithValue("$completedAt", (object?)execution.CompletedAt?.ToString("O") ?? DBNull.Value);
        command.Parameters.AddWithValue("$failureReason", (object?)execution.FailureReason ?? DBNull.Value);
        await command.ExecuteNonQueryAsync(cancellationToken);

        await InsertIdempotencyAsync(connection, transaction, "trigger-job", idempotencyKey, execution.Id, cancellationToken);
        await transaction.CommitAsync(cancellationToken);
        return execution;
    }

    public async Task<IReadOnlyList<JobDependency>> GetDependenciesAsync(Guid jobId, CancellationToken cancellationToken = default)
    {
        await using var connection = OpenConnection();
        await using var command = connection.CreateCommand();
        command.CommandText = "SELECT JobId, DependsOnJobId FROM JobDependencies WHERE JobId = $jobId ORDER BY DependsOnJobId";
        command.Parameters.AddWithValue("$jobId", jobId.ToString());
        await using var reader = await command.ExecuteReaderAsync(cancellationToken);
        var dependencies = new List<JobDependency>();
        while (await reader.ReadAsync(cancellationToken))
            dependencies.Add(new JobDependency(Guid.Parse(reader.GetString(0)), Guid.Parse(reader.GetString(1))));
        return dependencies;
    }

    public async Task AddDependencyAsync(Guid jobId, Guid dependsOnJobId, CancellationToken cancellationToken = default)
    {
        if (jobId == dependsOnJobId)
            throw new InvalidOperationException("A job cannot depend on itself.");

        await using var connection = OpenConnection();
        await using var transaction = await connection.BeginTransactionAsync(cancellationToken);

        if (!await JobExistsAsync(connection, transaction, jobId, cancellationToken) ||
            !await JobExistsAsync(connection, transaction, dependsOnJobId, cancellationToken))
            throw new InvalidOperationException("Both jobs must exist before a dependency can be created.");

        await using var cycleCommand = connection.CreateCommand();
        cycleCommand.Transaction = transaction;
        cycleCommand.CommandText = """
            WITH RECURSIVE dependency_path(job_id) AS (
                SELECT DependsOnJobId
                FROM JobDependencies
                WHERE JobId = $start
                UNION
                SELECT dependency.DependsOnJobId
                FROM JobDependencies dependency
                INNER JOIN dependency_path path ON dependency.JobId = path.job_id
            )
            SELECT EXISTS(
                SELECT 1
                FROM dependency_path
                WHERE job_id = $target
            );
            """;
        cycleCommand.Parameters.AddWithValue("$start", dependsOnJobId.ToString());
        cycleCommand.Parameters.AddWithValue("$target", jobId.ToString());

        if (Convert.ToInt32(await cycleCommand.ExecuteScalarAsync(cancellationToken)) == 1)
            throw new InvalidOperationException("The dependency would create a cycle.");

        await using var insert = connection.CreateCommand();
        insert.Transaction = transaction;
        insert.CommandText = "INSERT OR IGNORE INTO JobDependencies (JobId, DependsOnJobId) VALUES ($jobId, $dependsOnJobId)";
        insert.Parameters.AddWithValue("$jobId", jobId.ToString());
        insert.Parameters.AddWithValue("$dependsOnJobId", dependsOnJobId.ToString());
        await insert.ExecuteNonQueryAsync(cancellationToken);
        await transaction.CommitAsync(cancellationToken);
    }

    public async Task<bool> AreDependenciesSatisfiedAsync(Guid jobId, CancellationToken cancellationToken = default)
    {
        await using var connection = OpenConnection();
        await using var command = connection.CreateCommand();
        command.CommandText = """
            SELECT NOT EXISTS(
                SELECT 1
                FROM JobDependencies dependency
                WHERE dependency.JobId = $jobId
                  AND NOT EXISTS(
                      SELECT 1
                      FROM JobExecutions execution
                      WHERE execution.JobId = dependency.DependsOnJobId
                        AND execution.Status = $succeeded
                  )
            );
            """;
        command.Parameters.AddWithValue("$jobId", jobId.ToString());
        command.Parameters.AddWithValue("$succeeded", (int)JobExecutionStatus.Succeeded);
        return Convert.ToInt32(await command.ExecuteScalarAsync(cancellationToken)) == 1;
    }

    public async Task<bool> HasFailedDependencyAsync(Guid jobId, CancellationToken cancellationToken = default)
    {
        await using var connection = OpenConnection();
        await using var command = connection.CreateCommand();
        command.CommandText = """
            SELECT EXISTS(
                SELECT 1
                FROM JobDependencies dependency
                WHERE dependency.JobId = $jobId
                  AND NOT EXISTS(
                      SELECT 1
                      FROM JobExecutions execution
                      WHERE execution.JobId = dependency.DependsOnJobId
                        AND execution.Status = $succeeded
                  )
                  AND EXISTS(
                      SELECT 1
                      FROM JobExecutions execution
                      WHERE execution.JobId = dependency.DependsOnJobId
                        AND execution.Status IN ($failed, $deadLettered, $cancelled)
                  )
            );
            """;
        command.Parameters.AddWithValue("$jobId", jobId.ToString());
        command.Parameters.AddWithValue("$succeeded", (int)JobExecutionStatus.Succeeded);
        command.Parameters.AddWithValue("$failed", (int)JobExecutionStatus.Failed);
        command.Parameters.AddWithValue("$deadLettered", (int)JobExecutionStatus.DeadLettered);
        command.Parameters.AddWithValue("$cancelled", (int)JobExecutionStatus.Cancelled);
        return Convert.ToInt32(await command.ExecuteScalarAsync(cancellationToken)) == 1;
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
                Status INTEGER NOT NULL,
                NextExecutionAt TEXT NULL,
                TenantId TEXT NULL,
                ConcurrencyGroup TEXT NULL,
                MaxConcurrentExecutions INTEGER NULL
            );
            CREATE TABLE IF NOT EXISTS JobExecutions (
                Id TEXT PRIMARY KEY,
                JobId TEXT NOT NULL,
                Status INTEGER NOT NULL,
                Attempt INTEGER NOT NULL,
                StartedAt TEXT NULL,
                CompletedAt TEXT NULL,
                FailureReason TEXT NULL,
                LeaseOwner TEXT NULL,
                LeaseUntil TEXT NULL,
                FOREIGN KEY (JobId) REFERENCES Jobs(Id)
            );
            CREATE TABLE IF NOT EXISTS IdempotencyKeys (
                Operation TEXT NOT NULL,
                IdempotencyKey TEXT NOT NULL,
                ResourceId TEXT NOT NULL,
                PRIMARY KEY (Operation, IdempotencyKey)
            );
            CREATE TABLE IF NOT EXISTS SchedulerLeases (
                JobId TEXT PRIMARY KEY,
                Owner TEXT NOT NULL,
                LeaseUntil TEXT NOT NULL
            );
            CREATE TABLE IF NOT EXISTS JobDependencies (
                JobId TEXT NOT NULL,
                DependsOnJobId TEXT NOT NULL,
                PRIMARY KEY (JobId, DependsOnJobId),
                FOREIGN KEY (JobId) REFERENCES Jobs(Id),
                FOREIGN KEY (DependsOnJobId) REFERENCES Jobs(Id),
                CHECK (JobId <> DependsOnJobId)
            );
            CREATE INDEX IF NOT EXISTS IX_JobExecutions_JobId ON JobExecutions(JobId);
            CREATE INDEX IF NOT EXISTS IX_JobExecutions_Lease ON JobExecutions(LeaseUntil, LeaseOwner);
            CREATE INDEX IF NOT EXISTS IX_Jobs_Scheduling ON Jobs(Status, NextExecutionAt);
            """;
        command.ExecuteNonQuery();
        EnsureNextExecutionColumn(connection);
        EnsureExecutionLeaseColumns(connection);
        EnsureConcurrencyColumns(connection);

        using var indexCommand = connection.CreateCommand();
        indexCommand.CommandText = "CREATE INDEX IF NOT EXISTS IX_Jobs_Concurrency ON Jobs(TenantId, ConcurrencyGroup);";
        indexCommand.ExecuteNonQuery();
    }

    private static void EnsureNextExecutionColumn(SqliteConnection connection)
    {
        using var command = connection.CreateCommand();
        command.CommandText = "ALTER TABLE Jobs ADD COLUMN NextExecutionAt TEXT NULL";
        try
        {
            command.ExecuteNonQuery();
        }
        catch (SqliteException exception) when (exception.SqliteErrorCode == 1 && exception.Message.Contains("duplicate column name", StringComparison.OrdinalIgnoreCase))
        {
        }
    }

    private static void EnsureExecutionLeaseColumns(SqliteConnection connection)
    {
        using var command = connection.CreateCommand();
        foreach (var statement in new[]
        {
            "ALTER TABLE JobExecutions ADD COLUMN LeaseOwner TEXT NULL",
            "ALTER TABLE JobExecutions ADD COLUMN LeaseUntil TEXT NULL"
        })
        {
            command.CommandText = statement;
            try
            {
                command.ExecuteNonQuery();
            }
            catch (SqliteException exception) when (exception.SqliteErrorCode == 1 && exception.Message.Contains("duplicate column name", StringComparison.OrdinalIgnoreCase))
            {
            }
        }
    }

    private static void EnsureConcurrencyColumns(SqliteConnection connection)
    {
        using var command = connection.CreateCommand();
        foreach (var statement in new[]
        {
            "ALTER TABLE Jobs ADD COLUMN TenantId TEXT NULL",
            "ALTER TABLE Jobs ADD COLUMN ConcurrencyGroup TEXT NULL",
            "ALTER TABLE Jobs ADD COLUMN MaxConcurrentExecutions INTEGER NULL"
        })
        {
            command.CommandText = statement;
            try
            {
                command.ExecuteNonQuery();
            }
            catch (SqliteException exception) when (exception.SqliteErrorCode == 1 && exception.Message.Contains("duplicate column name", StringComparison.OrdinalIgnoreCase))
            {
            }
        }
    }

    private static async Task<bool> JobExistsAsync(SqliteConnection connection, SqliteTransaction transaction, Guid id, CancellationToken cancellationToken)
    {
        await using var command = connection.CreateCommand();
        command.Transaction = transaction;
        command.CommandText = "SELECT EXISTS(SELECT 1 FROM Jobs WHERE Id = $id)";
        command.Parameters.AddWithValue("$id", id.ToString());
        return Convert.ToInt32(await command.ExecuteScalarAsync(cancellationToken)) == 1;
    }

    private static string GetExecutionJobId(SqliteConnection connection, Guid executionId)
    {
        using var command = connection.CreateCommand();
        command.CommandText = "SELECT JobId FROM JobExecutions WHERE Id = $id";
        command.Parameters.AddWithValue("$id", executionId.ToString());
        return command.ExecuteScalar()?.ToString() ?? string.Empty;
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
        command.Parameters.AddWithValue("$nextExecutionAt", (object?)job.NextExecutionAt?.ToString("O") ?? DBNull.Value);
        command.Parameters.AddWithValue("$tenantId", (object?)job.TenantId ?? DBNull.Value);
        command.Parameters.AddWithValue("$concurrencyGroup", (object?)job.ConcurrencyGroup ?? DBNull.Value);
        command.Parameters.AddWithValue("$maxConcurrentExecutions", (object?)job.MaxConcurrentExecutions ?? DBNull.Value);
    }

    private static JobDefinition ReadJob(SqliteDataReader reader) =>
        new(
            Guid.Parse(reader.GetString(0)),
            reader.GetString(1),
            reader.IsDBNull(2) ? null : reader.GetString(2),
            (JobPriority)reader.GetInt32(3),
            new RetryPolicy(reader.GetInt32(4), reader.GetInt32(5), reader.GetDouble(6)),
            (JobStatus)reader.GetInt32(7),
            reader.IsDBNull(8) ? null : DateTimeOffset.Parse(reader.GetString(8)),
            reader.IsDBNull(9) ? null : reader.GetString(9),
            reader.IsDBNull(10) ? null : reader.GetString(10),
            reader.IsDBNull(11) ? null : reader.GetInt32(11));

    private static JobExecution ReadExecution(SqliteDataReader reader) =>
        new(
            Guid.Parse(reader.GetString(0)),
            Guid.Parse(reader.GetString(1)),
            (JobExecutionStatus)reader.GetInt32(2),
            reader.GetInt32(3),
            reader.IsDBNull(4) ? null : DateTimeOffset.Parse(reader.GetString(4)),
            reader.IsDBNull(5) ? null : DateTimeOffset.Parse(reader.GetString(5)),
            reader.IsDBNull(6) ? null : reader.GetString(6),
            reader.IsDBNull(7) ? null : reader.GetString(7),
            reader.IsDBNull(8) ? null : DateTimeOffset.Parse(reader.GetString(8)));
}
