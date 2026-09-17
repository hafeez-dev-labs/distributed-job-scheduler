using Microsoft.Data.Sqlite;
using DistributedJobScheduler.Application;
using DistributedJobScheduler.Domain;

namespace DistributedJobScheduler.Infrastructure;

public sealed class SqliteJobQueue : IJobQueue
{
    private readonly string connectionString;

    public SqliteJobQueue(string connectionString)
    {
        this.connectionString = connectionString;
        Initialize();
    }

    public async Task EnqueueAsync(JobExecution execution, JobPriority priority, DateTimeOffset availableAt, CancellationToken cancellationToken = default)
    {
        await using var connection = OpenConnection();
        await using var command = connection.CreateCommand();
        command.CommandText = "INSERT OR IGNORE INTO QueueMessages (Id, ExecutionId, JobId, Priority, AvailableAt, LeasedUntil, LeaseOwner) VALUES ($id, $executionId, $jobId, $priority, $availableAt, NULL, NULL)";
        command.Parameters.AddWithValue("$id", Guid.NewGuid().ToString());
        command.Parameters.AddWithValue("$executionId", execution.Id.ToString());
        command.Parameters.AddWithValue("$jobId", execution.JobId.ToString());
        command.Parameters.AddWithValue("$priority", (int)priority);
        command.Parameters.AddWithValue("$availableAt", availableAt.ToString("O"));
        await command.ExecuteNonQueryAsync(cancellationToken);
    }

    public async Task<QueueMessage?> TryDequeueAsync(string consumer, DateTimeOffset now, TimeSpan visibilityTimeout, CancellationToken cancellationToken = default)
    {
        if (string.IsNullOrWhiteSpace(consumer))
            throw new ArgumentException("Consumer is required.", nameof(consumer));

        await using var connection = OpenConnection();
        await using var transaction = await connection.BeginTransactionAsync(cancellationToken);
        var nowText = now.ToString("O");
        var leaseUntil = now.Add(visibilityTimeout).ToString("O");

        string? messageId;
        await using (var select = connection.CreateCommand())
        {
            select.Transaction = transaction;
            select.CommandText = "SELECT Id FROM QueueMessages WHERE AvailableAt <= $now AND (LeasedUntil IS NULL OR LeasedUntil <= $now) ORDER BY Priority DESC, AvailableAt ASC, Id ASC LIMIT 1";
            select.Parameters.AddWithValue("$now", nowText);
            messageId = await select.ExecuteScalarAsync(cancellationToken) as string;
        }

        if (messageId is null)
        {
            await transaction.CommitAsync(cancellationToken);
            return null;
        }

        await using (var claim = connection.CreateCommand())
        {
            claim.Transaction = transaction;
            claim.CommandText = "UPDATE QueueMessages SET LeaseOwner = $owner, LeasedUntil = $leaseUntil WHERE Id = $id AND (LeasedUntil IS NULL OR LeasedUntil <= $now)";
            claim.Parameters.AddWithValue("$owner", consumer);
            claim.Parameters.AddWithValue("$leaseUntil", leaseUntil);
            claim.Parameters.AddWithValue("$id", messageId);
            claim.Parameters.AddWithValue("$now", nowText);
            if (await claim.ExecuteNonQueryAsync(cancellationToken) != 1)
            {
                await transaction.RollbackAsync(cancellationToken);
                return null;
            }
        }

        QueueMessage message;
        await using (var read = connection.CreateCommand())
        {
            read.Transaction = transaction;
            read.CommandText = "SELECT Id, ExecutionId, JobId, Priority, AvailableAt, LeasedUntil, LeaseOwner FROM QueueMessages WHERE Id = $id";
            read.Parameters.AddWithValue("$id", messageId);
            await using var reader = await read.ExecuteReaderAsync(cancellationToken);
            if (!await reader.ReadAsync(cancellationToken))
            {
                await transaction.RollbackAsync(cancellationToken);
                return null;
            }

            message = ReadMessage(reader);
        }

        await transaction.CommitAsync(cancellationToken);
        return message;
    }

    public async Task<bool> AcknowledgeAsync(Guid messageId, string consumer, CancellationToken cancellationToken = default)
    {
        await using var connection = OpenConnection();
        await using var command = connection.CreateCommand();
        command.CommandText = "DELETE FROM QueueMessages WHERE Id = $id AND LeaseOwner = $owner";
        command.Parameters.AddWithValue("$id", messageId.ToString());
        command.Parameters.AddWithValue("$owner", consumer);
        if (await command.ExecuteNonQueryAsync(cancellationToken) == 1)
            return true;

        await using var exists = connection.CreateCommand();
        exists.CommandText = "SELECT COUNT(1) FROM QueueMessages WHERE Id = $id";
        exists.Parameters.AddWithValue("$id", messageId.ToString());
        return Convert.ToInt32(await exists.ExecuteScalarAsync(cancellationToken)) == 0;
    }

    public async Task<bool> RequeueAsync(Guid messageId, string consumer, CancellationToken cancellationToken = default)
    {
        await using var connection = OpenConnection();
        await using var command = connection.CreateCommand();
        command.CommandText = "UPDATE QueueMessages SET LeaseOwner = NULL, LeasedUntil = NULL WHERE Id = $id AND LeaseOwner = $owner";
        command.Parameters.AddWithValue("$id", messageId.ToString());
        command.Parameters.AddWithValue("$owner", consumer);
        return await command.ExecuteNonQueryAsync(cancellationToken) == 1;
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
            CREATE TABLE IF NOT EXISTS QueueMessages (
                Id TEXT PRIMARY KEY,
                ExecutionId TEXT NOT NULL UNIQUE,
                JobId TEXT NOT NULL,
                Priority INTEGER NOT NULL,
                AvailableAt TEXT NOT NULL,
                LeasedUntil TEXT NULL,
                LeaseOwner TEXT NULL
            );
            CREATE INDEX IF NOT EXISTS IX_QueueMessages_Availability ON QueueMessages(Priority DESC, AvailableAt, LeasedUntil);
            """;
        command.ExecuteNonQuery();
    }

    private static QueueMessage ReadMessage(SqliteDataReader reader) =>
        new(
            Guid.Parse(reader.GetString(0)),
            Guid.Parse(reader.GetString(1)),
            Guid.Parse(reader.GetString(2)),
            (JobPriority)reader.GetInt32(3),
            DateTimeOffset.Parse(reader.GetString(4)),
            reader.IsDBNull(5) ? null : DateTimeOffset.Parse(reader.GetString(5)),
            reader.IsDBNull(6) ? null : reader.GetString(6));
}
