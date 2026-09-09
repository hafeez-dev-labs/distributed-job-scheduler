using DistributedJobScheduler.Domain;
using Xunit;

namespace DistributedJobScheduler.UnitTests;

public sealed class JobDefinitionTests
{
    [Fact]
    public void NewJobHasExpectedIdentityAndRetryPolicy()
    {
        var job = new JobDefinition(Guid.NewGuid(), "GenerateMonthlyReport", "0 0 1 * *", JobPriority.Normal, new RetryPolicy(5));
        Assert.Equal("GenerateMonthlyReport", job.Name);
        Assert.Equal(5, job.RetryPolicy.MaxAttempts);
        Assert.Equal(JobStatus.Draft, job.Status);
    }
}
