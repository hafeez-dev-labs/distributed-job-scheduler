using DistributedJobScheduler.Application;
using DistributedJobScheduler.Domain;
using Xunit;
namespace DistributedJobScheduler.UnitTests;
public sealed class ReliabilityPolicyTests
{
    [Fact] public void UsesExponentialBackoff()
    {
        var policy = new RetryPolicy(5, 2, 2);
        Assert.Equal(TimeSpan.FromSeconds(2), ReliabilityPolicy.GetBackoff(1, policy));
        Assert.Equal(TimeSpan.FromSeconds(4), ReliabilityPolicy.GetBackoff(2, policy));
        Assert.Equal(TimeSpan.FromSeconds(8), ReliabilityPolicy.GetBackoff(3, policy));
    }
    [Fact] public void DeadLettersAtConfiguredMaximumAttempt()
    {
        var policy = new RetryPolicy(3, 1, 2);
        Assert.False(ReliabilityPolicy.ShouldDeadLetter(2, policy));
        Assert.True(ReliabilityPolicy.ShouldDeadLetter(3, policy));
    }
}
