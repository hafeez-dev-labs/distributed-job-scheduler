using DistributedJobScheduler.Domain;
namespace DistributedJobScheduler.Application;
public static class ReliabilityPolicy
{
    public static bool ShouldDeadLetter(int attempt, RetryPolicy policy) => attempt >= Math.Max(1, policy.MaxAttempts);
    public static TimeSpan GetBackoff(int attempt, RetryPolicy policy)
    {
        var normalizedAttempt = Math.Max(1, attempt);
        var seconds = policy.InitialDelaySeconds * Math.Pow(policy.BackoffMultiplier, normalizedAttempt - 1);
        return TimeSpan.FromSeconds(Math.Min(seconds, TimeSpan.FromHours(1).TotalSeconds));
    }
}
