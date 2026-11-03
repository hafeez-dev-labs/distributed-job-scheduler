using System.Net;
using Microsoft.AspNetCore.Mvc.Testing;
using Microsoft.Extensions.Configuration;
using Xunit;

namespace DistributedJobScheduler.IntegrationTests;

public sealed class ApiRateLimitingTests
{
    [Fact]
    public async Task RequestsBeyondConfiguredLimit_Return429WithRetryAfter()
    {
        using var factory = new WebApplicationFactory<Program>().WithWebHostBuilder(builder =>
            builder.ConfigureAppConfiguration((_, configuration) =>
                configuration.AddInMemoryCollection(new Dictionary<string, string?>
                {
                    ["RateLimiting:PermitLimit"] = "1",
                    ["RateLimiting:WindowSeconds"] = "30",
                    ["ConnectionStrings:Scheduler"] = "Data Source=:memory:"
                })));

        using var client = factory.CreateClient();
        var first = await client.GetAsync("/health");
        var second = await client.GetAsync("/health");

        Assert.Equal(HttpStatusCode.OK, first.StatusCode);
        Assert.Equal(HttpStatusCode.TooManyRequests, second.StatusCode);
        Assert.Equal("30", second.Headers.GetValues("Retry-After").Single());
    }
}
