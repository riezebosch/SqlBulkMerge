using DotNet.Testcontainers.Containers;

namespace SqlBulkMerge.Tests;

public static class TestContainersExt
{
    public static async Task<ExecResult> ThrowOnError(this Task<ExecResult> task)
    {
        var result = await task;
        if (result.ExitCode != 0)
        {
            throw new Exception(result.Stderr);
        }
        return result;
    }
}