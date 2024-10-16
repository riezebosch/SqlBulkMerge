using CommandLine;

namespace sql2yaml;

internal static class CommandlineExt
{
    public static async Task<ParserResult<object>> WithParsedAsync<T>(this Task<ParserResult<object>> task, Func<T, Task> action)
    {
        var result = await task;
        return await result.WithParsedAsync(action);
    }
}