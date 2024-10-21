using System.Collections.Generic;
using System.Data.Common;
using System.Linq;

namespace SqlBulkMerge;

internal static class DbColumnExt
{
    public static string FromSource(this IEnumerable<DbColumn> columns) => 
        string.Join(", ", columns.Select(x => $"SOURCE.[{x.ColumnName}]"));

    public static string Names(this IEnumerable<DbColumn> columns) => 
        string.Join(", ", columns.Select(c => $"[{c.ColumnName}]"));

    public static string FromSourceToTarget(this IEnumerable<DbColumn> columns) => 
        string.Join(", ", columns.Select(c => $"TARGET.[{c.ColumnName}] = SOURCE.[{c.ColumnName}]"));

    public static string MatchSourceToTarget(this IEnumerable<DbColumn> columns) => 
        string.Join(" AND ", columns.Select(c => $"TARGET.[{c.ColumnName}] = SOURCE.[{c.ColumnName}]"));

    public static IEnumerable<DbColumn> Keys(this IEnumerable<DbColumn> columns) => 
        columns.Where(x => x.IsKey == true);

    public static IEnumerable<DbColumn> Values(this IEnumerable<DbColumn> columns) => 
        columns.Where(x => x.IsKey == false);
}