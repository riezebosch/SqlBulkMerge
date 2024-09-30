using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.Threading.Tasks;

namespace SqlBulkMerge;

internal static class DbConnectionExt
{
    public static async Task<string> TemporaryTableFrom(this DbConnection connection, string table, DbTransaction? transaction)
    {
        var temp = $"#{table}";
        await using var command = connection.CreateCommand($"SELECT TOP 0 * INTO {temp} FROM {table}", transaction);
        await command.ExecuteNonQueryAsync();
        
        return temp;
    }

    public static async Task Merge(this DbConnection connection, string target, string source, DbTransaction? transaction)
    {
        var columns = await connection.Schema(target, transaction);
        await using var command = connection.CreateCommand($"""
                               SET IDENTITY_INSERT {target} ON
                               MERGE {target} AS TARGET
                               USING {source} as SOURCE
                               ON ({columns.Keys().MatchSourceToTarget()}) 
                               WHEN MATCHED THEN UPDATE SET {columns.Values().FromSourceToTarget()}
                               WHEN NOT MATCHED THEN INSERT ({columns.Names()}) VALUES ({columns.FromSource()});
                               SET IDENTITY_INSERT {target} OFF
                               """, transaction);

        await command.ExecuteNonQueryAsync();
    }

    private static async Task<ICollection<DbColumn>> Schema(this DbConnection connection, string table, DbTransaction? transaction)
    {
        await using var command = connection.CreateCommand($"SELECT TOP 0 * FROM {table}", transaction);
        await using var reader = await command.ExecuteReaderAsync(CommandBehavior.KeyInfo);
        return reader.GetColumnSchema();
    }

    private static DbCommand CreateCommand(this DbConnection connection, string text, DbTransaction? transaction)
    {
        var command = connection.CreateCommand();
        command.CommandText = text;
        command.Transaction = transaction;
        return command;
    }
}