using System;
using System.Threading.Tasks;
using Microsoft.Data.SqlClient;

namespace SqlBulkMerge;

public class SqlBulk(SqlConnection connection, SqlTransaction? transaction = null)
{
    public  async Task Upsert(string table, Func<SqlBulkCopy, Task> action)
    {
        var temp = await connection.TemporaryTableFrom(table, transaction);
        
        using var copy = new SqlBulkCopy(connection, SqlBulkCopyOptions.KeepIdentity, transaction);
        copy.DestinationTableName = temp;
        await action(copy);

        await connection.Merge(table, temp, transaction);
    }
}