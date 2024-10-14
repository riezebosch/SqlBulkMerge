using System;
using System.Threading.Tasks;
using Microsoft.Data.SqlClient;

namespace SqlBulkMerge;

public class SqlBulk(SqlConnection connection, SqlTransaction? transaction = null)
{
    public  async Task Upsert(string table, bool delete, Func<SqlBulkCopy, Task> action)
    {
        var temp = await connection.TemporaryTableFrom(table, transaction);
        
        using var copy = new SqlBulkCopy(connection, SqlBulkCopyOptions.KeepIdentity, transaction);
        copy.DestinationTableName = temp;
        await action(copy);

        var columns = await connection.Schema(table, transaction);
        if (delete)
        {
            // delete first to prevent from unique constraint violations on indexes that are not part of the primary key
            await connection.Delete(table, temp, transaction, columns);
        }
        
        await connection.Merge(table, temp, transaction, columns);
    }
}