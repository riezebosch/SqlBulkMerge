using System.Data;
using CommandLine;
using DustInTheWind.ConsoleTools.Controls.Spinners;
using Microsoft.Data.SqlClient;
using Microsoft.Extensions.Logging;
using sql2yaml;

var logger = LoggerFactory
    .Create(options => options.AddSimpleConsole(x => x.SingleLine = true))
    .CreateLogger<Program>();

var parsed = Parser.Default.ParseArguments<Options.ExportOptions, Options.ImportOptions>(args);
await parsed.WithParsedAsync<Options.ExportOptions>(async options =>
{
    await using var connection = new SqlConnection(options.ConnectionString);
    await connection.OpenAsync();
    
    await using var transaction = connection.BeginTransaction();
    var serializer = YamlDotNetDataReader.Factory.Serializer();
    foreach (var table in options.Tables)
    {
        logger.LogInformation("Export {table}", table);
        await Spinner.Run(async () =>
        {
            await using var command = connection.CreateCommand();
            command.Transaction = transaction;
            command.CommandText = $"SELECT * FROM {table}";

            await using var stream = File.Create(Path.Join(options.Directory, $"{table}.yaml"));
            await using var writer = new StreamWriter(stream, leaveOpen: true);

            await using var reader = await command.ExecuteReaderAsync();
            serializer.Serialize(writer, reader);
        });
        
    }
});

await parsed.WithParsedAsync<Options.ImportOptions>(async options =>
{
    await using var connection = new SqlConnection(options.ConnectionString);
    await connection.OpenAsync();
    
    await using var transaction = connection.BeginTransaction();
    await using var command = connection.CreateCommand();
    command.Transaction = transaction;
    
    logger.LogInformation("Disable Foreign Keys"); // https://stackoverflow.com/a/161410/129269
    command.CommandText = "EXEC sp_MSforeachtable 'ALTER TABLE ? NOCHECK CONSTRAINT all'";
    await command.ExecuteNonQueryAsync();

    logger.LogInformation("Disable Triggers"); // https://stackoverflow.com/a/7177806/129269
    command.CommandText = "sp_msforeachtable 'ALTER TABLE ? DISABLE TRIGGER all'";
    await command.ExecuteNonQueryAsync();
    
    var bulk = new SqlBulkMerge.SqlBulk(connection, transaction);
    var serializer = YamlDotNetDataReader.Factory.Deserializer();

    foreach (var file in new DirectoryInfo(options.Directory).EnumerateFiles("*.yaml"))
    {
        var table = Path.GetFileNameWithoutExtension(file.Name);
        logger.LogInformation("Import  {table}", table);

        await Spinner.Run(async () =>
        {
            await using var stream = file.OpenRead();
            using var reader = new StreamReader(stream, leaveOpen: true);

            var data = serializer.Deserialize<IDataReader>(reader);
            await bulk.Upsert(table, options.DeleteUnmatched, async c =>
            {
                for (var i = 0; i < data.FieldCount; i++)
                {
                    var name = data.GetName(i);
                    c.ColumnMappings.Add(name, name);
                }

                await c.WriteToServerAsync(data);
            });
        });
        
    }
    
    logger.LogInformation("Enable Triggers");
    command.CommandText = "sp_msforeachtable 'ALTER TABLE ? ENABLE TRIGGER all'";
    await command.ExecuteNonQueryAsync();

    logger.LogInformation("Enable Foreign Keys");
    command.CommandText = "EXEC sp_MSforeachtable 'ALTER TABLE ? WITH CHECK CHECK CONSTRAINT all'";
    await command.ExecuteNonQueryAsync();

    logger.LogInformation("Transaction commit");
    await transaction.CommitAsync();
});