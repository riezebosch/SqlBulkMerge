using System.Data;
using System.Globalization;
using CommandLine;
using Microsoft.Data.SqlClient;
using Microsoft.Extensions.Logging;
using NaturalSort.Extension;
using sql2yaml;

CultureInfo.CurrentCulture = CultureInfo.InvariantCulture;

var logger = LoggerFactory
    .Create(static builder => builder
        .AddFilter("Program", LogLevel.Debug)
        .AddSimpleConsole())
    .CreateLogger<Program>();

try
{
    await Parser
        .Default
        .ParseArguments<Options.ExportOptions, Options.ImportOptions>(args)
        .WithParsedAsync<Options.ExportOptions>(options => Export(options, logger))
        .WithParsedAsync<Options.ImportOptions>(options => Import(options, logger));
}
catch (Exception ex)
{
    logger.LogError(ex, "Unhandled exception occurred.");
    return 1;
}

return 0;

static async Task Import(Options.ImportOptions options, ILogger<Program> logger)
{
    logger.LogDebug("Connection string: {Directory}", options.ConnectionString);
    logger.LogDebug("Directory: {Directory}", options.Directory);
    logger.LogDebug("Delete unmatched: {DeleteUnmatched}", options.DeleteUnmatched);
    logger.LogDebug("Pre import scripts: {Scripts}", options.PreImportScripts);
    logger.LogDebug("Post import scripts: {Scripts}", options.PostImportScripts);
    
    await using var connection = new SqlConnection(options.ConnectionString);
    connection.InfoMessage += (_, e) => logger.LogDebug("{Source}: {Message}", e.Source, e.Message);
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

    await ExecuteScripts(connection, transaction, options.PreImportScripts, logger);

    var bulk = new SqlBulkMerge.SqlBulk(connection, transaction);
    var serializer = YamlDotNetDataReader.Factory.Deserializer();

    foreach (var file in new DirectoryInfo(options.Directory).EnumerateFiles("*.yaml"))
    {
        var table = Path.GetFileNameWithoutExtension(file.Name);
        logger.LogInformation("Import  {table}", table);

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
    }

    await ExecuteScripts(connection, transaction, options.PostImportScripts, logger);

    logger.LogInformation("Enable Triggers");
    command.CommandText = "sp_msforeachtable 'ALTER TABLE ? ENABLE TRIGGER all'";
    await command.ExecuteNonQueryAsync();

    logger.LogInformation("Enable Foreign Keys");
    command.CommandText = "EXEC sp_MSforeachtable 'ALTER TABLE ? WITH CHECK CHECK CONSTRAINT all'";
    await command.ExecuteNonQueryAsync();

    logger.LogInformation("Transaction commit");
    await transaction.CommitAsync();
}

static async Task Export(Options.ExportOptions options, ILogger<Program> logger)
{
    logger.LogDebug("Connection string: {Directory}", options.ConnectionString);
    logger.LogDebug("Directory: {Directory}", options.Directory);
    logger.LogDebug("Tables: {Tables}", options.Tables);
    logger.LogDebug("Pre export scripts: {Scripts}", options.PreExportScripts);
    logger.LogDebug("Post export scripts: {Scripts}", options.PostExportScripts);
    
    await using var connection = new SqlConnection(options.ConnectionString);
    connection.InfoMessage += (_, e) => logger.LogDebug("{Source}: {Message}", e.Source, e.Message);
    await connection.OpenAsync();
    
    await using var transaction = connection.BeginTransaction();
    await ExecuteScripts(connection, transaction, options.PreExportScripts, logger);
    
    var serializer = YamlDotNetDataReader.Factory.Serializer();
    foreach (var table in options.Tables)
    {
        logger.LogInformation("Export {table}", table);

        await using var command = connection.CreateCommand();
        command.Transaction = transaction;
        command.CommandText = $"SELECT * FROM {table}";

        await using var stream = File.Create(Path.Join(options.Directory, $"{table}.yaml"));
        await using var writer = new StreamWriter(stream, leaveOpen: true);

        await using var reader = await command.ExecuteReaderAsync();
        serializer.Serialize(writer, reader);
    }
    
    await ExecuteScripts(connection, transaction, options.PostExportScripts, logger);
}

static async Task ExecuteScript(SqlConnection connection, SqlTransaction transaction, string path,
    ILogger<Program> logger)
{
    logger.LogDebug("Execute script {path}", path);
    await using var command = connection.CreateCommand();
    command.Transaction = transaction;
    command.CommandTimeout = 300;
    command.CommandText = File.ReadAllText(path);
    
    await command.ExecuteNonQueryAsync();
}

static async Task ExecuteScripts(SqlConnection connection, SqlTransaction transaction, IEnumerable<string> scripts, ILogger<Program> logger)
{
    foreach (var script in scripts)
    {
        if (File.Exists(script))
        {
            await ExecuteScript(connection, transaction, script, logger);
        }
        else if (Directory.Exists(script))
        {
            foreach (var data in Directory.EnumerateFiles(script, "*.sql").Order(StringComparer.OrdinalIgnoreCase.WithNaturalSort()))
            {
                await ExecuteScript(connection, transaction, data, logger);
            }
        }
        else
        {
            logger.LogError("File or directory does not exist: {script}", script);
        }
    }
}