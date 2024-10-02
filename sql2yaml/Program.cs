using System.Data;
using CommandLine;
using Microsoft.Data.SqlClient;
using sql2yaml;

var parsed = Parser.Default.ParseArguments<Options.ExportOptions, Options.ImportOptions>(args);
await parsed.WithParsedAsync<Options.ExportOptions>(async options =>
{
    await using var connection = new SqlConnection(options.ConnectionString);
    await using var transaction = connection.BeginTransaction();

    var serializer = YamlDotNetDataReader.Factory.Serializer().Build();
    foreach (var table in options.Tables)
    {
        await using var command = connection.CreateCommand();
        command.Transaction = transaction;
        command.CommandText = $"SELECT * FROM {table}";

        await using var stream = File.Create(Path.Join(options.Directory, $"{table}.yaml"));
        await using var writer = new StreamWriter(stream, leaveOpen: true);

        using var reader = command.ExecuteReaderAsync();
        serializer.Serialize(writer, reader);
    }
});

await parsed.WithParsedAsync<Options.ImportOptions>(async options =>
{
    await using var connection = new SqlConnection(options.ConnectionString);
    await using var transaction = connection.BeginTransaction();

    var bulk = new SqlBulkMerge.SqlBulk(connection, transaction);
    var serializer = YamlDotNetDataReader.Factory.Deserializer().Build();
    
    foreach (var file in new DirectoryInfo(options.Directory).EnumerateFiles("*.yaml"))
    {
        await using var stream = file.OpenRead();
        using var reader = new StreamReader(stream, leaveOpen: true);

        var data = serializer.Deserialize<IDataReader>(reader);
        await bulk.Upsert(Path.GetFileNameWithoutExtension(file.Name), c => c.WriteToServerAsync(data));
    }
});