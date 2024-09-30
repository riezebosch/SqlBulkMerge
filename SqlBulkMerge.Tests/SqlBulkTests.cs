using System.Data;
using FluentAssertions;
using Microsoft.Data.SqlClient;
using Testcontainers.MsSql;

namespace SqlBulkMerge.Tests;

public class SqlBulkTests : IAsyncLifetime
{
    private const string Table = "Demo";

    private readonly MsSqlContainer _container = new MsSqlBuilder()
        .WithImage("mcr.microsoft.com/mssql/server:latest")
        .Build();

    [Fact]
    public async Task Update()
    {
        // Arrange
        const int id = 5;
        await _container.ExecScriptAsync($"INSERT INTO {Table} VALUES ({id}, 'aaa')");

        await using var connection = new SqlConnection(_container.GetConnectionString());
        await connection.OpenAsync();

        // Act
        using var data = TestData(id, "xxx");
        await new SqlBulk(connection).Upsert(Table, c => c.WriteToServerAsync(data));

        // Assert
        await using var reader = await Read(connection);
        reader.Read().Should().BeTrue();
        reader["Id"].Should().Be(id);
        reader["Data"].Should().Be("xxx");
    }

    [Fact]
    public async Task Insert()
    {
        // Arrange
        const int id = 2;
        await using var connection = new SqlConnection(_container.GetConnectionString());
        await connection.OpenAsync();

        // Act
        using var data = TestData(id, "yyy");
        await new SqlBulk(connection).Upsert(Table, c => c.WriteToServerAsync(data));
    
        // Assert
        await using var reader = await Read(connection);
        reader.Read().Should().BeTrue();
        reader["Id"].Should().Be(id);
        reader["Data"].Should().Be("yyy");
    }

    private static async Task CreateTable(MsSqlContainer connection) =>
        await connection.ExecScriptAsync($"""
                                         CREATE TABLE {Table} (
                                           Id int NOT NULL PRIMARY KEY IDENTITY(1, 1),
                                           Data varchar(255)
                                         );
                                         """);

    private static DataTable TestData(int id, string data)
    {
        var table = new DataTable();
        table.Columns.AddRange([
            new DataColumn("Id"),
            new DataColumn("Data")
        ]);
        table.Rows.Add(id, data);
        return table;
    }

    private static async Task<SqlDataReader> Read(SqlConnection connection)
    {
        await using var command = connection.CreateCommand();
        command.CommandText = $"SELECT * FROM {Table}";

        return await command.ExecuteReaderAsync();
    }

    async Task IAsyncLifetime.InitializeAsync()
    {
         await _container.StartAsync();
         await CreateTable(_container);
    }

    Task IAsyncLifetime.DisposeAsync() => _container.DisposeAsync().AsTask();
}