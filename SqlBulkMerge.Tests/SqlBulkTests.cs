using System.Data;
using FluentAssertions;
using Microsoft.Data.SqlClient;
using Testcontainers.MsSql;

namespace SqlBulkMerge.Tests;

public class SqlBulkTests : IAsyncLifetime
{
    private readonly MsSqlContainer _container = new MsSqlBuilder()
        .WithImage("mcr.microsoft.com/mssql/server:latest")
        .Build();

    [Fact]
    public async Task Update()
    {
        // Arrange
        var table = await CreateTable(_container);
        var insert = await _container.ExecScriptAsync($"INSERT INTO {table} VALUES ('aaa')");
        insert.ExitCode.Should().Be(0);

        await using var connection = new SqlConnection(_container.GetConnectionString());
        await connection.OpenAsync();

        // Act
        using var data = TestData(1, "xxx");
        await new SqlBulk(connection).Upsert(table, false, c => c.WriteToServerAsync(data));

        // Assert
        await using var reader = await Read(connection, table);
        reader.Read().Should().BeTrue();
        reader["Id"].Should().Be(1);
        reader["Data"].Should().Be("xxx");
    }

    [Fact]
    public async Task Insert()
    {
        // Arrange
        var table = await CreateTable(_container);
        var insert = await _container.ExecScriptAsync($"INSERT INTO {table} VALUES ('aaa')");
        insert.ExitCode.Should().Be(0);

        await using var connection = new SqlConnection(_container.GetConnectionString());
        await connection.OpenAsync();
        
        // Act
        using var data = TestData(5, "zzz");
        await new SqlBulk(connection).Upsert(table, false, c => c.WriteToServerAsync(data));
    
        // Assert
        await using var reader = await Read(connection, table);
        reader.Read().Should().BeTrue();
        reader.Read().Should().BeTrue();
        reader["Id"].Should().Be(5);
        reader["Data"].Should().Be("zzz");
    }
    
    [Fact]
    public async Task Delete()
    {
        // Arrange
        var table = await CreateTable(_container);
        var insert = await _container.ExecScriptAsync($"INSERT INTO {table} VALUES ('aaa')");
        insert.ExitCode.Should().Be(0);

        await using var connection = new SqlConnection(_container.GetConnectionString());
        await connection.OpenAsync();

        // Act
        using var data = new DataTable();
        await new SqlBulk(connection).Upsert(table, true, c => c.WriteToServerAsync(data));

        // Assert
        await using var reader = await Read(connection, table);
        reader.Read().Should().BeFalse();
    }
    
    [Fact]
    public async Task TableNoIdentity()
    {
        // Arrange
        const int id = 2;
        var table = await CreateTableNoIdentity(_container);
        
        await using var connection = new SqlConnection(_container.GetConnectionString());
        await connection.OpenAsync();
        
        // Act
        using var data = TestData(id, "yyy");
        await new SqlBulk(connection).Upsert($"{table}", false, c => c.WriteToServerAsync(data));
    
        // Assert
        await using var reader = await Read(connection, table);
        reader.Read().Should().BeTrue();
        reader["Id"].Should().Be(id);
        reader["Data"].Should().Be("yyy");
    }

    private static async Task<string> CreateTable(MsSqlContainer connection)
    {
        const string table = "demo";
        await connection.ExecScriptAsync($"""
                                          CREATE TABLE {table} (
                                            Id int NOT NULL PRIMARY KEY IDENTITY(1, 1),
                                            Data varchar(255)
                                          );
                                          """);

        return table;
    }

    private static async Task<string> CreateTableNoIdentity(MsSqlContainer connection)
    {
        const string table = "demo";
        await connection.ExecScriptAsync($"""
                                          CREATE TABLE {table} (
                                            Id int NOT NULL PRIMARY KEY,
                                            Data varchar(255)
                                          );
                                          """);

        return table;
    }

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

    private static async Task<SqlDataReader> Read(SqlConnection connection, string table)
    {
        await using var command = connection.CreateCommand();
        command.CommandText = $"SELECT * FROM {table}";

        return await command.ExecuteReaderAsync();
    }

    async Task IAsyncLifetime.InitializeAsync() => 
        await _container.StartAsync();

    Task IAsyncLifetime.DisposeAsync() => _container.DisposeAsync().AsTask();
}