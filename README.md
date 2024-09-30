# SqlBulkMerge

```csharp
await new SqlBulk(connection).Upsert(Table, c => c.WriteToServerAsync(data));
```

The upsert redirect `SqlBulkCopy` into a temporary table and merges the results into the target table.