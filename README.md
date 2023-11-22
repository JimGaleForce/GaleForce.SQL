# GaleForce SQL Library

GaleForce SQL is a library that simplifies working with SQL databases in C#. With this library, you can easily perform basic CRUD operations and build complex SQL queries using a straightforward and intuitive syntax.

## Installation

To use the GaleForce SQL library, simply add it as a dependency to your C# project.

## Basic Usage

### Reading Data

Here's how you can read data from a table:

```csharp
var context = new SimpleSqlBuilderContext();

var source = LocalTableRecord.GetData();
context.SetTable(LocalTableRecord.TableName, source);

// Selecting specific columns
var data = new SimpleSqlBuilder<LocalTableRecord>(LocalTableRecord.TableName)
    .Select(l => l.Id, l => l.Str1, l => l.Int1)
    .Execute(context)
    .ToList();
```

### Inserting Data

To insert data into a table, you can use the following code:

```csharp
var target = new List<LocalTableRecord>();
var context = new SimpleSqlBuilderContext();
context.SetTable(LocalTableRecord.TableName, target);

var data = LocalTableRecord.GetData();
var sql = await new SimpleSqlBuilder<LocalTableRecord>(LocalTableRecord.TableName)
    .Insert(data, l => l.Id, l => l.Str1, l => l.Int1)
    .ExecuteNonQueryAsync(context);
```

### Updating Data

To update data in a table, use the following code:

```csharp
var target = LocalTableRecord.GetData();
var context = new SimpleSqlBuilderContext();
context.SetTable(LocalTableRecord.TableName, target);

var data = LocalTableRecord.GetData();
for (var i = 0; i < data.Count; i++)
{
    data[i].Int1 = 1000 + i;
}

var sql = await new SimpleSqlBuilder<LocalTableRecord>(LocalTableRecord.TableName)
    .Match(l => l.Id)
    .Update(data, l => l.Id, l => l.Str1, l => l.Int1)
    .ExecuteNonQueryAsync(context);
```

### Merging Data

To merge data from one table into another, you can use the following code:

```csharp
var target = LocalTableRecord.GetData();
target.RemoveAt(2);

var context = new SimpleSqlBuilderContext();
context.SetTable(LocalTableRecord.TableName, target);

var source = LocalTableRecord.GetData();
for (var i = 0; i < source.Count; i++)
{
    source[i].Int1 = 1000 + i;
}

context.SetTable(LocalTableRecord.TableName + "_temp", source as IEnumerable<LocalTableRecord>);

var sql = await new SimpleSqlBuilder<LocalTableRecord>(LocalTableRecord.TableName + "_temp")
    .MergeInto(LocalTableRecord.TableName)
    .Match(l => l.Id)
    .WhenMatched(s => s.Update(l => l.Id, l => l.Str1, l => l.Int1))
    .WhenNotMatched(s => s.Insert(l => l.Id, l => l.Str1, l => l.Int1))
    .ExecuteNonQueryAsync(context);
```

## Bulk Copy

The GaleForce SQL library also supports bulk copy operations, which can significantly improve the performance of inserting large amounts of data.

To perform a bulk copy operation, use the following code:

```csharp
var connection = LocalConnection();
var context = new SimpleSqlBuilderContext(connection);

var data = LocalTableRecord.GetData();
var sql = await new SimpleSqlBuilder<LocalTableRecord>(LocalTableRecord.TableName)
    .Insert(data, l => l.Id, l => l.Str1, l => l.Int1)
    .UseBulkCopy(context);
```

```csharp
var connection = LocalConnection();
var data = LocalTableRecord.GetData();
var sql = await new SimpleSqlBuilder<LocalTableRecord>(LocalTableRecord.TableName)
    .Insert(data, l => l.Id, l => l.Str1, l => l.Int1)
    .ExecuteBulkCopy(connection);
```

## Complex Usages

GaleForce SQL library allows you to build complex queries with ease. You can join multiple tables, perform subqueries, and apply various conditions using a simple and intuitive syntax.

Here is an example of a complex query:

```csharp
var context = new SimpleSqlBuilderContext();

var source = LocalTableRecord.GetData();
context.SetTable(LocalTableRecord.TableName, source);

var source2 = LocalTableRecord.GetData();
source2[0].Int1 = 100;
context.SetTable(LocalTableRecord.TableName + "2", source2);

var source3 = LocalTableRecord.GetData();
source3[0].Int1 = 200;
context.SetTable(LocalTableRecord.TableName + "3", source3);

var data = new SimpleSqlBuilder<LocalTableRecord, LocalTableRecord, LocalTableRecord, LocalTableRecord>()
    .From(LocalTableRecord.TableName, LocalTableRecord.TableName + "2", LocalTableRecord.TableName + "3")
    .InnerJoin12On((a, b) => a.Id == b.Id)
    .InnerJoin13On((a, b) => a.Id == b.Id)
    .Select((a, b, c) => a.Id, (a, b, c) => b.Str1, (a, b, c) => c.Int1)
    .Execute(context)
    .ToList();
```

## Tracing

You can enable tracing for your queries by setting the `IsTracing` property of the `SimpleSqlBuilderContext` to `true`. This can be useful for debugging and performance analysis.

Here is an example of how to enable tracing:

```csharp
var context = new SimpleSqlBuilderContext();
context.IsTracing = true;

// Your query here

Assert.IsTrue(context.StageLogger.Collector.Items[1].Item.Message.Length > 0);
```

## Conclusion

The GaleForce SQL library provides a powerful and easy-to-use interface for working with SQL databases in C#. With its intuitive syntax and comprehensive feature set, you can build complex queries, perform CRUD operations, and manage your data effectively.
