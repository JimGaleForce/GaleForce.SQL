using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;
using System.Linq;
using System.Threading.Tasks;
using GaleForce.SQL.SQLServer;
using GaleForceCore.Builders;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace TestGaleForce.SQL
{
    using GaleForceCore.Logger;

    [TestClass]
    public class UnitTest1
    {
        private T TestValue<T>(T value)
        {
            var table = new DataTable();
            table.Columns.Add(new DataColumn { ColumnName = "Value", DataType = typeof(T) });
            var datarow = table.NewRow();
            datarow["Value"] = value;
            var testRecord = new TestRecord<T>();
            Utils.SetValue(datarow, testRecord, testRecord.GetType().GetProperty("Value"));
            return testRecord.Value;
        }

        [TestMethod]
        public void TestSetValueDecimal()
        {
            Assert.AreEqual((decimal) 123.4, TestValue<decimal>((decimal) 123.4), "Decimal not interpreted properly");
        }

        [TestMethod]
        public void TestSetValueFloat()
        {
            Assert.AreEqual((float) 123.4, TestValue<float>((float)123.4), "Float not interpreted properly");
        }

        [TestMethod]
        public void TestSetValueInt()
        {
            Assert.AreEqual(123, TestValue<int>(123), "Int not interpreted properly");
        }

        [TestMethod]
        public void TestSetValueString()
        {
            Assert.AreEqual("123", TestValue<string>("123"), "String not interpreted properly");
        }

        [TestMethod]
        public void TestSetValueBool()
        {
            Assert.AreEqual(true, TestValue<bool>(true), "Bool not interpreted properly");
        }

        private string LocalConnection()
        {
            SqlConnectionStringBuilder builder = new SqlConnectionStringBuilder();
            builder.DataSource = "MACHINENAME\\DATASOURCE";
            builder.InitialCatalog = "LOCALTEST";
            builder.IntegratedSecurity = true;

            return builder.ConnectionString;
        }

        // [TestMethod]
        // public void TestAllAsFields()
        // {
        // var connection = this.LocalConnection();
        // var context = new SimpleSqlBuilderContext(connection);

        // var actualx = new SimpleSqlBuilder<LocalNewTableRecord, LocalTableRecord, LocalTableRecord>()
        // .From(LocalTableRecord.TableName, LocalTableRecord.TableName)
        // .SelectAs(tu => tu.StringX, (t1, t3) => t3.Str1)
        // .SelectAs(tu => tu.IntX, (t1, t3) => t1.Int1)
        // .InnerJoinOn((t1, t3) => t1.Int1 == t3.Int1)
        // .Where((t1, t3) => t1.Int1 % 2 == 1);

        // var build = actualx.Build();

        // var actual = actualx.Execute(context);

        // var i = 0;
        // }

        // [TestMethod]
        // public void TestLocalParams()
        // {
        // var connection = this.LocalConnection();
        // var context = new SimpleSqlBuilderContext(connection);
        // var options = new SimpleSqlBuilderOptions { UseParameters = true };

        // var sql = new SimpleSqlBuilder<LocalTableRecord>(options, LocalTableRecord.TableName)
        // .Select(l => l.Str1, l => l.Int1)
        // .Where(l => l.Str2 == "four" || l.Str2 == "five")
        // .Execute(context);
        // }

        // [TestMethod]
        // public async Task TestLocalSqlBulkCopy()
        // {
        // var connection = LocalConnection();

        // var data = LocalTableRecord.GetData();
        // var sql = await new SimpleSqlBuilder<LocalTableRecord>(LocalTableRecord.TableName)
        // .Insert(data, l => l.Id, l => l.Str1, l => l.Int1)
        // .ExecuteBulkCopy(connection);
        // }

        // [TestMethod]
        // public async Task TestLocalSqlBulkCopyFromGeneric()
        // {

        // var connection = LocalConnection();

        // var context = new SimpleSqlBuilderContext(connection);

        // var data = LocalTableRecord.GetData();
        // var sql = await new SimpleSqlBuilder<LocalTableRecord>(LocalTableRecord.TableName)
        // .Insert(data, l => l.Id, l => l.Str1, l => l.Int1)
        // .UseBulkCopy()
        // .ExecuteNonQuery(context);
        // }

        // [TestMethod]
        // public async Task TestLocalSqlBulkCopyFromGenericTempTableInsert()
        // {
        // var connection = this.LocalConnection();

        // var context = new SimpleSqlBuilderContext(connection);

        // var data = LocalTableRecordExact.GetData();
        // var sql = await new SimpleSqlBuilder<LocalTableRecordExact>(LocalTableRecord.TableName)
        // .Insert(data, l => l.Id, l => l.Str1, l => l.Int1, l => l.Int3)
        // .UseTempTable()
        // .ExecuteNonQuery(context);
        // }

        // [TestMethod]
        // public async Task TestLocalSqlBulkCopyFromGenericTempTableUpdate()
        // {
        // var connection = this.LocalConnection();

        // var context = new SimpleSqlBuilderContext(connection);

        // var data = LocalTableRecordExact.GetData();
        // var sql = await new SimpleSqlBuilder<LocalTableRecordExact>(LocalTableRecord.TableName)
        // .Update(data, l => l.Id, l => l.Str1, l => l.Int1, l => l.Int3)
        // .Match((s, t) => s.Id == t.Id)
        // .UseMinTempTable()
        // .ExecuteNonQuery(context);
        // }

        [TestMethod]
        public async Task TestInsertFromGenericBulkCopyTempTable()
        {
            var target = new List<LocalTableRecord>();
            var context = new SimpleSqlBuilderContext();
            context.SetTable(LocalTableRecord.TableName, target);

            var data = LocalTableRecord.GetData();
            var sql = await new SimpleSqlBuilder<LocalTableRecord>(LocalTableRecord.TableName)
                .Insert(data, l => l.Id, l => l.Str1, l => l.Int1)
                .UseTempTable()
                .ExecuteNonQueryAsync(context);

            Assert.AreEqual(3, target.Count);
            Assert.AreEqual(data[0].Id, target[0].Id);
            Assert.AreEqual(data[1].Id, target[1].Id);
            Assert.AreEqual(data[2].Id, target[2].Id);
            Assert.AreNotEqual(data[0].Str2, target[0].Str2);
            Assert.AreNotEqual(data[1].Str2, target[1].Str2);
            Assert.AreNotEqual(data[2].Str2, target[2].Str2);
        }

        [TestMethod]
        public async Task TestInsertFromGenericBulkCopy()
        {
            var target = new List<LocalTableRecord>();
            var context = new SimpleSqlBuilderContext();
            context.SetTable(LocalTableRecord.TableName, target);

            var data = LocalTableRecord.GetData();
            var sql = await new SimpleSqlBuilder<LocalTableRecord>(LocalTableRecord.TableName)
                .Insert(data, l => l.Id, l => l.Str1, l => l.Int1)
                .UseBulkCopy()
                .ExecuteNonQueryAsync(context);

            Assert.AreEqual(3, target.Count);
            Assert.AreEqual(data[0].Id, target[0].Id);
            Assert.AreEqual(data[1].Id, target[1].Id);
            Assert.AreEqual(data[2].Id, target[2].Id);
            Assert.AreNotEqual(data[0].Str2, target[0].Str2);
            Assert.AreNotEqual(data[1].Str2, target[1].Str2);
            Assert.AreNotEqual(data[2].Str2, target[2].Str2);
        }

        [TestMethod]
        public async Task TestInsertFromGeneric()
        {
            var target = new List<LocalTableRecord>();
            var context = new SimpleSqlBuilderContext();
            context.SetTable(LocalTableRecord.TableName, target);

            var data = LocalTableRecord.GetData();
            var sql = await new SimpleSqlBuilder<LocalTableRecord>(LocalTableRecord.TableName)
                .Insert(data, l => l.Id, l => l.Str1, l => l.Int1)
                .ExecuteNonQueryAsync(context);

            Assert.AreEqual(3, target.Count);
            Assert.AreEqual(data[0].Id, target[0].Id);
            Assert.AreEqual(data[1].Id, target[1].Id);
            Assert.AreEqual(data[2].Id, target[2].Id);
            Assert.AreNotEqual(data[0].Str2, target[0].Str2);
            Assert.AreNotEqual(data[1].Str2, target[1].Str2);
            Assert.AreNotEqual(data[2].Str2, target[2].Str2);
        }

        [TestMethod]
        public async Task TestUpdateFromGeneric()
        {
            var target = LocalTableRecord.GetData();
            var context = new SimpleSqlBuilderContext();
            context.SetTable(LocalTableRecord.TableName, target);

            var data = LocalTableRecord.GetData();
            for (var i = 0; i < data.Count; i++)
            {
                data[i].Int1 = 1000 + i;
            }

            Assert.AreNotEqual(data[0].Int1, target[0].Int1);
            Assert.AreNotEqual(data[1].Int1, target[1].Int1);
            Assert.AreNotEqual(data[2].Int1, target[2].Int1);

            var sql = await new SimpleSqlBuilder<LocalTableRecord>(LocalTableRecord.TableName)
                .Match(l => l.Id)
                .Update(data, l => l.Id, l => l.Str1, l => l.Int1)
                .ExecuteNonQueryAsync(context);

            Assert.AreEqual(3, target.Count);
            Assert.AreEqual(data[0].Int1, target[0].Int1);
            Assert.AreEqual(data[1].Int1, target[1].Int1);
            Assert.AreEqual(data[2].Int1, target[2].Int1);
        }

        [TestMethod]
        public async Task TestMergeFromGeneric()
        {
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

            var targetSorted = target.OrderBy(t => t.Int1).ToList();

            Assert.AreEqual(3, target.Count);
            Assert.AreEqual(source[0].Int1, targetSorted[0].Int1);
            Assert.AreEqual(source[1].Int1, targetSorted[1].Int1);
            Assert.AreEqual(source[2].Int1, targetSorted[2].Int1);
        }

        [TestMethod]
        public void TestSelectExecute1()
        {
            var context = new SimpleSqlBuilderContext();

            var source = LocalTableRecord.GetData();
            context.SetTable(LocalTableRecord.TableName, source);

            var data = new SimpleSqlBuilder<LocalTableRecord>(LocalTableRecord.TableName)
                .Select(l => l.Id, l => l.Str1, l => l.Int1)
                .Execute(context)
                .ToList();

            Assert.AreEqual(3, data.Count());
            Assert.AreEqual(1, data[0].Id);
            Assert.AreEqual(2, data[1].Id);
            Assert.AreEqual(null, data[0].Str2);
        }

        [TestMethod]
        public void TestSelectExecute2()
        {
            var context = new SimpleSqlBuilderContext();

            var source = LocalTableRecord.GetData();
            context.SetTable(LocalTableRecord.TableName, source);

            var source2 = LocalTableRecord.GetData();
            source2[0].Int1 = 100;
            context.SetTable(LocalTableRecord.TableName + "2", source2);

            var data = new SimpleSqlBuilder<LocalTableRecord, LocalTableRecord, LocalTableRecord>()
                .From(LocalTableRecord.TableName, LocalTableRecord.TableName + "2")
                .InnerJoinOn((a, b) => a.Id == b.Id)
                .Select((a, b) => a.Id, (a, b) => b.Str1, (a, b) => b.Int1)
                .Execute(context)
                .ToList();

            Assert.AreEqual(3, data.Count());
            Assert.AreEqual(1, data[0].Id);
            Assert.AreEqual(2, data[1].Id);
            Assert.AreEqual(100, data[0].Int1);
            Assert.AreEqual(null, data[0].Str2);
        }

        [TestMethod]
        public void TestSelectExecute3()
        {
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

            Assert.AreEqual(3, data.Count());
            Assert.AreEqual(1, data[0].Id);
            Assert.AreEqual(2, data[1].Id);
            Assert.AreEqual(200, data[0].Int1);
            Assert.AreEqual(null, data[0].Str2);
        }

        [TestMethod]
        public void TestSelectExecute4()
        {
            var context = new SimpleSqlBuilderContext();

            var source = LocalTableRecord.GetData();
            context.SetTable(LocalTableRecord.TableName, source);

            var source2 = LocalTableRecord.GetData();
            source2[0].Int1 = 100;
            context.SetTable(LocalTableRecord.TableName + "2", source2);

            var source3 = LocalTableRecord.GetData();
            source3[0].Int1 = 200;
            context.SetTable(LocalTableRecord.TableName + "3", source3);

            var source4 = LocalTableRecord.GetData();
            source4[0].Int1 = 300;
            context.SetTable(LocalTableRecord.TableName + "4", source4);

            var data = new SimpleSqlBuilder<LocalTableRecord, LocalTableRecord, LocalTableRecord, LocalTableRecord, LocalTableRecord>()
                .From(
                    LocalTableRecord.TableName,
                    LocalTableRecord.TableName + "2",
                    LocalTableRecord.TableName + "3",
                    LocalTableRecord.TableName + "4")
                .InnerJoin12On((a, b) => a.Id == b.Id)
                .InnerJoin13On((a, b) => a.Id == b.Id)
                .InnerJoin14On((a, b) => a.Id == b.Id)
                .Select((a, b, c, d) => a.Id, (a, b, c, d) => b.Str1, (a, b, c, d) => d.Int1)
                .Execute(context)
                .ToList();

            Assert.AreEqual(3, data.Count());
            Assert.AreEqual(1, data[0].Id);
            Assert.AreEqual(2, data[1].Id);
            Assert.AreEqual(300, data[0].Int1);
            Assert.AreEqual(null, data[0].Str2);
        }

        [TestMethod]

        public async Task TestMergeCommand()
        {
            var context = new SimpleSqlBuilderContext();

            var source = LocalTableRecord.GetData();
            context.SetTable(LocalTableRecord.TableName, source);

            context.SetTable("Destination", new List<LocalTableRecord>());

            var updatedLocalRecords = await new SimpleSqlBuilder<LocalTableRecord>(LocalTableRecord.TableName)
                .MergeInto("Destination")
                .Match((t, p) => t.Id == p.Id)
                .WhenMatched(s => s.Update(p => p.Str1))
                .ExecuteNonQueryAsync(context);

            Assert.AreEqual(0, updatedLocalRecords); //empty data set, nothing to update.
        }

        [TestMethod]
        public void InsertFromSelectAllExecute()
        {
            var context = new SimpleSqlBuilderContext();
            context.IsTesting = true;
            var target = new List<LocalTableRecord>();
            context.SetTable(LocalTableRecord.TableName, target);
            context.SetTable("Temp" + LocalTableRecord.TableName, LocalTableRecord.GetData());

            // var target = new List<LocalTableRecord>();
            // var sources = new Dictionary<string, SourceData>();
            // sources["Temp" + LocalTableRecord.TableName] = SourceData.Create(
            // "Temp" + LocalTableRecord.TableName,
            // source);

            var mergeInsert = new SimpleSqlBuilder<LocalTableRecord>(LocalTableRecord.TableName)
                .Insert(s => s.From("Temp" + LocalTableRecord.TableName).Select())
                .ExecuteNonQueryAsync(context);

            Assert.AreEqual(3, target.Count());
        }

        // [TestMethod]
        public void TestSelectToSubClass()
        {
            var connection = LocalConnection();
            var context = new SimpleSqlBuilderContext(connection);

            var list = new SimpleSqlBuilder<LocalTableWithEnumsRecord>(LocalTableWithEnumsRecord.TableName)
            .Select()
                .Execute(context)
                .ToList();

            Assert.IsTrue(list[0].Int1 != null);
        }

        [TestMethod]
        public void TestSelectExecute3Tracing()
        {
            var context = new SimpleSqlBuilderContext();
            context.StageLogger = new StageLogger().AddCollector();

            var source = LocalTableRecord.GetData();
            context.SetTable(LocalTableRecord.TableName, source);

            var source2 = LocalTableRecord.GetData();
            source2[0].Int1 = 100;
            context.SetTable(LocalTableRecord.TableName + "2", source2);

            var source3 = LocalTableRecord.GetData();
            source3[0].Int1 = 200;
            context.SetTable(LocalTableRecord.TableName + "3", source3);
            context.IsTracing = true;

            var data = new SimpleSqlBuilder<LocalTableRecord, LocalTableRecord, LocalTableRecord, LocalTableRecord>()
                .From(LocalTableRecord.TableName, LocalTableRecord.TableName + "2", LocalTableRecord.TableName + "3")
                .InnerJoin12On((a, b) => a.Id == b.Id)
                .InnerJoin13On((a, b) => a.Id == b.Id)
                .Select((a, b, c) => a.Id, (a, b, c) => b.Str1, (a, b, c) => c.Int1)
                .Execute(context)
                .ToList();

            Assert.IsTrue(context.StageLogger.Collector.Items[1].Item.Message.Length > 0);
        }
    }

    public class TestRecord<T>
    {
        public T Value { get; set; }
    }

    public class LocalNewTableRecord
    {
        public string StringX { get; set; }

        public int IntX { get; set; }
    }

    public class LocalTableRecordSubclass : LocalTableRecord
    {
        [IgnoreField]
        public string SomethingElse { get; set; }
    }

    public class LocalTableWithEnumsRecord
    {
        public const string TableName = "LocalTable";

        public int Id { get; set; }

        public string Str1 { get; set; }

        public AnEnum Int1 { get; set; }

        public bool? Bool1 { get; set; }

        public AnEnum Str2 { get; set; }

        public int? Int2 { get; set; }

        public TokenRecordType Int3 { get; set; }
    }

    public enum TokenRecordType
    {
        /// <summary>
        /// Used for uninitialized variables or unknown types.
        /// </summary>
        Undefined,

        /// <summary>
        /// OCV Issue.
        /// </summary>
        Issue = 1,

        /// <summary>
        /// User feedback item.
        /// </summary>
        Feedback = 2,

        /// <summary>
        /// Area Path information.
        /// </summary>
        Area = 3
    }

    public enum AnEnum
    {
        None,
        One = 1,
        Two = 2,
        Three = 3,
        Four = 4
    }

    public class LocalTableRecordExact
    {
        public int Id { get; set; }

        public string Str1 { get; set; }

        public int? Int1 { get; set; }

        public bool? Bool1 { get; set; }

        public string Str2 { get; set; }

        public int? Int2 { get; set; }

        public int Int3 { get; set; }

        public static List<LocalTableRecordExact> GetData()
        {
            var recs = new List<LocalTableRecordExact>();
            recs.Add(new LocalTableRecordExact { Id = 1, Str1 = "Str1a", Int1 = 101, Bool1 = true, Str2 = "Str2a" });
            recs.Add(new LocalTableRecordExact { Id = 2, Str1 = "Str1b", Int1 = 102, Bool1 = false, Str2 = "Str2b" });
            recs.Add(new LocalTableRecordExact { Id = 3, Str1 = "Str1c", Int1 = 103, Bool1 = true, Str2 = "Str2c" });
            return recs;
        }
    }

    public class LocalTableRecord
    {
        public const string TableName = "LocalTable";

        public int Id { get; set; }

        public string Str1 { get; set; }

        public int? Int1 { get; set; }

        public bool? Bool1 { get; set; }

        public string Str2 { get; set; }

        public int? Int2 { get; set; }

        public static List<LocalTableRecord> GetData()
        {
            var recs = new List<LocalTableRecord>();
            recs.Add(new LocalTableRecord { Id = 1, Str1 = "Str1a", Int1 = 101, Bool1 = true, Str2 = "Str2a" });
            recs.Add(new LocalTableRecord { Id = 2, Str1 = "Str1b", Int1 = 102, Bool1 = false, Str2 = "Str2b" });
            recs.Add(new LocalTableRecord { Id = 3, Str1 = "Str1c", Int1 = 103, Bool1 = true, Str2 = "Str2c" });
            return recs;
        }
    }
}
