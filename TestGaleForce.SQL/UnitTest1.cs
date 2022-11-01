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
            builder.DataSource = "MACHINE\\DATASOURCE";
            builder.InitialCatalog = "LOCALTEST";
            builder.IntegratedSecurity = true;

            return builder.ConnectionString;
        }

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
                .ExecuteNonQuery(context);

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
                .ExecuteNonQuery(context);

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
                .ExecuteNonQuery(context);

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
                .ExecuteNonQuery(context);

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

        public async Task TestMergeCommand()
        {
            var context = new SimpleSqlBuilderContext();

            var source = LocalTableRecord.GetData();
            context.SetTable(LocalTableRecord.TableName, source);

            context.SetTable("Destination", new List<LocalTableRecord>());

            var updatedLocalRecords = await new SimpleSqlBuilder<LocalTableRecord>(LocalTableRecord.TableName)
                .MergeInto("Destination")
                .Match((t, p) => t.Id == p.Id)
                .WhenMatched(
                    s => s.Update(p => p.Str1))
                .ExecuteNonQuery(context);

            Assert.AreEqual(0, updatedLocalRecords); //empty data set, nothing to update.
        }
    }

    public class TestRecord<T>
    {
        public T Value { get; set; }
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
            recs.Add(
                new LocalTableRecord
                {
                    Id = 1,
                    Str1 = "Str1a",
                    Int1 = 101,
                    Bool1 = true,
                    Str2 = "Str2a"
                });
            recs.Add(
                new LocalTableRecord
                {
                    Id = 2,
                    Str1 = "Str1b",
                    Int1 = 102,
                    Bool1 = false,
                    Str2 = "Str2b"
                });
            recs.Add(
                new LocalTableRecord
                {
                    Id = 3,
                    Str1 = "Str1c",
                    Int1 = 103,
                    Bool1 = true,
                    Str2 = "Str2c"
                });
            return recs;
        }
    }
}
