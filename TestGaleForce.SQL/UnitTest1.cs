using System.Collections.Generic;
using System.Data;
using GaleForce.SQL.SQLServer;
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

        // [TestMethod]
        // public void TestLocalSqlBulkCopy()
        // {
        // SqlConnectionStringBuilder builder = new SqlConnectionStringBuilder();
        // builder.DataSource = "MS-BOSS-JIMGALE\\ML";
        // builder.InitialCatalog = "LOCALTEST";
        // builder.IntegratedSecurity = true;

        // var connection = builder.ConnectionString;

        // var data = LocalTableRecord.GetData();
        // var sql = new SimpleSqlBuilder<LocalTableRecord>(LocalTableRecord.TableName)
        // .Insert(l => l.Id, l => l.Str1, l => l.Int1)
        // .ExecuteBulkCopy(data, connection);
        // }
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
