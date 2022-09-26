using System;
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
    }

    public class TestRecord<T>
    {
        public T Value { get; set; }
    }
}
