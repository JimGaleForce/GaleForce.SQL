using System;
using System.Collections.Generic;
using System.Linq;
using GaleForceCore.Builders;

namespace GaleForce.SQL.SQLServer
{
    /// <summary>
    /// Class SimpleSqlUtils.
    /// </summary>
    public static class SimpleSqlUtils
    {
        /// <summary>
        /// Executes SQL on a SimpleSqlBuilder, using a connection.
        /// </summary>
        /// <typeparam name="TRecord">The type of the record.</typeparam>
        /// <param name="ssBuilder">The SimpleSqlBuilder builder.</param>
        /// <param name="connection">The connection.</param>
        /// <returns>IEnumerable&lt;TRecord&gt;.</returns>
        public static IEnumerable<TRecord> ExecuteSQL<TRecord>(
            this SimpleSqlBuilder<TRecord> ssBuilder,
            string connection)
        {
            var sql = ssBuilder.Build();
            var records = Utils.SqlCommandToLinq(sql, connection);
            var results = new List<TRecord>();

            var type = typeof(TRecord);
            var props = type.GetProperties();
            var fields = ssBuilder.Fields;

            foreach (var record in records)
            {
                var newRecord = (TRecord)Activator.CreateInstance(type);
                foreach (var originalField in fields)
                {
                    var field = originalField.Contains(".")
                        ? originalField.Substring(originalField.IndexOf(".") + 1)
                        : originalField;
                    var prop = props.FirstOrDefault(p => p.Name == field);
                    Utils.SetValue<TRecord>(record, newRecord, prop);
                }

                results.Add(newRecord);
            }

            return results;
        }

        /// <summary>
        /// Executes SQL on a SimpleSqlBuilder, using a connection.
        /// </summary>
        /// <typeparam name="TRecord">The type of the record.</typeparam>
        /// <param name="ssBuilder">The SimpleSqlBuilder builder.</param>
        /// <param name="connection">The connection.</param>
        /// <returns>IEnumerable&lt;TRecord&gt;.</returns>
        public static int ExecuteSQL<TRecord>(
            this SimpleSqlBuilder<TRecord> ssBuilder,
            IEnumerable<TRecord> source,
            string connection)
        {
            var sql = ssBuilder.Build(source);
            return Utils.SqlExecute(sql, connection);
        }
    }
}
