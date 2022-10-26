using System;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;
using System.Linq;
using System.Reflection;
using System.Threading.Tasks;
using GaleForceCore.Builders;
using GaleForceCore.Helpers;

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

        /// <summary>
        /// Executes the a bulk copy on insert fields.
        /// </summary>
        /// <typeparam name="TRecord">The type of the t record.</typeparam>
        /// <param name="ssBuilder">The ss builder.</param>
        /// <param name="source">The source dataset/records.</param>
        /// <param name="connection">The connection.</param>
        /// <param name="bulkSize">Size of the bulk.</param>
        /// <param name="retries">The retries.</param>
        /// <param name="timeoutInSeconds">The timeout in seconds (per copy).</param>
        /// <returns>System.Int32.</returns>
        public static async Task<int> ExecuteBulkCopy<TRecord>(
            this SimpleSqlBuilder<TRecord> ssBuilder,
            IEnumerable<TRecord> source,
            string connection,
            int bulkSize = 50000,
            int retries = 3,
            int timeoutInSeconds = 600)
        {
            var sql = ssBuilder.Build(source);
            var fields = ssBuilder.Inserts;
            var type = typeof(TRecord);
            var props = type.GetProperties();

            var dt = new DataTable();
            var fieldProps = new List<PropertyInfo>();

            var hasValues = ssBuilder.Valueset.Count() > 0;
            var values = hasValues ? ssBuilder.ValueExpressions.Select(v => v.Compile()).ToList() : null;

            foreach (var originalField in fields)
            {
                var field = originalField.Contains(".")
                    ? originalField.Substring(originalField.IndexOf(".") + 1)
                    : originalField;
                var property = props.FirstOrDefault(p => p.Name == field);
                if (property != null)
                {
                    Type propertyType = property.PropertyType;
                    if (propertyType.IsGenericType &&
                        propertyType.GetGenericTypeDefinition() == typeof(Nullable<>))
                    {
                        propertyType = Nullable.GetUnderlyingType(propertyType);
                    }

                    dt.Columns.Add(new DataColumn(property.Name, propertyType));
                    fieldProps.Add(property);
                }
            }

            var groupedData = source.ToArray().Split(bulkSize);

            var count = 0;
            using (var destConn = new SqlConnection(connection))
            {
                destConn.Open();
                var bulkCopy = new SqlBulkCopy(destConn)
                {
                    DestinationTableName = ssBuilder.TableName,
                    BulkCopyTimeout = timeoutInSeconds
                };

                foreach (var group in groupedData)
                {
                    dt.Rows.Clear();
                    foreach (var record in group)
                    {
                        var columns = new List<object>();
                        var fieldIndex = 0;
                        foreach (var fp in fieldProps)
                        {
                            var value = (hasValues ? values[fieldIndex].Invoke(record) : fp.GetValue(record, null)) ??
                                DBNull.Value;
                            columns.Add(value);
                        }

                        dt.Rows.Add(columns.ToArray());
                    }

                    var retryIndex = 0;
                    while (retryIndex < retries)
                    {
                        try
                        {
                            await bulkCopy.WriteToServerAsync(dt);
                            break;
                        }
                        catch
                        {
                            if (++retryIndex >= retries)
                            {
                                throw;
                            }
                        }
                    }

                    count += dt.Rows.Count;
                }

                destConn.Close();
            }

            return count;
        }
    }
}
