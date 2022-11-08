//-----------------------------------------------------------------------
// <copyright file="SimpleSqlUtils.cs" company="Gale-Force, LLC">
// Copyright (C) Gale-Force, LLC. All rights reserved.
// </copyright>
// -----------------------------------------------------------------------
using System;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;
using System.Linq;
using System.Reflection;
using System.Threading.Tasks;
using GaleForceCore.Builders;
using GaleForceCore.Helpers;
using GaleForceCore.Logger;

namespace GaleForce.SQL.SQLServer
{
    /// <summary>
    /// Class SimpleSqlUtils.
    /// </summary>
    public static class SimpleSqlUtils
    {
        /// <summary>
        /// Executes the specified context, whether local testing data, or actual SQL connection.
        /// </summary>
        /// <typeparam name="TRecord">The type of the record being accessed.</typeparam>
        /// <param name="ssBuilder">The simple SQL builder.</param>
        /// <param name="context">The context.</param>
        /// <returns>IEnumerable&lt;TRecord&gt;.</returns>
        public static IEnumerable<TRecord> Execute<TRecord>(
            this SimpleSqlBuilder<TRecord> ssBuilder,
            SimpleSqlBuilderContext context,
            StageLogger log = null)
        {
            log = log ?? context.StageLogger;
            if (context.IsTesting)
            {
                var tableName = ssBuilder.TableName;
                var data = context.GetTable<TRecord>(tableName);
                if (data != null)
                {
                    var sql = ssBuilder.Build();
                    using (var sqllog = log?.Item("sql.select." + sql.GetHashCode(), "SQL"))
                    {
                        sqllog?.AddEvent("SQL", sql);
                        var result = ssBuilder.Execute(data);
                        sqllog?.AddMetric("SQLCount", result.Count());
                        return result;
                    }
                }
                else
                {
                    throw new MissingDataTableException(
                        $"{tableName} needs to exist for testing in SimpleSqlBuilder");
                }
            }
            else
            {
                return ssBuilder.ExecuteSQL(context.Connection, log: log);
            }
        }

        /// <summary>
        /// Executes the specified context.
        /// </summary>
        /// <typeparam name="TRecord">The type of the t record.</typeparam>
        /// <typeparam name="TRecord1">The type of the t record1.</typeparam>
        /// <typeparam name="TRecord2">The type of the t record2.</typeparam>
        /// <param name="ssBuilder">The ss builder.</param>
        /// <param name="context">The context.</param>
        /// <returns>IEnumerable&lt;TRecord&gt;.</returns>
        public static IEnumerable<TRecord> Execute<TRecord, TRecord1, TRecord2>(
            this SimpleSqlBuilder<TRecord, TRecord1, TRecord2> ssBuilder,
            SimpleSqlBuilderContext context,
            StageLogger log = null)
        {
            log = log ?? context.StageLogger;
            if (context.IsTesting)
            {
                if (ssBuilder.TableNames.Length > 1)
                {
                    var data1 = context.GetTable<TRecord1>(ssBuilder.TableNames[0]) ??
                        context.GetTable<TRecord1>(ssBuilder.TableNamesActual[0]);
                    var data2 = context.GetTable<TRecord2>(ssBuilder.TableNames[1]) ??
                        context.GetTable<TRecord2>(ssBuilder.TableNamesActual[1]);

                    if (data1 == null)
                    {
                        throw new MissingDataTableException(
                            $"{ssBuilder.TableNames[0]} needs to exist for testing in SimpleSqlBuilder");
                    }

                    if (data2 == null)
                    {
                        throw new MissingDataTableException(
                            $"{ssBuilder.TableNames[1]} needs to exist for testing in SimpleSqlBuilder");
                    }

                    var sql = ssBuilder.Build();
                    using (var sqllog = log?.Item("sql.select." + sql.GetHashCode(), "SQL"))
                    {
                        sqllog?.AddEvent("SQL", sql);
                        var result = ssBuilder.Execute(data1, data2);
                        sqllog?.AddMetric("SQLCount", result.Count());
                        return result;
                    }
                }

                throw new MissingDataTableException(
                    $"2 From tables are required for testing in a 2 source-table SimpleSqlBuilder<T,T1,T2>");
            }
            else
            {
                return ssBuilder.ExecuteSQL(context.Connection, log: log);
            }
        }

        /// <summary>
        /// Executes the specified context.
        /// </summary>
        /// <typeparam name="TRecord">The type of the t record.</typeparam>
        /// <typeparam name="TRecord1">The type of the t record1.</typeparam>
        /// <typeparam name="TRecord2">The type of the t record2.</typeparam>
        /// <param name="ssBuilder">The ss builder.</param>
        /// <param name="context">The context.</param>
        /// <returns>IEnumerable&lt;TRecord&gt;.</returns>
        public static IEnumerable<TRecord> Execute<TRecord, TRecord1, TRecord2, TRecord3>(
            this SimpleSqlBuilder<TRecord, TRecord1, TRecord2, TRecord3> ssBuilder,
            SimpleSqlBuilderContext context,
            StageLogger log = null)
        {
            log = log ?? context.StageLogger;
            if (context.IsTesting)
            {
                if (ssBuilder.TableNames.Length > 2)
                {
                    var data1 = context.GetTable<TRecord1>(ssBuilder.TableNames[0]) ??
                        context.GetTable<TRecord1>(ssBuilder.TableNamesActual[0]);
                    var data2 = context.GetTable<TRecord2>(ssBuilder.TableNames[1]) ??
                        context.GetTable<TRecord2>(ssBuilder.TableNamesActual[1]);
                    var data3 = context.GetTable<TRecord3>(ssBuilder.TableNames[2]) ??
                        context.GetTable<TRecord3>(ssBuilder.TableNamesActual[2]);

                    if (data1 == null)
                    {
                        throw new MissingDataTableException(
                            $"{ssBuilder.TableNames[0]} needs to exist for testing in SimpleSqlBuilder");
                    }

                    if (data2 == null)
                    {
                        throw new MissingDataTableException(
                            $"{ssBuilder.TableNames[1]} needs to exist for testing in SimpleSqlBuilder");
                    }

                    if (data3 == null)
                    {
                        throw new MissingDataTableException(
                            $"{ssBuilder.TableNames[2]} needs to exist for testing in SimpleSqlBuilder");
                    }

                    var sql = ssBuilder.Build();
                    using (var sqllog = log?.Item("sql.select." + sql.GetHashCode(), "SQL"))
                    {
                        sqllog?.AddEvent("SQL", sql);
                        var result = ssBuilder.Execute(data1, data2, data3);
                        sqllog?.AddMetric("SQLCount", result.Count());
                        return result;
                    }
                }

                throw new MissingDataTableException(
                    $"3 From tables are required for testing in a 3 source-table SimpleSqlBuilder<T,T1,T2>");
            }
            else
            {
                return ssBuilder.ExecuteSQL(context.Connection, log: log);
            }
        }

        /// <summary>
        /// Executes the specified context.
        /// </summary>
        /// <typeparam name="TRecord">The type of the t record.</typeparam>
        /// <typeparam name="TRecord1">The type of the t record1.</typeparam>
        /// <typeparam name="TRecord2">The type of the t record2.</typeparam>
        /// <param name="ssBuilder">The ss builder.</param>
        /// <param name="context">The context.</param>
        /// <returns>IEnumerable&lt;TRecord&gt;.</returns>
        public static IEnumerable<TRecord> Execute<TRecord, TRecord1, TRecord2, TRecord3, TRecord4>(
            this SimpleSqlBuilder<TRecord, TRecord1, TRecord2, TRecord3, TRecord4> ssBuilder,
            SimpleSqlBuilderContext context,
            StageLogger log = null)
        {
            log = log ?? context.StageLogger;
            if (context.IsTesting)
            {
                if (ssBuilder.TableNames.Length > 3)
                {
                    var data1 = context.GetTable<TRecord1>(ssBuilder.TableNames[0]) ??
                        context.GetTable<TRecord1>(ssBuilder.TableNamesActual[0]);
                    var data2 = context.GetTable<TRecord2>(ssBuilder.TableNames[1]) ??
                        context.GetTable<TRecord2>(ssBuilder.TableNamesActual[1]);
                    var data3 = context.GetTable<TRecord3>(ssBuilder.TableNames[2]) ??
                        context.GetTable<TRecord3>(ssBuilder.TableNamesActual[2]);
                    var data4 = context.GetTable<TRecord4>(ssBuilder.TableNames[3]) ??
                        context.GetTable<TRecord4>(ssBuilder.TableNamesActual[3]);

                    if (data1 == null)
                    {
                        throw new MissingDataTableException(
                            $"{ssBuilder.TableNames[0]} needs to exist for testing in SimpleSqlBuilder");
                    }

                    if (data2 == null)
                    {
                        throw new MissingDataTableException(
                            $"{ssBuilder.TableNames[1]} needs to exist for testing in SimpleSqlBuilder");
                    }

                    if (data3 == null)
                    {
                        throw new MissingDataTableException(
                            $"{ssBuilder.TableNames[2]} needs to exist for testing in SimpleSqlBuilder");
                    }

                    if (data4 == null)
                    {
                        throw new MissingDataTableException(
                            $"{ssBuilder.TableNames[3]} needs to exist for testing in SimpleSqlBuilder");
                    }

                    var sql = ssBuilder.Build();
                    using (var sqllog = log?.Item("sql.select." + sql.GetHashCode(), "SQL"))
                    {
                        sqllog?.AddEvent("SQL", sql);
                        var result = ssBuilder.Execute(data1, data2, data3, data4);
                        sqllog?.AddMetric("SQLCount", result.Count());
                        return result;
                    }
                }

                throw new MissingDataTableException(
                    $"4 From tables are required for testing in a 4 source-table SimpleSqlBuilder<T,T1,T2>");
            }
            else
            {
                return ssBuilder.ExecuteSQL(context.Connection, log: log);
            }
        }

        /// <summary>
        /// Executes the non query.
        /// </summary>
        /// <typeparam name="TRecord">The type of the t record.</typeparam>
        /// <param name="ssBuilder">The ss builder.</param>
        /// <param name="context">The context.</param>
        /// <returns>System.Int32.</returns>
        /// <exception cref="GaleForce.SQL.SQLServer.MissingDataTableException"></exception>
        public static async Task<int> ExecuteNonQuery<TRecord>(
            this SimpleSqlBuilder<TRecord> ssBuilder,
            SimpleSqlBuilderContext context,
            StageLogger log = null)
        {
            log = log ?? context.StageLogger;
            if (context.IsTesting)
            {
                var data = context.GetList<TRecord>(ssBuilder.TableName);
                if (data != null)
                {
                    IEnumerable<TRecord> source = null;
                    if (ssBuilder.Command == "MERGE")
                    {
                        source = data;
                        data = context.GetList<TRecord>(ssBuilder.MergeIntoTableName);

                        if (data == null)
                        {
                            throw new MissingDataTableException(
                                $"{ssBuilder.MergeIntoTableName} needs to exist as a List<{typeof(TRecord).Name}> to merge into for testing in SimpleSqlBuilder");
                        }
                    }

                    var sql = ssBuilder.Build();
                    using (var sqllog = log?.Item("sql." + ssBuilder.Command + "." + sql.GetHashCode(), "SQL"))
                    {
                        sqllog?.AddEvent("SQL", sql);
                        var result = ssBuilder.ExecuteNonQuery(data, source);
                        sqllog?.AddMetric("SQLCount", result);
                        return result;
                    }
                }
                else
                {
                    throw new MissingDataTableException(
                        $"{ssBuilder.TableName} needs to exist for testing in SimpleSqlBuilder");
                }
            }
            else
            {
                return await ssBuilder.ExecuteSQLNonQuery(context.Connection, log: log);
            }
        }

        /// <summary>
        /// Uses the bulk copy.
        /// </summary>
        /// <typeparam name="TRecord">The type of the t record.</typeparam>
        /// <param name="ssBuilder">The ss builder.</param>
        /// <param name="useBulkCopy">if set to <c>true</c> [use bulk copy].</param>
        /// <returns>SimpleSqlBuilder&lt;TRecord&gt;.</returns>
        public static SimpleSqlBuilder<TRecord> UseBulkCopy<TRecord>(
            this SimpleSqlBuilder<TRecord> ssBuilder,
            bool useBulkCopy = true)
        {
            ssBuilder.Metadata["UseBulkCopy"] = useBulkCopy;
            return ssBuilder;
        }

        /// <summary>
        /// Executes SQL on a SimpleSqlBuilder, using a connection.
        /// </summary>
        /// <typeparam name="TRecord">The type of the record.</typeparam>
        /// <param name="ssBuilder">The SimpleSqlBuilder builder.</param>
        /// <param name="connection">The connection.</param>
        /// <returns>IEnumerable&lt;TRecord&gt;.</returns>
        public static IEnumerable<TRecord> ExecuteSQL<TRecord>(
            this SimpleSqlBuilder<TRecord> ssBuilder,
            string connection,
            StageLogger log = null)
        {
            var sql = ssBuilder.Build();
            using (var sqllog = log?.Item("sql.select." + sql.GetHashCode(), "SQL"))
            {
                sqllog?.AddEvent("SQL", sql);
                var records = Utils.SqlCommandToLinq(
                    sql,
                    connection,
                    parameters: ssBuilder.Options.UseParameters ? ssBuilder.GetParameters() : null);
                var results = new List<TRecord>();

                var type = typeof(TRecord);
                var props = ssBuilder.GetNonIgnoreProperties<TRecord>();
                var fields = ssBuilder.Fields;

                if (fields.Count == 0)
                {
                    fields = ssBuilder.FieldList(ssBuilder.Fields);
                }

                foreach (var record in records)
                {
                    var newRecord = (TRecord)Activator.CreateInstance(type);
                    foreach (var originalField in fields)
                    {
                        var field = originalField.Contains(".")
                            ? originalField.Substring(originalField.IndexOf(".") + 1)
                            : originalField;
                        var asField = field.Contains(" AS ") ? field.Substring(field.IndexOf(" AS ") + 4) : field;

                        var prop = props.FirstOrDefault(p => p.Name == asField);
                        Utils.SetValue<TRecord>(record, newRecord, prop);
                    }

                    results.Add(newRecord);
                }

                sqllog?.AddMetric("SQLCount", results.Count());
                return results;
            }
        }

        /// <summary>
        /// Executes SQL on a SimpleSqlBuilder, using a connection.
        /// </summary>
        /// <typeparam name="TRecord">The type of the record.</typeparam>
        /// <param name="ssBuilder">The SimpleSqlBuilder builder.</param>
        /// <param name="connection">The connection.</param>
        /// <returns>IEnumerable&lt;TRecord&gt;.</returns>
        public static async Task<int> ExecuteSQLNonQuery<TRecord>(
            this SimpleSqlBuilder<TRecord> ssBuilder,
            string connection,
            StageLogger log = null)
        {
            if (ssBuilder.Command == "INSERT"
                &&
                ssBuilder.Metadata.ContainsKey("UseBulkCopy")
                &&
                (bool) ssBuilder.Metadata["UseBulkCopy"])
            {
                int bulkSize = ssBuilder.Metadata.ContainsKey("BulkCopySize")
                    ? (int) ssBuilder.Metadata["BulkCopySize"]
                    : 50000;
                return await ssBuilder.ExecuteBulkCopy(connection, log: log, bulkSize: bulkSize);
            }
            else
            {
                var sql = ssBuilder.Build();
                using (var sqllog = log?.Item("sql." + ssBuilder.Command + "." + sql.GetHashCode(), "SQL"))
                {
                    sqllog?.AddEvent("SQL", sql);
                    if (!string.IsNullOrEmpty(sql))
                    {
                        var result = await Utils.SqlExecuteNonQuery(
                            sql,
                            connection,
                            parameters: ssBuilder.Options.UseParameters ? ssBuilder.GetParameters() : null);
                        sqllog?.AddMetric("SQLCount", result);
                        return result;
                    }

                    return 0;
                }
            }
        }

        /// <summary>
        /// Executes the a bulk copy on insert fields.
        /// </summary>
        /// <typeparam name="TRecord">The type of the t record.</typeparam>
        /// <param name="ssBuilder">The ss builder.</param>
        /// <param name="connection">The connection.</param>
        /// <param name="bulkSize">Size of the bulk.</param>
        /// <param name="retries">The retries.</param>
        /// <param name="timeoutInSeconds">The timeout in seconds (per copy).</param>
        /// <returns>System.Int32.</returns>
        public static async Task<int> ExecuteBulkCopy<TRecord>(
            this SimpleSqlBuilder<TRecord> ssBuilder,
            string connection,
            int bulkSize = 50000,
            int retries = 3,
            int timeoutInSeconds = 600,
            StageLogger log = null)
        {
            var sql = "--bulkcopy:" + ssBuilder.SourceData.GetHashCode();
            using (var sqllog = log?.Item("sql.bulkcopy." + sql.GetHashCode(), "SQL"))
            {
                sqllog?.AddEvent("SQL", sql);
                var fields = ssBuilder.Inserts;

                if (fields.Count == 0)
                {
                    // fields = typeof(TRecord).GetProperties().Select(p => p.Name).ToList();
                    fields = ssBuilder.FieldList(ssBuilder.Fields);
                }

                var type = typeof(TRecord);
                var props = ssBuilder.GetNonIgnoreProperties<TRecord>();

                var dt = new DataTable();
                var fieldProps = new List<PropertyInfo>();

                var hasValues = ssBuilder.Valueset.Count() > 0;
                var values = hasValues ? ssBuilder.ValueExpressions.Select(v => v.Compile()).ToList() : null;

                log?.Log("Fields:", StageLevel.Trace);
                foreach (var originalField in fields)
                {
                    var field = originalField.Contains(".")
                        ? originalField.Substring(originalField.IndexOf(".") + 1)
                        : originalField;
                    var property = props.FirstOrDefault(p => p.Name == field);
                    if (property != null)
                    {
                        var isNullable = false;
                        Type propertyType = property.PropertyType;
                        if (propertyType.IsGenericType &&
                            propertyType.GetGenericTypeDefinition() == typeof(Nullable<>))
                        {
                            propertyType = Nullable.GetUnderlyingType(propertyType);
                            isNullable = true;
                        }

                        var col = new DataColumn(property.Name, propertyType);
                        if (isNullable)
                        {
                            col.AllowDBNull = isNullable;
                        }

                        dt.Columns.Add(col);
                        fieldProps.Add(property);
                        log?.Log($"  orig={originalField}, field={field}, type={propertyType.Name}", StageLevel.Trace);
                    }
                    else
                    {
                        log?.Log($"  orig={originalField}, field={field}, property is null", StageLevel.Trace);
                    }
                }

                var source = ssBuilder.SourceData;
                var groupedData = source.ToArray().Split(bulkSize);

                var logged = false;
                var count = 0;
                using (var destConn = new SqlConnection(connection))
                {
                    await destConn.OpenAsync();
                    var bulkCopy = new SqlBulkCopy(destConn)
                {
                    DestinationTableName = ssBuilder.TableName,
                    BulkCopyTimeout = timeoutInSeconds                   
                };

                    dt.Columns
                        .Cast<DataColumn>()
                        .ToList()
                        .ForEach(
                            col =>
                    bulkCopy.ColumnMappings.Add(new SqlBulkCopyColumnMapping(col.ColumnName, col.ColumnName)));

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
                                ++fieldIndex;
                            }

                            dt.Rows.Add(columns.ToArray());
                        }

                        if (!logged)
                        {
                            logged = true;
                            if (group.Count() > 0)
                            {
                                log?.Log("One Record Example:", StageLevel.Trace);
                                log?.Log(
                                "  " +
                                    string.Join(
                                        ",",
                                        dt.Rows[0].ItemArray.Select(i => Utils.GetSQLValue(i.GetType(), i))),
                                StageLevel.Trace);
                            }
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
                                    if (group.Count() > 0)
                                    {
                                        log?.Log("Error Record Example, First in Group:", StageLevel.Trace);
                                        log?.Log(
                                        "  " +
                                            string.Join(
                                                ",",
                                                dt.Rows[0].ItemArray.Select(i => Utils.GetSQLValue(i.GetType(), i))),
                                        StageLevel.Trace);
                                    }

                                    throw;
                                }
                            }
                        }

                        count += dt.Rows.Count;
                    }

                    await destConn.CloseAsync();
                }

                sqllog?.AddMetric("SQLCount", count);
                return count;
            }
        }
    }
}