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

                if (data == null && context.TestAutoCreateTables)
                {
                    data = new List<TRecord>();
                    context.SetTable(tableName, data);
                }

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
                return ssBuilder.ExecuteSQL(context.Connection, log: log, context.TimeoutInSeconds);
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
                        if (context.TestAutoCreateTables)
                        {
                            data1 = new List<TRecord1>();
                            context.SetTable(ssBuilder.TableNames[0], data1);
                        }
                        else
                        {
                            throw new MissingDataTableException(
                                $"{ssBuilder.TableNames[0]} needs to exist for testing in SimpleSqlBuilder");
                        }
                    }

                    if (data2 == null)
                    {
                        if (context.TestAutoCreateTables)
                        {
                            data2 = new List<TRecord2>();
                            context.SetTable(ssBuilder.TableNames[1], data2);
                        }
                        else
                        {
                            throw new MissingDataTableException(
                                $"{ssBuilder.TableNames[1]} needs to exist for testing in SimpleSqlBuilder");
                        }
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
                return ssBuilder.ExecuteSQL(context.Connection, log: log, context.TimeoutInSeconds);
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
                        if (context.TestAutoCreateTables)
                        {
                            data1 = new List<TRecord1>();
                            context.SetTable(ssBuilder.TableNames[0], data1);
                        }
                        else
                        {
                            throw new MissingDataTableException(
                                $"{ssBuilder.TableNames[0]} needs to exist for testing in SimpleSqlBuilder");
                        }
                    }

                    if (data2 == null)
                    {
                        if (context.TestAutoCreateTables)
                        {
                            data2 = new List<TRecord2>();
                            context.SetTable(ssBuilder.TableNames[1], data2);
                        }
                        else
                        {
                            throw new MissingDataTableException(
                                $"{ssBuilder.TableNames[1]} needs to exist for testing in SimpleSqlBuilder");
                        }
                    }

                    if (data3 == null)
                    {
                        if (context.TestAutoCreateTables)
                        {
                            data3 = new List<TRecord3>();
                            context.SetTable(ssBuilder.TableNames[2], data3);
                        }
                        else
                        {
                            throw new MissingDataTableException(
                                $"{ssBuilder.TableNames[2]} needs to exist for testing in SimpleSqlBuilder");
                        }
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
                return ssBuilder.ExecuteSQL(context.Connection, log: log, context.TimeoutInSeconds);
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
                        if (context.TestAutoCreateTables)
                        {
                            data1 = new List<TRecord1>();
                            context.SetTable(ssBuilder.TableNames[0], data1);
                        }
                        else
                        {
                            throw new MissingDataTableException(
                                $"{ssBuilder.TableNames[0]} needs to exist for testing in SimpleSqlBuilder");
                        }
                    }

                    if (data2 == null)
                    {
                        if (context.TestAutoCreateTables)
                        {
                            data2 = new List<TRecord2>();
                            context.SetTable(ssBuilder.TableNames[1], data2);
                        }
                        else
                        {
                            throw new MissingDataTableException(
                                $"{ssBuilder.TableNames[1]} needs to exist for testing in SimpleSqlBuilder");
                        }
                    }

                    if (data3 == null)
                    {
                        if (context.TestAutoCreateTables)
                        {
                            data3 = new List<TRecord3>();
                            context.SetTable(ssBuilder.TableNames[2], data3);
                        }
                        else
                        {
                            throw new MissingDataTableException(
                                $"{ssBuilder.TableNames[2]} needs to exist for testing in SimpleSqlBuilder");
                        }
                    }

                    if (data4 == null)
                    {
                        if (context.TestAutoCreateTables)
                        {
                            data4 = new List<TRecord4>();
                            context.SetTable(ssBuilder.TableNames[3], data4);
                        }
                        else
                        {
                            throw new MissingDataTableException(
                                $"{ssBuilder.TableNames[3]} needs to exist for testing in SimpleSqlBuilder");
                        }
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
                return ssBuilder.ExecuteSQL(context.Connection, log: log, context.TimeoutInSeconds);
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

                if (data == null && context.TestAutoCreateTables)
                {
                    data = new List<TRecord>();
                    context.SetTable(ssBuilder.TableName, data);
                }

                if (data != null)
                {
                    Dictionary<string, SourceData> sourceData = context.GetSourceData<TRecord>();
                    IEnumerable<TRecord> source = null;
                    if (ssBuilder.Command == "MERGE")
                    {
                        source = data;
                        sourceData.Add(
                            "__source",
                            new SourceData
                            {
                                Data = source as IEnumerable<object>,
                                Name = "__source",
                                SourceType = typeof(TRecord)
                            });
                        data = context.GetList<TRecord>(ssBuilder.MergeIntoTableName);

                        if (data == null)
                        {
                            if (context.TestAutoCreateTables)
                            {
                                data = new List<TRecord>();
                                context.SetTable(ssBuilder.TableName, data);
                            }
                            else
                            {
                                throw new MissingDataTableException(
                                    $"{ssBuilder.MergeIntoTableName} needs to exist as a List<{typeof(TRecord).Name}> to merge into for testing in SimpleSqlBuilder");
                            }
                        }
                    }

                    var sql = ssBuilder.Build();
                    using (var sqllog = log?.Item("sql." + ssBuilder.Command + "." + sql.GetHashCode(), "SQL"))
                    {
                        sqllog?.AddEvent("SQL", sql);
                        var result = ssBuilder.ExecuteNonQuery(data, sourceData);
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
                return await ssBuilder.ExecuteSQLNonQuery(context.Connection, log: log, context.TimeoutInSeconds);
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
        /// Uses a temp table and bulk copy.
        /// </summary>
        /// <typeparam name="TRecord">The type of the t record.</typeparam>
        /// <param name="ssBuilder">The ss builder.</param>
        /// <param name="useBulkCopy">if set to <c>true</c> [use bulk copy].</param>
        /// <returns>SimpleSqlBuilder&lt;TRecord&gt;.</returns>
        public static SimpleSqlBuilder<TRecord> UseTempTable<TRecord>(
            this SimpleSqlBuilder<TRecord> ssBuilder,
            string optionalFilename = null)
        {
            UseBulkCopy(ssBuilder);
            ssBuilder.Metadata["UseTempTable"] = true;
            ssBuilder.Metadata["TempTableName"] = optionalFilename;
            return ssBuilder;
        }

        /// <summary>
        /// Uses a temp table and bulk copy.
        /// </summary>
        /// <typeparam name="TRecord">The type of the t record.</typeparam>
        /// <param name="ssBuilder">The ss builder.</param>
        /// <param name="useBulkCopy">if set to <c>true</c> [use bulk copy].</param>
        /// <returns>SimpleSqlBuilder&lt;TRecord&gt;.</returns>
        public static SimpleSqlBuilder<TRecord> UseMinTempTable<TRecord>(
            this SimpleSqlBuilder<TRecord> ssBuilder,
            string optionalFilename = null)
        {
            UseTempTable(ssBuilder, optionalFilename);
            ssBuilder.Metadata["UseMinTempTable"] = true;
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
            StageLogger log = null,
            int timeoutSecondsDefault = 600)
        {
            var sql = ssBuilder.Build();
            using (var sqllog = log?.Item("sql.select." + sql.GetHashCode(), "SQL"))
            {
                sqllog?.AddEvent("SQL", sql);

                int timeout = timeoutSecondsDefault;
                if (ssBuilder.Metadata.ContainsKey("TimeoutInSeconds"))
                {
                    int.TryParse(ssBuilder.Metadata["TimeoutInSeconds"].ToString(), out timeout);
                }

                var records = Utils.SqlCommandToLinq(
                    sql,
                    connection,
                    parameters: ssBuilder.Options.UseParameters ? ssBuilder.GetParameters() : null,
                    timeoutSeconds: timeout);
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
            StageLogger log = null,
            int timeoutSecondsDefault = 600)
        {
            var isBulkCopy = ssBuilder.Metadata.ContainsKey("UseBulkCopy") &&
                (bool) ssBuilder.Metadata["UseBulkCopy"];
            var isTempTable = ssBuilder.Metadata.ContainsKey("UseTempTable") &&
                (bool) ssBuilder.Metadata["UseTempTable"];
            var isMinTempTable = ssBuilder.Metadata.ContainsKey("UseMinTempTable") &&
                (bool) ssBuilder.Metadata["UseMinTempTable"] &&
                ssBuilder.Command == "UPDATE" &&
                ssBuilder.Updates != null &&
                ssBuilder.Updates.Count() > 0;

            if ((ssBuilder.Command == "INSERT" && isBulkCopy) || (ssBuilder.Command == "UPDATE" && isTempTable))
            {
                int bulkSize = ssBuilder.Metadata.ContainsKey("BulkCopySize")
                    ? (int) ssBuilder.Metadata["BulkCopySize"]
                    : 50000;

                int timeout = timeoutSecondsDefault;
                if (ssBuilder.Metadata.ContainsKey("TimeoutInSeconds"))
                {
                    int.TryParse(ssBuilder.Metadata["TimeoutInSeconds"].ToString(), out timeout);
                }

                var prevTableName = ssBuilder.TableName;
                var prevCommand = ssBuilder.Command;
                string tempTableName = null;

                if (isTempTable)
                {
                    // switch to temptable for insert
                    tempTableName = (ssBuilder.Metadata.ContainsKey("TempTableName")
                            ?
                        (string) ssBuilder.Metadata["TempTableName"]
                            : null) ??
                        prevTableName +
                        "*";
                    if (tempTableName.Contains("*"))
                    {
                        tempTableName = tempTableName.Replace("*", Guid.NewGuid().ToString().Substring(24));
                    }

                    ssBuilder.OverrideTableName(tempTableName);

                    if (isMinTempTable)
                    {
                        var columnsInfo = GetTableColumns(GetColumnInfo(ssBuilder, log, ssBuilder.Updates));
                        var cols = string.Join(", ", columnsInfo);
                        var createSql = $"CREATE TABLE {tempTableName} ({cols}) ON [PRIMARY]";
                        Console.WriteLine(createSql);

                        var resultCreate = await Utils.SqlExecuteNonQuery(
                            createSql,
                            connection,
                            timeoutSeconds: timeoutSecondsDefault);

                        ssBuilder.OverrideFields(ssBuilder.Updates);
                    }
                    else
                    {
                        var copySql = $"SELECT * INTO {tempTableName} FROM {prevTableName} WHERE 1=0;";
                        var resultCopy = await Utils.SqlExecuteNonQuery(
                            copySql,
                            connection,
                            timeoutSeconds: timeoutSecondsDefault);
                    }

                    if (ssBuilder.Command == "UPDATE")
                    {
                        ssBuilder.OverrideCommand("INSERT");
                    }
                }

                var result = await ssBuilder.ExecuteBulkCopy(
                    connection,
                    log: log,
                    bulkSize: bulkSize,
                    timeoutInSeconds: timeout);

                if (isTempTable)
                {
                    // merge from temp
                    ssBuilder.OverrideTableName(null);
                    ssBuilder.OverrideCommand(null);
                    ssBuilder.OverrideFields(null);

                    SimpleSqlBuilder<TRecord> ssbMerge = null;
                    if (prevCommand == "UPDATE")
                    {
                        ssbMerge = new SimpleSqlBuilder<TRecord>(tempTableName)
                            .MergeInto(ssBuilder.TableName)
                            .If(ssBuilder.MatchKey1 != null, s => s.Match(ssBuilder.MatchKey1))
                            .If(ssBuilder.MatchKey2 != null, s => s.Match(ssBuilder.MatchKey2))
                            .WhenMatched(s => s.Update(ssBuilder.UpdateExpressions));
                    }
                    else if (prevCommand == "INSERT")
                    {
                        ssbMerge = new SimpleSqlBuilder<TRecord>(ssBuilder.TableName)
                            .Insert(s => s.From(tempTableName).Select());
                    }

                    var result2 = await ExecuteSQLNonQuery(ssbMerge, connection, log, timeoutSecondsDefault);
                    var dropSql = $"DROP TABLE {tempTableName};";
                    var resultDrop = await Utils.SqlExecuteNonQuery(
                        dropSql,
                        connection,
                        timeoutSeconds: timeoutSecondsDefault);
                    return result2;
                }

                return result;
            }
            else
            {
                var sql = ssBuilder.Build();
                using (var sqllog = log?.Item("sql." + ssBuilder.Command + "." + sql.GetHashCode(), "SQL"))
                {
                    sqllog?.AddEvent("SQL", sql);
                    if (!string.IsNullOrEmpty(sql))
                    {
                        int timeout = timeoutSecondsDefault;
                        if (ssBuilder.Metadata.ContainsKey("TimeoutInSeconds"))
                        {
                            int.TryParse(ssBuilder.Metadata["TimeoutInSeconds"].ToString(), out timeout);
                        }

                        try
                        {
                            var result = await Utils.SqlExecuteNonQuery(
                                sql,
                                connection,
                                parameters: ssBuilder.Options.UseParameters ? ssBuilder.GetParameters() : null,
                                timeoutSeconds: timeout);

                            sqllog?.AddMetric("SQLCount", result);
                            return result;
                        }
                        catch (Exception e)
                        {
                            log?.Log(e);
                            throw;
                        }
                    }

                    return 0;
                }
            }
        }

        private static List<string> GetTableColumns(List<ColumnInfo> columns)
        {
            var list = new List<string>();
            foreach (var col in columns)
            {
                var type = SqlHelpers.GetBetterPropTypeName(col.Property).Replace("?", "");
                var cType = "";
                switch (type)
                {
                    case "string":
                        cType = "[varchar](MAX)";
                        break;
                    case "bool":
                        cType = "[bit]";
                        break;
                    case "double":
                        cType = "[float]";
                        break;
                    default:
                        cType = $"[{type}]";
                        break;
                }

                var nullable = col.IsNullable ? "NULL" : "NOT NULL";
                list.Add($"[{col.Name}] {cType} {nullable}");
            }

            return list;
        }

        private static List<ColumnInfo> GetColumnInfo<TRecord>(
            SimpleSqlBuilder<TRecord> ssBuilder,
            StageLogger log,
            List<string> fields = null)
        {
            if (fields == null)
            {
                fields = ssBuilder.Inserts;
            }

            var props = ssBuilder.GetNonIgnoreProperties<TRecord>();

            if (fields.Count == 0)
            {
                // fields = typeof(TRecord).GetProperties().Select(p => p.Name).ToList();
                fields = ssBuilder.FieldList(ssBuilder.Fields);
            }

            return GetColumnInfo(log, fields, props);
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

                var type = typeof(TRecord);

                var dt = new DataTable();
                var fieldProps = new List<PropertyInfo>();

                var hasValues = ssBuilder.Valueset.Count() > 0;
                var values = hasValues ? ssBuilder.ValueExpressions.Select(v => v.Compile()).ToList() : null;

                log?.Log("Fields:", StageLevel.Trace);

                var columnsInfo = GetColumnInfo(ssBuilder, log);

                foreach (var columnInfo in columnsInfo)
                {
                    var col = new DataColumn(columnInfo.Name, columnInfo.Type);
                    if (columnInfo.IsNullable)
                    {
                        col.AllowDBNull = columnInfo.IsNullable;
                    }

                    dt.Columns.Add(col);
                    fieldProps.Add(columnInfo.Property);
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

                        var retryIndex = 0;
                        while (retryIndex < retries)
                        {
                            try
                            {
                                await bulkCopy.WriteToServerAsync(dt);
                                break;
                            }
                            catch (Exception e)
                            {
                                log?.Log(e);
                                if (++retryIndex >= retries)
                                {
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

        private static List<ColumnInfo> GetColumnInfo(StageLogger log, List<string> fields, PropertyInfo[] props)
        {
            var columns = new List<ColumnInfo>();
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

                    columns.Add(
                        new ColumnInfo
                        {
                            Name = property.Name,
                            IsNullable = isNullable,
                            Type = propertyType,
                            Property = property
                        });

                    log?.Log($"  orig={originalField}, field={field}, type={propertyType.Name}", StageLevel.Trace);
                }
                else
                {
                    log?.Log($"  orig={originalField}, field={field}, property is null", StageLevel.Trace);
                }
            }

            return columns;
        }
    }

    public class ColumnInfo
    {
        public string Name { get; set; }

        public Type Type { get; set; }

        public bool IsNullable { get; set; }

        public PropertyInfo Property { get; set; }
    }
}