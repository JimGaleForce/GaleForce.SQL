using System;
using System.Data;
using System.Data.SqlClient;
using System.Reflection;
using System.Threading.Tasks;
using GaleForceCore.Helpers;

namespace GaleForce.SQL.SQLServer
{
    /// <summary>
    /// Class Utils.
    /// </summary>
    public class Utils
    {
        /// <summary>
        /// Gets a SQL value, using a property type, from an object.
        /// </summary>
        /// <param name="prop">The property.</param>
        /// <param name="record">The record.</param>
        /// <returns>System.String.</returns>
        public static string GetSQLValue(PropertyInfo prop, object record)
        {
            var value = prop.GetValue(record);
            return GetSQLValue(prop.PropertyType, value);
        }

        /// <summary>
        /// Gets a SQL value, using a property type, from an object.
        /// </summary>
        /// <param name="type">The type.</param>
        /// <param name="value">The value.</param>
        /// <returns>System.String.</returns>
        /// <exception cref="System.ArgumentException">Unsupported object type '{type}', cannot convert to SQL value.</exception>
        public static string GetSQLValue(Type type, object value)
        {
            var name = SqlHelpers.GetBetterPropTypeName(type);

            switch (name)
            {
                case "string":
                    value = "'" + value + "'";
                    break;
                case "DateTime":
                    if (value is DateTime)
                    {
                        value = "'" + ((DateTime)value).ToString("o") + "'";
                    }
                    else if (value is string)
                    {
                        value = "'" + DateTime.Parse(value.ToString()).ToString("o") + "'";
                    }

                    break;
                case "DateTime?":
                    if (value is DateTime?)
                    {
                        if (((DateTime?)value).HasValue)
                        {
                            value = "'" + ((DateTime?)value).Value.ToString("o") + "'";
                        }
                    }
                    else if (value is string)
                    {
                        value = "'" + DateTime.Parse(value.ToString()).ToString("o") + "'";
                    }

                    break;
                case "bool":
                    value = ((bool)value) ? 1 : 0;
                    break;
                case "bool?":
                    if (((bool?)value).HasValue)
                    {
                        value = ((bool)value) ? 1 : 0;
                    }

                    break;
                case "int":
                case "double":
                case "float":
                case "int?":
                case "double?":
                case "float?":
                case "decimal":
                case "decimal?":

                    // already in correct format
                    break;

                default:
                    if (type.BaseType.Name == "Enum")
                    {
                        value = (int)Enum.Parse(type, value.ToString());
                        break;
                    }

#if RELEASE
                    break;
#else
                    throw new ArgumentException($"Unsupported object type '{type}', cannot convert to SQL value.");
#endif
            }

            return (value ?? "null").ToString();
        }

        /// <summary>
        /// Sets a value in a T record from a DataRow.
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="record">The DataRow record.</param>
        /// <param name="newRecord">The new record.</param>
        /// <param name="prop">The property.</param>
        public static void SetValue<T>(DataRow record, T newRecord, PropertyInfo prop)
        {
            var name = SqlHelpers.GetBetterPropTypeName(prop);
            switch (name)
            {
                case "string":
                    prop.SetValue(newRecord, record.Field<string>(prop.Name));
                    break;
                case "DateTime":
                    prop.SetValue(newRecord, record.Field<DateTime>(prop.Name));
                    break;
                case "DateTime?":
                    prop.SetValue(newRecord, record.Field<DateTime?>(prop.Name));
                    break;
                case "int":
                case "Int32":
                    prop.SetValue(newRecord, record.Field<int>(prop.Name));
                    break;
                case "int?":
                case "Int32?":
                    prop.SetValue(newRecord, record.Field<int?>(prop.Name));
                    break;
                case "bool":
                    prop.SetValue(newRecord, record.Field<bool>(prop.Name));
                    break;
                case "bool?":
                    prop.SetValue(newRecord, record.Field<bool?>(prop.Name));
                    break;
                case "double":
                    prop.SetValue(newRecord, record.Field<double>(prop.Name));
                    break;
                case "double?":
                    prop.SetValue(newRecord, record.Field<double?>(prop.Name));
                    break;
                case "decimal":
                case "Decimal":
                    prop.SetValue(newRecord, record.Field<decimal>(prop.Name));
                    break;
                case "decimal?":
                case "Decimal?":
                    prop.SetValue(newRecord, record.Field<decimal?>(prop.Name));
                    break;
                case "float":
                case "Float":
                case "Single":
                    prop.SetValue(newRecord, record.Field<float>(prop.Name));
                    break;
                case "float?":
                case "Float?":
                case "Single?":
                    prop.SetValue(newRecord, record.Field<float?>(prop.Name));
                    break;
            }
        }

        /// <summary>
        /// Executes a sql command and returns the DataRows as enumerable.
        /// </summary>
        /// <param name="command">The command.</param>
        /// <param name="connection">The connection.</param>
        /// <param name="timeoutSeconds">The timeout in seconds.</param>
        /// <param name="retryIfTransportError">if set to <c>true</c> [retry if transport error].</param>
        /// <returns>EnumerableRowCollection&lt;DataRow&gt;.</returns>
        public static EnumerableRowCollection<DataRow> SqlCommandToLinq(
            string command,
            string connection,
            int timeoutSeconds = 500,
            bool retryIfTransportError = true)
        {
            return SqlCommandToLinq(command, new SqlConnection(connection), timeoutSeconds, retryIfTransportError);
        }

        /// <summary>
        /// Executes a sql command and returns the count.
        /// </summary>
        /// <param name="command">The command.</param>
        /// <param name="connection">The connection.</param>
        /// <param name="timeoutSeconds">The timeout in seconds.</param>
        public static async Task<int> SqlExecuteNonQuery(string command, string connection, int timeoutSeconds = 500)
        {
            var count = 0;
            using (var conn = new SqlConnection(connection))
            {
                await conn.OpenAsync();
                var cmdx = new SqlCommand(command, conn);
                cmdx.CommandTimeout = timeoutSeconds;
                count = await cmdx.ExecuteNonQueryAsync();
                await conn.CloseAsync();
            }

            return count;
        }

        /// <summary>
        /// Executes a sql command and returns the DataRows as enumerable.
        /// </summary>
        /// <param name="command">The command.</param>
        /// <param name="connection">The connection.</param>
        /// <param name="timeoutSeconds">The timeout in seconds.</param>
        /// <param name="retryIfTransportError">if set to <c>true</c> [retry if transport error].</param>
        /// <returns>EnumerableRowCollection&lt;DataRow&gt;.</returns>
        public static EnumerableRowCollection<DataRow> SqlCommandToLinq(
            string command,
            SqlConnection connection,
            int timeoutSeconds = 500,
            bool retryIfTransportError = true)
        {
            var cmd = new SqlCommand(command, connection);
            cmd.CommandTimeout = timeoutSeconds;
            var dt = new DataSet();

            var adapter = new SqlDataAdapter();
            adapter.SelectCommand = cmd;

            try
            {
                adapter.Fill(dt);
            }
            catch (Exception e)
            {
                if (retryIfTransportError &&
                    (e.Message.Contains("forcibly closed") || e.Message.Contains("transport-level")))
                {
                    adapter.Fill(dt);
                }
                else
                {
                    throw;
                }
            }

            return dt.Tables[0].AsEnumerable();
        }

        /// <summary>
        /// Creates the connection string.
        /// </summary>
        /// <param name="server">The server.</param>
        /// <param name="database">The database.</param>
        /// <param name="userID">The user identifier.</param>
        /// <param name="password">The password.</param>
        /// <returns>System.String.</returns>
        public string CreateConnectionString(string server, string database, string userID, string password)
        {
            var builder = new SqlConnectionStringBuilder();
            builder.DataSource = server;
            builder.InitialCatalog = database;
            builder.Password = password;
            builder.UserID = userID;
            return builder.ConnectionString;
        }
    }
}
