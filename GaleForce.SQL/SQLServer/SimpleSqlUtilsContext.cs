//-----------------------------------------------------------------------
// <copyright file="SimpleSqlUtilsContext.cs" company="Gale-Force, LLC">
// Copyright (C) Gale-Force, LLC. All rights reserved.
// </copyright>
// -----------------------------------------------------------------------
namespace GaleForce.SQL.SQLServer
{
    using System.Collections.Generic;

    /// <summary>
    /// Class SimpleSqlBuilderContext.
    /// </summary>
    public class SimpleSqlBuilderContext
    {
        /// <summary>
        /// Initializes a new instance of the <see cref="SimpleSqlBuilderContext"/> class.
        /// </summary>
        public SimpleSqlBuilderContext()
        {
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="SimpleSqlBuilderContext"/> class.
        /// </summary>
        /// <param name="tables">The tables.</param>
        public SimpleSqlBuilderContext(Dictionary<string, object> tables)
        {
            this.IsLocal = true;
            this.Tables = tables;
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="SimpleSqlBuilderContext"/> class.
        /// </summary>
        /// <param name="connection">The connection.</param>
        public SimpleSqlBuilderContext(string connection)
        {
            this.IsLocal = false;
            this.Connection = connection;
        }

        /// <summary>
        /// Gets or sets the connection.
        /// </summary>
        public string Connection { get; set; }

        /// <summary>
        /// Gets or sets a value indicating whether this instance is local (testing) or otherwise a
        /// SQL connection.
        /// </summary>
        public bool IsLocal { get; set; }

        /// <summary>
        /// Gets or sets a dictionary of tables, by tablename.
        /// </summary>
        public Dictionary<string, object> Tables { get; set; } = new Dictionary<string, object>();

        /// <summary>
        /// Gets or sets the timeout in seconds.
        /// </summary>
        public int TimeoutInSeconds { get; set; } = 3600;

        /// <summary>
        /// Gets the table.
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="tableName">Name of the table.</param>
        /// <returns>IEnumerable&lt;T&gt;.</returns>
        public SimpleSqlBuilderContextTable<T> GetContextTable<T>(string tableName)
        {
            return this.Tables.ContainsKey(tableName) ? this.Tables[tableName] as SimpleSqlBuilderContextTable<T> : null;
        }

        /// <summary>
        /// Sets the table.
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="tableName">Name of the table.</param>
        /// <param name="contextTable">The records.</param>
        public void SetContextTable<T>(string tableName, SimpleSqlBuilderContextTable<T> contextTable)
        {
            this.Tables[tableName] = contextTable;
        }

        /// <summary>
        /// Gets the table.
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="tableName">Name of the table.</param>
        /// <returns>IEnumerable&lt;T&gt;.</returns>
        public IEnumerable<T> GetTable<T>(string tableName)
        {
            return this.GetContextTable<T>(tableName)?.Data;
        }

        /// <summary>
        /// Gets the table.
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="tableName">Name of the table.</param>
        /// <returns>IEnumerable&lt;T&gt;.</returns>
        public List<T> GetList<T>(string tableName)
        {
            return this.GetContextTable<T>(tableName)?.Data as List<T>;
        }

        /// <summary>
        /// Sets the table.
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="tableName">Name of the table.</param>
        /// <param name="records">The records.</param>
        public void SetTable<T>(string tableName, IEnumerable<T> records)
        {
            this.IsLocal = true;
            this.SetContextTable<T>(tableName, new SimpleSqlBuilderContextTable<T>(tableName, records));
        }

        /// <summary>
        /// Determines whether the specified table list has a named table.
        /// </summary>
        /// <param name="tableName">Name of the table.</param>
        /// <returns>
        /// <c>true</c> if the specified table name has table; otherwise, <c>false</c>.
        /// </returns>
        public bool HasTable(string tableName)
        {
            return this.Tables.ContainsKey(tableName);
        }
    }
}
