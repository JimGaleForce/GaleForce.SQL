//-----------------------------------------------------------------------
// <copyright file="SimpleSqlBuilderContextTable.cs" company="Gale-Force, LLC">
// Copyright (C) Gale-Force, LLC. All rights reserved.
// </copyright>
// -----------------------------------------------------------------------
namespace GaleForce.SQL.SQLServer
{
    using System;
    using System.Collections.Generic;

    /// <summary>
    /// Class SimpleSqlBuilderContextTable.
    /// </summary>
    /// <typeparam name="TRecord">The type of the t record.</typeparam>
    public class SimpleSqlBuilderContextTable<TRecord>
    {
        /// <summary>
        /// Initializes a new instance of the <see cref="SimpleSqlBuilderContextTable{TRecord}"/>
        /// class.
        /// </summary>
        /// <param name="tableName">Name of the table.</param>
        /// <param name="data">The data.</param>
        public SimpleSqlBuilderContextTable(string tableName, IEnumerable<TRecord> data)
        {
            this.TableName = tableName;
            this.Data = data;
            this.Type = typeof(TRecord);
        }

        public SimpleSqlBuilderContextTable(string tableName, List<TRecord> data)
        {
            this.TableName = tableName;
            this.Data = data;
            this.Type = typeof(TRecord);
        }

        /// <summary>
        /// Gets or sets the local data to execute against.
        /// </summary>
        public IEnumerable<TRecord> Data { get; set; }

        /// <summary>
        /// Gets or sets the name of the table.
        /// </summary>
        public string TableName { get; set; }

        /// <summary>
        /// Gets or sets the type.
        /// </summary>
        public Type Type { get; set; }
    }
}
