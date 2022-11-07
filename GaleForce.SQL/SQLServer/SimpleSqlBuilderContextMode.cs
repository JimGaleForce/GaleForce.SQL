//-----------------------------------------------------------------------
// <copyright file="SimpleSqlBuilderContextMode.cs" company="Gale-Force, LLC">
// Copyright (C) Gale-Force, LLC. All rights reserved.
// </copyright>
// -----------------------------------------------------------------------
namespace GaleForce.SQL.SQLServer
{
    /// <summary>
    /// Enum SimpleSqlBuilderContextMode
    /// </summary>
    public enum SimpleSqlBuilderContextMode
    {
        /// <summary>
        /// No Action - no logging, no testing, no sql.
        /// </summary>
        NoAction,

        /// <summary>
        /// Testing - use local lists to test, not sql.
        /// </summary>
        Testing,

        /// <summary>
        /// Use a SQL Connection.
        /// </summary>
        SQL
    }
}
