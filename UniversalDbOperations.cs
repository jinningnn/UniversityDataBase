using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Data.SQLite;

using MySql.Data.MySqlClient;
using System.Data.Common;
using System.Data;
using System.Data.SqlClient;
namespace UniversalDatabase
{

    // 定义数据库类型的枚举
    public enum DatabaseType
    {
        SqlServer,
        MySql,
        SQLite
    }
    public class UniversalDbOperations
    {

        private DbProviderFactory iFactory;

        public UniversalDbOperations()
        {
        }

        public UniversalDbOperations(DatabaseType databaseType)
        {
            DbProviderFactory factory = GetDbProviderFactory(databaseType);
            iFactory = factory;
        }

        public int Insert(string connectionString, string tableName, string[] columnNames, object[] values)
        {
            if(iFactory == null)
            {
                return -1;
            }

            DbProviderFactory factory = iFactory;
            string sql = BuildInsertQuery(tableName, columnNames);
            DbParameter[] parameters = BuildParameters(columnNames, values);
            using (DbConnection connection = factory.CreateConnection())
            {
                connection.ConnectionString = connectionString;
                connection.Open();
                using (DbCommand command = connection.CreateCommand())
                {
                    command.CommandText = sql;
                    foreach (var param in parameters)
                    {
                        command.Parameters.Add(param);
                    }
                    return command.ExecuteNonQuery();
                }
            }
        }


        public int Insert(DatabaseType databaseType, string connectionString, string tableName, string[] columnNames, object[] values)
        {
            DbProviderFactory factory = GetDbProviderFactory(databaseType);
            string sql = BuildInsertQuery(tableName, columnNames);
            DbParameter[] parameters = BuildParameters(databaseType, columnNames, values);
            using (DbConnection connection = factory.CreateConnection())
            {
                connection.ConnectionString = connectionString;
                connection.Open();
                using (DbCommand command = connection.CreateCommand())
                {
                    command.CommandText = sql;
                    foreach (var param in parameters)
                    {
                        command.Parameters.Add(param);
                    }
                    return command.ExecuteNonQuery();
                }
            }
        }

        public int Delete(DatabaseType databaseType, string connectionString, string tableName, string condition)
        {
            DbProviderFactory factory = GetDbProviderFactory(databaseType);
            string sql = $"delete from {tableName} where {condition}";
            using (DbConnection connection = factory.CreateConnection())
            {
                connection.ConnectionString = connectionString;
                connection.Open();
                using (DbCommand command = connection.CreateCommand())
                {
                    command.CommandText = sql;
                    return command.ExecuteNonQuery();
                }
            }
        }

        public DataTable Select(DatabaseType databaseType, string connectionString, string tableName, string[] columnNames, string condition = "")
        {
            DbProviderFactory factory = GetDbProviderFactory(databaseType);
            string sql = BuildSelectQuery(tableName, columnNames, condition);
            using (DbConnection connection = factory.CreateConnection())
            {
                connection.ConnectionString = connectionString;
                connection.Open();
                using (DbCommand command = connection.CreateCommand())
                {
                    command.CommandText = sql;
                    using (DbDataReader reader = command.ExecuteReader())
                    {
                        DataTable dataTable = new DataTable();
                        dataTable.Load(reader);
                        return dataTable;
                    }
                }
            }
        }

        public DataTable SelectAll(DatabaseType databaseType, string connectionString, string tableName, string condition = "")
        {
            DbProviderFactory factory = GetDbProviderFactory(databaseType);
            string sql;
            if (string.IsNullOrEmpty(condition))
            {
                sql = $"select * from {tableName}";
            }else
            {
                sql = $"select * from {tableName} where {condition}";
            }
            using (DbConnection connection = factory.CreateConnection())
            {
                connection.ConnectionString = connectionString;
                connection.Open();
                using (DbCommand command = connection.CreateCommand())
                {
                    command.CommandText = sql;
                    using (DbDataReader reader = command.ExecuteReader())
                    {
                        DataTable dataTable = new DataTable();
                        dataTable.Load(reader);
                        return dataTable;
                    }
                }
            }
        }


        public int Update(DatabaseType databaseType, string connectionString, string tableName, string[] columnNames, object[] values, string condition)
        {
            DbProviderFactory factory = GetDbProviderFactory(databaseType);
            string sql = BuildUpdateQuery(tableName, columnNames, condition);
            DbParameter[] parameters = BuildParameters(databaseType, columnNames, values);
            using (DbConnection connection = factory.CreateConnection())
            {
                connection.ConnectionString = connectionString;
                connection.Open();
                using (DbCommand command = connection.CreateCommand())
                {
                    command.CommandText = sql;
                    foreach (var param in parameters)
                    {
                        command.Parameters.Add(param);
                    }
                    return command.ExecuteNonQuery();
                }
            }
        }

        private DbProviderFactory GetDbProviderFactory(DatabaseType databaseType)
        {
            
            switch (databaseType)
            {
                case DatabaseType.SqlServer:
                    return SqlClientFactory.Instance;
                case DatabaseType.MySql:
                    return MySqlClientFactory.Instance;
                case DatabaseType.SQLite:
                    return SQLiteFactory.Instance;
                default:
                    throw new ArgumentException("错误数据库类型");
            }
        }

        private string BuildInsertQuery(string tableName, string[] columnNames)
        {
            string columns = string.Join(",", columnNames);
            string placeholders = string.Join(",", Array.ConvertAll(columnNames, col => $"@{col}"));
            return $"INSERT INTO {tableName} ({columns}) VALUES ({placeholders})";
        }


        private string BuildSelectQuery(string tableName, string[] columnNames, string condition)
        {
            string columns = string.Join(",", columnNames);
            if (string.IsNullOrEmpty(condition))
            {
                return $"SELECT {columns} FROM {tableName}";
            }
            return $"SELECT {columns} FROM {tableName} WHERE {condition}";
        }


        private string BuildUpdateQuery(string tableName, string[] columnNames, string condition)
        {
            string setClause = string.Join(",", Array.ConvertAll(columnNames, col => $"{col}=@{col}"));
            return $"UPDATE {tableName} SET {setClause} WHERE {condition}";
        }


        private DbParameter[] BuildParameters(DatabaseType databaseType, string[] columnNames, object[] values)
        {
            DbProviderFactory factory = GetDbProviderFactory(databaseType);
            DbParameter[] parameters = new DbParameter[columnNames.Length];
            for (int i = 0; i < columnNames.Length; i++)
            {
                DbParameter parameter = factory.CreateParameter();
                parameter.ParameterName = $"@{columnNames[i]}";
                parameter.Value = values[i];
                parameters[i] = parameter;
            }
            return parameters;
        }


        private DbParameter[] BuildParameters(string[] columnNames, object[] values)
        {
            if (iFactory == null)
            {
                return null;
            }

            DbProviderFactory factory = iFactory;
            DbParameter[] parameters = new DbParameter[columnNames.Length];
            for (int i = 0; i < columnNames.Length; i++)
            {
                DbParameter parameter = factory.CreateParameter();
                parameter.ParameterName = $"@{columnNames[i]}";
                parameter.Value = values[i];
                parameters[i] = parameter;
            }
            return parameters;
        }

        private DbConnection GetDbConnection(DatabaseType dbType, string connectionString)
        {
            DbProviderFactory factory = GetDbProviderFactory(dbType);
            DbConnection connection = factory.CreateConnection();
            connection.ConnectionString = connectionString;
            return connection;
        }

        private DbCommand GetDbCommand(DatabaseType dbType, string commandText, DbConnection connection)
        {
            DbProviderFactory factory = GetDbProviderFactory(dbType);
            DbCommand command = factory.CreateCommand();
            command.Connection = connection;
            command.CommandText = commandText;
            return command;
        }

        private DbParameter GetDbParameter(DatabaseType dbType, string parameterName, Type dataType)
        {
            DbProviderFactory factory = GetDbProviderFactory(dbType);
            DbParameter parameter = factory.CreateParameter();
            parameter.ParameterName = parameterName;

            // 根据数据类型设置适当的 DbType
            if (dataType == typeof(int) || dataType == typeof(long))
                parameter.DbType = DbType.Int32;
            else if (dataType == typeof(decimal))
                parameter.DbType = DbType.Decimal;
            else if (dataType == typeof(double) || dataType == typeof(float))
                parameter.DbType = DbType.Double;
            else if (dataType == typeof(DateTime))
                parameter.DbType = DbType.DateTime;
            else if (dataType == typeof(bool))
                parameter.DbType = DbType.Boolean;
            else
                parameter.DbType = DbType.String;

            return parameter;
        }

        public void CreateTableFromDataTable(DatabaseType dbType, string connectionString, string tableName, DataTable dataTable)
        {
            using (var conn = GetDbConnection(dbType, connectionString))
            {
                conn.Open();

                // 检查表是否存在，如果存在则删除
                if (TableExists(conn, tableName))
                {
                    using (var cmd = GetDbCommand(dbType, $"DROP TABLE {tableName}", conn))
                    {
                        cmd.ExecuteNonQuery();
                    }
                }

                // 生成创建表的SQL
                var createSql = new StringBuilder($"CREATE TABLE {tableName} (");

                foreach (DataColumn column in dataTable.Columns)
                {
                    string sqlType = GetSqlTypeFromColumn(column);
                    createSql.Append($"{column.ColumnName} {sqlType}, ");
                }

                createSql.Length -= 2; // 移除最后的逗号和空格
                createSql.Append(")");

                // 执行创建表
                using (var cmd = GetDbCommand(dbType, createSql.ToString(), conn))
                {
                    cmd.ExecuteNonQuery();
                }
            }
        }

        private string GetSqlTypeFromColumn(DataColumn column)
        {
            if (column.DataType == typeof(int) || column.DataType == typeof(long))
                return "INTEGER";
            if (column.DataType == typeof(decimal) || column.DataType == typeof(double) || column.DataType == typeof(float))
                return "REAL";
            if (column.DataType == typeof(DateTime))
                return "TEXT"; // SQLite没有专门的日期类型
            if (column.DataType == typeof(bool))
                return "INTEGER"; // SQLite用0和1表示布尔值
            return "TEXT"; // 默认文本类型
        }

        public int Count(DatabaseType databaseType, string connectionString, string tableName, string condition = "")
        {
            DbProviderFactory factory = GetDbProviderFactory(databaseType);
            string sql = string.IsNullOrEmpty(condition)
                ? $"SELECT COUNT(*) FROM {tableName}"
                : $"SELECT COUNT(*) FROM {tableName} WHERE {condition}";

            using (DbConnection connection = factory.CreateConnection())
            {
                connection.ConnectionString = connectionString;
                connection.Open();
                using (DbCommand command = connection.CreateCommand())
                {
                    command.CommandText = sql;
                    object result = command.ExecuteScalar();
                    return Convert.ToInt32(result);
                }
            }
        }

        // 参数化查询版本（更安全）
        public int Count(DatabaseType databaseType, string connectionString, string tableName,
                        string[] paramNames, object[] paramValues, string condition = "")
        {
            DbProviderFactory factory = GetDbProviderFactory(databaseType);
            string sql = $"SELECT COUNT(*) FROM {tableName}";

            if (!string.IsNullOrEmpty(condition))
            {
                sql += " WHERE " + condition;
            }

            using (DbConnection connection = factory.CreateConnection())
            {
                connection.ConnectionString = connectionString;
                connection.Open();
                using (DbCommand command = connection.CreateCommand())
                {
                    command.CommandText = sql;

                    // 添加参数
                    if (paramNames != null && paramValues != null)
                    {
                        DbParameter[] parameters = BuildParameters(databaseType, paramNames, paramValues);
                        foreach (var param in parameters)
                        {
                            command.Parameters.Add(param);
                        }
                    }

                    object result = command.ExecuteScalar();
                    return Convert.ToInt32(result);
                }
            }
        }
        private bool TableExists(DbConnection connection, string tableName)
        {
            try
            {
                // 通用表存在检查方法（针对不同数据库可能需要调整）
                string sql = "";
                DatabaseType dbType = DatabaseType.SQLite; // 默认

                if (connection is SQLiteConnection)
                    dbType = DatabaseType.SQLite;
                else if (connection is SqlConnection)
                    dbType = DatabaseType.SqlServer;
                else if (connection is MySqlConnection)
                    dbType = DatabaseType.MySql;

                switch (dbType)
                {
                    case DatabaseType.SQLite:
                        sql = $"SELECT 1 FROM sqlite_master WHERE type='table' AND name='{tableName}'";
                        break;
                    case DatabaseType.SqlServer:
                        sql = $"SELECT 1 FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME = '{tableName}'";
                        break;
                    case DatabaseType.MySql:
                        sql = $"SELECT 1 FROM information_schema.tables WHERE table_schema = DATABASE() AND table_name = '{tableName}'";
                        break;
                }

                using (var cmd = connection.CreateCommand())
                {
                    cmd.CommandText = sql;
                    return cmd.ExecuteScalar() != null;
                }
            }
            catch
            {
                return false;
            }
        }

        //显示进度, DataTable dataTable, Action<int> progressCallback = null
        public void BulkInsert(DatabaseType dbType, string connectionString, string tableName, DataTable dataTable, Action<int> progressCallback = null)
        {
            if (dataTable == null || dataTable.Rows.Count == 0) return;

            using (var conn = GetDbConnection(dbType, connectionString))
            {
                conn.Open();
                using (var transaction = conn.BeginTransaction())
                {
                    try
                    {
                        // 准备列名
                        var columnNames = dataTable.Columns.Cast<DataColumn>()
                            .Select(c => c.ColumnName)
                            .ToArray();

                        // 构建参数化插入语句
                        string sql = $"INSERT INTO {tableName} ({string.Join(", ", columnNames)}) " +
                                     $"VALUES ({string.Join(", ", columnNames.Select(c => "@" + c))})";

                        using (var cmd = GetDbCommand(dbType, sql, conn))
                        {
                            cmd.Transaction = transaction;

                            // 添加参数
                            foreach (DataColumn column in dataTable.Columns)
                            {
                                cmd.Parameters.Add(GetDbParameter(dbType, "@" + column.ColumnName, column.DataType));
                            }

                            // 插入每一行
                            for (int i = 0; i < dataTable.Rows.Count; i++)
                            {
                                DataRow row = dataTable.Rows[i];
                                foreach (DataColumn column in dataTable.Columns)
                                {
                                    cmd.Parameters["@" + column.ColumnName].Value = row[column] ?? DBNull.Value;
                                }
                                cmd.ExecuteNonQuery();

                                //// 报告进度
                                //if (progressCallback != null && (i % 100 == 0 || i == dataTable.Rows.Count - 1))
                                //{
                                //    int percent = (i + 1) * 100 / dataTable.Rows.Count;
                                //    progressCallback(percent);
                                //}
                            }
                        }

                        transaction.Commit();
                    }
                    catch (Exception ex)
                    {
                        transaction.Rollback();
                        throw new Exception("批量插入失败: " + ex.Message, ex);
                    }
                }
            }
        }

        /// <summary>
        /// 统计指定列的不同值的数量（DISTINCT COUNT）
        /// </summary>
        /// <param name="databaseType">数据库类型</param>
        /// <param name="connectionString">连接字符串</param>
        /// <param name="tableName">表名</param>
        /// <param name="distinctColumn">要统计的列名</param>
        /// <param name="condition">查询条件（可选）</param>
        /// <returns>不同值的数量</returns>
        public int CountDistinct(DatabaseType databaseType, string connectionString,
                                string tableName, string distinctColumn, string condition = "")
        {
            DbProviderFactory factory = GetDbProviderFactory(databaseType);

            // 构建SQL：COUNT(DISTINCT column)
            string sql = string.IsNullOrEmpty(condition)
                ? $"SELECT COUNT(DISTINCT {distinctColumn}) FROM {tableName}"
                : $"SELECT COUNT(DISTINCT {distinctColumn}) FROM {tableName} WHERE {condition}";

            using (DbConnection connection = factory.CreateConnection())
            {
                connection.ConnectionString = connectionString;
                connection.Open();
                using (DbCommand command = connection.CreateCommand())
                {
                    command.CommandText = sql;
                    object result = command.ExecuteScalar();
                    return Convert.ToInt32(result);
                }
            }
        }



    }
}
