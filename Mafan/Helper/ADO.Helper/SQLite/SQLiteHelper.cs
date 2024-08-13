using System;
using System.Data;
using System.Data.SQLite;

using System.Threading.Tasks;

namespace Mafan.Helper.ADO.Helper.SQLite
{
    public class SQLiteHelper : IDisposable
    {
        private readonly string _connectionString;

        public SQLiteHelper(string connectionString)
        {
            _connectionString = connectionString;
        }

        // 创建并返回一个SQLiteConnection
        private SQLiteConnection CreateConnection()
        {
            return new SQLiteConnection(_connectionString);
        }

        // 打开连接（同步）
        private void OpenConnection(SQLiteConnection connection)
        {
            if (connection.State != ConnectionState.Open)
            {
                connection.Open();
            }
        }

        // 打开连接（异步）
        private async Task OpenConnectionAsync(SQLiteConnection connection)
        {
            if (connection.State != ConnectionState.Open)
            {
                await connection.OpenAsync();
            }
        }

        // 执行查询，返回第一行第一列的值（同步）
        public object ExecuteScalar(string commandText, params SQLiteParameter[] parameters)
        {
            using (var connection = CreateConnection())
            {
                OpenConnection(connection);
                using (var command = new SQLiteCommand(commandText, connection))
                {
                    if (parameters != null)
                    {
                        command.Parameters.AddRange(parameters);
                    }
                    return command.ExecuteScalar();
                }
            }
        }

        // 执行查询，返回第一行第一列的值（异步）
        public async Task<object> ExecuteScalarAsync(string commandText, params SQLiteParameter[] parameters)
        {
            using (var connection = CreateConnection())
            {
                await OpenConnectionAsync(connection);
                using (var command = new SQLiteCommand(commandText, connection))
                {
                    if (parameters != null)
                    {
                        command.Parameters.AddRange(parameters);
                    }
                    return await command.ExecuteScalarAsync();
                }
            }
        }

        // 执行查询，返回SQLiteDataReader（同步）
        public SQLiteDataReader ExecuteReader(string commandText, params SQLiteParameter[] parameters)
        {
            var connection = CreateConnection();
            OpenConnection(connection);
            var command = new SQLiteCommand(commandText, connection);
            if (parameters != null)
            {
                command.Parameters.AddRange(parameters);
            }
            return command.ExecuteReader(CommandBehavior.CloseConnection);
        }

        // 执行查询，返回SQLiteDataReader（异步）
        public async Task<SQLiteDataReader> ExecuteReaderAsync(string commandText, params SQLiteParameter[] parameters)
        {
            var connection = CreateConnection();
            await OpenConnectionAsync(connection);
            var command = new SQLiteCommand(commandText, connection);
            if (parameters != null)
            {
                command.Parameters.AddRange(parameters);
            }
            return (SQLiteDataReader)await command.ExecuteReaderAsync(CommandBehavior.CloseConnection);
        }

        // 执行查询，返回DataTable（同步）
        public DataTable ExecuteDataTable(string commandText, params SQLiteParameter[] parameters)
        {
            using (var connection = CreateConnection())
            {
                OpenConnection(connection);
                using (var command = new SQLiteCommand(commandText, connection))
                {
                    if (parameters != null)
                    {
                        command.Parameters.AddRange(parameters);
                    }
                    using (var adapter = new SQLiteDataAdapter(command))
                    {
                        var dataTable = new DataTable();
                        adapter.Fill(dataTable);
                        return dataTable;
                    }
                }
            }
        }

        // 执行查询，返回DataTable（异步）
        public async Task<DataTable> ExecuteDataTableAsync(string commandText, params SQLiteParameter[] parameters)
        {
            using (var connection = CreateConnection())
            {
                await OpenConnectionAsync(connection);
                using (var command = new SQLiteCommand(commandText, connection))
                {
                    if (parameters != null)
                    {
                        command.Parameters.AddRange(parameters);
                    }
                    using (var adapter = new SQLiteDataAdapter(command))
                    {
                        var dataTable = new DataTable();
                        await Task.Run(() => adapter.Fill(dataTable)); // 使用Task.Run包装同步方法
                        return dataTable;
                    }
                }
            }
        }

        // 执行SQL命令（同步）
        public int ExecuteNonQuery(string commandText, params SQLiteParameter[] parameters)
        {
            using (var connection = CreateConnection())
            {
                OpenConnection(connection);
                using (var command = new SQLiteCommand(commandText, connection))
                {
                    if (parameters != null)
                    {
                        command.Parameters.AddRange(parameters);
                    }
                    return command.ExecuteNonQuery();
                }
            }
        }

        // 执行SQL命令（异步）
        public async Task<int> ExecuteNonQueryAsync(string commandText, params SQLiteParameter[] parameters)
        {
            using (var connection = CreateConnection())
            {
                await OpenConnectionAsync(connection);
                using (var command = new SQLiteCommand(commandText, connection))
                {
                    if (parameters != null)
                    {
                        command.Parameters.AddRange(parameters);
                    }
                    return await command.ExecuteNonQueryAsync();
                }
            }
        }

        // 开始事务（同步）
        public SQLiteTransaction BeginTransaction()
        {
            var connection = CreateConnection();
            OpenConnection(connection);
            return connection.BeginTransaction();
        }

        // 开始事务（异步）
        public async Task<SQLiteTransaction> BeginTransactionAsync()
        {
            var connection = CreateConnection();
            await OpenConnectionAsync(connection);
            return connection.BeginTransaction();
        }

        // 提交事务（同步）
        public void CommitTransaction(SQLiteTransaction transaction)
        {
            transaction?.Commit();
            transaction.Connection?.Close();
        }

        // 提交事务（异步）
        public async Task CommitTransactionAsync(SQLiteTransaction transaction)
        {
            if (transaction != null)
            {
                await Task.Run(() => transaction.Commit());
                transaction.Connection?.Close();
            }
        }

        // 回滚事务（同步）
        public void RollbackTransaction(SQLiteTransaction transaction)
        {
            transaction?.Rollback();
            transaction.Connection?.Close();
        }

        // 回滚事务（异步）
        public async Task RollbackTransactionAsync(SQLiteTransaction transaction)
        {
            if (transaction != null)
            {
                await Task.Run(() => transaction.Rollback());
                transaction.Connection?.Close();
            }
        }

        // 实现IDisposable接口
        public void Dispose()
        {
            // 清理资源
        }
    }
}
