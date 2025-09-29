/*
   Copyright 2012-2025 Marco De Salvo

   Licensed under the Apache License, Version 2.0 (the "License");
   you may not use this file except in compliance with the License.
   You may obtain a copy of the License at

     http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.
*/

using System;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Data.SqlClient;

namespace RDFSharp.Extensions.SQLServer
{
    /// <summary>
    /// RDFSQLServerStoreManager handles initialization of a store backed on SQL Server engine
    /// </summary>

    internal class RDFSQLServerStoreManager
    {
        private readonly string _connectionString;

        internal RDFSQLServerStoreManager(string connectionString)
        {
            _connectionString = connectionString;
        }

        internal async Task EnsureQuadruplesTableExistsAsync()
        {
            using (SqlConnection sqlServerConnection = new SqlConnection(_connectionString))
            {
#if NET8_0_OR_GREATER
                await sqlServerConnection.OpenAsync();
#else
                sqlServerConnection.Open();
#endif
                if (!await TableExistsAsync(sqlServerConnection, "Quadruples"))
                    await CreateQuadruplesTableAsync(sqlServerConnection);
            }
        }

        private async Task<bool> TableExistsAsync(SqlConnection sqlServerConnection, string tableName)
        {
            try
            {
                using (SqlCommand command = new SqlCommand($"SELECT COUNT(*) FROM sys.tables WHERE name='{tableName}' AND type_desc='USER_TABLE'", sqlServerConnection))
                {
#if NET8_0_OR_GREATER
                    object result = await command.ExecuteScalarAsync(CancellationToken.None);
#else
                    object result = command.ExecuteScalar();
#endif
                    return Convert.ToInt32(result) > 0;
                }
            }
            catch
            {
                return false;
            }
        }

        private async Task CreateQuadruplesTableAsync(SqlConnection sqlServerConnection)
        {
            using (SqlCommand createCommand = new SqlCommand("CREATE TABLE [dbo].[Quadruples] ([QuadrupleID] BIGINT PRIMARY KEY NOT NULL, [TripleFlavor] INTEGER NOT NULL, [Context] VARCHAR(1000) NOT NULL, [ContextID] BIGINT NOT NULL, [Subject] VARCHAR(1000) NOT NULL, [SubjectID] BIGINT NOT NULL, [Predicate] VARCHAR(1000) NOT NULL, [PredicateID] BIGINT NOT NULL, [Object] VARCHAR(1000) NOT NULL, [ObjectID] BIGINT NOT NULL); CREATE NONCLUSTERED INDEX [IDX_ContextID] ON [dbo].[Quadruples]([ContextID]);CREATE NONCLUSTERED INDEX [IDX_SubjectID] ON [dbo].[Quadruples]([SubjectID]);CREATE NONCLUSTERED INDEX [IDX_PredicateID] ON [dbo].[Quadruples]([PredicateID]);CREATE NONCLUSTERED INDEX [IDX_ObjectID] ON [dbo].[Quadruples]([ObjectID],[TripleFlavor]);CREATE NONCLUSTERED INDEX [IDX_SubjectID_PredicateID] ON [dbo].[Quadruples]([SubjectID],[PredicateID]);CREATE NONCLUSTERED INDEX [IDX_SubjectID_ObjectID] ON [dbo].[Quadruples]([SubjectID],[ObjectID],[TripleFlavor]);CREATE NONCLUSTERED INDEX [IDX_PredicateID_ObjectID] ON [dbo].[Quadruples]([PredicateID],[ObjectID],[TripleFlavor]);", sqlServerConnection))
            {
#if NET8_0_OR_GREATER
                await createCommand.ExecuteNonQueryAsync();
#else
                createCommand.ExecuteNonQuery();
#endif
            }
        }

        public async Task<SqlConnection> GetConnectionAsync()
        {
            SqlConnection connection = new SqlConnection(_connectionString);
#if NET8_0_OR_GREATER
            await connection.OpenAsync();
#else
            connection.Open();
#endif
            return connection;
        }
    }
}