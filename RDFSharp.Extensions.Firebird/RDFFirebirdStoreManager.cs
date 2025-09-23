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
using System.IO;
using System.Threading.Tasks;
using FirebirdSql.Data.FirebirdClient;

namespace RDFSharp.Extensions.Firebird
{
    /// <summary>
    /// RDFFirebirdStoreManager handles initialization of a store backed on Firebird engine
    /// </summary>

    internal class RDFFirebirdStoreManager
    {
        private readonly string _connectionString;
        private readonly string _databasePath;
        
        internal RDFFirebirdStoreManager(string connectionString)
        {
            _connectionString = connectionString;
            _databasePath = ExtractDatabasePath(connectionString);
        }
        
        internal async Task InitializeDatabaseAndTableAsync()
        {
            await EnsureDatabaseExistsAsync();
            await EnsureQuadruplesTableExistsAsync();
        }
        
        private async Task EnsureDatabaseExistsAsync()
        {
            if (File.Exists(_databasePath))
            {
                using (FbConnection fbConnection = new FbConnection(_connectionString))
                    await fbConnection.OpenAsync();
            }
            else
            {
                string directory = Path.GetDirectoryName(_databasePath);
                if (!Directory.Exists(directory))
                    Directory.CreateDirectory(directory);
                await FbConnection.CreateDatabaseAsync(_connectionString);
            }
        }
        
        private async Task EnsureQuadruplesTableExistsAsync()
        {
            using (FbConnection fbConnection = new FbConnection(_connectionString))
            {
                await fbConnection.OpenAsync();
                if (!await TableExistsAsync(fbConnection, "Quadruples"))
                    await CreateQuadruplesTableAsync(fbConnection);
            }
        }
        
        private async Task<bool> TableExistsAsync(FbConnection connection, string tableName)
        {
            try
            {
                using (FbCommand command = new FbCommand("SELECT COUNT(*) FROM RDB$RELATIONS WHERE RDB$RELATION_NAME = @TableName AND RDB$SYSTEM_FLAG = 0", connection))
                {
                    command.Parameters.AddWithValue("@TableName", tableName.ToUpper());
                    object result = await command.ExecuteScalarAsync();
                    return Convert.ToInt32(result) > 0;
                }
            }
            catch
            {
                return false;
            }
        }
        
        private async Task CreateQuadruplesTableAsync(FbConnection connection)
        {
            FbCommand createCommand = new FbCommand("CREATE TABLE Quadruples (QuadrupleID BIGINT NOT NULL PRIMARY KEY, TripleFlavor INTEGER NOT NULL, Context VARCHAR(1000) NOT NULL, ContextID BIGINT NOT NULL, Subject VARCHAR(1000) NOT NULL, SubjectID BIGINT NOT NULL, Predicate VARCHAR(1000) NOT NULL, PredicateID BIGINT NOT NULL, Object VARCHAR(1000) NOT NULL, ObjectID BIGINT NOT NULL)", connection) { CommandTimeout = 120 };
            await createCommand.ExecuteNonQueryAsync();
            createCommand.CommandText = "CREATE UNIQUE INDEX IDX_QuadrupleID_UNIQUE ON Quadruples(QuadrupleID)";
            await createCommand.ExecuteNonQueryAsync();
            createCommand.CommandText = "CREATE INDEX IDX_ContextID ON Quadruples(ContextID)";
            await createCommand.ExecuteNonQueryAsync();
            createCommand.CommandText = "CREATE INDEX IDX_SubjectID ON Quadruples(SubjectID)";
            await createCommand.ExecuteNonQueryAsync();
            createCommand.CommandText = "CREATE INDEX IDX_PredicateID ON Quadruples(PredicateID)";
            await createCommand.ExecuteNonQueryAsync();
            createCommand.CommandText = "CREATE INDEX IDX_ObjectID ON Quadruples(ObjectID,TripleFlavor)";
            await createCommand.ExecuteNonQueryAsync();
            createCommand.CommandText = "CREATE INDEX IDX_SubjectID_PredicateID ON Quadruples(SubjectID,PredicateID)";
            await createCommand.ExecuteNonQueryAsync();
            createCommand.CommandText = "CREATE INDEX IDX_SubjectID_ObjectID ON Quadruples(SubjectID,ObjectID,TripleFlavor)";
            await createCommand.ExecuteNonQueryAsync();
            createCommand.CommandText = "CREATE INDEX IDX_PredicateID_ObjectID ON Quadruples(PredicateID,ObjectID,TripleFlavor)";
            await createCommand.ExecuteNonQueryAsync();
            createCommand.Dispose();
        }
        
        private string ExtractDatabasePath(string connectionString)
        {
            foreach (string part in connectionString.Split(';'))
            {
                if (part.Trim().StartsWith("Database=", StringComparison.OrdinalIgnoreCase))
                    return part.Substring(9).Trim();
            }
            throw new ArgumentException("Database path not found in connection string");
        }
        
        public async Task<FbConnection> GetConnectionAsync()
        {
            FbConnection connection = new FbConnection(_connectionString);
            await connection.OpenAsync();
            return connection;
        }
    }
}