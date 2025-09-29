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
using System.Data.SQLite;
using System.IO;
using System.Threading.Tasks;

namespace RDFSharp.Extensions.SQLite
{
    /// <summary>
    /// RDFSQLiteStoreManager handles initialization of a store backed on SQLite engine
    /// </summary>

    internal class RDFSQLiteStoreManager
    {
        private readonly string _connectionString;

        internal RDFSQLiteStoreManager(string connectionString)
            => _connectionString = connectionString;

        internal async Task InitializeDatabaseAndTableAsync()
        {
            await EnsureDatabaseExistsAsync();
            await EnsureQuadruplesTableExistsAsync();
        }

        private async Task EnsureDatabaseExistsAsync()
        {
            string dataSource = new SQLiteConnectionStringBuilder(_connectionString).DataSource;
            if (File.Exists(dataSource))
            {
                using (SQLiteConnection sqliteConnection = new SQLiteConnection(_connectionString))
                    await sqliteConnection.OpenAsync();
            }
            else
            {
                string directory = Path.GetDirectoryName(dataSource);
                if (!string.IsNullOrEmpty(directory) && !Directory.Exists(directory))
                    Directory.CreateDirectory(directory);
                SQLiteConnection.CreateFile(dataSource);
            }
        }

        private async Task EnsureQuadruplesTableExistsAsync()
        {
            using (SQLiteConnection sqliteConnection = new SQLiteConnection(_connectionString))
            {
                await sqliteConnection.OpenAsync();
                if (!await TableExistsAsync(sqliteConnection, "Quadruples"))
                    await CreateQuadruplesTableAsync(sqliteConnection);
            }
        }

        private async Task<bool> TableExistsAsync(SQLiteConnection sqliteConnection, string tableName)
        {
            using (SQLiteCommand command = new SQLiteCommand($"SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name='{tableName}'", sqliteConnection))
            {
                object result = await command.ExecuteScalarAsync();
                return Convert.ToInt32(result) > 0;
            }
        }

        private async Task CreateQuadruplesTableAsync(SQLiteConnection sqliteConnection)
        {
            using (SQLiteCommand createCommand = sqliteConnection.CreateCommand())
            {
                createCommand.CommandText = @"
                    CREATE TABLE IF NOT EXISTS Quadruples (
                        QuadrupleID INTEGER PRIMARY KEY,
                        TripleFlavor INTEGER NOT NULL,
                        Context TEXT,
                        ContextID INTEGER NOT NULL,
                        Subject TEXT,
                        SubjectID INTEGER NOT NULL,
                        Predicate TEXT,
                        PredicateID INTEGER NOT NULL,
                        Object TEXT,
                        ObjectID INTEGER NOT NULL
                    );";
                await createCommand.ExecuteNonQueryAsync();
                createCommand.CommandText = "CREATE UNIQUE INDEX IF NOT EXISTS IX_Quadruples_QuadrupleID ON Quadruples(QuadrupleID);";
                await createCommand.ExecuteNonQueryAsync();
                createCommand.CommandText = "CREATE INDEX IF NOT EXISTS IX_Quadruples_ContextID ON Quadruples(ContextID);";
                await createCommand.ExecuteNonQueryAsync();
                createCommand.CommandText = "CREATE INDEX IF NOT EXISTS IX_Quadruples_SubjectID ON Quadruples(SubjectID);";
                await createCommand.ExecuteNonQueryAsync();
                createCommand.CommandText = "CREATE INDEX IF NOT EXISTS IX_Quadruples_PredicateID ON Quadruples(PredicateID);";
                await createCommand.ExecuteNonQueryAsync();
                createCommand.CommandText = "CREATE INDEX IF NOT EXISTS IX_Quadruples_ObjectID_TripleFlavor ON Quadruples(ObjectID, TripleFlavor);";
                await createCommand.ExecuteNonQueryAsync();
                createCommand.CommandText = "CREATE INDEX IF NOT EXISTS IX_Quadruples_SubjectID_PredicateID ON Quadruples(SubjectID, PredicateID);";
                await createCommand.ExecuteNonQueryAsync();
                createCommand.CommandText = "CREATE INDEX IF NOT EXISTS IX_Quadruples_SubjectID_ObjectID_TripleFlavor ON Quadruples(SubjectID, ObjectID, TripleFlavor);";
                await createCommand.ExecuteNonQueryAsync();
                createCommand.CommandText = "CREATE INDEX IF NOT EXISTS IX_Quadruples_PredicateID_ObjectID_TripleFlavor ON Quadruples(PredicateID, ObjectID, TripleFlavor);";
                await createCommand.ExecuteNonQueryAsync();
            }
        }

        public async Task<SQLiteConnection> GetConnectionAsync()
        {
            SQLiteConnection sqliteConnection = new SQLiteConnection(_connectionString);
            await sqliteConnection.OpenAsync();
            return sqliteConnection;
        }
    }
}