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
using System.Reflection;
using System.Threading.Tasks;

namespace RDFSharp.Extensions.SQLite
{
    /// <summary>
    /// RDFSQLiteStoreManager handles initialization of a store backed on SQLite engine
    /// </summary>

    internal class RDFSQLiteStoreManager
    {
        private readonly string _databasePath;

        internal RDFSQLiteStoreManager(string databasePath)
            => _databasePath = databasePath;

        internal Task InitializeDatabaseAndTableAsync()
            => EnsureDatabaseExistsAsync();

        private async Task EnsureDatabaseExistsAsync()
        {
            if (File.Exists(_databasePath))
            {
                await EnsureQuadruplesTableExistsAsync();
                return;
            }

            //Template
            string directory = Path.GetDirectoryName(_databasePath);
            if (!Directory.Exists(directory))
                Directory.CreateDirectory(directory);
            using (Stream templateDB = Assembly.GetExecutingAssembly().GetManifestResourceStream("RDFSharp.Extensions.SQLite.RDFSQLiteTemplate.db"))
            {
                using (FileStream targetDB = new FileStream(_databasePath, FileMode.Create, FileAccess.ReadWrite))
#if NET8_0_OR_GREATER
                    await templateDB.CopyToAsync(targetDB);
#else
                    templateDB.CopyTo(targetDB);
#endif
            }
        }

        private async Task EnsureQuadruplesTableExistsAsync()
        {
            using (SQLiteConnection sqliteConnection = new SQLiteConnection($"Data Source={_databasePath}"))
            {
                await sqliteConnection.OpenAsync();
                if (!await TableExistsAsync(sqliteConnection, "Quadruples"))
                    await CreateQuadruplesTableAsync(sqliteConnection);
            }
        }

        private async Task<bool> TableExistsAsync(SQLiteConnection connection, string tableName)
        {
            using (SQLiteCommand command = new SQLiteCommand($"SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name='{tableName}'", connection))
            {
                object result = await command.ExecuteScalarAsync();
                return Convert.ToInt32(result) > 0;
            }
        }

        private async Task CreateQuadruplesTableAsync(SQLiteConnection connection)
        {
            using (SQLiteCommand createCommand = new SQLiteCommand("CREATE TABLE Quadruples (QuadrupleID INTEGER NOT NULL PRIMARY KEY, TripleFlavor INTEGER NOT NULL, Context VARCHAR(1000) NOT NULL, ContextID INTEGER NOT NULL, Subject VARCHAR(1000) NOT NULL, SubjectID INTEGER NOT NULL, Predicate VARCHAR(1000) NOT NULL, PredicateID INTEGER NOT NULL, Object VARCHAR(1000) NOT NULL, ObjectID INTEGER NOT NULL);CREATE INDEX IDX_ContextID ON Quadruples (ContextID);CREATE INDEX IDX_SubjectID ON Quadruples (SubjectID);CREATE INDEX IDX_PredicateID ON Quadruples (PredicateID);CREATE INDEX IDX_ObjectID ON Quadruples (ObjectID,TripleFlavor);CREATE INDEX IDX_SubjectID_PredicateID ON Quadruples (SubjectID,PredicateID);CREATE INDEX IDX_SubjectID_ObjectID ON Quadruples (SubjectID,ObjectID,TripleFlavor);CREATE INDEX IDX_PredicateID_ObjectID ON Quadruples (PredicateID,ObjectID,TripleFlavor);", connection))
                await createCommand.ExecuteNonQueryAsync();
        }

        public async Task<SQLiteConnection> GetConnectionAsync()
        {
            SQLiteConnection connection = new SQLiteConnection($"Data Source={_databasePath}");
            await connection.OpenAsync();
            return connection;
        }
    }
}