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
using System.Threading.Tasks;
using MySql.Data.MySqlClient;

namespace RDFSharp.Extensions.MySQL
{
    /// <summary>
    /// RDFMySQLStoreManager handles initialization of a store backed on MySQL engine
    /// </summary>

    internal class RDFMySQLStoreManager
    {
        private readonly string _connectionString;

        internal RDFMySQLStoreManager(string connectionString)
        {
            _connectionString = connectionString;
        }

        internal async Task EnsureQuadruplesTableExistsAsync()
        {
            using (MySqlConnection mySqlConnection = new MySqlConnection(_connectionString))
            {
                await mySqlConnection.OpenAsync();
                if (!await TableExistsAsync(mySqlConnection, "Quadruples"))
                    await CreateQuadruplesTableAsync(mySqlConnection);
            }
        }

        private async Task<bool> TableExistsAsync(MySqlConnection connection, string tableName)
        {
            try
            {
                using (MySqlCommand command = new MySqlCommand("SELECT COUNT(*) FROM information_schema.tables WHERE table_schema = @DatabaseName AND table_name = @TableName", connection))
                {
                    command.Parameters.AddWithValue("@DatabaseName", connection.Database);
                    command.Parameters.AddWithValue("@TableName", tableName);
                    object result = await command.ExecuteScalarAsync();
                    return Convert.ToInt32(result) > 0;
                }
            }
            catch
            {
                return false;
            }
        }

        private async Task CreateQuadruplesTableAsync(MySqlConnection connection)
        {
            using (MySqlCommand createCommand = new MySqlCommand(
                "CREATE TABLE IF NOT EXISTS Quadruples (" +
                "QuadrupleID BIGINT NOT NULL PRIMARY KEY, " +
                "TripleFlavor INT NOT NULL, " +
                "Context VARCHAR(1000) NOT NULL, " +
                "ContextID BIGINT NOT NULL, " +
                "Subject VARCHAR(1000) NOT NULL, " +
                "SubjectID BIGINT NOT NULL, " +
                "Predicate VARCHAR(1000) NOT NULL, " +
                "PredicateID BIGINT NOT NULL, " +
                "Object VARCHAR(1000) NOT NULL, " +
                "ObjectID BIGINT NOT NULL, " +
                "INDEX IDX_ContextID(ContextID), " +
                "INDEX IDX_SubjectID(SubjectID), " +
                "INDEX IDX_PredicateID(PredicateID), " +
                "INDEX IDX_ObjectID(ObjectID,TripleFlavor), " +
                "INDEX IDX_SubjectID_PredicateID(SubjectID,PredicateID), " +
                "INDEX IDX_SubjectID_ObjectID(SubjectID,ObjectID,TripleFlavor), " +
                "INDEX IDX_PredicateID_ObjectID(PredicateID,ObjectID,TripleFlavor)" +
                ") ENGINE=InnoDB;", connection))
            {
                await createCommand.ExecuteNonQueryAsync();
            }
        }

        public async Task<MySqlConnection> GetConnectionAsync()
        {
            MySqlConnection connection = new MySqlConnection(_connectionString);
            await connection.OpenAsync();
            return connection;
        }
    }
}