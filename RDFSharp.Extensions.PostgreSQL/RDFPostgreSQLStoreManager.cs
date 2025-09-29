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
using Npgsql;

namespace RDFSharp.Extensions.PostgreSQL
{
    /// <summary>
    /// RDFPostgreSQLStoreManager handles initialization of a store backed on PostgreSQL engine
    /// </summary>

    internal class RDFPostgreSQLStoreManager
    {
        private readonly string _connectionString;

        internal RDFPostgreSQLStoreManager(string connectionString)
        {
            _connectionString = connectionString;
        }

        internal async Task EnsureQuadruplesTableExistsAsync()
        {
            using (NpgsqlConnection pgSqlConnection = new NpgsqlConnection(_connectionString))
            {
                await pgSqlConnection.OpenAsync();
                if (!await TableExistsAsync(pgSqlConnection, "Quadruples"))
                    await CreateQuadruplesTableAsync(pgSqlConnection);
            }
        }

        private async Task<bool> TableExistsAsync(NpgsqlConnection connection, string tableName)
        {
            try
            {
                using (NpgsqlCommand command = new NpgsqlCommand($"SELECT COUNT(*) FROM pg_catalog.pg_class c JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace WHERE n.nspname = 'public' AND c.relname = '{tableName.ToLower()}'", connection))
                {
                    object result = await command.ExecuteScalarAsync();
                    return Convert.ToInt32(result) > 0;
                }
            }
            catch
            {
                return false;
            }
        }

        private async Task CreateQuadruplesTableAsync(NpgsqlConnection connection)
        {
            using (NpgsqlCommand createCommand = new NpgsqlCommand("CREATE TABLE quadruples (\"quadrupleid\" BIGINT NOT NULL PRIMARY KEY, \"tripleflavor\" INTEGER NOT NULL, \"contextid\" bigint NOT NULL, \"context\" VARCHAR NOT NULL, \"subjectid\" BIGINT NOT NULL, \"subject\" VARCHAR NOT NULL, \"predicateid\" BIGINT NOT NULL, \"predicate\" VARCHAR NOT NULL, \"objectid\" BIGINT NOT NULL, \"object\" VARCHAR NOT NULL);CREATE INDEX \"idx_contextid\" ON quadruples USING btree (\"contextid\");CREATE INDEX \"idx_subjectid\" ON quadruples USING btree (\"subjectid\");CREATE INDEX \"idx_predicateid\" ON quadruples USING btree (\"predicateid\");CREATE INDEX \"idx_objectid\" ON quadruples USING btree (\"objectid\",\"tripleflavor\");CREATE INDEX \"idx_subjectid_predicateid\" ON quadruples USING btree (\"subjectid\",\"predicateid\");CREATE INDEX \"idx_subjectid_objectid\" ON quadruples USING btree (\"subjectid\",\"objectid\",\"tripleflavor\");CREATE INDEX \"idx_predicateid_objectid\" ON quadruples USING btree (\"predicateid\",\"objectid\",\"tripleflavor\");", connection))
            {
                await createCommand.ExecuteNonQueryAsync();
            }
        }

        public async Task<NpgsqlConnection> GetConnectionAsync()
        {
            NpgsqlConnection connection = new NpgsqlConnection(_connectionString);
            await connection.OpenAsync();
            return connection;
        }
    }
}