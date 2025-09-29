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
using Oracle.ManagedDataAccess.Client;

namespace RDFSharp.Extensions.Oracle
{
    /// <summary>
    /// RDFOracleStoreManager handles initialization of a store backed on Oracle engine
    /// </summary>

    internal class RDFOracleStoreManager
    {
        private readonly string _connectionString;

        internal RDFOracleStoreManager(string connectionString)
        {
            _connectionString = connectionString;
        }

        internal void EnsureQuadruplesTableExists()
        {
            using (OracleConnection oracleConnection = new OracleConnection(_connectionString))
            {
                oracleConnection.Open();
                if (!TableExists(oracleConnection, "Quadruples"))
                    CreateQuadruplesTable(oracleConnection, new OracleConnectionStringBuilder(oracleConnection.ConnectionString));
            }
        }

        private bool TableExists(OracleConnection connection, string tableName)
        {
            try
            {
                using (OracleCommand command = new OracleCommand($"SELECT COUNT(*) FROM ALL_OBJECTS WHERE OBJECT_TYPE = 'TABLE' AND OBJECT_NAME = '{tableName.ToUpper()}'", connection))
                {
                    object result = command.ExecuteScalar();
                    return Convert.ToInt32(result) > 0;
                }
            }
            catch
            {
                return false;
            }
        }

        private void CreateQuadruplesTable(OracleConnection connection, OracleConnectionStringBuilder connectionBuilder)
        {
            using (OracleCommand createCommand = new OracleCommand($"CREATE TABLE {connectionBuilder.UserID}.QUADRUPLES(QUADRUPLEID NUMBER(19, 0) NOT NULL ENABLE,TRIPLEFLAVOR NUMBER(10, 0) NOT NULL ENABLE,CONTEXTID NUMBER(19, 0) NOT NULL ENABLE,CONTEXT VARCHAR2(1000) NOT NULL ENABLE,SUBJECTID NUMBER(19, 0) NOT NULL ENABLE,SUBJECT VARCHAR2(1000) NOT NULL ENABLE,PREDICATEID NUMBER(19, 0) NOT NULL ENABLE,PREDICATE VARCHAR2(1000) NOT NULL ENABLE,OBJECTID NUMBER(19, 0) NOT NULL ENABLE,OBJECT VARCHAR2(1000) NOT NULL ENABLE,PRIMARY KEY(QUADRUPLEID) ENABLE)", connection))
            {
                createCommand.ExecuteNonQuery();
                createCommand.CommandText = $"CREATE INDEX {connectionBuilder.UserID}.IDX_CONTEXTID ON {connectionBuilder.UserID}.QUADRUPLES(CONTEXTID)";
                createCommand.ExecuteNonQuery();
                createCommand.CommandText = $"CREATE INDEX {connectionBuilder.UserID}.IDX_SUBJECTID ON {connectionBuilder.UserID}.QUADRUPLES(SUBJECTID)";
                createCommand.ExecuteNonQuery();
                createCommand.CommandText = $"CREATE INDEX {connectionBuilder.UserID}.IDX_PREDICATEID ON {connectionBuilder.UserID}.QUADRUPLES(PREDICATEID)";
                createCommand.ExecuteNonQuery();
                createCommand.CommandText = $"CREATE INDEX {connectionBuilder.UserID}.IDX_OBJECTID ON {connectionBuilder.UserID}.QUADRUPLES(OBJECTID,TRIPLEFLAVOR)";
                createCommand.ExecuteNonQuery();
                createCommand.CommandText = $"CREATE INDEX {connectionBuilder.UserID}.IDX_SUBJECTID_PREDICATEID ON {connectionBuilder.UserID}.QUADRUPLES(SUBJECTID,PREDICATEID)";
                createCommand.ExecuteNonQuery();
                createCommand.CommandText = $"CREATE INDEX {connectionBuilder.UserID}.IDX_SUBJECTID_OBJECTID ON {connectionBuilder.UserID}.QUADRUPLES(SUBJECTID,OBJECTID,TRIPLEFLAVOR)";
                createCommand.ExecuteNonQuery();
                createCommand.CommandText = $"CREATE INDEX {connectionBuilder.UserID}.IDX_PREDICATEID_OBJECTID ON {connectionBuilder.UserID}.QUADRUPLES(PREDICATEID,OBJECTID,TRIPLEFLAVOR)";
                createCommand.ExecuteNonQuery();
            }
        }

        public OracleConnection GetConnection()
        {
            OracleConnection connection = new OracleConnection(_connectionString);
            connection.Open();
            return connection;
        }
    }
}