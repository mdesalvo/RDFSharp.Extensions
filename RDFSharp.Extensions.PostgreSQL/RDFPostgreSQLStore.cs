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
using System.Collections.Generic;
using System.Data;
using System.Threading;
using System.Threading.Tasks;
using Npgsql;
using NpgsqlTypes;
using RDFSharp.Model;
using RDFSharp.Store;

namespace RDFSharp.Extensions.PostgreSQL
{
    /// <summary>
    /// RDFPostgreSQLStore represents a store backed on PostgreSQL engine
    /// </summary>
    #if NET8_0_OR_GREATER
    public sealed class RDFPostgreSQLStore : RDFStore, IDisposable, IAsyncDisposable
    #else
    public sealed class RDFPostgreSQLStore : RDFStore, IDisposable
    #endif
    {
        #region Properties
        /// <summary>
        /// Count of the PostgreSQL database quadruples (-1 in case of errors)
        /// </summary>
        public override long QuadruplesCount
            => GetQuadruplesCountAsync().GetAwaiter().GetResult();

        /// <summary>
        /// Asynchronous count of the PostgreSQL database quadruples (-1 in case of errors)
        /// </summary>
        public override Task<long> QuadruplesCountAsync
            => GetQuadruplesCountAsync();

        /// <summary>
        /// Connection to the PostgreSQL database
        /// </summary>
        private NpgsqlConnection Connection { get; set; }

        /// <summary>
        /// Command to execute SELECT queries on the PostgreSQL database
        /// </summary>
        private NpgsqlCommand SelectCommand { get; set; }

        /// <summary>
        /// Command to execute INSERT queries on the PostgreSQL database
        /// </summary>
        private NpgsqlCommand InsertCommand { get; set; }

        /// <summary>
        /// Command to execute DELETE queries on the PostgreSQL database
        /// </summary>
        private NpgsqlCommand DeleteCommand { get; set; }

        /// <summary>
        /// Flag indicating that the PostgreSQL store instance has already been disposed
        /// </summary>
        private bool Disposed { get; set; }
        #endregion

        #region Ctors
        /// <summary>
        /// Default-ctor to build a PostgreSQL store instance (with eventual options)
        /// </summary>
        public RDFPostgreSQLStore(string pgsqlConnectionString, RDFPostgreSQLStoreOptions pgsqlStoreOptions = null)
        {
            #region Guards
            if (string.IsNullOrEmpty(pgsqlConnectionString))
                throw new RDFStoreException("Cannot connect to PostgreSQL store because: given \"pgsqlConnectionString\" parameter is null or empty.");
            #endregion

            //Initialize options
            if (pgsqlStoreOptions == null)
                pgsqlStoreOptions = new RDFPostgreSQLStoreOptions();

            //Initialize store structures
            try
            {
                RDFPostgreSQLStoreManager pgSqlStoreManager = new RDFPostgreSQLStoreManager(pgsqlConnectionString);
                pgSqlStoreManager.EnsureQuadruplesTableExistsAsync().GetAwaiter().GetResult();

                StoreType = "POSTGRESQL";
                Connection = pgSqlStoreManager.GetConnectionAsync().GetAwaiter().GetResult();
                SelectCommand = new NpgsqlCommand { Connection = Connection, CommandTimeout = pgsqlStoreOptions.SelectTimeout };
                DeleteCommand = new NpgsqlCommand { Connection = Connection, CommandTimeout = pgsqlStoreOptions.DeleteTimeout };
                InsertCommand = new NpgsqlCommand { Connection = Connection, CommandTimeout = pgsqlStoreOptions.InsertTimeout };
                StoreID = RDFModelUtilities.CreateHash(ToString());
                Disposed = false;
            }
            catch (Exception ex)
            {
                throw new RDFStoreException("Cannot create PostgreSQL store because: " + ex.Message, ex);
            }
        }

        /// <summary>
        /// Destroys the PostgreSQL store instance
        /// </summary>
        ~RDFPostgreSQLStore()
            => Dispose(false);
        #endregion

        #region Interfaces
        /// <summary>
        /// Gives the string representation of the PostgreSQL store
        /// </summary>
        public override string ToString()
            => $"{base.ToString()}|SERVER={Connection.DataSource};DATABASE={Connection.Database}";

        /// <summary>
        /// Disposes the PostgreSQL store instance
        /// </summary>
        public void Dispose()
        {
            Dispose(true);
            GC.SuppressFinalize(this);
        }

#if NET8_0_OR_GREATER
        /// <summary>
        /// Asynchronously disposes the PostgreSQL store (IAsyncDisposable)
        /// </summary>
        ValueTask IAsyncDisposable.DisposeAsync()
        {
            Dispose(true);
            GC.SuppressFinalize(this);
            return ValueTask.CompletedTask;
        }
#endif

        /// <summary>
        /// Disposes the PostgreSQL store instance  (business logic of resources disposal)
        /// </summary>
        private void Dispose(bool disposing)
        {
            if (Disposed)
                return;

            if (disposing)
            {
                //Dispose
                SelectCommand?.Dispose();
                InsertCommand?.Dispose();
                DeleteCommand?.Dispose();
                Connection?.Dispose();
                //Delete
                SelectCommand = null;
                InsertCommand = null;
                DeleteCommand = null;
                Connection = null;
            }

            Disposed = true;
        }
        #endregion

        #region Methods

        #region Add
        /// <summary>
        /// Merges the given graph into the store
        /// </summary>
        public override RDFStore MergeGraph(RDFGraph graph)
            => MergeGraphAsync(graph).GetAwaiter().GetResult();

        /// <summary>
        /// Asynchronously merges the given graph into the store
        /// </summary>
        public override async Task<RDFStore> MergeGraphAsync(RDFGraph graph)
        {
            if (graph != null)
            {
                RDFContext graphCtx = new RDFContext(graph.Context);

                //Create command
                InsertCommand.CommandText = "INSERT INTO quadruples(quadrupleid, tripleflavor, context, contextid, subject, subjectid, predicate, predicateid, object, objectid) SELECT @QID, @TFV, @CTX, @CTXID, @SUBJ, @SUBJID, @PRED, @PREDID, @OBJ, @OBJID WHERE NOT EXISTS (SELECT 1 FROM quadruples WHERE quadrupleid = @QID)";
                InsertCommand.Parameters.Clear();
                InsertCommand.Parameters.Add(new NpgsqlParameter("QID", NpgsqlDbType.Bigint));
                InsertCommand.Parameters.Add(new NpgsqlParameter("TFV", NpgsqlDbType.Integer));
                InsertCommand.Parameters.Add(new NpgsqlParameter("CTX", NpgsqlDbType.Varchar, 1000));
                InsertCommand.Parameters.Add(new NpgsqlParameter("CTXID", NpgsqlDbType.Bigint));
                InsertCommand.Parameters.Add(new NpgsqlParameter("SUBJ", NpgsqlDbType.Varchar, 1000));
                InsertCommand.Parameters.Add(new NpgsqlParameter("SUBJID", NpgsqlDbType.Bigint));
                InsertCommand.Parameters.Add(new NpgsqlParameter("PRED", NpgsqlDbType.Varchar, 1000));
                InsertCommand.Parameters.Add(new NpgsqlParameter("PREDID", NpgsqlDbType.Bigint));
                InsertCommand.Parameters.Add(new NpgsqlParameter("OBJ", NpgsqlDbType.Varchar, 1000));
                InsertCommand.Parameters.Add(new NpgsqlParameter("OBJID", NpgsqlDbType.Bigint));

                try
                {
                    //Open connection
                    await EnsureConnectionIsOpenAsync();

                    //Prepare command
                    await InsertCommand.PrepareAsync();

                    //Open transaction
                    InsertCommand.Transaction = Connection.BeginTransaction();

                    //Iterate triples
                    foreach (RDFTriple triple in graph)
                    {
                        //Valorize parameters
                        InsertCommand.Parameters["QID"].Value = RDFModelUtilities.CreateHash($"{graphCtx} {triple.Subject} {triple.Predicate} {triple.Object}");
                        InsertCommand.Parameters["TFV"].Value = (int)triple.TripleFlavor;
                        InsertCommand.Parameters["CTX"].Value = graphCtx.ToString();
                        InsertCommand.Parameters["CTXID"].Value = graphCtx.PatternMemberID;
                        InsertCommand.Parameters["SUBJ"].Value = triple.Subject.ToString();
                        InsertCommand.Parameters["SUBJID"].Value = triple.Subject.PatternMemberID;
                        InsertCommand.Parameters["PRED"].Value = triple.Predicate.ToString();
                        InsertCommand.Parameters["PREDID"].Value = triple.Predicate.PatternMemberID;
                        InsertCommand.Parameters["OBJ"].Value = triple.Object.ToString();
                        InsertCommand.Parameters["OBJID"].Value = triple.Object.PatternMemberID;

                        //Execute command
                        await InsertCommand.ExecuteNonQueryAsync();
                    }

                    //Close transaction
                    if (InsertCommand.Transaction != null)
                        await InsertCommand.Transaction.CommitAsync();

                    //Close connection
                    await Connection.CloseAsync();
                }
                catch (Exception ex)
                {
                    //Rollback transaction
                    if (InsertCommand.Transaction != null)
                        await InsertCommand.Transaction.RollbackAsync();

                    //Close connection
                    await Connection.CloseAsync();

                    //Propagate exception
                    throw new RDFStoreException("Cannot insert data into PostgreSQL store because: " + ex.Message, ex);
                }
            }
            return this;
        }

        /// <summary>
        /// Adds the given quadruple to the store
        /// </summary>
        public override RDFStore AddQuadruple(RDFQuadruple quadruple)
            => AddQuadrupleAsync(quadruple).GetAwaiter().GetResult();

        /// <summary>
        /// Asynchronously adds the given quadruple to the store
        /// </summary>
        public override async Task<RDFStore> AddQuadrupleAsync(RDFQuadruple quadruple)
        {
            if (quadruple != null)
            {
                //Create command
                InsertCommand.CommandText = "INSERT INTO quadruples(quadrupleid, tripleflavor, context, contextid, subject, subjectid, predicate, predicateid, object, objectid) SELECT @QID, @TFV, @CTX, @CTXID, @SUBJ, @SUBJID, @PRED, @PREDID, @OBJ, @OBJID WHERE NOT EXISTS (SELECT 1 FROM quadruples WHERE quadrupleid = @QID)";
                InsertCommand.Parameters.Clear();
                InsertCommand.Parameters.Add(new NpgsqlParameter("QID", NpgsqlDbType.Bigint));
                InsertCommand.Parameters.Add(new NpgsqlParameter("TFV", NpgsqlDbType.Integer));
                InsertCommand.Parameters.Add(new NpgsqlParameter("CTX", NpgsqlDbType.Varchar, 1000));
                InsertCommand.Parameters.Add(new NpgsqlParameter("CTXID", NpgsqlDbType.Bigint));
                InsertCommand.Parameters.Add(new NpgsqlParameter("SUBJ", NpgsqlDbType.Varchar, 1000));
                InsertCommand.Parameters.Add(new NpgsqlParameter("SUBJID", NpgsqlDbType.Bigint));
                InsertCommand.Parameters.Add(new NpgsqlParameter("PRED", NpgsqlDbType.Varchar, 1000));
                InsertCommand.Parameters.Add(new NpgsqlParameter("PREDID", NpgsqlDbType.Bigint));
                InsertCommand.Parameters.Add(new NpgsqlParameter("OBJ", NpgsqlDbType.Varchar, 1000));
                InsertCommand.Parameters.Add(new NpgsqlParameter("OBJID", NpgsqlDbType.Bigint));

                //Valorize parameters
                InsertCommand.Parameters["QID"].Value = quadruple.QuadrupleID;
                InsertCommand.Parameters["TFV"].Value = (int)quadruple.TripleFlavor;
                InsertCommand.Parameters["CTX"].Value = quadruple.Context.ToString();
                InsertCommand.Parameters["CTXID"].Value = quadruple.Context.PatternMemberID;
                InsertCommand.Parameters["SUBJ"].Value = quadruple.Subject.ToString();
                InsertCommand.Parameters["SUBJID"].Value = quadruple.Subject.PatternMemberID;
                InsertCommand.Parameters["PRED"].Value = quadruple.Predicate.ToString();
                InsertCommand.Parameters["PREDID"].Value = quadruple.Predicate.PatternMemberID;
                InsertCommand.Parameters["OBJ"].Value = quadruple.Object.ToString();
                InsertCommand.Parameters["OBJID"].Value = quadruple.Object.PatternMemberID;

                try
                {
                    //Open connection
                    await EnsureConnectionIsOpenAsync();

                    //Prepare command
                    await InsertCommand.PrepareAsync();

                    //Open transaction
                    InsertCommand.Transaction = Connection.BeginTransaction();

                    //Execute command
                    await InsertCommand.ExecuteNonQueryAsync();

                    //Close transaction
                    if (InsertCommand.Transaction != null)
                        await InsertCommand.Transaction.CommitAsync();

                    //Close connection
                    await Connection.CloseAsync();
                }
                catch (Exception ex)
                {
                    //Rollback transaction
                    if (InsertCommand.Transaction != null)
                        await InsertCommand.Transaction.RollbackAsync();

                    //Close connection
                    await Connection.CloseAsync();

                    //Propagate exception
                    throw new RDFStoreException("Cannot insert data into PostgreSQL store because: " + ex.Message, ex);
                }
            }
            return this;
        }
        #endregion

        #region Remove
        /// <summary>
        /// Removes the given quadruple from the store
        /// </summary>
        public override RDFStore RemoveQuadruple(RDFQuadruple quadruple)
            => RemoveQuadrupleAsync(quadruple).GetAwaiter().GetResult();

        /// <summary>
        /// Asynchronously removes the given quadruple from the store
        /// </summary>
        public override async Task<RDFStore> RemoveQuadrupleAsync(RDFQuadruple quadruple)
        {
            if (quadruple != null)
            {
                //Create command
                DeleteCommand.CommandText = "DELETE FROM quadruples WHERE quadrupleid = @QID";
                DeleteCommand.Parameters.Clear();
                DeleteCommand.Parameters.Add(new NpgsqlParameter("QID", NpgsqlDbType.Bigint));

                //Valorize parameters
                DeleteCommand.Parameters["QID"].Value = quadruple.QuadrupleID;

                try
                {
                    //Open connection
                    await EnsureConnectionIsOpenAsync();

                    //Prepare command
                    await DeleteCommand.PrepareAsync();

                    //Open transaction
                    DeleteCommand.Transaction = Connection.BeginTransaction();

                    //Execute command
                    await DeleteCommand.ExecuteNonQueryAsync();

                    //Close transaction
                    if (DeleteCommand.Transaction != null)
                        await DeleteCommand.Transaction.CommitAsync();

                    //Close connection
                    await Connection.CloseAsync();
                }
                catch (Exception ex)
                {
                    //Rollback transaction
                    if (DeleteCommand.Transaction != null)
                        await DeleteCommand.Transaction.RollbackAsync();

                    //Close connection
                    await Connection.CloseAsync();

                    //Propagate exception
                    throw new RDFStoreException("Cannot delete data from PostgreSQL store because: " + ex.Message, ex);
                }
            }
            return this;
        }

        /// <summary>
        /// Removes the quadruples which satisfy the given combination of CSPOL accessors<br/>
        /// (null values are handled as * selectors. Object and Literal params, if given, must be mutually exclusive!)
        /// </summary>
        public override RDFStore RemoveQuadruples(RDFContext c=null, RDFResource s=null, RDFResource p=null, RDFResource o=null, RDFLiteral l=null)
            => RemoveQuadruplesAsync(c, s, p, o, l).GetAwaiter().GetResult();

        /// <summary>
        /// Asynchronously removes the quadruples which satisfy the given combination of CSPOL accessors<br/>
        /// (null values are handled as * selectors. Object and Literal params, if given, must be mutually exclusive!)
        /// </summary>
        public override async Task<RDFStore> RemoveQuadruplesAsync(RDFContext c=null, RDFResource s=null, RDFResource p=null, RDFResource o=null, RDFLiteral l=null)
        {
            #region Guards
            if (o != null && l != null)
                throw new RDFStoreException("Cannot access a store when both object and literals are given: they must be mutually exclusive!");
            #endregion

            //Prepare command
            PrepareSelectDeleteCommand(DeleteCommand, "DELETE FROM quadruples", c, s, p, o, l);

            try
            {
                //Open connection
                await EnsureConnectionIsOpenAsync();

                //Prepare command
                await DeleteCommand.PrepareAsync();

                //Open transaction
                DeleteCommand.Transaction = Connection.BeginTransaction();

                //Execute command
                await DeleteCommand.ExecuteNonQueryAsync();

                //Close transaction
                if (DeleteCommand.Transaction != null)
                    await DeleteCommand.Transaction.CommitAsync();

                //Close connection
                await Connection.CloseAsync();
            }
            catch (Exception ex)
            {
                //Rollback transaction
                if (DeleteCommand.Transaction != null)
                    await DeleteCommand.Transaction.RollbackAsync();

                //Close connection
                await Connection.CloseAsync();

                //Propagate exception
                throw new RDFStoreException("Cannot delete data from PostgreSQL store because: " + ex.Message, ex);
            }

            return this;
        }

        /// <summary>
        /// Clears the quadruples of the store
        /// </summary>
        public override void ClearQuadruples()
            => ClearQuadruplesAsync().GetAwaiter().GetResult();

        /// <summary>
        /// Asynchronously clears the quadruples of the store
        /// </summary>
        public override async Task ClearQuadruplesAsync()
        {
            //Create command
            DeleteCommand.CommandText = "DELETE FROM quadruples";
            DeleteCommand.Parameters.Clear();

            try
            {
                //Open connection
                await EnsureConnectionIsOpenAsync();

                //Prepare command
                await DeleteCommand.PrepareAsync();

                //Open transaction
                DeleteCommand.Transaction = Connection.BeginTransaction();

                //Execute command
                await DeleteCommand.ExecuteNonQueryAsync();

                //Close transaction
                if (DeleteCommand.Transaction != null)
                    await DeleteCommand.Transaction.CommitAsync();

                //Close connection
                await Connection.CloseAsync();
            }
            catch (Exception ex)
            {
                //Rollback transaction
                if (DeleteCommand.Transaction != null)
                    await DeleteCommand.Transaction.RollbackAsync();

                //Close connection
                await Connection.CloseAsync();

                //Propagate exception
                throw new RDFStoreException("Cannot delete data from PostgreSQL store because: " + ex.Message, ex);
            }
        }
        #endregion

        #region Select
        /// <summary>
        /// Checks if the given quadruple is found in the store
        /// </summary>
        public override bool ContainsQuadruple(RDFQuadruple quadruple)
            => ContainsQuadrupleAsync(quadruple).GetAwaiter().GetResult();

        /// <summary>
        /// Asynchronously checks if the given quadruple is found in the store
        /// </summary>
        public override async Task<bool> ContainsQuadrupleAsync(RDFQuadruple quadruple)
        {
            //Guard against tricky input
            if (quadruple == null)
                return false;

            //Create command
            SelectCommand.CommandText = "SELECT COUNT(1) WHERE EXISTS(SELECT 1 FROM quadruples WHERE quadrupleid = @QID)";
            SelectCommand.Parameters.Clear();
            SelectCommand.Parameters.Add(new NpgsqlParameter("QID", NpgsqlDbType.Bigint));

            //Valorize parameters
            SelectCommand.Parameters["QID"].Value = quadruple.QuadrupleID;

            //Prepare and execute command
            try
            {
                //Open connection
                await EnsureConnectionIsOpenAsync();

                //Prepare command
                await SelectCommand.PrepareAsync();

                //Execute command
                int result = int.Parse((await SelectCommand.ExecuteScalarAsync()).ToString());

                //Close connection
                await Connection.CloseAsync();

                //Give result
                return result == 1;
            }
            catch (Exception ex)
            {
                //Close connection
                await Connection.CloseAsync();

                //Propagate exception
                throw new RDFStoreException("Cannot read data from PostgreSQL store because: " + ex.Message, ex);
            }
        }

        /// <summary>
        /// Selects the quadruples which satisfy the given combination of CSPOL accessors<br/>
        /// (null values are handled as * selectors. object and Literal params, if given, must be mutually exclusive!)
        /// </summary>
        /// <exception cref="RDFStoreException"></exception>
        public override List<RDFQuadruple> SelectQuadruples(RDFContext c=null, RDFResource s=null, RDFResource p=null, RDFResource o=null, RDFLiteral l=null)
            => SelectQuadruplesAsync(c,s,p,o,l).GetAwaiter().GetResult();

        /// <summary>
        /// Asynchronously selects the quadruples which satisfy the given combination of CSPOL accessors<br/>
        /// (null values are handled as * selectors. object and Literal params, if given, must be mutually exclusive!)
        /// </summary>
        /// <exception cref="RDFStoreException"></exception>
        public override async Task<List<RDFQuadruple>> SelectQuadruplesAsync(RDFContext c=null, RDFResource s=null, RDFResource p=null, RDFResource o=null, RDFLiteral l=null)
        {
            #region Guards
            if (o != null && l != null)
                throw new RDFStoreException("Cannot access a store when both object and literals are given: they must be mutually exclusive!");
            #endregion

            List<RDFQuadruple>  result = new List<RDFQuadruple>();

            //Prepare command
            PrepareSelectDeleteCommand(SelectCommand, "SELECT tripleflavor, context, subject, predicate, object FROM quadruples", c, s, p, o, l);

            //Prepare and execute command
            try
            {
                //Open connection
                await EnsureConnectionIsOpenAsync();

                //Execute command
                using (NpgsqlDataReader quadruples = await SelectCommand.ExecuteReaderAsync(CommandBehavior.Default))
                {
                    while (quadruples.Read())
                        result.Add(RDFStoreUtilities.ParseQuadruple(quadruples));
                }

                //Close connection
                await Connection.CloseAsync();
            }
            catch (Exception ex)
            {
                //Close connection
                await Connection.CloseAsync();

                //Propagate exception
                throw new RDFStoreException("Cannot read data from PostgreSQL store because: " + ex.Message, ex);
            }

            return result;
        }

        /// <summary>
        /// Asynchronously counts the PostgreSQL database quadruples
        /// </summary>
        private async Task<long> GetQuadruplesCountAsync()
        {
            try
            {
                //Open connection
                await EnsureConnectionIsOpenAsync();

                //Create command
                SelectCommand.CommandText = "SELECT COUNT(*) FROM quadruples";
                SelectCommand.Parameters.Clear();

                //Execute command
                long result = long.Parse((await SelectCommand.ExecuteScalarAsync(CancellationToken.None)).ToString());

                //Close connection
                await Connection.CloseAsync();

                //Return the quadruples count
                return result;
            }
            catch
            {
                //Close connection
                await Connection.CloseAsync();

                //Return the quadruples count (-1 to indicate error)
                return -1;
            }
        }
        #endregion

        #region Optimize
        /// <summary>
        /// Asynchronously executes a VACUUM command to optimize PostgreSQL store
        /// </summary>
        public async Task OptimizeAsync()
        {
            try
            {
                //Open connection
                await EnsureConnectionIsOpenAsync();

                //Create command
                using (NpgsqlCommand optimizeCommand = new NpgsqlCommand("VACUUM ANALYZE quadruples", Connection) { CommandTimeout = 120 })
                {
                    //Execute command
                    await optimizeCommand.ExecuteNonQueryAsync();
                }

                //Close connection
                await Connection.CloseAsync();
            }
            catch (Exception ex)
            {
                //Close connection
                await Connection.CloseAsync();

                //Propagate exception
                throw new RDFStoreException("Cannot optimize PostgreSQL store because: " + ex.Message, ex);
            }
        }
        #endregion

        #endregion

        #region Utilities
        private void PrepareSelectDeleteCommand(NpgsqlCommand command, string baseSql, RDFContext c, RDFResource s, RDFResource p, RDFResource o, RDFLiteral l)
        {
            command.Parameters.Clear();
            List<string> conditions = new List<string>();

            if (c != null)
            {
                conditions.Add("contextid = @CTXID");
                command.Parameters.Add(new NpgsqlParameter("CTXID", NpgsqlDbType.Bigint));
                command.Parameters["CTXID"].Value = c.PatternMemberID;
            }
            if (s != null)
            {
                conditions.Add("subjectid = @SUBJID");
                command.Parameters.Add(new NpgsqlParameter("SUBJID", NpgsqlDbType.Bigint));
                command.Parameters["SUBJID"].Value = s.PatternMemberID;
            }
            if (p != null)
            {
                conditions.Add("predicateid = @PREDID");
                command.Parameters.Add(new NpgsqlParameter("PREDID", NpgsqlDbType.Bigint));
                command.Parameters["PREDID"].Value = p.PatternMemberID;
            }
            if (o != null)
            {
                conditions.Add("objectid = @OBJID");
                command.Parameters.Add(new NpgsqlParameter("OBJID", NpgsqlDbType.Bigint));
                command.Parameters["OBJID"].Value = o.PatternMemberID;
                conditions.Add("tripleflavor = @TFV");
                command.Parameters.Add(new NpgsqlParameter("TFV", NpgsqlDbType.Integer));
                command.Parameters["TFV"].Value = (int)RDFModelEnums.RDFTripleFlavors.SPO;
            }
            if (l != null)
            {
                conditions.Add("objectid = @OBJID");
                command.Parameters.Add(new NpgsqlParameter("OBJID", NpgsqlDbType.Bigint));
                command.Parameters["OBJID"].Value = l.PatternMemberID;
                conditions.Add("tripleflavor = @TFV");
                command.Parameters.Add(new NpgsqlParameter("TFV", NpgsqlDbType.Integer));
                command.Parameters["TFV"].Value = (int)RDFModelEnums.RDFTripleFlavors.SPL;
            }

            command.CommandText = conditions.Count > 0
                ? $"{baseSql} WHERE {string.Join(" AND ", conditions)}"
                : baseSql;
        }

        private async Task EnsureConnectionIsOpenAsync()
        {
            switch (Connection.State)
            {
                case ConnectionState.Closed:
                    await Connection.OpenAsync();
                    break;

                case ConnectionState.Broken:
                case ConnectionState.Connecting:
                    await Connection.CloseAsync();
                    await Connection.OpenAsync();
                    break;
            }
        }
        #endregion
    }
}