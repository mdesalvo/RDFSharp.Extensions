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
        /// Connection string to the PostgreSQL database (a new connection is opened for each operation,
        /// relying on ADO.NET's own connection pooling, so that the store is safe to use concurrently
        /// -es. as a singleton registered in an ASP.NET Core DI container)
        /// </summary>
        private readonly string ConnectionString;

        /// <summary>
        /// Options customizing the behaviour of the store
        /// </summary>
        private readonly RDFPostgreSQLStoreOptions Options;

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
            Options = pgsqlStoreOptions ?? new RDFPostgreSQLStoreOptions();

            //Initialize store structures
            try
            {
                RDFPostgreSQLStoreManager pgSqlStoreManager = new RDFPostgreSQLStoreManager(pgsqlConnectionString);
                pgSqlStoreManager.EnsureQuadruplesTableExistsAsync().GetAwaiter().GetResult();

                StoreType = "POSTGRESQL";
                ConnectionString = pgsqlConnectionString;
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
        {
            NpgsqlConnectionStringBuilder builder = new NpgsqlConnectionStringBuilder(ConnectionString);
            return $"{base.ToString()}|SERVER={builder.Host};DATABASE={builder.Database}";
        }

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
            => Disposed = true;
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

                NpgsqlConnection connection = new NpgsqlConnection(ConnectionString);
                NpgsqlTransaction transaction = null;
                try
                {
                    //Open connection
                    await connection.OpenAsync();

                    //Create command
                    using (NpgsqlCommand insertCommand = new NpgsqlCommand(
                        "INSERT INTO quadruples(quadrupleid, tripleflavor, context, contextid, subject, subjectid, predicate, predicateid, object, objectid) SELECT @QID, @TFV, @CTX, @CTXID, @SUBJ, @SUBJID, @PRED, @PREDID, @OBJ, @OBJID WHERE NOT EXISTS (SELECT 1 FROM quadruples WHERE quadrupleid = @QID)",
                        connection) { CommandTimeout = Options.InsertTimeout })
                    {
                        insertCommand.Parameters.Add(new NpgsqlParameter("QID", NpgsqlDbType.Bigint));
                        insertCommand.Parameters.Add(new NpgsqlParameter("TFV", NpgsqlDbType.Integer));
                        insertCommand.Parameters.Add(new NpgsqlParameter("CTX", NpgsqlDbType.Varchar, 1000));
                        insertCommand.Parameters.Add(new NpgsqlParameter("CTXID", NpgsqlDbType.Bigint));
                        insertCommand.Parameters.Add(new NpgsqlParameter("SUBJ", NpgsqlDbType.Varchar, 1000));
                        insertCommand.Parameters.Add(new NpgsqlParameter("SUBJID", NpgsqlDbType.Bigint));
                        insertCommand.Parameters.Add(new NpgsqlParameter("PRED", NpgsqlDbType.Varchar, 1000));
                        insertCommand.Parameters.Add(new NpgsqlParameter("PREDID", NpgsqlDbType.Bigint));
                        insertCommand.Parameters.Add(new NpgsqlParameter("OBJ", NpgsqlDbType.Varchar, 1000));
                        insertCommand.Parameters.Add(new NpgsqlParameter("OBJID", NpgsqlDbType.Bigint));

                        //Prepare command
                        await insertCommand.PrepareAsync();

                        //Open transaction
                        transaction = connection.BeginTransaction();
                        insertCommand.Transaction = transaction;

                        //Iterate triples
                        foreach (RDFTriple triple in graph)
                        {
                            //Valorize parameters
                            insertCommand.Parameters["QID"].Value = RDFModelUtilities.CreateHash($"{graphCtx} {triple.Subject} {triple.Predicate} {triple.Object}");
                            insertCommand.Parameters["TFV"].Value = (int)triple.TripleFlavor;
                            insertCommand.Parameters["CTX"].Value = graphCtx.ToString();
                            insertCommand.Parameters["CTXID"].Value = graphCtx.PatternMemberID;
                            insertCommand.Parameters["SUBJ"].Value = triple.Subject.ToString();
                            insertCommand.Parameters["SUBJID"].Value = triple.Subject.PatternMemberID;
                            insertCommand.Parameters["PRED"].Value = triple.Predicate.ToString();
                            insertCommand.Parameters["PREDID"].Value = triple.Predicate.PatternMemberID;
                            insertCommand.Parameters["OBJ"].Value = triple.Object.ToString();
                            insertCommand.Parameters["OBJID"].Value = triple.Object.PatternMemberID;

                            //Execute command
                            await insertCommand.ExecuteNonQueryAsync();
                        }

                        //Commit transaction
                        await transaction.CommitAsync();
                    }
                }
                catch (Exception ex)
                {
                    //Rollback transaction
                    if (transaction != null)
                        await transaction.RollbackAsync();

                    //Propagate exception
                    throw new RDFStoreException("Cannot insert data into PostgreSQL store because: " + ex.Message, ex);
                }
                finally
                {
                    //Close connection
                    await connection.CloseAsync();
                    await connection.DisposeAsync();
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
                NpgsqlConnection connection = new NpgsqlConnection(ConnectionString);
                NpgsqlTransaction transaction = null;
                try
                {
                    //Open connection
                    await connection.OpenAsync();

                    //Create command
                    using (NpgsqlCommand insertCommand = new NpgsqlCommand(
                        "INSERT INTO quadruples(quadrupleid, tripleflavor, context, contextid, subject, subjectid, predicate, predicateid, object, objectid) SELECT @QID, @TFV, @CTX, @CTXID, @SUBJ, @SUBJID, @PRED, @PREDID, @OBJ, @OBJID WHERE NOT EXISTS (SELECT 1 FROM quadruples WHERE quadrupleid = @QID)",
                        connection) { CommandTimeout = Options.InsertTimeout })
                    {
                        insertCommand.Parameters.Add(new NpgsqlParameter("QID", NpgsqlDbType.Bigint));
                        insertCommand.Parameters.Add(new NpgsqlParameter("TFV", NpgsqlDbType.Integer));
                        insertCommand.Parameters.Add(new NpgsqlParameter("CTX", NpgsqlDbType.Varchar, 1000));
                        insertCommand.Parameters.Add(new NpgsqlParameter("CTXID", NpgsqlDbType.Bigint));
                        insertCommand.Parameters.Add(new NpgsqlParameter("SUBJ", NpgsqlDbType.Varchar, 1000));
                        insertCommand.Parameters.Add(new NpgsqlParameter("SUBJID", NpgsqlDbType.Bigint));
                        insertCommand.Parameters.Add(new NpgsqlParameter("PRED", NpgsqlDbType.Varchar, 1000));
                        insertCommand.Parameters.Add(new NpgsqlParameter("PREDID", NpgsqlDbType.Bigint));
                        insertCommand.Parameters.Add(new NpgsqlParameter("OBJ", NpgsqlDbType.Varchar, 1000));
                        insertCommand.Parameters.Add(new NpgsqlParameter("OBJID", NpgsqlDbType.Bigint));

                        //Valorize parameters
                        insertCommand.Parameters["QID"].Value = quadruple.QuadrupleID;
                        insertCommand.Parameters["TFV"].Value = (int)quadruple.TripleFlavor;
                        insertCommand.Parameters["CTX"].Value = quadruple.Context.ToString();
                        insertCommand.Parameters["CTXID"].Value = quadruple.Context.PatternMemberID;
                        insertCommand.Parameters["SUBJ"].Value = quadruple.Subject.ToString();
                        insertCommand.Parameters["SUBJID"].Value = quadruple.Subject.PatternMemberID;
                        insertCommand.Parameters["PRED"].Value = quadruple.Predicate.ToString();
                        insertCommand.Parameters["PREDID"].Value = quadruple.Predicate.PatternMemberID;
                        insertCommand.Parameters["OBJ"].Value = quadruple.Object.ToString();
                        insertCommand.Parameters["OBJID"].Value = quadruple.Object.PatternMemberID;

                        //Prepare command
                        await insertCommand.PrepareAsync();

                        //Open transaction
                        transaction = connection.BeginTransaction();
                        insertCommand.Transaction = transaction;

                        //Execute command
                        await insertCommand.ExecuteNonQueryAsync();

                        //Commit transaction
                        await transaction.CommitAsync();
                    }
                }
                catch (Exception ex)
                {
                    //Rollback transaction
                    if (transaction != null)
                        await transaction.RollbackAsync();

                    //Propagate exception
                    throw new RDFStoreException("Cannot insert data into PostgreSQL store because: " + ex.Message, ex);
                }
                finally
                {
                    //Close connection
                    await connection.CloseAsync();
                    await connection.DisposeAsync();
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
                NpgsqlConnection connection = new NpgsqlConnection(ConnectionString);
                NpgsqlTransaction transaction = null;
                try
                {
                    //Open connection
                    await connection.OpenAsync();

                    //Create command
                    using (NpgsqlCommand deleteCommand = new NpgsqlCommand("DELETE FROM quadruples WHERE quadrupleid = @QID", connection) { CommandTimeout = Options.DeleteTimeout })
                    {
                        deleteCommand.Parameters.Add(new NpgsqlParameter("QID", NpgsqlDbType.Bigint));

                        //Valorize parameters
                        deleteCommand.Parameters["QID"].Value = quadruple.QuadrupleID;

                        //Prepare command
                        await deleteCommand.PrepareAsync();

                        //Open transaction
                        transaction = connection.BeginTransaction();
                        deleteCommand.Transaction = transaction;

                        //Execute command
                        await deleteCommand.ExecuteNonQueryAsync();

                        //Commit transaction
                        await transaction.CommitAsync();
                    }
                }
                catch (Exception ex)
                {
                    //Rollback transaction
                    if (transaction != null)
                        await transaction.RollbackAsync();

                    //Propagate exception
                    throw new RDFStoreException("Cannot delete data from PostgreSQL store because: " + ex.Message, ex);
                }
                finally
                {
                    //Close connection
                    await connection.CloseAsync();
                    await connection.DisposeAsync();
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

            NpgsqlConnection connection = new NpgsqlConnection(ConnectionString);
            NpgsqlTransaction transaction = null;
            try
            {
                //Open connection
                await connection.OpenAsync();

                //Create command
                using (NpgsqlCommand deleteCommand = new NpgsqlCommand { Connection = connection, CommandTimeout = Options.DeleteTimeout })
                {
                    PrepareSelectDeleteCommand(deleteCommand, "DELETE FROM quadruples", c, s, p, o, l);

                    //Prepare command
                    await deleteCommand.PrepareAsync();

                    //Open transaction
                    transaction = connection.BeginTransaction();
                    deleteCommand.Transaction = transaction;

                    //Execute command
                    await deleteCommand.ExecuteNonQueryAsync();

                    //Commit transaction
                    await transaction.CommitAsync();
                }
            }
            catch (Exception ex)
            {
                //Rollback transaction
                if (transaction != null)
                    await transaction.RollbackAsync();

                //Propagate exception
                throw new RDFStoreException("Cannot delete data from PostgreSQL store because: " + ex.Message, ex);
            }
            finally
            {
                //Close connection
                await connection.CloseAsync();
                await connection.DisposeAsync();
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
            NpgsqlConnection connection = new NpgsqlConnection(ConnectionString);
            NpgsqlTransaction transaction = null;
            try
            {
                //Open connection
                await connection.OpenAsync();

                //Create command
                using (NpgsqlCommand deleteCommand = new NpgsqlCommand("DELETE FROM quadruples", connection) { CommandTimeout = Options.DeleteTimeout })
                {
                    //Prepare command
                    await deleteCommand.PrepareAsync();

                    //Open transaction
                    transaction = connection.BeginTransaction();
                    deleteCommand.Transaction = transaction;

                    //Execute command
                    await deleteCommand.ExecuteNonQueryAsync();

                    //Commit transaction
                    await transaction.CommitAsync();
                }
            }
            catch (Exception ex)
            {
                //Rollback transaction
                if (transaction != null)
                    await transaction.RollbackAsync();

                //Propagate exception
                throw new RDFStoreException("Cannot delete data from PostgreSQL store because: " + ex.Message, ex);
            }
            finally
            {
                //Close connection
                await connection.CloseAsync();
                await connection.DisposeAsync();
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

            NpgsqlConnection connection = new NpgsqlConnection(ConnectionString);
            try
            {
                //Open connection
                await connection.OpenAsync();

                //Create command
                using (NpgsqlCommand selectCommand = new NpgsqlCommand("SELECT COUNT(1) WHERE EXISTS(SELECT 1 FROM quadruples WHERE quadrupleid = @QID)", connection) { CommandTimeout = Options.SelectTimeout })
                {
                    selectCommand.Parameters.Add(new NpgsqlParameter("QID", NpgsqlDbType.Bigint));

                    //Valorize parameters
                    selectCommand.Parameters["QID"].Value = quadruple.QuadrupleID;

                    //Prepare command
                    await selectCommand.PrepareAsync();

                    //Execute command
                    int result = int.Parse((await selectCommand.ExecuteScalarAsync()).ToString());

                    //Give result
                    return result == 1;
                }
            }
            catch (Exception ex)
            {
                //Propagate exception
                throw new RDFStoreException("Cannot read data from PostgreSQL store because: " + ex.Message, ex);
            }
            finally
            {
                //Close connection
                await connection.CloseAsync();
                await connection.DisposeAsync();
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

            NpgsqlConnection connection = new NpgsqlConnection(ConnectionString);
            try
            {
                //Open connection
                await connection.OpenAsync();

                //Create command
                using (NpgsqlCommand selectCommand = new NpgsqlCommand { Connection = connection, CommandTimeout = Options.SelectTimeout })
                {
                    PrepareSelectDeleteCommand(selectCommand, "SELECT tripleflavor, context, subject, predicate, object FROM quadruples", c, s, p, o, l);

                    //Execute command
                    using (NpgsqlDataReader quadruples = await selectCommand.ExecuteReaderAsync(CommandBehavior.Default))
                    {
                        while (quadruples.Read())
                            result.Add(RDFStoreUtilities.ParseQuadruple(quadruples));
                    }
                }
            }
            catch (Exception ex)
            {
                //Propagate exception
                throw new RDFStoreException("Cannot read data from PostgreSQL store because: " + ex.Message, ex);
            }
            finally
            {
                //Close connection
                await connection.CloseAsync();
                await connection.DisposeAsync();
            }

            return result;
        }

        /// <summary>
        /// Asynchronously counts the PostgreSQL database quadruples
        /// </summary>
        private async Task<long> GetQuadruplesCountAsync()
        {
            NpgsqlConnection connection = new NpgsqlConnection(ConnectionString);
            try
            {
                //Open connection
                await connection.OpenAsync();

                //Create command
                using (NpgsqlCommand selectCommand = new NpgsqlCommand("SELECT COUNT(*) FROM quadruples", connection) { CommandTimeout = Options.SelectTimeout })
                {
                    //Execute command
                    long result = long.Parse((await selectCommand.ExecuteScalarAsync(CancellationToken.None)).ToString());

                    //Return the quadruples count
                    return result;
                }
            }
            catch
            {
                //Return the quadruples count (-1 to indicate error)
                return -1;
            }
            finally
            {
                //Close connection
                await connection.CloseAsync();
                await connection.DisposeAsync();
            }
        }
        #endregion

        #region Optimize
        /// <summary>
        /// Asynchronously executes a VACUUM command to optimize PostgreSQL store
        /// </summary>
        public async Task OptimizeAsync()
        {
            NpgsqlConnection connection = new NpgsqlConnection(ConnectionString);
            try
            {
                //Open connection
                await connection.OpenAsync();

                //Create command
                using (NpgsqlCommand optimizeCommand = new NpgsqlCommand("VACUUM ANALYZE quadruples", connection) { CommandTimeout = 120 })
                {
                    //Execute command
                    await optimizeCommand.ExecuteNonQueryAsync();
                }
            }
            catch (Exception ex)
            {
                //Propagate exception
                throw new RDFStoreException("Cannot optimize PostgreSQL store because: " + ex.Message, ex);
            }
            finally
            {
                //Close connection
                await connection.CloseAsync();
                await connection.DisposeAsync();
            }
        }
        #endregion

        #endregion

        #region Utilities
        private void PrepareSelectDeleteCommand(NpgsqlCommand command, string baseSql, RDFContext c, RDFResource s, RDFResource p, RDFResource o, RDFLiteral l)
        {
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
        #endregion
    }
}
