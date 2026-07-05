/*
   Copyright 2012-2026 Marco De Salvo

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

using RDFSharp.Model;
using RDFSharp.Store;
using System;
using System.Collections.Generic;
using System.Data;
using System.Data.SQLite;
using System.Text;
using System.Threading.Tasks;

namespace RDFSharp.Extensions.SQLite
{
    /// <summary>
    /// RDFSQLiteStore represents a store backed on SQLite engine
    /// </summary>
#if NET8_0_OR_GREATER
    public sealed class RDFSQLiteStore : RDFStore, IDisposable, IAsyncDisposable
#else
    public sealed class RDFSQLiteStore : RDFStore, IDisposable
#endif
    {
        #region Properties
        /// <summary>
        /// Count of the SQLite database quadruples (-1 in case of errors)
        /// </summary>
        public override long QuadruplesCount
            => GetQuadruplesCountAsync().GetAwaiter().GetResult();

        /// <summary>
        /// Asynchronous count of the SQLite database quadruples (-1 in case of errors)
        /// </summary>
        public override Task<long> QuadruplesCountAsync
            => GetQuadruplesCountAsync();

        /// <summary>
        /// Connection string to the SQLite database (a new connection is opened for each operation,
        /// relying on ADO.NET's own connection pooling, so that the store is safe to use concurrently
        /// -es. as a singleton registered in an ASP.NET Core DI container)
        /// </summary>
        private readonly string ConnectionString;

        /// <summary>
        /// Options customizing the behaviour of the store
        /// </summary>
        private readonly RDFSQLiteStoreOptions Options;

        /// <summary>
        /// Flag indicating that the SQLite store instance has already been disposed
        /// </summary>
        private bool Disposed { get; set; }
        #endregion

        #region Ctors
        /// <summary>
        /// Default-ctor to build a SQLite store instance (with eventual options)
        /// </summary>
        public RDFSQLiteStore(string sqliteConnectionString, RDFSQLiteStoreOptions sqliteStoreOptions=null)
        {
            #region Guards
            if (string.IsNullOrWhiteSpace(sqliteConnectionString))
                throw new RDFStoreException("Cannot connect to SQLite store because: given \"sqliteConnectionString\" parameter is null or empty.");
            #endregion

            //Initialize options
            Options = sqliteStoreOptions ?? new RDFSQLiteStoreOptions();

            //Initialize store structures
            try
            {
                RDFSQLiteStoreManager sqliteStoreManager = new RDFSQLiteStoreManager(sqliteConnectionString);
                sqliteStoreManager.InitializeDatabaseAndTableAsync().GetAwaiter().GetResult();

                StoreType = "SQLITE";
                ConnectionString = sqliteConnectionString;
                StoreID = RDFModelUtilities.CreateHash(ToString());
                Disposed = false;
            }
            catch (Exception ex)
            {
                throw new RDFStoreException("Cannot create SQLite store because: " + ex.Message, ex);
            }
        }

        /// <summary>
        /// Destroys the SQLite store instance
        /// </summary>
        ~RDFSQLiteStore()
            => Dispose(false);
        #endregion

        #region Interfaces
        /// <summary>
        /// Gives the string representation of the SQLite store
        /// </summary>
        public override string ToString()
        {
            string dataSource = new SQLiteConnectionStringBuilder(ConnectionString).DataSource;
            return $"{base.ToString()}|SERVER={dataSource};DATABASE={dataSource}";
        }

        /// <summary>
        /// Disposes the SQLite store instance
        /// </summary>
        public void Dispose()
        {
            Dispose(true);
            GC.SuppressFinalize(this);
        }

#if NET8_0_OR_GREATER
        /// <summary>
        /// Asynchronously disposes the SQLite store (IAsyncDisposable)
        /// </summary>
        ValueTask IAsyncDisposable.DisposeAsync()
        {
            Dispose(true);
            GC.SuppressFinalize(this);
            return ValueTask.CompletedTask;
        }
#endif

        /// <summary>
        /// Disposes the SQLite store instance  (business logic of resources disposal)
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

                SQLiteConnection connection = new SQLiteConnection(ConnectionString);
                SQLiteTransaction transaction = null;
                try
                {
                    //Open connection
                    await connection.OpenAsync();

                    //Create command
                    using (SQLiteCommand insertCommand = new SQLiteCommand(
                        "INSERT OR IGNORE INTO Quadruples(QuadrupleID, TripleFlavor, Context, ContextID, Subject, SubjectID, Predicate, PredicateID, Object, ObjectID) VALUES (@QID, @TFV, @CTX, @CTXID, @SUBJ, @SUBJID, @PRED, @PREDID, @OBJ, @OBJID)",
                        connection) { CommandTimeout = Options.InsertTimeout })
                    {
                        insertCommand.Parameters.Add(new SQLiteParameter("QID", DbType.Int64));
                        insertCommand.Parameters.Add(new SQLiteParameter("TFV", DbType.Int32));
                        insertCommand.Parameters.Add(new SQLiteParameter("CTX", DbType.String));
                        insertCommand.Parameters.Add(new SQLiteParameter("CTXID", DbType.Int64));
                        insertCommand.Parameters.Add(new SQLiteParameter("SUBJ", DbType.String));
                        insertCommand.Parameters.Add(new SQLiteParameter("SUBJID", DbType.Int64));
                        insertCommand.Parameters.Add(new SQLiteParameter("PRED", DbType.String));
                        insertCommand.Parameters.Add(new SQLiteParameter("PREDID", DbType.Int64));
                        insertCommand.Parameters.Add(new SQLiteParameter("OBJ", DbType.String));
                        insertCommand.Parameters.Add(new SQLiteParameter("OBJID", DbType.Int64));

                        //Prepare command
#if NET8_0_OR_GREATER
                        await insertCommand.PrepareAsync();
#else
                        insertCommand.Prepare();
#endif

                        //Open transaction
#if NET8_0_OR_GREATER
                        transaction = (SQLiteTransaction)await connection.BeginTransactionAsync();
#else
                        transaction = connection.BeginTransaction();
#endif
                        insertCommand.Transaction = transaction;

                        //Iterate triples
                        foreach (RDFTriple triple in graph)
                        {
                            //Valorize parameters
                            insertCommand.Parameters["QID"].Value = RDFModelUtilities.CreateHash($"{graphCtx} {triple.Subject} {triple.Predicate} {triple.Object}");
                            insertCommand.Parameters["TFV"].Value = triple.TripleFlavor;
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
#if NET8_0_OR_GREATER
                        await transaction.CommitAsync();
#else
                        transaction.Commit();
#endif
                    }
                }
                catch (Exception ex)
                {
                    //Rollback transaction
                    if (transaction != null)
#if NET8_0_OR_GREATER
                        await transaction.RollbackAsync();
#else
                        transaction.Rollback();
#endif

                    //Propagate exception
                    throw new RDFStoreException("Cannot insert data into SQLite store because: " + ex.Message, ex);
                }
                finally
                {
                    //Close connection
#if NET8_0_OR_GREATER
                    await connection.CloseAsync();
#else
                    connection.Close();
#endif
                    connection.Dispose();
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
                SQLiteConnection connection = new SQLiteConnection(ConnectionString);
                SQLiteTransaction transaction = null;
                try
                {
                    //Open connection
                    await connection.OpenAsync();

                    //Create command
                    using (SQLiteCommand insertCommand = new SQLiteCommand(
                        "INSERT OR IGNORE INTO Quadruples(QuadrupleID, TripleFlavor, Context, ContextID, Subject, SubjectID, Predicate, PredicateID, Object, ObjectID) VALUES (@QID, @TFV, @CTX, @CTXID, @SUBJ, @SUBJID, @PRED, @PREDID, @OBJ, @OBJID)",
                        connection) { CommandTimeout = Options.InsertTimeout })
                    {
                        insertCommand.Parameters.Add(new SQLiteParameter("QID", DbType.Int64));
                        insertCommand.Parameters.Add(new SQLiteParameter("TFV", DbType.Int32));
                        insertCommand.Parameters.Add(new SQLiteParameter("CTX", DbType.String));
                        insertCommand.Parameters.Add(new SQLiteParameter("CTXID", DbType.Int64));
                        insertCommand.Parameters.Add(new SQLiteParameter("SUBJ", DbType.String));
                        insertCommand.Parameters.Add(new SQLiteParameter("SUBJID", DbType.Int64));
                        insertCommand.Parameters.Add(new SQLiteParameter("PRED", DbType.String));
                        insertCommand.Parameters.Add(new SQLiteParameter("PREDID", DbType.Int64));
                        insertCommand.Parameters.Add(new SQLiteParameter("OBJ", DbType.String));
                        insertCommand.Parameters.Add(new SQLiteParameter("OBJID", DbType.Int64));

                        //Valorize parameters
                        insertCommand.Parameters["QID"].Value = quadruple.QuadrupleID;
                        insertCommand.Parameters["TFV"].Value = quadruple.TripleFlavor;
                        insertCommand.Parameters["CTX"].Value = quadruple.Context.ToString();
                        insertCommand.Parameters["CTXID"].Value = quadruple.Context.PatternMemberID;
                        insertCommand.Parameters["SUBJ"].Value = quadruple.Subject.ToString();
                        insertCommand.Parameters["SUBJID"].Value = quadruple.Subject.PatternMemberID;
                        insertCommand.Parameters["PRED"].Value = quadruple.Predicate.ToString();
                        insertCommand.Parameters["PREDID"].Value = quadruple.Predicate.PatternMemberID;
                        insertCommand.Parameters["OBJ"].Value = quadruple.Object.ToString();
                        insertCommand.Parameters["OBJID"].Value = quadruple.Object.PatternMemberID;

                        //Prepare command
#if NET8_0_OR_GREATER
                        await insertCommand.PrepareAsync();
#else
                        insertCommand.Prepare();
#endif

                        //Open transaction
#if NET8_0_OR_GREATER
                        transaction = (SQLiteTransaction)await connection.BeginTransactionAsync();
#else
                        transaction = connection.BeginTransaction();
#endif
                        insertCommand.Transaction = transaction;

                        //Execute command
                        await insertCommand.ExecuteNonQueryAsync();

                        //Commit transaction
#if NET8_0_OR_GREATER
                        await transaction.CommitAsync();
#else
                        transaction.Commit();
#endif
                    }
                }
                catch (Exception ex)
                {
                    //Rollback transaction
                    if (transaction != null)
#if NET8_0_OR_GREATER
                        await transaction.RollbackAsync();
#else
                        transaction.Rollback();
#endif

                    //Propagate exception
                    throw new RDFStoreException("Cannot insert data into SQLite store because: " + ex.Message, ex);
                }
                finally
                {
                    //Close connection
#if NET8_0_OR_GREATER
                    await connection.CloseAsync();
#else
                    connection.Close();
#endif
                    connection.Dispose();
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
                SQLiteConnection connection = new SQLiteConnection(ConnectionString);
                SQLiteTransaction transaction = null;
                try
                {
                    //Open connection
                    await connection.OpenAsync();

                    //Create command
                    using (SQLiteCommand deleteCommand = new SQLiteCommand("DELETE FROM Quadruples WHERE QuadrupleID = @QID", connection) { CommandTimeout = Options.DeleteTimeout })
                    {
                        deleteCommand.Parameters.Add(new SQLiteParameter("QID", DbType.Int64));

                        //Valorize parameters
                        deleteCommand.Parameters["QID"].Value = quadruple.QuadrupleID;

                        //Prepare command
#if NET8_0_OR_GREATER
                        await deleteCommand.PrepareAsync();
#else
                        deleteCommand.Prepare();
#endif

                        //Open transaction
#if NET8_0_OR_GREATER
                        transaction = (SQLiteTransaction)await connection.BeginTransactionAsync();
#else
                        transaction = connection.BeginTransaction();
#endif
                        deleteCommand.Transaction = transaction;

                        //Execute command
                        await deleteCommand.ExecuteNonQueryAsync();

                        //Commit transaction
#if NET8_0_OR_GREATER
                        await transaction.CommitAsync();
#else
                        transaction.Commit();
#endif
                    }
                }
                catch (Exception ex)
                {
                    //Rollback transaction
                    if (transaction != null)
#if NET8_0_OR_GREATER
                        await transaction.RollbackAsync();
#else
                        transaction.Rollback();
#endif

                    //Propagate exception
                    throw new RDFStoreException("Cannot delete data from SQLite store because: " + ex.Message, ex);
                }
                finally
                {
                    //Close connection
#if NET8_0_OR_GREATER
                    await connection.CloseAsync();
#else
                    connection.Close();
#endif
                    connection.Dispose();
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

            SQLiteConnection connection = new SQLiteConnection(ConnectionString);
            SQLiteTransaction transaction = null;
            try
            {
                //Open connection
                await connection.OpenAsync();

                //Create command
                using (SQLiteCommand deleteCommand = new SQLiteCommand(connection) { CommandTimeout = Options.DeleteTimeout })
                {
                    PrepareSelectDeleteCommand(deleteCommand, "DELETE FROM Quadruples", c, s, p, o, l);

                    //Prepare command
#if NET8_0_OR_GREATER
                    await deleteCommand.PrepareAsync();
#else
                    deleteCommand.Prepare();
#endif

                    //Open transaction
#if NET8_0_OR_GREATER
                    transaction = (SQLiteTransaction)await connection.BeginTransactionAsync();
#else
                    transaction = connection.BeginTransaction();
#endif
                    deleteCommand.Transaction = transaction;

                    //Execute command
#if NET8_0_OR_GREATER
                    await deleteCommand.ExecuteNonQueryAsync();
#else
                    deleteCommand.ExecuteNonQuery();
#endif

                    //Commit transaction
#if NET8_0_OR_GREATER
                    await transaction.CommitAsync();
#else
                    transaction.Commit();
#endif
                }
            }
            catch (Exception ex)
            {
                //Rollback transaction
                if (transaction != null)
#if NET8_0_OR_GREATER
                    await transaction.RollbackAsync();
#else
                    transaction.Rollback();
#endif

                //Propagate exception
                throw new RDFStoreException("Cannot delete data from SQLite store because: " + ex.Message, ex);
            }
            finally
            {
                //Close connection
#if NET8_0_OR_GREATER
                await connection.CloseAsync();
#else
                connection.Close();
#endif
                connection.Dispose();
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
            SQLiteConnection connection = new SQLiteConnection(ConnectionString);
            SQLiteTransaction transaction = null;
            try
            {
                //Open connection
                await connection.OpenAsync();

                //Create command
                using (SQLiteCommand deleteCommand = new SQLiteCommand("DELETE FROM Quadruples", connection) { CommandTimeout = Options.DeleteTimeout })
                {
                    //Prepare command
#if NET8_0_OR_GREATER
                    await deleteCommand.PrepareAsync();
#else
                    deleteCommand.Prepare();
#endif

                    //Open transaction
#if NET8_0_OR_GREATER
                    transaction = (SQLiteTransaction)await connection.BeginTransactionAsync();
#else
                    transaction = connection.BeginTransaction();
#endif
                    deleteCommand.Transaction = transaction;

                    //Execute command
                    await deleteCommand.ExecuteNonQueryAsync();

                    //Commit transaction
#if NET8_0_OR_GREATER
                    await transaction.CommitAsync();
#else
                    transaction.Commit();
#endif
                }
            }
            catch (Exception ex)
            {
                //Rollback transaction
                if (transaction != null)
#if NET8_0_OR_GREATER
                    await transaction.RollbackAsync();
#else
                    transaction.Rollback();
#endif

                //Propagate exception
                throw new RDFStoreException("Cannot delete data from SQLite store because: " + ex.Message, ex);
            }
            finally
            {
                //Close connection
#if NET8_0_OR_GREATER
                await connection.CloseAsync();
#else
                connection.Close();
#endif
                connection.Dispose();
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

            SQLiteConnection connection = new SQLiteConnection(ConnectionString);
            try
            {
                //Open connection
                await connection.OpenAsync();

                //Create command
                using (SQLiteCommand selectCommand = new SQLiteCommand("SELECT EXISTS(SELECT 1 FROM Quadruples WHERE QuadrupleID = @QID)", connection) { CommandTimeout = Options.SelectTimeout })
                {
                    selectCommand.Parameters.Add(new SQLiteParameter("QID", DbType.Int64));

                    //Valorize parameters
                    selectCommand.Parameters["QID"].Value = quadruple.QuadrupleID;

                    //Prepare command
#if NET8_0_OR_GREATER
                    await selectCommand.PrepareAsync();
#else
                    selectCommand.Prepare();
#endif

                    //Execute command
#if NET8_0_OR_GREATER
                    int result = int.Parse((await selectCommand.ExecuteScalarAsync()).ToString());
#else
                    int result = int.Parse(selectCommand.ExecuteScalar().ToString());
#endif

                    //Give result
                    return result == 1;
                }
            }
            catch (Exception ex)
            {
                //Propagate exception
                throw new RDFStoreException("Cannot read data from SQLite store because: " + ex.Message, ex);
            }
            finally
            {
                //Close connection
#if NET8_0_OR_GREATER
                await connection.CloseAsync();
#else
                connection.Close();
#endif
                connection.Dispose();
            }
        }

        /// <summary>
        /// Selects the quadruples which satisfy the given combination of CSPOL accessors<br/>
        /// (null values are handled as * selectors. Object and Literal params, if given, must be mutually exclusive!)
        /// </summary>
        /// <exception cref="RDFStoreException"></exception>
        public override List<RDFQuadruple> SelectQuadruples(RDFContext c=null, RDFResource s=null, RDFResource p=null, RDFResource o=null, RDFLiteral l=null)
            => SelectQuadruplesAsync(c, s, p, o, l).GetAwaiter().GetResult();

        /// <summary>
        /// Asynchronously selects the quadruples which satisfy the given combination of CSPOL accessors<br/>
        /// (null values are handled as * selectors. Object and Literal params, if given, must be mutually exclusive!)
        /// </summary>
        /// <exception cref="RDFStoreException"></exception>
        public override async Task<List<RDFQuadruple>> SelectQuadruplesAsync(RDFContext c=null, RDFResource s=null, RDFResource p=null, RDFResource o=null, RDFLiteral l=null)
        {
            #region Guards
            if (o != null && l != null)
                throw new RDFStoreException("Cannot access a store when both object and literals are given: they must be mutually exclusive!");
            #endregion

            List<RDFQuadruple> result = new List<RDFQuadruple>();

            SQLiteConnection connection = new SQLiteConnection(ConnectionString);
            try
            {
                //Open connection
                await connection.OpenAsync();

                //Create command
                using (SQLiteCommand selectCommand = new SQLiteCommand(connection) { CommandTimeout = Options.SelectTimeout })
                {
                    PrepareSelectDeleteCommand(selectCommand, "SELECT TripleFlavor, Context, Subject, Predicate, Object FROM Quadruples", c, s, p, o, l);

                    //Execute command
#if NET8_0_OR_GREATER
                    using (SQLiteDataReader quadruples = (SQLiteDataReader)await selectCommand.ExecuteReaderAsync(CommandBehavior.Default))
#else
                    using (SQLiteDataReader quadruples = selectCommand.ExecuteReader(CommandBehavior.Default))
#endif
                    {
                        while (quadruples.Read())
                            result.Add(RDFStoreUtilities.ParseQuadruple(quadruples));
                    }
                }
            }
            catch (Exception ex)
            {
                //Propagate exception
                throw new RDFStoreException("Cannot read data from SQLite store because: " + ex.Message, ex);
            }
            finally
            {
                //Close connection
#if NET8_0_OR_GREATER
                await connection.CloseAsync();
#else
                connection.Close();
#endif
                connection.Dispose();
            }

            return result;
        }

        /// <summary>
        /// Asynchronously counts the SQLite database quadruples
        /// </summary>
        private async Task<long> GetQuadruplesCountAsync()
        {
            SQLiteConnection connection = new SQLiteConnection(ConnectionString);
            try
            {
                //Open connection
                await connection.OpenAsync();

                //Create command
                using (SQLiteCommand selectCommand = new SQLiteCommand("SELECT COUNT(*) FROM Quadruples", connection) { CommandTimeout = Options.SelectTimeout })
                {
                    //Execute command
#if NET8_0_OR_GREATER
                    long result = long.Parse((await selectCommand.ExecuteScalarAsync()).ToString());
#else
                    long result = long.Parse(selectCommand.ExecuteScalar().ToString());
#endif

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
#if NET8_0_OR_GREATER
                await connection.CloseAsync();
#else
                connection.Close();
#endif
                connection.Dispose();
            }
        }
#endregion

        #region Optimize
        /// <summary>
        /// Asynchronously executes a special command to optimize SQLite store
        /// </summary>
        public async Task OptimizeAsync()
        {
            SQLiteConnection connection = new SQLiteConnection(ConnectionString);
            try
            {
                //Open connection
                await connection.OpenAsync();

                //Create command
                using (SQLiteCommand optimizeCommand = new SQLiteCommand("VACUUM", connection))
                {
                    //Execute command
                    await optimizeCommand.ExecuteNonQueryAsync();
                }
            }
            catch (Exception ex)
            {
                //Propagate exception
                throw new RDFStoreException("Cannot optimize SQLite store because: " + ex.Message, ex);
            }
            finally
            {
                //Close connection
#if NET8_0_OR_GREATER
                await connection.CloseAsync();
#else
                connection.Close();
#endif
                connection.Dispose();
            }
        }
        #endregion

        #region Utilities
        private void PrepareSelectDeleteCommand(SQLiteCommand command, string baseSql, RDFContext c, RDFResource s, RDFResource p, RDFResource o, RDFLiteral l)
        {
            List<string> conditions = new List<string>();

            if (c != null)
            {
                conditions.Add("ContextID = @CTXID");
                command.Parameters.Add(new SQLiteParameter("CTXID", DbType.Int64));
                command.Parameters["CTXID"].Value = c.PatternMemberID;
            }
            if (s != null)
            {
                conditions.Add("SubjectID = @SUBJID");
                command.Parameters.Add(new SQLiteParameter("SUBJID", DbType.Int64));
                command.Parameters["SUBJID"].Value = s.PatternMemberID;
            }
            if (p != null)
            {
                conditions.Add("PredicateID = @PREDID");
                command.Parameters.Add(new SQLiteParameter("PREDID", DbType.Int64));
                command.Parameters["PREDID"].Value = p.PatternMemberID;
            }
            if (o != null)
            {
                conditions.Add("ObjectID = @OBJID");
                command.Parameters.Add(new SQLiteParameter("OBJID", DbType.Int64));
                command.Parameters["OBJID"].Value = o.PatternMemberID;
                conditions.Add("TripleFlavor = @TFV");
                command.Parameters.Add(new SQLiteParameter("TFV", DbType.Int32));
                command.Parameters["TFV"].Value = (int)RDFModelEnums.RDFTripleFlavors.SPO;
            }
            if (l != null)
            {
                conditions.Add("ObjectID = @OBJID");
                command.Parameters.Add(new SQLiteParameter("OBJID", DbType.Int64));
                command.Parameters["OBJID"].Value = l.PatternMemberID;
                conditions.Add("TripleFlavor = @TFV");
                command.Parameters.Add(new SQLiteParameter("TFV", DbType.Int32));
                command.Parameters["TFV"].Value = (int)RDFModelEnums.RDFTripleFlavors.SPL;
            }

            command.CommandText = conditions.Count > 0
                ? $"{baseSql} WHERE {string.Join(" AND ", conditions)}"
                : baseSql;
        }
        #endregion

        #endregion
    }
}
