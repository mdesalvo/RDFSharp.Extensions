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

using Microsoft.Data.SqlClient;
using RDFSharp.Model;
using RDFSharp.Store;
using System;
using System.Collections.Generic;
using System.Data;
using System.Threading;
using System.Threading.Tasks;

namespace RDFSharp.Extensions.SQLServer
{
    /// <summary>
    /// RDFSQLServerStore represents a RDFStore backed on SQL Server engine
    /// </summary>
    #if NET8_0_OR_GREATER
    public sealed class RDFSQLServerStore : RDFStore, IDisposable, IAsyncDisposable
    #else
    public sealed class RDFSQLServerStore : RDFStore, IDisposable
    #endif
    {
        #region Properties
        /// <summary>
        /// Count of the SQL Server database quadruples (-1 in case of errors)
        /// </summary>
        public override long QuadruplesCount
            => GetQuadruplesCountAsync().GetAwaiter().GetResult();

        /// <summary>
        /// Asynchronous count of the SQL Server database quadruples (-1 in case of errors)
        /// </summary>
        public override Task<long> QuadruplesCountAsync
            => GetQuadruplesCountAsync();

        /// <summary>
        /// Connection string to the SQL Server database (a new connection is opened for each operation,
        /// relying on ADO.NET's own connection pooling, so that the store is safe to use concurrently
        /// -es. as a singleton registered in an ASP.NET Core DI container)
        /// </summary>
        private readonly string ConnectionString;

        /// <summary>
        /// Options customizing the behaviour of the store
        /// </summary>
        private readonly RDFSQLServerStoreOptions Options;

        /// <summary>
        /// Flag indicating that the SQL Server store instance has already been disposed
        /// </summary>
        private bool Disposed { get; set; }
        #endregion

        #region Ctors
        /// <summary>
        /// Default-ctor to build a SQL Server store instance (with eventual options)
        /// </summary>
        public RDFSQLServerStore(string sqlserverConnectionString, RDFSQLServerStoreOptions sqlserverStoreOptions=null)
        {
            #region Guards
            if (string.IsNullOrEmpty(sqlserverConnectionString))
                throw new RDFStoreException("Cannot connect to SQL Server store because: given \"sqlserverConnectionString\" parameter is null or empty.");
            #endregion

            //Initialize options
            Options = sqlserverStoreOptions ?? new RDFSQLServerStoreOptions();

            //Initialize store structures
            try
            {
                RDFSQLServerStoreManager sqlserverStoreManager = new RDFSQLServerStoreManager(sqlserverConnectionString);
                sqlserverStoreManager.EnsureQuadruplesTableExistsAsync().GetAwaiter().GetResult();

                StoreType = "SQLSERVER";
                ConnectionString = sqlserverConnectionString;
                StoreID = RDFModelUtilities.CreateHash(ToString());
                Disposed = false;
            }
            catch (Exception ex)
            {
                throw new RDFStoreException("Cannot create SQL Server store because: " + ex.Message, ex);
            }
        }

        /// <summary>
        /// Destroys the SQL Server store instance
        /// </summary>
        ~RDFSQLServerStore()
            => Dispose(false);
        #endregion

        #region Interfaces
        /// <summary>
        /// Gives the string representation of the SQL Server store
        /// </summary>
        public override string ToString()
        {
            SqlConnectionStringBuilder builder = new SqlConnectionStringBuilder(ConnectionString);
            return $"{base.ToString()}|SERVER={builder.DataSource};DATABASE={builder.InitialCatalog}";
        }

        /// <summary>
        /// Disposes the SQL Server store instance
        /// </summary>
        public void Dispose()
        {
            Dispose(true);
            GC.SuppressFinalize(this);
        }

#if NET8_0_OR_GREATER
        /// <summary>
        /// Asynchronously disposes the SQL Server store (IAsyncDisposable)
        /// </summary>
        ValueTask IAsyncDisposable.DisposeAsync()
        {
            Dispose(true);
            GC.SuppressFinalize(this);
            return ValueTask.CompletedTask;
        }
#endif

        /// <summary>
        /// Disposes the SQL Server store instance  (business logic of resources disposal)
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

                SqlConnection connection = new SqlConnection(ConnectionString);
                SqlTransaction transaction = null;
                try
                {
                    //Open connection
#if NET8_0_OR_GREATER
                    await connection.OpenAsync();
#else
                    connection.Open();
#endif

                    //Create command
                    using (SqlCommand insertCommand = new SqlCommand(
                        "IF NOT EXISTS(SELECT 1 FROM [dbo].[Quadruples] WHERE [QuadrupleID] = @QID) BEGIN INSERT INTO [dbo].[Quadruples]([QuadrupleID], [TripleFlavor], [Context], [ContextID], [Subject], [SubjectID], [Predicate], [PredicateID], [Object], [ObjectID]) VALUES (@QID, @TFV, @CTX, @CTXID, @SUBJ, @SUBJID, @PRED, @PREDID, @OBJ, @OBJID) END",
                        connection) { CommandTimeout = Options.InsertTimeout })
                    {
                        insertCommand.Parameters.Add(new SqlParameter("QID", SqlDbType.BigInt));
                        insertCommand.Parameters.Add(new SqlParameter("TFV", SqlDbType.Int));
                        insertCommand.Parameters.Add(new SqlParameter("CTX", SqlDbType.VarChar, 1000));
                        insertCommand.Parameters.Add(new SqlParameter("CTXID", SqlDbType.BigInt));
                        insertCommand.Parameters.Add(new SqlParameter("SUBJ", SqlDbType.VarChar, 1000));
                        insertCommand.Parameters.Add(new SqlParameter("SUBJID", SqlDbType.BigInt));
                        insertCommand.Parameters.Add(new SqlParameter("PRED", SqlDbType.VarChar, 1000));
                        insertCommand.Parameters.Add(new SqlParameter("PREDID", SqlDbType.BigInt));
                        insertCommand.Parameters.Add(new SqlParameter("OBJ", SqlDbType.VarChar, 1000));
                        insertCommand.Parameters.Add(new SqlParameter("OBJID", SqlDbType.BigInt));

                        //Prepare command
#if NET8_0_OR_GREATER
                        await insertCommand.PrepareAsync();
#else
                        insertCommand.Prepare();
#endif

                        //Open transaction
#if NET8_0_OR_GREATER
                        transaction = (SqlTransaction)await connection.BeginTransactionAsync(CancellationToken.None);
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
#if NET8_0_OR_GREATER
                            await insertCommand.ExecuteNonQueryAsync();
#else
                            insertCommand.ExecuteNonQuery();
#endif
                        }

                        //Commit transaction
#if NET8_0_OR_GREATER
                        await transaction.CommitAsync(CancellationToken.None);
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
                        await transaction.RollbackAsync(CancellationToken.None);
#else
                        transaction.Rollback();
#endif

                    //Propagate exception
                    throw new RDFStoreException("Cannot insert data into SQL Server store because: " + ex.Message, ex);
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
                SqlConnection connection = new SqlConnection(ConnectionString);
                SqlTransaction transaction = null;
                try
                {
                    //Open connection
#if NET8_0_OR_GREATER
                    await connection.OpenAsync();
#else
                    connection.Open();
#endif

                    //Create command
                    using (SqlCommand insertCommand = new SqlCommand(
                        "IF NOT EXISTS(SELECT 1 FROM [dbo].[Quadruples] WHERE [QuadrupleID] = @QID) BEGIN INSERT INTO [dbo].[Quadruples]([QuadrupleID], [TripleFlavor], [Context], [ContextID], [Subject], [SubjectID], [Predicate], [PredicateID], [Object], [ObjectID]) VALUES (@QID, @TFV, @CTX, @CTXID, @SUBJ, @SUBJID, @PRED, @PREDID, @OBJ, @OBJID) END",
                        connection) { CommandTimeout = Options.InsertTimeout })
                    {
                        insertCommand.Parameters.Add(new SqlParameter("QID", SqlDbType.BigInt));
                        insertCommand.Parameters.Add(new SqlParameter("TFV", SqlDbType.Int));
                        insertCommand.Parameters.Add(new SqlParameter("CTX", SqlDbType.VarChar, 1000));
                        insertCommand.Parameters.Add(new SqlParameter("CTXID", SqlDbType.BigInt));
                        insertCommand.Parameters.Add(new SqlParameter("SUBJ", SqlDbType.VarChar, 1000));
                        insertCommand.Parameters.Add(new SqlParameter("SUBJID", SqlDbType.BigInt));
                        insertCommand.Parameters.Add(new SqlParameter("PRED", SqlDbType.VarChar, 1000));
                        insertCommand.Parameters.Add(new SqlParameter("PREDID", SqlDbType.BigInt));
                        insertCommand.Parameters.Add(new SqlParameter("OBJ", SqlDbType.VarChar, 1000));
                        insertCommand.Parameters.Add(new SqlParameter("OBJID", SqlDbType.BigInt));

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
                        transaction = (SqlTransaction)await connection.BeginTransactionAsync(CancellationToken.None);
#else
                        transaction = connection.BeginTransaction();
#endif
                        insertCommand.Transaction = transaction;

                        //Execute command
#if NET8_0_OR_GREATER
                        await insertCommand.ExecuteNonQueryAsync();
#else
                        insertCommand.ExecuteNonQuery();
#endif

                        //Commit transaction
#if NET8_0_OR_GREATER
                        await transaction.CommitAsync(CancellationToken.None);
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
                        await transaction.RollbackAsync(CancellationToken.None);
#else
                        transaction.Rollback();
#endif

                    //Propagate exception
                    throw new RDFStoreException("Cannot insert data into SQL Server store because: " + ex.Message, ex);
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
                SqlConnection connection = new SqlConnection(ConnectionString);
                SqlTransaction transaction = null;
                try
                {
                    //Open connection
#if NET8_0_OR_GREATER
                    await connection.OpenAsync();
#else
                    connection.Open();
#endif

                    //Create command
                    using (SqlCommand deleteCommand = new SqlCommand("DELETE FROM [dbo].[Quadruples] WHERE [QuadrupleID] = @QID", connection) { CommandTimeout = Options.DeleteTimeout })
                    {
                        deleteCommand.Parameters.Add(new SqlParameter("QID", SqlDbType.BigInt));

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
                        transaction = (SqlTransaction)await connection.BeginTransactionAsync(CancellationToken.None);
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
                        await transaction.CommitAsync(CancellationToken.None);
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
                        await transaction.RollbackAsync(CancellationToken.None);
#else
                        transaction.Rollback();
#endif

                    //Propagate exception
                    throw new RDFStoreException("Cannot delete data from SQL Server store because: " + ex.Message, ex);
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

            SqlConnection connection = new SqlConnection(ConnectionString);
            SqlTransaction transaction = null;
            try
            {
                //Open connection
#if NET8_0_OR_GREATER
                await connection.OpenAsync();
#else
                connection.Open();
#endif

                //Create command
                using (SqlCommand deleteCommand = new SqlCommand { Connection = connection, CommandTimeout = Options.DeleteTimeout })
                {
                    PrepareSelectDeleteCommand(deleteCommand, "DELETE FROM [dbo].[Quadruples]", c, s, p, o, l);

                    //Prepare command
#if NET8_0_OR_GREATER
                    await deleteCommand.PrepareAsync();
#else
                    deleteCommand.Prepare();
#endif

                    //Open transaction
#if NET8_0_OR_GREATER
                    transaction = (SqlTransaction)await connection.BeginTransactionAsync(CancellationToken.None);
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
                throw new RDFStoreException("Cannot delete data from SQL Server store because: " + ex.Message, ex);
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
            SqlConnection connection = new SqlConnection(ConnectionString);
            SqlTransaction transaction = null;
            try
            {
                //Open connection
#if NET8_0_OR_GREATER
                await connection.OpenAsync();
#else
                connection.Open();
#endif

                //Create command
                using (SqlCommand deleteCommand = new SqlCommand("DELETE FROM [dbo].[Quadruples]", connection) { CommandTimeout = Options.DeleteTimeout })
                {
                    //Prepare command
#if NET8_0_OR_GREATER
                    await deleteCommand.PrepareAsync();
#else
                    deleteCommand.Prepare();
#endif

                    //Open transaction
#if NET8_0_OR_GREATER
                    transaction = (SqlTransaction)await connection.BeginTransactionAsync(CancellationToken.None);
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
                throw new RDFStoreException("Cannot delete data from SQL Server store because: " + ex.Message, ex);
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

            SqlConnection connection = new SqlConnection(ConnectionString);
            try
            {
                //Open connection
#if NET8_0_OR_GREATER
                await connection.OpenAsync();
#else
                connection.Open();
#endif

                //Create command
                using (SqlCommand selectCommand = new SqlCommand("SELECT COUNT(1) WHERE EXISTS(SELECT 1 FROM [dbo].[Quadruples] WHERE [QuadrupleID] = @QID)", connection) { CommandTimeout = Options.SelectTimeout })
                {
                    selectCommand.Parameters.Add(new SqlParameter("QID", SqlDbType.BigInt));

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
                throw new RDFStoreException("Cannot read data from SQLServer store because: " + ex.Message, ex);
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

            SqlConnection connection = new SqlConnection(ConnectionString);
            try
            {
                //Open connection
#if NET8_0_OR_GREATER
                await connection.OpenAsync();
#else
                connection.Open();
#endif

                //Create command
                using (SqlCommand selectCommand = new SqlCommand { Connection = connection, CommandTimeout = Options.SelectTimeout })
                {
                    PrepareSelectDeleteCommand(selectCommand, "SELECT [TripleFlavor], [Context], [Subject], [Predicate], [Object] FROM [dbo].[Quadruples]", c, s, p, o, l);

                    //Execute command
#if NET8_0_OR_GREATER
                    using (SqlDataReader quadruples = await selectCommand.ExecuteReaderAsync(CommandBehavior.Default))
#else
                    using (SqlDataReader quadruples = selectCommand.ExecuteReader(CommandBehavior.Default))
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
                throw new RDFStoreException("Cannot read data from SQL Server store because: " + ex.Message, ex);
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
        /// Asynchronously counts the SQL Server database quadruples
        /// </summary>
        private async Task<long> GetQuadruplesCountAsync()
        {
            SqlConnection connection = new SqlConnection(ConnectionString);
            try
            {
                //Open connection
#if NET8_0_OR_GREATER
                await connection.OpenAsync();
#else
                connection.Open();
#endif

                //Create command
                using (SqlCommand selectCommand = new SqlCommand("SELECT COUNT(*) FROM [dbo].[Quadruples]", connection) { CommandTimeout = Options.SelectTimeout })
                {
                    //Execute command
#if NET8_0_OR_GREATER
                    long result = long.Parse((await selectCommand.ExecuteScalarAsync(CancellationToken.None)).ToString());
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
        /// Asynchronously executes a special command to optimize SQL Server store
        /// </summary>
        public async Task OptimizeAsync()
        {
            SqlConnection connection = new SqlConnection(ConnectionString);
            try
            {
                //Open connection
#if NET8_0_OR_GREATER
                await connection.OpenAsync();
#else
                connection.Open();
#endif

                //Create command
                using (SqlCommand optimizeCommand = new SqlCommand("ALTER INDEX ALL ON [dbo].[Quadruples] REORGANIZE;", connection))
                {
                    //Execute command
#if NET8_0_OR_GREATER
                    await optimizeCommand.ExecuteNonQueryAsync();
#else
                    optimizeCommand.ExecuteNonQuery();
#endif
                }
            }
            catch (Exception ex)
            {
                //Propagate exception
                throw new RDFStoreException("Cannot optimize SQL Server store because: " + ex.Message, ex);
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
        private void PrepareSelectDeleteCommand(SqlCommand command, string baseSql, RDFContext c, RDFResource s, RDFResource p, RDFResource o, RDFLiteral l)
        {
            List<string> conditions = new List<string>();

            if (c != null)
            {
                conditions.Add("[ContextID] = @CTXID");
                command.Parameters.Add(new SqlParameter("CTXID", SqlDbType.BigInt));
                command.Parameters["CTXID"].Value = c.PatternMemberID;
            }
            if (s != null)
            {
                conditions.Add("[SubjectID] = @SUBJID");
                command.Parameters.Add(new SqlParameter("SUBJID", SqlDbType.BigInt));
                command.Parameters["SUBJID"].Value = s.PatternMemberID;
            }
            if (p != null)
            {
                conditions.Add("[PredicateID] = @PREDID");
                command.Parameters.Add(new SqlParameter("PREDID", SqlDbType.BigInt));
                command.Parameters["PREDID"].Value = p.PatternMemberID;
            }
            if (o != null)
            {
                conditions.Add("[ObjectID] = @OBJID");
                command.Parameters.Add(new SqlParameter("OBJID", SqlDbType.BigInt));
                command.Parameters["OBJID"].Value = o.PatternMemberID;
                conditions.Add("[TripleFlavor] = @TFV");
                command.Parameters.Add(new SqlParameter("TFV", SqlDbType.Int));
                command.Parameters["TFV"].Value = RDFModelEnums.RDFTripleFlavors.SPO;
            }
            if (l != null)
            {
                conditions.Add("[ObjectID] = @OBJID");
                command.Parameters.Add(new SqlParameter("OBJID", SqlDbType.BigInt));
                command.Parameters["OBJID"].Value = l.PatternMemberID;
                conditions.Add("[TripleFlavor] = @TFV");
                command.Parameters.Add(new SqlParameter("TFV", SqlDbType.Int));
                command.Parameters["TFV"].Value = RDFModelEnums.RDFTripleFlavors.SPL;
            }

            command.CommandText = conditions.Count > 0
                ? $"{baseSql} WHERE {string.Join(" AND ", conditions)}"
                : baseSql;
        }
        #endregion

        #endregion
    }
}
