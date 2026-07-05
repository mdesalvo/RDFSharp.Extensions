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

using System;
using System.Collections.Generic;
using System.Data;
using System.Threading;
using System.Threading.Tasks;
using MySql.Data.MySqlClient;
using RDFSharp.Model;
using RDFSharp.Store;

namespace RDFSharp.Extensions.MySQL
{
    /// <summary>
    /// RDFMySQLStore represents a store backed on MySQL engine
    /// </summary>
    #if NET8_0_OR_GREATER
    public sealed class RDFMySQLStore : RDFStore, IDisposable, IAsyncDisposable
    #else
    public sealed class RDFMySQLStore : RDFStore, IDisposable
    #endif
    {
        #region Properties
        /// <summary>
        /// Count of the MySQL database quadruples (-1 in case of errors)
        /// </summary>
        public override long QuadruplesCount
            => GetQuadruplesCountAsync().GetAwaiter().GetResult();

        /// <summary>
        /// Asynchronous count of the MySQL database quadruples (-1 in case of errors)
        /// </summary>
        public override Task<long> QuadruplesCountAsync
            => GetQuadruplesCountAsync();

        /// <summary>
        /// Connection string to the MySQL database (a new connection is opened for each operation,
        /// relying on ADO.NET's own connection pooling, so that the store is safe to use concurrently
        /// -es. as a singleton registered in an ASP.NET Core DI container)
        /// </summary>
        private readonly string ConnectionString;

        /// <summary>
        /// Options customizing the behaviour of the store
        /// </summary>
        private readonly RDFMySQLStoreOptions Options;

        /// <summary>
        /// Flag indicating that the MySQL store instance has already been disposed
        /// </summary>
        private bool Disposed { get; set; }
        #endregion

        #region Ctors
        /// <summary>
        /// Default-ctor to build a MySQL store instance (with eventual options)
        /// </summary>
        public RDFMySQLStore(string mysqlConnectionString, RDFMySQLStoreOptions mysqlStoreOptions = null)
        {
            #region Guards
            if (string.IsNullOrEmpty(mysqlConnectionString))
                throw new RDFStoreException("Cannot connect to MySQL store because: given \"mysqlConnectionString\" parameter is null or empty.");
            #endregion

            //Initialize options
            Options = mysqlStoreOptions ?? new RDFMySQLStoreOptions();

            //Initialize store structures
            try
            {
                RDFMySQLStoreManager mySqlStoreManager = new RDFMySQLStoreManager(mysqlConnectionString);
                mySqlStoreManager.EnsureQuadruplesTableExistsAsync().GetAwaiter().GetResult();

                StoreType = "MYSQL";
                ConnectionString = mysqlConnectionString;
                StoreID = RDFModelUtilities.CreateHash(ToString());
                Disposed = false;
            }
            catch (Exception ex)
            {
                throw new RDFStoreException("Cannot create MySQL store because: " + ex.Message, ex);
            }
        }

        /// <summary>
        /// Destroys the MySQL store instance
        /// </summary>
        ~RDFMySQLStore()
            => Dispose(false);
        #endregion

        #region Interfaces
        /// <summary>
        /// Gives the string representation of the MySQL store
        /// </summary>
        public override string ToString()
        {
            MySqlConnectionStringBuilder builder = new MySqlConnectionStringBuilder(ConnectionString);
            return $"{base.ToString()}|SERVER={builder.Server};DATABASE={builder.Database}";
        }

        /// <summary>
        /// Disposes the MySQL store instance
        /// </summary>
        public void Dispose()
        {
            Dispose(true);
            GC.SuppressFinalize(this);
        }

#if NET8_0_OR_GREATER
        /// <summary>
        /// Asynchronously disposes the MySQL store (IAsyncDisposable)
        /// </summary>
        ValueTask IAsyncDisposable.DisposeAsync()
        {
            Dispose(true);
            GC.SuppressFinalize(this);
            return ValueTask.CompletedTask;
        }
#endif

        /// <summary>
        /// Disposes the MySQL store instance  (business logic of resources disposal)
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

                MySqlConnection connection = new MySqlConnection(ConnectionString);
                MySqlTransaction transaction = null;
                try
                {
                    //Open connection
                    await connection.OpenAsync();

                    //Create command
                    using (MySqlCommand insertCommand = new MySqlCommand(
                        "INSERT IGNORE INTO Quadruples(QuadrupleID, TripleFlavor, Context, ContextID, Subject, SubjectID, Predicate, PredicateID, Object, ObjectID) VALUES (@QID, @TFV, @CTX, @CTXID, @SUBJ, @SUBJID, @PRED, @PREDID, @OBJ, @OBJID)",
                        connection) { CommandTimeout = Options.InsertTimeout })
                    {
                        insertCommand.Parameters.Add(new MySqlParameter("QID", MySqlDbType.Int64));
                        insertCommand.Parameters.Add(new MySqlParameter("TFV", MySqlDbType.Int32));
                        insertCommand.Parameters.Add(new MySqlParameter("CTX", MySqlDbType.VarChar, 1000));
                        insertCommand.Parameters.Add(new MySqlParameter("CTXID", MySqlDbType.Int64));
                        insertCommand.Parameters.Add(new MySqlParameter("SUBJ", MySqlDbType.VarChar, 1000));
                        insertCommand.Parameters.Add(new MySqlParameter("SUBJID", MySqlDbType.Int64));
                        insertCommand.Parameters.Add(new MySqlParameter("PRED", MySqlDbType.VarChar, 1000));
                        insertCommand.Parameters.Add(new MySqlParameter("PREDID", MySqlDbType.Int64));
                        insertCommand.Parameters.Add(new MySqlParameter("OBJ", MySqlDbType.VarChar, 1000));
                        insertCommand.Parameters.Add(new MySqlParameter("OBJID", MySqlDbType.Int64));

                        //Prepare command
                        await insertCommand.PrepareAsync();

                        //Open transaction
                        transaction = await connection.BeginTransactionAsync();
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
                        await transaction.CommitAsync();
                    }
                }
                catch (Exception ex)
                {
                    //Rollback transaction
                    if (transaction != null)
                        await transaction.RollbackAsync();

                    //Propagate exception
                    throw new RDFStoreException("Cannot insert data into MySQL store because: " + ex.Message, ex);
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
                MySqlConnection connection = new MySqlConnection(ConnectionString);
                MySqlTransaction transaction = null;
                try
                {
                    //Open connection
                    await connection.OpenAsync();

                    //Create command
                    using (MySqlCommand insertCommand = new MySqlCommand(
                        "INSERT IGNORE INTO Quadruples(QuadrupleID, TripleFlavor, Context, ContextID, Subject, SubjectID, Predicate, PredicateID, Object, ObjectID) VALUES (@QID, @TFV, @CTX, @CTXID, @SUBJ, @SUBJID, @PRED, @PREDID, @OBJ, @OBJID)",
                        connection) { CommandTimeout = Options.InsertTimeout })
                    {
                        insertCommand.Parameters.Add(new MySqlParameter("QID", MySqlDbType.Int64));
                        insertCommand.Parameters.Add(new MySqlParameter("TFV", MySqlDbType.Int32));
                        insertCommand.Parameters.Add(new MySqlParameter("CTX", MySqlDbType.VarChar, 1000));
                        insertCommand.Parameters.Add(new MySqlParameter("CTXID", MySqlDbType.Int64));
                        insertCommand.Parameters.Add(new MySqlParameter("SUBJ", MySqlDbType.VarChar, 1000));
                        insertCommand.Parameters.Add(new MySqlParameter("SUBJID", MySqlDbType.Int64));
                        insertCommand.Parameters.Add(new MySqlParameter("PRED", MySqlDbType.VarChar, 1000));
                        insertCommand.Parameters.Add(new MySqlParameter("PREDID", MySqlDbType.Int64));
                        insertCommand.Parameters.Add(new MySqlParameter("OBJ", MySqlDbType.VarChar, 1000));
                        insertCommand.Parameters.Add(new MySqlParameter("OBJID", MySqlDbType.Int64));

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
                        await insertCommand.PrepareAsync();

                        //Open transaction
                        transaction = await connection.BeginTransactionAsync();
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
                    throw new RDFStoreException("Cannot insert data into MySQL store because: " + ex.Message, ex);
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
                MySqlConnection connection = new MySqlConnection(ConnectionString);
                MySqlTransaction transaction = null;
                try
                {
                    //Open connection
                    await connection.OpenAsync();

                    //Create command
                    using (MySqlCommand deleteCommand = new MySqlCommand("DELETE FROM Quadruples WHERE QuadrupleID = @QID", connection) { CommandTimeout = Options.DeleteTimeout })
                    {
                        deleteCommand.Parameters.Add(new MySqlParameter("QID", MySqlDbType.Int64));

                        //Valorize parameters
                        deleteCommand.Parameters["QID"].Value = quadruple.QuadrupleID;

                        //Prepare command
                        await deleteCommand.PrepareAsync();

                        //Open transaction
                        transaction = await connection.BeginTransactionAsync();
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
                    throw new RDFStoreException("Cannot delete data from MySQL store because: " + ex.Message, ex);
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

            MySqlConnection connection = new MySqlConnection(ConnectionString);
            MySqlTransaction transaction = null;
            try
            {
                //Open connection
                await connection.OpenAsync();

                //Create command
                using (MySqlCommand deleteCommand = new MySqlCommand { Connection = connection, CommandTimeout = Options.DeleteTimeout })
                {
                    PrepareSelectDeleteCommand(deleteCommand, "DELETE FROM Quadruples", c, s, p, o, l);

                    //Prepare command
                    await deleteCommand.PrepareAsync();

                    //Open transaction
                    transaction = await connection.BeginTransactionAsync();
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
                throw new RDFStoreException("Cannot delete data from MySQL store because: " + ex.Message, ex);
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
            MySqlConnection connection = new MySqlConnection(ConnectionString);
            MySqlTransaction transaction = null;
            try
            {
                //Open connection
                await connection.OpenAsync();

                //Create command
                using (MySqlCommand deleteCommand = new MySqlCommand("DELETE FROM Quadruples", connection) { CommandTimeout = Options.DeleteTimeout })
                {
                    //Prepare command
                    await deleteCommand.PrepareAsync();

                    //Open transaction
                    transaction = await connection.BeginTransactionAsync();
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
                throw new RDFStoreException("Cannot delete data from MySQL store because: " + ex.Message, ex);
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

            MySqlConnection connection = new MySqlConnection(ConnectionString);
            try
            {
                //Open connection
                await connection.OpenAsync();

                //Create command
                using (MySqlCommand selectCommand = new MySqlCommand("SELECT COUNT(1) WHERE EXISTS(SELECT 1 FROM Quadruples WHERE QuadrupleID = @QID)", connection) { CommandTimeout = Options.SelectTimeout })
                {
                    selectCommand.Parameters.Add(new MySqlParameter("QID", MySqlDbType.Int64));

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
                throw new RDFStoreException("Cannot read data from MySQL store because: " + ex.Message, ex);
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
        /// (null values are handled as * selectors. Object and Literal params, if given, must be mutually exclusive!)
        /// </summary>
        /// <exception cref="RDFStoreException"></exception>
        public override List<RDFQuadruple> SelectQuadruples(RDFContext c=null, RDFResource s=null, RDFResource p=null, RDFResource o=null, RDFLiteral l=null)
            => SelectQuadruplesAsync(c,s,p,o,l).GetAwaiter().GetResult();

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

            List<RDFQuadruple>  result = new List<RDFQuadruple>();

            MySqlConnection connection = new MySqlConnection(ConnectionString);
            try
            {
                //Open connection
                await connection.OpenAsync();

                //Create command
                using (MySqlCommand selectCommand = new MySqlCommand { Connection = connection, CommandTimeout = Options.SelectTimeout })
                {
                    PrepareSelectDeleteCommand(selectCommand, "SELECT TripleFlavor, Context, Subject, Predicate, Object FROM Quadruples", c, s, p, o, l);

                    //Execute command
                    using (MySqlDataReader quadruples = await selectCommand.ExecuteReaderAsync(CommandBehavior.Default))
                    {
                        while (quadruples.Read())
                            result.Add(RDFStoreUtilities.ParseQuadruple(quadruples));
                    }
                }
            }
            catch (Exception ex)
            {
                //Propagate exception
                throw new RDFStoreException("Cannot read data from MySQL store because: " + ex.Message, ex);
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
        /// Asynchronously counts the MySQL database quadruples
        /// </summary>
        private async Task<long> GetQuadruplesCountAsync()
        {
            MySqlConnection connection = new MySqlConnection(ConnectionString);
            try
            {
                //Open connection
                await connection.OpenAsync();

                //Create command
                using (MySqlCommand selectCommand = new MySqlCommand("SELECT COUNT(*) FROM Quadruples", connection) { CommandTimeout = Options.SelectTimeout })
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
        /// Asynchronously optimizes "Quadruples" table of MySQL store
        /// </summary>
        public async Task OptimizeAsync()
        {
            MySqlConnection connection = new MySqlConnection(ConnectionString);
            try
            {
                //Open connection
                await connection.OpenAsync();

                //Create command
                using (MySqlCommand optimizeCommand = new MySqlCommand("OPTIMIZE TABLE Quadruples", connection))
                {
                    //Execute command
                    await optimizeCommand.ExecuteNonQueryAsync();
                }
            }
            catch (Exception ex)
            {
                //Propagate exception
                throw new RDFStoreException("Cannot optimize MySQL store because: " + ex.Message, ex);
            }
            finally
            {
                //Close connection
                await connection.CloseAsync();
                await connection.DisposeAsync();
            }
        }
        #endregion

        #region Utilities
        private void PrepareSelectDeleteCommand(MySqlCommand command, string baseSql, RDFContext c, RDFResource s, RDFResource p, RDFResource o, RDFLiteral l)
        {
            List<string> conditions = new List<string>();

            if (c != null)
            {
                conditions.Add("ContextID = @CTXID");
                command.Parameters.Add(new MySqlParameter("CTXID", MySqlDbType.Int64));
                command.Parameters["CTXID"].Value = c.PatternMemberID;
            }
            if (s != null)
            {
                conditions.Add("SubjectID = @SUBJID");
                command.Parameters.Add(new MySqlParameter("SUBJID", MySqlDbType.Int64));
                command.Parameters["SUBJID"].Value = s.PatternMemberID;
            }
            if (p != null)
            {
                conditions.Add("PredicateID = @PREDID");
                command.Parameters.Add(new MySqlParameter("PREDID", MySqlDbType.Int64));
                command.Parameters["PREDID"].Value = p.PatternMemberID;
            }
            if (o != null)
            {
                conditions.Add("ObjectID = @OBJID");
                command.Parameters.Add(new MySqlParameter("OBJID", MySqlDbType.Int64));
                command.Parameters["OBJID"].Value = o.PatternMemberID;
                conditions.Add("TripleFlavor = @TFV");
                command.Parameters.Add(new MySqlParameter("TFV", MySqlDbType.Int32));
                command.Parameters["TFV"].Value = (int)RDFModelEnums.RDFTripleFlavors.SPO;
            }
            if (l != null)
            {
                conditions.Add("ObjectID = @OBJID");
                command.Parameters.Add(new MySqlParameter("OBJID", MySqlDbType.Int64));
                command.Parameters["OBJID"].Value = l.PatternMemberID;
                conditions.Add("TripleFlavor = @TFV");
                command.Parameters.Add(new MySqlParameter("TFV", MySqlDbType.Int32));
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
