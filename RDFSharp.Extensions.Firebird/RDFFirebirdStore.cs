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
using FirebirdSql.Data.FirebirdClient;
using RDFSharp.Model;
using RDFSharp.Store;

namespace RDFSharp.Extensions.Firebird
{
    /// <summary>
    /// RDFFirebirdStore represents a store backed on Firebird engine
    /// </summary>
    #if NET8_0_OR_GREATER
    public sealed class RDFFirebirdStore : RDFStore, IDisposable, IAsyncDisposable
    #else
    public sealed class RDFFirebirdStore : RDFStore, IDisposable
    #endif
    {
        #region Properties
        /// <summary>
        /// Count of the Firebird service quadruples (-1 in case of errors)
        /// </summary>
        public override long QuadruplesCount
            => GetQuadruplesCountAsync().GetAwaiter().GetResult();

        /// <summary>
        /// Asynchronous count of the Firebird service quadruples (-1 in case of errors)
        /// </summary>
        public override Task<long> QuadruplesCountAsync
            => GetQuadruplesCountAsync();

        /// <summary>
        /// Connection string to the Firebird database (a new connection is opened for each operation,
        /// relying on ADO.NET's own connection pooling, so that the store is safe to use concurrently
        /// -es. as a singleton registered in an ASP.NET Core DI container)
        /// </summary>
        private readonly string ConnectionString;

        /// <summary>
        /// Options customizing the behaviour of the store
        /// </summary>
        private readonly RDFFirebirdStoreOptions Options;

        /// <summary>
        /// Flag indicating that the Firebird store instance has already been disposed
        /// </summary>
        private bool Disposed { get; set; }
        #endregion

        #region Ctors
        /// <summary>
        /// Default-ctor to build a Firebird store instance
        /// </summary>
        public RDFFirebirdStore(string firebirdConnectionString, RDFFirebirdStoreOptions firebirdStoreOptions=null)
        {
            #region Guards
            if (string.IsNullOrEmpty(firebirdConnectionString))
                throw new RDFStoreException("Cannot connect to Firebird store because: given \"firebirdConnectionString\" parameter is null or empty.");
            #endregion

            //Initialize options
            Options = firebirdStoreOptions ?? new RDFFirebirdStoreOptions();

            //Initialize store structures
            try
            {
                RDFFirebirdStoreManager fbStoreManager = new RDFFirebirdStoreManager(firebirdConnectionString);
                fbStoreManager.InitializeDatabaseAndTableAsync().GetAwaiter().GetResult();

                StoreType = "FIREBIRD";
                ConnectionString = firebirdConnectionString;
                StoreID = RDFModelUtilities.CreateHash(ToString());
                Disposed = false;
            }
            catch (Exception ex)
            {
                throw new RDFStoreException("Cannot create Firebird store because: " + ex.Message, ex);
            }
        }

        /// <summary>
        /// Destroys the Firebird store instance
        /// </summary>
        ~RDFFirebirdStore()
            => Dispose(false);
        #endregion

        #region Interfaces
        /// <summary>
        /// Gives the string representation of the Firebird store
        /// </summary>
        public override string ToString()
        {
            FbConnectionStringBuilder builder = new FbConnectionStringBuilder(ConnectionString);
            return $"{base.ToString()}|SERVER={builder.DataSource};DATABASE={builder.Database}";
        }

        /// <summary>
        /// Disposes the Firebird store instance
        /// </summary>
        public void Dispose()
        {
            Dispose(true);
            GC.SuppressFinalize(this);
        }

#if NET8_0_OR_GREATER
        /// <summary>
        /// Asynchronously disposes the Firebird store (IAsyncDisposable)
        /// </summary>
        ValueTask IAsyncDisposable.DisposeAsync()
        {
            Dispose(true);
            GC.SuppressFinalize(this);
            return ValueTask.CompletedTask;
        }
#endif

        /// <summary>
        /// Disposes the Firebird store instance  (business logic of resources disposal)
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

                FbConnection connection = new FbConnection(ConnectionString);
                FbTransaction transaction = null;
                try
                {
                    //Open connection
                    await connection.OpenAsync();

                    //Create command
                    using (FbCommand insertCommand = new FbCommand(
                        "UPDATE OR INSERT INTO Quadruples (QuadrupleID, TripleFlavor, Context, ContextID, Subject, SubjectID, Predicate, PredicateID, Object, ObjectID) VALUES (@QID, @TFV, @CTX, @CTXID, @SUBJ, @SUBJID, @PRED, @PREDID, @OBJ, @OBJID) MATCHING (QuadrupleID)",
                        connection) { CommandTimeout = Options.InsertTimeout })
                    {
                        insertCommand.Parameters.Add(new FbParameter("QID", FbDbType.BigInt));
                        insertCommand.Parameters.Add(new FbParameter("TFV", FbDbType.Integer));
                        insertCommand.Parameters.Add(new FbParameter("CTX", FbDbType.VarChar, 1000));
                        insertCommand.Parameters.Add(new FbParameter("CTXID", FbDbType.BigInt));
                        insertCommand.Parameters.Add(new FbParameter("SUBJ", FbDbType.VarChar, 1000));
                        insertCommand.Parameters.Add(new FbParameter("SUBJID", FbDbType.BigInt));
                        insertCommand.Parameters.Add(new FbParameter("PRED", FbDbType.VarChar, 1000));
                        insertCommand.Parameters.Add(new FbParameter("PREDID", FbDbType.BigInt));
                        insertCommand.Parameters.Add(new FbParameter("OBJ", FbDbType.VarChar, 1000));
                        insertCommand.Parameters.Add(new FbParameter("OBJID", FbDbType.BigInt));

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
                    throw new RDFStoreException("Cannot insert data into Firebird store because: " + ex.Message, ex);
                }
                finally
                {
                    //Close connection
                    await connection.CloseAsync();
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
                FbConnection connection = new FbConnection(ConnectionString);
                FbTransaction transaction = null;
                try
                {
                    //Open connection
                    await connection.OpenAsync();

                    //Create command
                    using (FbCommand insertCommand = new FbCommand(
                        "UPDATE OR INSERT INTO Quadruples (QuadrupleID, TripleFlavor, Context, ContextID, Subject, SubjectID, Predicate, PredicateID, Object, ObjectID) VALUES (@QID, @TFV, @CTX, @CTXID, @SUBJ, @SUBJID, @PRED, @PREDID, @OBJ, @OBJID) MATCHING (QuadrupleID)",
                        connection) { CommandTimeout = Options.InsertTimeout })
                    {
                        insertCommand.Parameters.Add(new FbParameter("QID", FbDbType.BigInt));
                        insertCommand.Parameters.Add(new FbParameter("TFV", FbDbType.Integer));
                        insertCommand.Parameters.Add(new FbParameter("CTX", FbDbType.VarChar, 1000));
                        insertCommand.Parameters.Add(new FbParameter("CTXID", FbDbType.BigInt));
                        insertCommand.Parameters.Add(new FbParameter("SUBJ", FbDbType.VarChar, 1000));
                        insertCommand.Parameters.Add(new FbParameter("SUBJID", FbDbType.BigInt));
                        insertCommand.Parameters.Add(new FbParameter("PRED", FbDbType.VarChar, 1000));
                        insertCommand.Parameters.Add(new FbParameter("PREDID", FbDbType.BigInt));
                        insertCommand.Parameters.Add(new FbParameter("OBJ", FbDbType.VarChar, 1000));
                        insertCommand.Parameters.Add(new FbParameter("OBJID", FbDbType.BigInt));

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
                    throw new RDFStoreException("Cannot insert data into Firebird store because: " + ex.Message, ex);
                }
                finally
                {
                    //Close connection
                    await connection.CloseAsync();
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
                FbConnection connection = new FbConnection(ConnectionString);
                FbTransaction transaction = null;
                try
                {
                    //Open connection
                    await connection.OpenAsync();

                    //Create command
                    using (FbCommand deleteCommand = new FbCommand("DELETE FROM Quadruples WHERE QuadrupleID = @QID", connection) { CommandTimeout = Options.DeleteTimeout })
                    {
                        deleteCommand.Parameters.Add(new FbParameter("QID", FbDbType.BigInt));

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
                    throw new RDFStoreException("Cannot delete data from Firebird store because: " + ex.Message, ex);
                }
                finally
                {
                    //Close connection
                    await connection.CloseAsync();
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

            FbConnection connection = new FbConnection(ConnectionString);
            FbTransaction transaction = null;
            try
            {
                //Open connection
                await connection.OpenAsync();

                //Create command
                using (FbCommand deleteCommand = new FbCommand { Connection = connection, CommandTimeout = Options.DeleteTimeout })
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
                throw new RDFStoreException("Cannot delete data from Firebird store because: " + ex.Message, ex);
            }
            finally
            {
                //Close connection
                await connection.CloseAsync();
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
            FbConnection connection = new FbConnection(ConnectionString);
            FbTransaction transaction = null;
            try
            {
                //Open connection
                await connection.OpenAsync();

                //Create command
                using (FbCommand deleteCommand = new FbCommand("DELETE FROM Quadruples", connection) { CommandTimeout = Options.DeleteTimeout })
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
                throw new RDFStoreException("Cannot delete data from Firebird store because: " + ex.Message, ex);
            }
            finally
            {
                //Close connection
                await connection.CloseAsync();
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

            FbConnection connection = new FbConnection(ConnectionString);
            try
            {
                //Open connection
                await connection.OpenAsync();

                //Create command
                using (FbCommand selectCommand = new FbCommand("SELECT COUNT(1) FROM RDB$DATABASE WHERE EXISTS(SELECT 1 FROM Quadruples WHERE QuadrupleID = @QID)", connection) { CommandTimeout = Options.SelectTimeout })
                {
                    selectCommand.Parameters.Add(new FbParameter("QID", FbDbType.BigInt));
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
                throw new RDFStoreException("Cannot read data from Firebird store because: " + ex.Message, ex);
            }
            finally
            {
                //Close connection
                await connection.CloseAsync();
                connection.Dispose();
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

            FbConnection connection = new FbConnection(ConnectionString);
            try
            {
                //Open connection
                await connection.OpenAsync();

                //Create command
                using (FbCommand selectCommand = new FbCommand { Connection = connection, CommandTimeout = Options.SelectTimeout })
                {
                    PrepareSelectDeleteCommand(selectCommand, "SELECT TripleFlavor, Context, Subject, Predicate, Object FROM Quadruples", c, s, p, o, l);

                    //Execute command
                    using (FbDataReader quadruples = await selectCommand.ExecuteReaderAsync())
                    {
                        while (quadruples.Read())
                            result.Add(RDFStoreUtilities.ParseQuadruple(quadruples));
                    }
                }
            }
            catch (Exception ex)
            {
                //Propagate exception
                throw new RDFStoreException("Cannot read data from Firebird store because: " + ex.Message, ex);
            }
            finally
            {
                //Close connection
                await connection.CloseAsync();
                connection.Dispose();
            }

            return result;
        }

        /// <summary>
        /// Asynchronously counts the Firebird database quadruples
        /// </summary>
        private async Task<long> GetQuadruplesCountAsync()
        {
            FbConnection connection = new FbConnection(ConnectionString);
            try
            {
                //Open connection
                await connection.OpenAsync();

                //Create command
                using (FbCommand selectCommand = new FbCommand("SELECT COUNT(*) FROM Quadruples", connection) { CommandTimeout = Options.SelectTimeout })
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
                connection.Dispose();
            }
        }
        #endregion

        #region Utilities
        private void PrepareSelectDeleteCommand(FbCommand command, string baseSql, RDFContext c, RDFResource s, RDFResource p, RDFResource o, RDFLiteral l)
        {
            List<string> conditions = new List<string>();

            if (c != null)
            {
                conditions.Add("ContextID = @CTXID");
                command.Parameters.Add(new FbParameter("CTXID", FbDbType.BigInt));
                command.Parameters["CTXID"].Value = c.PatternMemberID;
            }
            if (s != null)
            {
                conditions.Add("SubjectID = @SUBJID");
                command.Parameters.Add(new FbParameter("SUBJID", FbDbType.BigInt));
                command.Parameters["SUBJID"].Value = s.PatternMemberID;
            }
            if (p != null)
            {
                conditions.Add("PredicateID = @PREDID");
                command.Parameters.Add(new FbParameter("PREDID", FbDbType.BigInt));
                command.Parameters["PREDID"].Value = p.PatternMemberID;
            }
            if (o != null)
            {
                conditions.Add("ObjectID = @OBJID");
                command.Parameters.Add(new FbParameter("OBJID", FbDbType.BigInt));
                command.Parameters["OBJID"].Value = o.PatternMemberID;
                conditions.Add("TripleFlavor = @TFV");
                command.Parameters.Add(new FbParameter("TFV", FbDbType.Integer));
                command.Parameters["TFV"].Value = RDFModelEnums.RDFTripleFlavors.SPO;
            }
            if (l != null)
            {
                conditions.Add("ObjectID = @OBJID");
                command.Parameters.Add(new FbParameter("OBJID", FbDbType.BigInt));
                command.Parameters["OBJID"].Value = l.PatternMemberID;
                conditions.Add("TripleFlavor = @TFV");
                command.Parameters.Add(new FbParameter("TFV", FbDbType.Integer));
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
