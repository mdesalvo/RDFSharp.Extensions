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
        /// Connection to the Firebird database
        /// </summary>
        private FbConnection Connection { get; set; }

        /// <summary>
        /// Command to execute SELECT queries on the Firebird database
        /// </summary>
        private FbCommand SelectCommand { get; set; }

        /// <summary>
        /// Command to execute INSERT queries on the Firebird database
        /// </summary>
        private FbCommand InsertCommand { get; set; }

        /// <summary>
        /// Command to execute DELETE queries on the Firebird database
        /// </summary>
        private FbCommand DeleteCommand { get; set; }

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
            if (firebirdStoreOptions == null)
                firebirdStoreOptions = new RDFFirebirdStoreOptions();

            //Initialize store structures
            try
            {
                RDFFirebirdStoreManager fbStoreManager = new RDFFirebirdStoreManager(firebirdConnectionString);
                fbStoreManager.InitializeDatabaseAndTableAsync().GetAwaiter().GetResult();

                StoreType = "FIREBIRD";
                Connection = fbStoreManager.GetConnectionAsync().GetAwaiter().GetResult();
                SelectCommand = new FbCommand { Connection = Connection, CommandTimeout = firebirdStoreOptions.SelectTimeout };
                DeleteCommand = new FbCommand { Connection = Connection, CommandTimeout = firebirdStoreOptions.DeleteTimeout };
                InsertCommand = new FbCommand { Connection = Connection, CommandTimeout = firebirdStoreOptions.InsertTimeout };
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
            => $"{base.ToString()}|SERVER={Connection.DataSource};DATABASE={Connection.Database}";

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
                InsertCommand.CommandText = "UPDATE OR INSERT INTO Quadruples (QuadrupleID, TripleFlavor, Context, ContextID, Subject, SubjectID, Predicate, PredicateID, Object, ObjectID) VALUES (@QID, @TFV, @CTX, @CTXID, @SUBJ, @SUBJID, @PRED, @PREDID, @OBJ, @OBJID) MATCHING (QuadrupleID)";
                InsertCommand.Parameters.Clear();
                InsertCommand.Parameters.Add(new FbParameter("QID", FbDbType.BigInt));
                InsertCommand.Parameters.Add(new FbParameter("TFV", FbDbType.Integer));
                InsertCommand.Parameters.Add(new FbParameter("CTX", FbDbType.VarChar, 1000));
                InsertCommand.Parameters.Add(new FbParameter("CTXID", FbDbType.BigInt));
                InsertCommand.Parameters.Add(new FbParameter("SUBJ", FbDbType.VarChar, 1000));
                InsertCommand.Parameters.Add(new FbParameter("SUBJID", FbDbType.BigInt));
                InsertCommand.Parameters.Add(new FbParameter("PRED", FbDbType.VarChar, 1000));
                InsertCommand.Parameters.Add(new FbParameter("PREDID", FbDbType.BigInt));
                InsertCommand.Parameters.Add(new FbParameter("OBJ", FbDbType.VarChar, 1000));
                InsertCommand.Parameters.Add(new FbParameter("OBJID", FbDbType.BigInt));

                try
                {
                    //Open connection
                    await EnsureConnectionIsOpenAsync();

                    //Prepare command
                    await InsertCommand.PrepareAsync();

                    //Open transaction
                    InsertCommand.Transaction = await Connection.BeginTransactionAsync();

                    //Iterate triples
                    foreach (RDFTriple triple in graph)
                    {
                        //Valorize parameters
                        InsertCommand.Parameters["QID"].Value = RDFModelUtilities.CreateHash($"{graphCtx} {triple.Subject} {triple.Predicate} {triple.Object}");
                        InsertCommand.Parameters["TFV"].Value = triple.TripleFlavor;
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
                    throw new RDFStoreException("Cannot insert data into Firebird store because: " + ex.Message, ex);
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
                InsertCommand.CommandText = "UPDATE OR INSERT INTO Quadruples (QuadrupleID, TripleFlavor, Context, ContextID, Subject, SubjectID, Predicate, PredicateID, Object, ObjectID) VALUES (@QID, @TFV, @CTX, @CTXID, @SUBJ, @SUBJID, @PRED, @PREDID, @OBJ, @OBJID) MATCHING (QuadrupleID)";
                InsertCommand.Parameters.Clear();
                InsertCommand.Parameters.Add(new FbParameter("QID", FbDbType.BigInt));
                InsertCommand.Parameters.Add(new FbParameter("TFV", FbDbType.Integer));
                InsertCommand.Parameters.Add(new FbParameter("CTX", FbDbType.VarChar, 1000));
                InsertCommand.Parameters.Add(new FbParameter("CTXID", FbDbType.BigInt));
                InsertCommand.Parameters.Add(new FbParameter("SUBJ", FbDbType.VarChar, 1000));
                InsertCommand.Parameters.Add(new FbParameter("SUBJID", FbDbType.BigInt));
                InsertCommand.Parameters.Add(new FbParameter("PRED", FbDbType.VarChar, 1000));
                InsertCommand.Parameters.Add(new FbParameter("PREDID", FbDbType.BigInt));
                InsertCommand.Parameters.Add(new FbParameter("OBJ", FbDbType.VarChar, 1000));
                InsertCommand.Parameters.Add(new FbParameter("OBJID", FbDbType.BigInt));

                //Valorize parameters
                InsertCommand.Parameters["QID"].Value = quadruple.QuadrupleID;
                InsertCommand.Parameters["TFV"].Value = quadruple.TripleFlavor;
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
                    InsertCommand.Transaction = await Connection.BeginTransactionAsync();

                    //Execute command
                    await InsertCommand.ExecuteNonQueryAsync();

                    //Close transaction
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
                    throw new RDFStoreException("Cannot insert data into Firebird store because: " + ex.Message, ex);
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
                DeleteCommand.CommandText = "DELETE FROM Quadruples WHERE QuadrupleID = @QID";
                DeleteCommand.Parameters.Clear();
                DeleteCommand.Parameters.Add(new FbParameter("QID", FbDbType.BigInt));

                //Valorize parameters
                DeleteCommand.Parameters["QID"].Value = quadruple.QuadrupleID;

                try
                {
                    //Open connection
                    await EnsureConnectionIsOpenAsync();

                    //Prepare command
                    await DeleteCommand.PrepareAsync();

                    //Open transaction
                    DeleteCommand.Transaction = await Connection.BeginTransactionAsync();

                    //Execute command
                    await DeleteCommand.ExecuteNonQueryAsync();

                    //Close transaction
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
                    throw new RDFStoreException("Cannot delete data from Firebird store because: " + ex.Message, ex);
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

            try
            {
                //Prepare command
                PrepareSelectDeleteCommand(DeleteCommand, "DELETE FROM Quadruples", c, s, p, o, l);

                //Open connection
                await EnsureConnectionIsOpenAsync();

                //Prepare command
                await DeleteCommand.PrepareAsync();

                //Open transaction
                DeleteCommand.Transaction = await Connection.BeginTransactionAsync();

                //Execute command
                await DeleteCommand.ExecuteNonQueryAsync();

                //Close transaction
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
                throw new RDFStoreException("Cannot delete data from Firebird store because: " + ex.Message, ex);
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
            DeleteCommand.CommandText = "DELETE FROM Quadruples";
            DeleteCommand.Parameters.Clear();

            try
            {
                //Open connection
                await EnsureConnectionIsOpenAsync();

                //Prepare command
                await DeleteCommand.PrepareAsync();

                //Open transaction
                DeleteCommand.Transaction = await Connection.BeginTransactionAsync();

                //Execute command
                await DeleteCommand.ExecuteNonQueryAsync();

                //Close transaction
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
                throw new RDFStoreException("Cannot delete data from Firebird store because: " + ex.Message, ex);
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
            SelectCommand.CommandText = "SELECT COUNT(1) FROM RDB$DATABASE WHERE EXISTS(SELECT 1 FROM Quadruples WHERE QuadrupleID = @QID)";
            SelectCommand.Parameters.Clear();
            SelectCommand.Parameters.Add(new FbParameter("QID", FbDbType.BigInt));
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
                throw new RDFStoreException("Cannot read data from Firebird store because: " + ex.Message, ex);
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

            //Prepare and execute command
            try
            {
                //Prepare command
                PrepareSelectDeleteCommand(SelectCommand, "SELECT TripleFlavor, Context, Subject, Predicate, Object FROM Quadruples", c, s, p, o, l);

                //Open connection
                await EnsureConnectionIsOpenAsync();

                //Execute command
                using (FbDataReader quadruples = await SelectCommand.ExecuteReaderAsync())
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
                throw new RDFStoreException("Cannot read data from Firebird store because: " + ex.Message, ex);
            }

            return result;
        }

        /// <summary>
        /// Asynchronously counts the Firebird database quadruples
        /// </summary>
        private async Task<long> GetQuadruplesCountAsync()
        {
            try
            {
                //Open connection
                await EnsureConnectionIsOpenAsync();

                //Create command
                SelectCommand.CommandText = "SELECT COUNT(*) FROM Quadruples";
                SelectCommand.Parameters.Clear();

                //Execute command
                long result = long.Parse((await SelectCommand.ExecuteScalarAsync(CancellationToken.None)).ToString());

                //Close connection
                await Connection.CloseAsync();

                //Return the quadruples count
                return  result;
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

        #region Utilities
        private void PrepareSelectDeleteCommand(FbCommand command, string baseSql, RDFContext c, RDFResource s, RDFResource p, RDFResource o, RDFLiteral l)
        {
            command.Parameters.Clear();
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

        #endregion
    }
}