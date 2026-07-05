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
using System.Text;
using System.Threading.Tasks;
using Oracle.ManagedDataAccess.Client;
using RDFSharp.Model;
using RDFSharp.Store;

namespace RDFSharp.Extensions.Oracle
{
    /// <summary>
    /// RDFOracleStore represents a store backed on Oracle engine
    /// </summary>
    #if NET8_0_OR_GREATER
    public sealed class RDFOracleStore : RDFStore, IDisposable, IAsyncDisposable
    #else
    public sealed class RDFOracleStore : RDFStore, IDisposable
    #endif
    {
        #region Properties
        /// <summary>
        /// Count of the Oracle database quadruples (-1 in case of errors)
        /// </summary>
        public override long QuadruplesCount
            => GetQuadruplesCount();

        /// <summary>
        /// Asynchronous count of the Oracle database quadruples (-1 in case of errors)
        /// </summary>
        public override Task<long> QuadruplesCountAsync
            => Task.Run(GetQuadruplesCount);

        /// <summary>
        /// Connection string to the Oracle database (a new connection is opened for each operation,
        /// relying on ADO.NET's own connection pooling, so that the store is safe to use concurrently
        /// -es. as a singleton registered in an ASP.NET Core DI container)
        /// </summary>
        private readonly string ConnectionString;

        /// <summary>
        /// Utility for getting fields of the connection
        /// </summary>
        private readonly OracleConnectionStringBuilder ConnectionBuilder;

        /// <summary>
        /// Options customizing the behaviour of the store
        /// </summary>
        private readonly RDFOracleStoreOptions Options;

        /// <summary>
        /// Flag indicating that the Oracle store instance has already been disposed
        /// </summary>
        private bool Disposed { get; set; }
        #endregion

        #region Ctors
        /// <summary>
        /// Default-ctor to build an Oracle store instance (with eventual options)
        /// </summary>
        public RDFOracleStore(string oracleConnectionString, RDFOracleStoreOptions oracleStoreOptions = null)
        {
            #region Guards
            if (string.IsNullOrEmpty(oracleConnectionString))
                throw new RDFStoreException("Cannot connect to Oracle store because: given \"oracleConnectionString\" parameter is null or empty.");
            #endregion

            //Initialize options
            Options = oracleStoreOptions ?? new RDFOracleStoreOptions();

            //Initialize store structures
            try
            {
                RDFOracleStoreManager oracleStoreManager = new RDFOracleStoreManager(oracleConnectionString);
                oracleStoreManager.EnsureQuadruplesTableExists();

                StoreType = "ORACLE";
                ConnectionString = oracleConnectionString;
                ConnectionBuilder = new OracleConnectionStringBuilder(oracleConnectionString);
                StoreID = RDFModelUtilities.CreateHash(ToString());
                Disposed = false;
            }
            catch (Exception ex)
            {
                throw new RDFStoreException("Cannot create Oracle store because: " + ex.Message, ex);
            }
        }

        /// <summary>
        /// Destroys the Oracle store instance
        /// </summary>
        ~RDFOracleStore()
            => Dispose(false);
        #endregion

        #region Interfaces
        /// <summary>
        /// Gives the string representation of the Oracle store
        /// </summary>
        public override string ToString()
            => $"{base.ToString()}|SERVER={ConnectionBuilder.DataSource};DATABASE={ConnectionBuilder.UserID}";

        /// <summary>
        /// Disposes the Oracle store instance
        /// </summary>
        public void Dispose()
        {
            Dispose(true);
            GC.SuppressFinalize(this);
        }

#if NET8_0_OR_GREATER
        /// <summary>
        /// Asynchronously disposes the Oracle store (IAsyncDisposable)
        /// </summary>
        ValueTask IAsyncDisposable.DisposeAsync()
        {
            Dispose(true);
            GC.SuppressFinalize(this);
            return ValueTask.CompletedTask;
        }
#endif

        /// <summary>
        /// Disposes the Oracle store instance  (business logic of resources disposal)
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
        {
            if (graph != null)
            {
                RDFContext graphCtx = new RDFContext(graph.Context);

                OracleConnection connection = new OracleConnection(ConnectionString);
                OracleTransaction transaction = null;
                try
                {
                    //Open connection
                    connection.Open();

                    //Create command
                    using (OracleCommand insertCommand = new OracleCommand(
                        $"INSERT INTO {ConnectionBuilder.UserID}.QUADRUPLES(QUADRUPLEID, TRIPLEFLAVOR, CONTEXT, CONTEXTID, SUBJECT, SUBJECTID, PREDICATE, PREDICATEID, OBJECT, OBJECTID) SELECT :QID, :TFV, :CTX, :CTXID, :SUBJ, :SUBJID, :PRED, :PREDID, :OBJ, :OBJID FROM DUAL WHERE NOT EXISTS(SELECT QUADRUPLEID FROM {ConnectionBuilder.UserID}.QUADRUPLES WHERE QUADRUPLEID = :QID)",
                        connection) { CommandTimeout = Options.InsertTimeout })
                    {
                        insertCommand.Parameters.Add(new OracleParameter("QID", OracleDbType.Int64));
                        insertCommand.Parameters.Add(new OracleParameter("TFV", OracleDbType.Int32));
                        insertCommand.Parameters.Add(new OracleParameter("CTX", OracleDbType.Varchar2, 1000));
                        insertCommand.Parameters.Add(new OracleParameter("CTXID", OracleDbType.Int64));
                        insertCommand.Parameters.Add(new OracleParameter("SUBJ", OracleDbType.Varchar2, 1000));
                        insertCommand.Parameters.Add(new OracleParameter("SUBJID", OracleDbType.Int64));
                        insertCommand.Parameters.Add(new OracleParameter("PRED", OracleDbType.Varchar2, 1000));
                        insertCommand.Parameters.Add(new OracleParameter("PREDID", OracleDbType.Int64));
                        insertCommand.Parameters.Add(new OracleParameter("OBJ", OracleDbType.Varchar2, 1000));
                        insertCommand.Parameters.Add(new OracleParameter("OBJID", OracleDbType.Int64));

                        //Prepare command
                        insertCommand.Prepare();

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
                            insertCommand.ExecuteNonQuery();
                        }

                        //Commit transaction
                        transaction.Commit();
                    }
                }
                catch (Exception ex)
                {
                    //Rollback transaction
                    transaction?.Rollback();

                    //Propagate exception
                    throw new RDFStoreException("Cannot insert data into Oracle store because: " + ex.Message, ex);
                }
                finally
                {
                    //Close connection
                    connection.Close();
                    connection.Dispose();
                }
            }
            return this;
        }

        /// <summary>
        /// Asynchronously merges the given graph into the store
        /// </summary>
        public override Task<RDFStore> MergeGraphAsync(RDFGraph graph)
            => Task.Run(() => MergeGraph(graph));

        /// <summary>
        /// Adds the given quadruple to the store
        /// </summary>
        public override RDFStore AddQuadruple(RDFQuadruple quadruple)
        {
            if (quadruple != null)
            {
                OracleConnection connection = new OracleConnection(ConnectionString);
                OracleTransaction transaction = null;
                try
                {
                    //Open connection
                    connection.Open();

                    //Create command
                    using (OracleCommand insertCommand = new OracleCommand(
                        $"INSERT INTO {ConnectionBuilder.UserID}.QUADRUPLES(QUADRUPLEID, TRIPLEFLAVOR, CONTEXT, CONTEXTID, SUBJECT, SUBJECTID, PREDICATE, PREDICATEID, OBJECT, OBJECTID) SELECT :QID, :TFV, :CTX, :CTXID, :SUBJ, :SUBJID, :PRED, :PREDID, :OBJ, :OBJID FROM DUAL WHERE NOT EXISTS(SELECT QUADRUPLEID FROM {ConnectionBuilder.UserID}.QUADRUPLES WHERE QUADRUPLEID = :QID)",
                        connection) { CommandTimeout = Options.InsertTimeout })
                    {
                        insertCommand.Parameters.Add(new OracleParameter("QID", OracleDbType.Int64));
                        insertCommand.Parameters.Add(new OracleParameter("TFV", OracleDbType.Int32));
                        insertCommand.Parameters.Add(new OracleParameter("CTX", OracleDbType.Varchar2, 1000));
                        insertCommand.Parameters.Add(new OracleParameter("CTXID", OracleDbType.Int64));
                        insertCommand.Parameters.Add(new OracleParameter("SUBJ", OracleDbType.Varchar2, 1000));
                        insertCommand.Parameters.Add(new OracleParameter("SUBJID", OracleDbType.Int64));
                        insertCommand.Parameters.Add(new OracleParameter("PRED", OracleDbType.Varchar2, 1000));
                        insertCommand.Parameters.Add(new OracleParameter("PREDID", OracleDbType.Int64));
                        insertCommand.Parameters.Add(new OracleParameter("OBJ", OracleDbType.Varchar2, 1000));
                        insertCommand.Parameters.Add(new OracleParameter("OBJID", OracleDbType.Int64));

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
                        insertCommand.Prepare();

                        //Open transaction
                        transaction = connection.BeginTransaction();
                        insertCommand.Transaction = transaction;

                        //Execute command
                        insertCommand.ExecuteNonQuery();

                        //Commit transaction
                        transaction.Commit();
                    }
                }
                catch (Exception ex)
                {
                    //Rollback transaction
                    transaction?.Rollback();

                    //Propagate exception
                    throw new RDFStoreException("Cannot insert data into Oracle store because: " + ex.Message, ex);
                }
                finally
                {
                    //Close connection
                    connection.Close();
                    connection.Dispose();
                }
            }
            return this;
        }

        /// <summary>
        /// Asynchronously adds the given quadruple to the store
        /// </summary>
        public override Task<RDFStore> AddQuadrupleAsync(RDFQuadruple quadruple)
            => Task.Run(() => AddQuadruple(quadruple));
        #endregion

        #region Remove
        /// <summary>
        /// Removes the given quadruple from the store
        /// </summary>
        public override RDFStore RemoveQuadruple(RDFQuadruple quadruple)
        {
            if (quadruple != null)
            {
                OracleConnection connection = new OracleConnection(ConnectionString);
                OracleTransaction transaction = null;
                try
                {
                    //Open connection
                    connection.Open();

                    //Create command
                    using (OracleCommand deleteCommand = new OracleCommand($"DELETE FROM {ConnectionBuilder.UserID}.QUADRUPLES WHERE QUADRUPLEID = :QID", connection) { CommandTimeout = Options.DeleteTimeout })
                    {
                        deleteCommand.Parameters.Add(new OracleParameter("QID", OracleDbType.Int64));

                        //Valorize parameters
                        deleteCommand.Parameters["QID"].Value = quadruple.QuadrupleID;

                        //Prepare command
                        deleteCommand.Prepare();

                        //Open transaction
                        transaction = connection.BeginTransaction();
                        deleteCommand.Transaction = transaction;

                        //Execute command
                        deleteCommand.ExecuteNonQuery();

                        //Commit transaction
                        transaction.Commit();
                    }
                }
                catch (Exception ex)
                {
                    //Rollback transaction
                    transaction?.Rollback();

                    //Propagate exception
                    throw new RDFStoreException("Cannot delete data from Oracle store because: " + ex.Message, ex);
                }
                finally
                {
                    //Close connection
                    connection.Close();
                    connection.Dispose();
                }
            }
            return this;
        }

        /// <summary>
        /// Asynchronously removes the given quadruple from the store
        /// </summary>
        public override Task<RDFStore> RemoveQuadrupleAsync(RDFQuadruple quadruple)
            => Task.Run(() => RemoveQuadruple(quadruple));

        /// <summary>
        /// Removes the quadruples which satisfy the given combination of CSPOL accessors<br/>
        /// (null values are handled as * selectors. Object and Literal params, if given, must be mutually exclusive!)
        /// </summary>
        public override RDFStore RemoveQuadruples(RDFContext c=null, RDFResource s=null, RDFResource p=null, RDFResource o=null, RDFLiteral l=null)
        {
            #region Guards
            if (o != null && l != null)
                throw new RDFStoreException("Cannot access a store when both object and literals are given: they must be mutually exclusive!");
            #endregion

            OracleConnection connection = new OracleConnection(ConnectionString);
            OracleTransaction transaction = null;
            try
            {
                //Open connection
                connection.Open();

                //Create command
                using (OracleCommand deleteCommand = new OracleCommand { Connection = connection, CommandTimeout = Options.DeleteTimeout })
                {
                    PrepareSelectDeleteCommand(deleteCommand, $"DELETE FROM {ConnectionBuilder.UserID}.QUADRUPLES", c, s, p, o, l);

                    //Prepare command
                    deleteCommand.Prepare();

                    //Open transaction
                    transaction = connection.BeginTransaction();
                    deleteCommand.Transaction = transaction;

                    //Execute command
                    deleteCommand.ExecuteNonQuery();

                    //Commit transaction
                    transaction.Commit();
                }
            }
            catch (Exception ex)
            {
                //Rollback transaction
                transaction?.Rollback();

                //Propagate exception
                throw new RDFStoreException("Cannot delete data from Oracle store because: " + ex.Message, ex);
            }
            finally
            {
                //Close connection
                connection.Close();
                connection.Dispose();
            }

            return this;
        }

        /// <summary>
        /// Asynchronously removes the quadruples which satisfy the given combination of CSPOL accessors<br/>
        /// (null values are handled as * selectors. Object and Literal params, if given, must be mutually exclusive!)
        /// </summary>
        public override Task<RDFStore> RemoveQuadruplesAsync(RDFContext c=null, RDFResource s=null, RDFResource p=null, RDFResource o=null, RDFLiteral l=null)
            => Task.Run(() => RemoveQuadruples(c,s,p,o,l));

        /// <summary>
        /// Clears the quadruples of the store
        /// </summary>
        public override void ClearQuadruples()
        {
            OracleConnection connection = new OracleConnection(ConnectionString);
            OracleTransaction transaction = null;
            try
            {
                //Open connection
                connection.Open();

                //Create command
                using (OracleCommand deleteCommand = new OracleCommand($"DELETE FROM {ConnectionBuilder.UserID}.QUADRUPLES", connection) { CommandTimeout = Options.DeleteTimeout })
                {
                    //Prepare command
                    deleteCommand.Prepare();

                    //Open transaction
                    transaction = connection.BeginTransaction();
                    deleteCommand.Transaction = transaction;

                    //Execute command
                    deleteCommand.ExecuteNonQuery();

                    //Commit transaction
                    transaction.Commit();
                }
            }
            catch (Exception ex)
            {
                //Rollback transaction
                transaction?.Rollback();

                //Propagate exception
                throw new RDFStoreException("Cannot delete data from Oracle store because: " + ex.Message, ex);
            }
            finally
            {
                //Close connection
                connection.Close();
                connection.Dispose();
            }
        }

        /// <summary>
        /// Asynchronously clears the quadruples of the store
        /// </summary>
        public override Task ClearQuadruplesAsync()
            => Task.Run(ClearQuadruples);
        #endregion

        #region Select
        /// <summary>
        /// Checks if the given quadruple is found in the store
        /// </summary>
        public override bool ContainsQuadruple(RDFQuadruple quadruple)
        {
            //Guard against tricky input
            if (quadruple == null)
                return false;

            OracleConnection connection = new OracleConnection(ConnectionString);
            try
            {
                //Open connection
                connection.Open();

                //Create command
                using (OracleCommand selectCommand = new OracleCommand($"SELECT CASE WHEN EXISTS (SELECT 1 FROM {ConnectionBuilder.UserID}.QUADRUPLES WHERE QUADRUPLEID = :QID) THEN 1 ELSE 0 END FROM DUAL", connection) { CommandTimeout = Options.SelectTimeout })
                {
                    selectCommand.Parameters.Add(new OracleParameter("QID", OracleDbType.Int64));

                    //Valorize parameters
                    selectCommand.Parameters["QID"].Value = quadruple.QuadrupleID;

                    //Prepare command
                    selectCommand.Prepare();

                    //Execute command
                    int result = int.Parse(selectCommand.ExecuteScalar().ToString());

                    //Give result
                    return result == 1;
                }
            }
            catch (Exception ex)
            {
                //Propagate exception
                throw new RDFStoreException("Cannot read data from Oracle store because: " + ex.Message, ex);
            }
            finally
            {
                //Close connection
                connection.Close();
                connection.Dispose();
            }
        }

        /// <summary>
        /// Asynchronously checks if the given quadruple is found in the store
        /// </summary>
        public override Task<bool> ContainsQuadrupleAsync(RDFQuadruple quadruple)
            => Task.Run(() => ContainsQuadruple(quadruple));

        /// <summary>
        /// Selects the quadruples which satisfy the given combination of CSPOL accessors<br/>
        /// (null values are handled as * selectors. Object and Literal params, if given, must be mutually exclusive!)
        /// </summary>
        /// <exception cref="RDFStoreException"></exception>
        public override List<RDFQuadruple> SelectQuadruples(RDFContext c=null, RDFResource s=null, RDFResource p=null, RDFResource o=null, RDFLiteral l=null)
        {
            #region Guards
            if (o != null && l != null)
                throw new RDFStoreException("Cannot access a store when both object and literals are given: they must be mutually exclusive!");
            #endregion

            List<RDFQuadruple>  result = new List<RDFQuadruple>();

            OracleConnection connection = new OracleConnection(ConnectionString);
            try
            {
                //Open connection
                connection.Open();

                //Create command
                using (OracleCommand selectCommand = new OracleCommand { Connection = connection, CommandTimeout = Options.SelectTimeout })
                {
                    PrepareSelectDeleteCommand(selectCommand, $"SELECT TRIPLEFLAVOR, CONTEXT, SUBJECT, PREDICATE, OBJECT FROM {ConnectionBuilder.UserID}.QUADRUPLES", c, s, p, o, l);

                    //Execute command
                    using (OracleDataReader quadruples = selectCommand.ExecuteReader(CommandBehavior.Default))
                    {
                        while (quadruples.Read())
                            result.Add(RDFStoreUtilities.ParseQuadruple(quadruples));
                    }
                }
            }
            catch (Exception ex)
            {
                //Propagate exception
                throw new RDFStoreException("Cannot read data from Oracle store because: " + ex.Message, ex);
            }
            finally
            {
                //Close connection
                connection.Close();
                connection.Dispose();
            }

            return result;
        }

        /// <summary>
        /// Asynchronously selects the quadruples which satisfy the given combination of CSPOL accessors<br/>
        /// (null values are handled as * selectors. Object and Literal params, if given, must be mutually exclusive!)
        /// </summary>
        /// <exception cref="RDFStoreException"></exception>
        public override Task<List<RDFQuadruple>> SelectQuadruplesAsync(RDFContext c = null, RDFResource s = null,
            RDFResource p = null, RDFResource o = null, RDFLiteral l = null)
            => Task.Run(() => SelectQuadruples(c,s,p,o,l));

        /// <summary>
        /// Counts the Oracle database quadruples
        /// </summary>
        private long GetQuadruplesCount()
        {
            OracleConnection connection = new OracleConnection(ConnectionString);
            try
            {
                //Open connection
                connection.Open();

                //Create command
                using (OracleCommand selectCommand = new OracleCommand($"SELECT COUNT(*) FROM {ConnectionBuilder.UserID}.QUADRUPLES", connection) { CommandTimeout = Options.SelectTimeout })
                {
                    //Execute command
                    long result = long.Parse(selectCommand.ExecuteScalar().ToString());

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
                connection.Close();
                connection.Dispose();
            }
        }
        #endregion

        #region Optimize
        /// <summary>
        /// Optimizes "Quadruples" table of Oracle store
        /// </summary>
        public void Optimize()
        {
            OracleConnection connection = new OracleConnection(ConnectionString);
            try
            {
                //Open connection
                connection.Open();

                //Create command
                using (OracleCommand optimizeCommand = new OracleCommand($"ALTER INDEX {ConnectionBuilder.UserID}.IDX_CONTEXTID REBUILD", connection))
                {
                    optimizeCommand.ExecuteNonQuery();
                    optimizeCommand.CommandText = $"ALTER INDEX {ConnectionBuilder.UserID}.IDX_SUBJECTID REBUILD";
                    optimizeCommand.ExecuteNonQuery();
                    optimizeCommand.CommandText = $"ALTER INDEX {ConnectionBuilder.UserID}.IDX_PREDICATEID REBUILD";
                    optimizeCommand.ExecuteNonQuery();
                    optimizeCommand.CommandText = $"ALTER INDEX {ConnectionBuilder.UserID}.IDX_OBJECTID REBUILD";
                    optimizeCommand.ExecuteNonQuery();
                    optimizeCommand.CommandText = $"ALTER INDEX {ConnectionBuilder.UserID}.IDX_SUBJECTID_PREDICATEID REBUILD";
                    optimizeCommand.ExecuteNonQuery();
                    optimizeCommand.CommandText = $"ALTER INDEX {ConnectionBuilder.UserID}.IDX_SUBJECTID_OBJECTID REBUILD";
                    optimizeCommand.ExecuteNonQuery();
                    optimizeCommand.CommandText = $"ALTER INDEX {ConnectionBuilder.UserID}.IDX_PREDICATEID_OBJECTID REBUILD";
                    optimizeCommand.ExecuteNonQuery();
                }
            }
            catch (Exception ex)
            {
                //Propagate exception
                throw new RDFStoreException("Cannot optimize Oracle store because: " + ex.Message, ex);
            }
            finally
            {
                //Close connection
                connection.Close();
                connection.Dispose();
            }
        }
        #endregion

        #region Utilities
        private void PrepareSelectDeleteCommand(OracleCommand command, string baseSql, RDFContext c, RDFResource s, RDFResource p, RDFResource o, RDFLiteral l)
        {
            List<string> conditions = new List<string>();

            if (c != null)
            {
                conditions.Add("CONTEXTID = :CTXID");
                command.Parameters.Add(new OracleParameter("CTXID", OracleDbType.Int64));
                command.Parameters["CTXID"].Value = c.PatternMemberID;
            }
            if (s != null)
            {
                conditions.Add("SUBJECTID = :SUBJID");
                command.Parameters.Add(new OracleParameter("SUBJID", OracleDbType.Int64));
                command.Parameters["SUBJID"].Value = s.PatternMemberID;
            }
            if (p != null)
            {
                conditions.Add("PREDICATEID = :PREDID");
                command.Parameters.Add(new OracleParameter("PREDID", OracleDbType.Int64));
                command.Parameters["PREDID"].Value = p.PatternMemberID;
            }
            if (o != null)
            {
                conditions.Add("OBJECTID = :OBJID");
                command.Parameters.Add(new OracleParameter("OBJID", OracleDbType.Int64));
                command.Parameters["OBJID"].Value = o.PatternMemberID;
                conditions.Add("TRIPLEFLAVOR = :TFV");
                command.Parameters.Add(new OracleParameter("TFV", OracleDbType.Int32));
                command.Parameters["TFV"].Value = (int)RDFModelEnums.RDFTripleFlavors.SPO;
            }
            if (l != null)
            {
                conditions.Add("OBJECTID = :OBJID");
                command.Parameters.Add(new OracleParameter("OBJID", OracleDbType.Int64));
                command.Parameters["OBJID"].Value = l.PatternMemberID;
                conditions.Add("TRIPLEFLAVOR = :TFV");
                command.Parameters.Add(new OracleParameter("TFV", OracleDbType.Int32));
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
