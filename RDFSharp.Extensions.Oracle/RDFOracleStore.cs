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
        /// Connection to the Oracle database
        /// </summary>
        private OracleConnection Connection { get; set; }

        /// <summary>
        /// Utility for getting fields of the connection
        /// </summary>
        private OracleConnectionStringBuilder ConnectionBuilder { get; }

        /// <summary>
        /// Command to execute SELECT queries on the Oracle database
        /// </summary>
        private OracleCommand SelectCommand { get; set; }

        /// <summary>
        /// Command to execute INSERT queries on the Oracle database
        /// </summary>
        private OracleCommand InsertCommand { get; set; }

        /// <summary>
        /// Command to execute DELETE queries on the Oracle database
        /// </summary>
        private OracleCommand DeleteCommand { get; set; }

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
            if (oracleStoreOptions == null)
                oracleStoreOptions = new RDFOracleStoreOptions();

            //Initialize store structures
            try
            {
                RDFOracleStoreManager oracleStoreManager = new RDFOracleStoreManager(oracleConnectionString);
                oracleStoreManager.EnsureQuadruplesTableExists();

                StoreType = "ORACLE";
                Connection = oracleStoreManager.GetConnection();
                ConnectionBuilder = new OracleConnectionStringBuilder(Connection.ConnectionString);
                SelectCommand = new OracleCommand { Connection = Connection, CommandTimeout = oracleStoreOptions.SelectTimeout };
                DeleteCommand = new OracleCommand { Connection = Connection, CommandTimeout = oracleStoreOptions.DeleteTimeout };
                InsertCommand = new OracleCommand { Connection = Connection, CommandTimeout = oracleStoreOptions.InsertTimeout };
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
            => $"{base.ToString()}|SERVER={Connection.DataSource};DATABASE={Connection.Database}";

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
        {
            if (graph != null)
            {
                RDFContext graphCtx = new RDFContext(graph.Context);

                //Create command
                InsertCommand.CommandText = $"INSERT INTO {ConnectionBuilder.UserID}.QUADRUPLES(QUADRUPLEID, TRIPLEFLAVOR, CONTEXT, CONTEXTID, SUBJECT, SUBJECTID, PREDICATE, PREDICATEID, OBJECT, OBJECTID) SELECT :QID, :TFV, :CTX, :CTXID, :SUBJ, :SUBJID, :PRED, :PREDID, :OBJ, :OBJID FROM DUAL WHERE NOT EXISTS(SELECT QUADRUPLEID FROM {ConnectionBuilder.UserID}.QUADRUPLES WHERE QUADRUPLEID = :QID)";
                InsertCommand.Parameters.Clear();
                InsertCommand.Parameters.Add(new OracleParameter("QID", OracleDbType.Int64));
                InsertCommand.Parameters.Add(new OracleParameter("TFV", OracleDbType.Int32));
                InsertCommand.Parameters.Add(new OracleParameter("CTX", OracleDbType.Varchar2, 1000));
                InsertCommand.Parameters.Add(new OracleParameter("CTXID", OracleDbType.Int64));
                InsertCommand.Parameters.Add(new OracleParameter("SUBJ", OracleDbType.Varchar2, 1000));
                InsertCommand.Parameters.Add(new OracleParameter("SUBJID", OracleDbType.Int64));
                InsertCommand.Parameters.Add(new OracleParameter("PRED", OracleDbType.Varchar2, 1000));
                InsertCommand.Parameters.Add(new OracleParameter("PREDID", OracleDbType.Int64));
                InsertCommand.Parameters.Add(new OracleParameter("OBJ", OracleDbType.Varchar2, 1000));
                InsertCommand.Parameters.Add(new OracleParameter("OBJID", OracleDbType.Int64));

                try
                {
                    //Open connection
                    EnsureConnectionIsOpen();

                    //Prepare command
                    InsertCommand.Prepare();

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
                        InsertCommand.ExecuteNonQuery();
                    }

                    //Close transaction
                    InsertCommand.Transaction.Commit();

                    //Close connection
                    Connection.Close();
                }
                catch (Exception ex)
                {
                    //Rollback transaction
                    if (InsertCommand.Transaction != null)
                        InsertCommand.Transaction.Rollback();

                    //Close connection
                    Connection.Close();

                    //Propagate exception
                    throw new RDFStoreException("Cannot insert data into Oracle store because: " + ex.Message, ex);
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
                //Create command
                InsertCommand.CommandText = $"INSERT INTO {ConnectionBuilder.UserID}.QUADRUPLES(QUADRUPLEID, TRIPLEFLAVOR, CONTEXT, CONTEXTID, SUBJECT, SUBJECTID, PREDICATE, PREDICATEID, OBJECT, OBJECTID) SELECT :QID, :TFV, :CTX, :CTXID, :SUBJ, :SUBJID, :PRED, :PREDID, :OBJ, :OBJID FROM DUAL WHERE NOT EXISTS(SELECT QUADRUPLEID FROM {ConnectionBuilder.UserID}.QUADRUPLES WHERE QUADRUPLEID = :QID)";
                InsertCommand.Parameters.Clear();
                InsertCommand.Parameters.Add(new OracleParameter("QID", OracleDbType.Int64));
                InsertCommand.Parameters.Add(new OracleParameter("TFV", OracleDbType.Int32));
                InsertCommand.Parameters.Add(new OracleParameter("CTX", OracleDbType.Varchar2, 1000));
                InsertCommand.Parameters.Add(new OracleParameter("CTXID", OracleDbType.Int64));
                InsertCommand.Parameters.Add(new OracleParameter("SUBJ", OracleDbType.Varchar2, 1000));
                InsertCommand.Parameters.Add(new OracleParameter("SUBJID", OracleDbType.Int64));
                InsertCommand.Parameters.Add(new OracleParameter("PRED", OracleDbType.Varchar2, 1000));
                InsertCommand.Parameters.Add(new OracleParameter("PREDID", OracleDbType.Int64));
                InsertCommand.Parameters.Add(new OracleParameter("OBJ", OracleDbType.Varchar2, 1000));
                InsertCommand.Parameters.Add(new OracleParameter("OBJID", OracleDbType.Int64));

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
                    EnsureConnectionIsOpen();

                    //Prepare command
                    InsertCommand.Prepare();

                    //Open transaction
                    InsertCommand.Transaction = Connection.BeginTransaction();

                    //Execute command
                    InsertCommand.ExecuteNonQuery();

                    //Close transaction
                    InsertCommand.Transaction.Commit();

                    //Close connection
                    Connection.Close();
                }
                catch (Exception ex)
                {
                    //Rollback transaction
                    if (InsertCommand.Transaction != null)
                        InsertCommand.Transaction.Rollback();

                    //Close connection
                    Connection.Close();

                    //Propagate exception
                    throw new RDFStoreException("Cannot insert data into Oracle store because: " + ex.Message, ex);
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
                //Create command
                DeleteCommand.CommandText = $"DELETE FROM {ConnectionBuilder.UserID}.QUADRUPLES WHERE QUADRUPLEID = :QID";
                DeleteCommand.Parameters.Clear();
                DeleteCommand.Parameters.Add(new OracleParameter("QID", OracleDbType.Int64));

                //Valorize parameters
                DeleteCommand.Parameters["QID"].Value = quadruple.QuadrupleID;

                try
                {
                    //Open connection
                    EnsureConnectionIsOpen();

                    //Prepare command
                    DeleteCommand.Prepare();

                    //Open transaction
                    DeleteCommand.Transaction = Connection.BeginTransaction();

                    //Execute command
                    DeleteCommand.ExecuteNonQuery();

                    //Close transaction
                    DeleteCommand.Transaction.Commit();

                    //Close connection
                    Connection.Close();
                }
                catch (Exception ex)
                {
                    //Rollback transaction
                    if (DeleteCommand.Transaction != null)
                        DeleteCommand.Transaction.Rollback();

                    //Close connection
                    Connection.Close();

                    //Propagate exception
                    throw new RDFStoreException("Cannot delete data from Oracle store because: " + ex.Message, ex);
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

            //Build filters
            StringBuilder queryFilters = new StringBuilder();
            if (c != null) queryFilters.Append('C');
            if (s != null) queryFilters.Append('S');
            if (p != null) queryFilters.Append('P');
            if (o != null) queryFilters.Append('O');
            if (l != null) queryFilters.Append('L');

            try
            {
                switch (queryFilters.ToString())
                {
                    case "C":
                        DeleteCommand.CommandText = $"DELETE FROM {ConnectionBuilder.UserID}.QUADRUPLES WHERE CONTEXTID = :CTXID";
                        DeleteCommand.Parameters.Clear();
                        DeleteCommand.Parameters.Add(new OracleParameter("CTXID", OracleDbType.Int64));
                        DeleteCommand.Parameters["CTXID"].Value = c.PatternMemberID;
                        break;
                    case "S":
                        DeleteCommand.CommandText = $"DELETE FROM {ConnectionBuilder.UserID}.QUADRUPLES WHERE SUBJECTID = :SUBJID";
                        DeleteCommand.Parameters.Clear();
                        DeleteCommand.Parameters.Add(new OracleParameter("SUBJID", OracleDbType.Int64));
                        DeleteCommand.Parameters["SUBJID"].Value = s.PatternMemberID;
                        break;
                    case "P":
                        DeleteCommand.CommandText = $"DELETE FROM {ConnectionBuilder.UserID}.QUADRUPLES WHERE PREDICATEID = :PREDID";
                        DeleteCommand.Parameters.Clear();
                        DeleteCommand.Parameters.Add(new OracleParameter("PREDID", OracleDbType.Int64));
                        DeleteCommand.Parameters["PREDID"].Value = p.PatternMemberID;
                        break;
                    case "O":
                        DeleteCommand.CommandText = $"DELETE FROM {ConnectionBuilder.UserID}.QUADRUPLES WHERE OBJECTID = :OBJID AND TRIPLEFLAVOR = :TFV";
                        DeleteCommand.Parameters.Clear();
                        DeleteCommand.Parameters.Add(new OracleParameter("OBJID", OracleDbType.Int64));
                        DeleteCommand.Parameters.Add(new OracleParameter("TFV", OracleDbType.Int32));
                        DeleteCommand.Parameters["OBJID"].Value = o.PatternMemberID;
                        DeleteCommand.Parameters["TFV"].Value = (int)RDFModelEnums.RDFTripleFlavors.SPO;
                        break;
                    case "L":
                        DeleteCommand.CommandText = $"DELETE FROM {ConnectionBuilder.UserID}.QUADRUPLES WHERE OBJECTID = :OBJID AND TRIPLEFLAVOR = :TFV";
                        DeleteCommand.Parameters.Clear();
                        DeleteCommand.Parameters.Add(new OracleParameter("OBJID", OracleDbType.Int64));
                        DeleteCommand.Parameters.Add(new OracleParameter("TFV", OracleDbType.Int32));
                        DeleteCommand.Parameters["OBJID"].Value = l.PatternMemberID;
                        DeleteCommand.Parameters["TFV"].Value = (int)RDFModelEnums.RDFTripleFlavors.SPL;
                        break;
                    case "CS":
                        DeleteCommand.CommandText = $"DELETE FROM {ConnectionBuilder.UserID}.QUADRUPLES WHERE CONTEXTID = :CTXID AND SUBJECTID = :SUBJID";
                        DeleteCommand.Parameters.Clear();
                        DeleteCommand.Parameters.Add(new OracleParameter("CTXID", OracleDbType.Int64));
                        DeleteCommand.Parameters.Add(new OracleParameter("SUBJID", OracleDbType.Int64));
                        DeleteCommand.Parameters["CTXID"].Value = c.PatternMemberID;
                        DeleteCommand.Parameters["SUBJID"].Value = s.PatternMemberID;
                        break;
                    case "CP":
                        DeleteCommand.CommandText = $"DELETE FROM {ConnectionBuilder.UserID}.QUADRUPLES WHERE CONTEXTID = :CTXID AND PREDICATEID = :PREDID";
                        DeleteCommand.Parameters.Clear();
                        DeleteCommand.Parameters.Add(new OracleParameter("CTXID", OracleDbType.Int64));
                        DeleteCommand.Parameters.Add(new OracleParameter("PREDID", OracleDbType.Int64));
                        DeleteCommand.Parameters["CTXID"].Value = c.PatternMemberID;
                        DeleteCommand.Parameters["PREDID"].Value = p.PatternMemberID;
                        break;
                    case "CO":
                        DeleteCommand.CommandText = $"DELETE FROM {ConnectionBuilder.UserID}.QUADRUPLES WHERE CONTEXTID = :CTXID AND OBJECTID = :OBJID AND TRIPLEFLAVOR = :TFV";
                        DeleteCommand.Parameters.Clear();
                        DeleteCommand.Parameters.Add(new OracleParameter("CTXID", OracleDbType.Int64));
                        DeleteCommand.Parameters.Add(new OracleParameter("OBJID", OracleDbType.Int64));
                        DeleteCommand.Parameters.Add(new OracleParameter("TFV", OracleDbType.Int32));
                        DeleteCommand.Parameters["CTXID"].Value = c.PatternMemberID;
                        DeleteCommand.Parameters["OBJID"].Value = o.PatternMemberID;
                        DeleteCommand.Parameters["TFV"].Value = (int)RDFModelEnums.RDFTripleFlavors.SPO;
                        break;
                    case "CL":
                        DeleteCommand.CommandText = $"DELETE FROM {ConnectionBuilder.UserID}.QUADRUPLES WHERE CONTEXTID = :CTXID AND OBJECTID = :OBJID AND TRIPLEFLAVOR = :TFV";
                        DeleteCommand.Parameters.Clear();
                        DeleteCommand.Parameters.Add(new OracleParameter("CTXID", OracleDbType.Int64));
                        DeleteCommand.Parameters.Add(new OracleParameter("OBJID", OracleDbType.Int64));
                        DeleteCommand.Parameters.Add(new OracleParameter("TFV", OracleDbType.Int32));
                        DeleteCommand.Parameters["CTXID"].Value = c.PatternMemberID;
                        DeleteCommand.Parameters["OBJID"].Value = l.PatternMemberID;
                        DeleteCommand.Parameters["TFV"].Value = (int)RDFModelEnums.RDFTripleFlavors.SPL;
                        break;
                    case "CSP":
                        DeleteCommand.CommandText = $"DELETE FROM {ConnectionBuilder.UserID}.QUADRUPLES WHERE CONTEXTID = :CTXID AND SUBJECTID = :SUBJID AND PREDICATEID = :PREDID";
                        DeleteCommand.Parameters.Clear();
                        DeleteCommand.Parameters.Add(new OracleParameter("CTXID", OracleDbType.Int64));
                        DeleteCommand.Parameters.Add(new OracleParameter("SUBJID", OracleDbType.Int64));
                        DeleteCommand.Parameters.Add(new OracleParameter("PREDID", OracleDbType.Int64));
                        DeleteCommand.Parameters["CTXID"].Value = c.PatternMemberID;
                        DeleteCommand.Parameters["SUBJID"].Value = s.PatternMemberID;
                        DeleteCommand.Parameters["PREDID"].Value = p.PatternMemberID;
                        break;
                    case "CSO":
                        DeleteCommand.CommandText = $"DELETE FROM {ConnectionBuilder.UserID}.QUADRUPLES WHERE CONTEXTID = :CTXID AND SUBJECTID = :SUBJID AND OBJECTID = :OBJID AND TRIPLEFLAVOR = :TFV";
                        DeleteCommand.Parameters.Clear();
                        DeleteCommand.Parameters.Add(new OracleParameter("CTXID", OracleDbType.Int64));
                        DeleteCommand.Parameters.Add(new OracleParameter("SUBJID", OracleDbType.Int64));
                        DeleteCommand.Parameters.Add(new OracleParameter("OBJID", OracleDbType.Int64));
                        DeleteCommand.Parameters.Add(new OracleParameter("TFV", OracleDbType.Int32));
                        DeleteCommand.Parameters["CTXID"].Value = c.PatternMemberID;
                        DeleteCommand.Parameters["SUBJID"].Value = s.PatternMemberID;
                        DeleteCommand.Parameters["OBJID"].Value = o.PatternMemberID;
                        DeleteCommand.Parameters["TFV"].Value = (int)RDFModelEnums.RDFTripleFlavors.SPO;
                        break;
                    case "CSL":
                        DeleteCommand.CommandText = $"DELETE FROM {ConnectionBuilder.UserID}.QUADRUPLES WHERE CONTEXTID = :CTXID AND SUBJECTID = :SUBJID AND OBJECTID = :OBJID AND TRIPLEFLAVOR = :TFV";
                        DeleteCommand.Parameters.Clear();
                        DeleteCommand.Parameters.Add(new OracleParameter("CTXID", OracleDbType.Int64));
                        DeleteCommand.Parameters.Add(new OracleParameter("SUBJID", OracleDbType.Int64));
                        DeleteCommand.Parameters.Add(new OracleParameter("OBJID", OracleDbType.Int64));
                        DeleteCommand.Parameters.Add(new OracleParameter("TFV", OracleDbType.Int32));
                        DeleteCommand.Parameters["CTXID"].Value = c.PatternMemberID;
                        DeleteCommand.Parameters["SUBJID"].Value = s.PatternMemberID;
                        DeleteCommand.Parameters["OBJID"].Value = l.PatternMemberID;
                        DeleteCommand.Parameters["TFV"].Value = (int)RDFModelEnums.RDFTripleFlavors.SPL;
                        break;
                    case "CPO":
                        DeleteCommand.CommandText = $"DELETE FROM {ConnectionBuilder.UserID}.QUADRUPLES WHERE CONTEXTID = :CTXID AND PREDICATEID = :PREDID AND OBJECTID = :OBJID AND TRIPLEFLAVOR = :TFV";
                        DeleteCommand.Parameters.Clear();
                        DeleteCommand.Parameters.Add(new OracleParameter("CTXID", OracleDbType.Int64));
                        DeleteCommand.Parameters.Add(new OracleParameter("PREDID", OracleDbType.Int64));
                        DeleteCommand.Parameters.Add(new OracleParameter("OBJID", OracleDbType.Int64));
                        DeleteCommand.Parameters.Add(new OracleParameter("TFV", OracleDbType.Int32));
                        DeleteCommand.Parameters["CTXID"].Value = c.PatternMemberID;
                        DeleteCommand.Parameters["PREDID"].Value = p.PatternMemberID;
                        DeleteCommand.Parameters["OBJID"].Value = o.PatternMemberID;
                        DeleteCommand.Parameters["TFV"].Value = (int)RDFModelEnums.RDFTripleFlavors.SPO;
                        break;
                    case "CPL":
                        DeleteCommand.CommandText = $"DELETE FROM {ConnectionBuilder.UserID}.QUADRUPLES WHERE CONTEXTID = :CTXID AND PREDICATEID = :PREDID AND OBJECTID = :OBJID AND TRIPLEFLAVOR = :TFV";
                        DeleteCommand.Parameters.Clear();
                        DeleteCommand.Parameters.Add(new OracleParameter("CTXID", OracleDbType.Int64));
                        DeleteCommand.Parameters.Add(new OracleParameter("PREDID", OracleDbType.Int64));
                        DeleteCommand.Parameters.Add(new OracleParameter("OBJID", OracleDbType.Int64));
                        DeleteCommand.Parameters.Add(new OracleParameter("TFV", OracleDbType.Int32));
                        DeleteCommand.Parameters["CTXID"].Value = c.PatternMemberID;
                        DeleteCommand.Parameters["PREDID"].Value = p.PatternMemberID;
                        DeleteCommand.Parameters["OBJID"].Value = l.PatternMemberID;
                        DeleteCommand.Parameters["TFV"].Value = (int)RDFModelEnums.RDFTripleFlavors.SPL;
                        break;
                    case "CSPO":
                        DeleteCommand.CommandText = $"DELETE FROM {ConnectionBuilder.UserID}.QUADRUPLES WHERE CONTEXTID = :CTXID AND SUBJECTID = :SUBJID AND PREDICATEID = :PREDID AND OBJECTID = :OBJID AND TRIPLEFLAVOR = :TFV";
                        DeleteCommand.Parameters.Clear();
                        DeleteCommand.Parameters.Add(new OracleParameter("CTXID", OracleDbType.Int64));
                        DeleteCommand.Parameters.Add(new OracleParameter("SUBJID", OracleDbType.Int64));
                        DeleteCommand.Parameters.Add(new OracleParameter("PREDID", OracleDbType.Int64));
                        DeleteCommand.Parameters.Add(new OracleParameter("OBJID", OracleDbType.Int64));
                        DeleteCommand.Parameters.Add(new OracleParameter("TFV", OracleDbType.Int32));
                        DeleteCommand.Parameters["CTXID"].Value = c.PatternMemberID;
                        DeleteCommand.Parameters["SUBJID"].Value = s.PatternMemberID;
                        DeleteCommand.Parameters["PREDID"].Value = p.PatternMemberID;
                        DeleteCommand.Parameters["OBJID"].Value = o.PatternMemberID;
                        DeleteCommand.Parameters["TFV"].Value = (int)RDFModelEnums.RDFTripleFlavors.SPO;
                        break;
                    case "CSPL":
                        DeleteCommand.CommandText = $"DELETE FROM {ConnectionBuilder.UserID}.QUADRUPLES WHERE CONTEXTID = :CTXID AND SUBJECTID = :SUBJID AND PREDICATEID = :PREDID AND OBJECTID = :OBJID AND TRIPLEFLAVOR = :TFV";
                        DeleteCommand.Parameters.Clear();
                        DeleteCommand.Parameters.Add(new OracleParameter("CTXID", OracleDbType.Int64));
                        DeleteCommand.Parameters.Add(new OracleParameter("SUBJID", OracleDbType.Int64));
                        DeleteCommand.Parameters.Add(new OracleParameter("PREDID", OracleDbType.Int64));
                        DeleteCommand.Parameters.Add(new OracleParameter("OBJID", OracleDbType.Int64));
                        DeleteCommand.Parameters.Add(new OracleParameter("TFV", OracleDbType.Int32));
                        DeleteCommand.Parameters["CTXID"].Value = c.PatternMemberID;
                        DeleteCommand.Parameters["SUBJID"].Value = s.PatternMemberID;
                        DeleteCommand.Parameters["PREDID"].Value = p.PatternMemberID;
                        DeleteCommand.Parameters["OBJID"].Value = l.PatternMemberID;
                        DeleteCommand.Parameters["TFV"].Value = (int)RDFModelEnums.RDFTripleFlavors.SPL;
                        break;
                    case "SP":
                        DeleteCommand.CommandText = $"DELETE FROM {ConnectionBuilder.UserID}.QUADRUPLES WHERE SUBJECTID = :SUBJID AND PREDICATEID = :PREDID";
                        DeleteCommand.Parameters.Clear();
                        DeleteCommand.Parameters.Add(new OracleParameter("SUBJID", OracleDbType.Int64));
                        DeleteCommand.Parameters.Add(new OracleParameter("PREDID", OracleDbType.Int64));
                        DeleteCommand.Parameters["SUBJID"].Value = s.PatternMemberID;
                        DeleteCommand.Parameters["PREDID"].Value = p.PatternMemberID;
                        break;
                    case "SO":
                        DeleteCommand.CommandText = $"DELETE FROM {ConnectionBuilder.UserID}.QUADRUPLES WHERE SUBJECTID = :SUBJID AND OBJECTID = :OBJID AND TRIPLEFLAVOR = :TFV";
                        DeleteCommand.Parameters.Clear();
                        DeleteCommand.Parameters.Add(new OracleParameter("SUBJID", OracleDbType.Int64));
                        DeleteCommand.Parameters.Add(new OracleParameter("OBJID", OracleDbType.Int64));
                        DeleteCommand.Parameters.Add(new OracleParameter("TFV", OracleDbType.Int32));
                        DeleteCommand.Parameters["SUBJID"].Value = s.PatternMemberID;
                        DeleteCommand.Parameters["OBJID"].Value = o.PatternMemberID;
                        DeleteCommand.Parameters["TFV"].Value = (int)RDFModelEnums.RDFTripleFlavors.SPO;
                        break;
                    case "SL":
                        DeleteCommand.CommandText = $"DELETE FROM {ConnectionBuilder.UserID}.QUADRUPLES WHERE SUBJECTID = :SUBJID AND OBJECTID = :OBJID AND TRIPLEFLAVOR = :TFV";
                        DeleteCommand.Parameters.Clear();
                        DeleteCommand.Parameters.Add(new OracleParameter("SUBJID", OracleDbType.Int64));
                        DeleteCommand.Parameters.Add(new OracleParameter("OBJID", OracleDbType.Int64));
                        DeleteCommand.Parameters.Add(new OracleParameter("TFV", OracleDbType.Int32));
                        DeleteCommand.Parameters["SUBJID"].Value = s.PatternMemberID;
                        DeleteCommand.Parameters["OBJID"].Value = l.PatternMemberID;
                        DeleteCommand.Parameters["TFV"].Value = (int)RDFModelEnums.RDFTripleFlavors.SPL;
                        break;
                    case "SPO":
                        DeleteCommand.CommandText = $"DELETE FROM {ConnectionBuilder.UserID}.QUADRUPLES WHERE SUBJECTID = :SUBJID AND PREDICATEID = :PREDID AND OBJECTID = :OBJID AND TRIPLEFLAVOR = :TFV";
                        DeleteCommand.Parameters.Clear();
                        DeleteCommand.Parameters.Add(new OracleParameter("SUBJID", OracleDbType.Int64));
                        DeleteCommand.Parameters.Add(new OracleParameter("PREDID", OracleDbType.Int64));
                        DeleteCommand.Parameters.Add(new OracleParameter("OBJID", OracleDbType.Int64));
                        DeleteCommand.Parameters.Add(new OracleParameter("TFV", OracleDbType.Int32));
                        DeleteCommand.Parameters["SUBJID"].Value = s.PatternMemberID;
                        DeleteCommand.Parameters["PREDID"].Value = p.PatternMemberID;
                        DeleteCommand.Parameters["OBJID"].Value = o.PatternMemberID;
                        DeleteCommand.Parameters["TFV"].Value = (int)RDFModelEnums.RDFTripleFlavors.SPO;
                        break;
                    case "SPL":
                        DeleteCommand.CommandText = $"DELETE FROM {ConnectionBuilder.UserID}.QUADRUPLES WHERE SUBJECTID = :SUBJID AND PREDICATEID = :PREDID AND OBJECTID = :OBJID AND TRIPLEFLAVOR = :TFV";
                        DeleteCommand.Parameters.Clear();
                        DeleteCommand.Parameters.Add(new OracleParameter("SUBJID", OracleDbType.Int64));
                        DeleteCommand.Parameters.Add(new OracleParameter("PREDID", OracleDbType.Int64));
                        DeleteCommand.Parameters.Add(new OracleParameter("OBJID", OracleDbType.Int64));
                        DeleteCommand.Parameters.Add(new OracleParameter("TFV", OracleDbType.Int32));
                        DeleteCommand.Parameters["SUBJID"].Value = s.PatternMemberID;
                        DeleteCommand.Parameters["PREDID"].Value = p.PatternMemberID;
                        DeleteCommand.Parameters["OBJID"].Value = l.PatternMemberID;
                        DeleteCommand.Parameters["TFV"].Value = (int)RDFModelEnums.RDFTripleFlavors.SPL;
                        break;
                    case "PO":
                        DeleteCommand.CommandText = $"DELETE FROM {ConnectionBuilder.UserID}.QUADRUPLES WHERE PREDICATEID = :PREDID AND OBJECTID = :OBJID AND TRIPLEFLAVOR = :TFV";
                        DeleteCommand.Parameters.Clear();
                        DeleteCommand.Parameters.Add(new OracleParameter("PREDID", OracleDbType.Int64));
                        DeleteCommand.Parameters.Add(new OracleParameter("OBJID", OracleDbType.Int64));
                        DeleteCommand.Parameters.Add(new OracleParameter("TFV", OracleDbType.Int32));
                        DeleteCommand.Parameters["PREDID"].Value = p.PatternMemberID;
                        DeleteCommand.Parameters["OBJID"].Value = o.PatternMemberID;
                        DeleteCommand.Parameters["TFV"].Value = (int)RDFModelEnums.RDFTripleFlavors.SPO;
                        break;
                    case "PL":
                        DeleteCommand.CommandText = $"DELETE FROM {ConnectionBuilder.UserID}.QUADRUPLES WHERE PREDICATEID = :PREDID AND OBJECTID = :OBJID AND TRIPLEFLAVOR = :TFV";
                        DeleteCommand.Parameters.Clear();
                        DeleteCommand.Parameters.Add(new OracleParameter("PREDID", OracleDbType.Int64));
                        DeleteCommand.Parameters.Add(new OracleParameter("OBJID", OracleDbType.Int64));
                        DeleteCommand.Parameters.Add(new OracleParameter("TFV", OracleDbType.Int32));
                        DeleteCommand.Parameters["PREDID"].Value = p.PatternMemberID;
                        DeleteCommand.Parameters["OBJID"].Value = l.PatternMemberID;
                        DeleteCommand.Parameters["TFV"].Value = (int)RDFModelEnums.RDFTripleFlavors.SPL;
                        break;
                    //SELECT *
                    default:
                        DeleteCommand.CommandText = $"DELETE FROM {ConnectionBuilder.UserID}.QUADRUPLES";
                        DeleteCommand.Parameters.Clear();
                        break;
                }
                
                //Open connection
                EnsureConnectionIsOpen();

                //Prepare command
                DeleteCommand.Prepare();

                //Open transaction
                DeleteCommand.Transaction = Connection.BeginTransaction();

                //Execute command
                DeleteCommand.ExecuteNonQuery();

                //Close transaction
                DeleteCommand.Transaction.Commit();

                //Close connection
                Connection.Close();
            }
            catch (Exception ex)
            {
                //Rollback transaction
                if (DeleteCommand.Transaction != null)
                    DeleteCommand.Transaction.Rollback();

                //Close connection
                Connection.Close();

                //Propagate exception
                throw new RDFStoreException("Cannot delete data from Oracle store because: " + ex.Message, ex);
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
            //Create command
            DeleteCommand.CommandText = $"DELETE FROM {ConnectionBuilder.UserID}.QUADRUPLES";
            DeleteCommand.Parameters.Clear();

            try
            {
                //Open connection
                EnsureConnectionIsOpen();

                //Prepare command
                DeleteCommand.Prepare();

                //Open transaction
                DeleteCommand.Transaction = Connection.BeginTransaction();

                //Execute command
                DeleteCommand.ExecuteNonQuery();

                //Close transaction
                DeleteCommand.Transaction.Commit();

                //Close connection
                Connection.Close();
            }
            catch (Exception ex)
            {
                //Rollback transaction
                if (DeleteCommand.Transaction != null)
                    DeleteCommand.Transaction.Rollback();

                //Close connection
                Connection.Close();

                //Propagate exception
                throw new RDFStoreException("Cannot delete data from Oracle store because: " + ex.Message, ex);
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

            //Create command
            SelectCommand.CommandText = $"SELECT CASE WHEN EXISTS (SELECT 1 FROM {ConnectionBuilder.UserID}.QUADRUPLES WHERE QUADRUPLEID = :QID) THEN 1 ELSE 0 END FROM DUAL";
            SelectCommand.Parameters.Clear();
            SelectCommand.Parameters.Add(new OracleParameter("QID", OracleDbType.Int64));

            //Valorize parameters
            SelectCommand.Parameters["QID"].Value = quadruple.QuadrupleID;

            //Prepare and execute command
            try
            {
                //Open connection
                EnsureConnectionIsOpen();

                //Prepare command
                SelectCommand.Prepare();

                //Execute command
                int result = int.Parse(SelectCommand.ExecuteScalar().ToString());

                //Close connection
                Connection.Close();

                //Give result
                return result == 1;
            }
            catch (Exception ex)
            {
                //Close connection
                Connection.Close();

                //Propagate exception
                throw new RDFStoreException("Cannot read data from Oracle store because: " + ex.Message, ex);
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
            List<RDFQuadruple>  result = new List<RDFQuadruple>();

            //Build filters
            StringBuilder queryFilters = new StringBuilder();
            if (c != null) queryFilters.Append('C');
            if (s != null) queryFilters.Append('S');
            if (p != null) queryFilters.Append('P');
            if (o != null) queryFilters.Append('O');
            if (l != null) queryFilters.Append('L');

            //Prepare and execute command
            try
            {
                switch (queryFilters.ToString())
                {
                    case "C":
                        SelectCommand.CommandText = $"SELECT TRIPLEFLAVOR, CONTEXT, SUBJECT, PREDICATE, OBJECT FROM {ConnectionBuilder.UserID}.QUADRUPLES WHERE CONTEXTID = :CTXID";
                        SelectCommand.Parameters.Clear();
                        SelectCommand.Parameters.Add(new OracleParameter("CTXID", OracleDbType.Int64));
                        SelectCommand.Parameters["CTXID"].Value = c.PatternMemberID;
                        break;
                    case "S":
                        SelectCommand.CommandText = $"SELECT TRIPLEFLAVOR, CONTEXT, SUBJECT, PREDICATE, OBJECT FROM {ConnectionBuilder.UserID}.QUADRUPLES WHERE SUBJECTID = :SUBJID";
                        SelectCommand.Parameters.Clear();
                        SelectCommand.Parameters.Add(new OracleParameter("SUBJID", OracleDbType.Int64));
                        SelectCommand.Parameters["SUBJID"].Value = s.PatternMemberID;
                        break;
                    case "P":
                        SelectCommand.CommandText = $"SELECT TRIPLEFLAVOR, CONTEXT, SUBJECT, PREDICATE, OBJECT FROM {ConnectionBuilder.UserID}.QUADRUPLES WHERE PREDICATEID = :PREDID";
                        SelectCommand.Parameters.Clear();
                        SelectCommand.Parameters.Add(new OracleParameter("PREDID", OracleDbType.Int64));
                        SelectCommand.Parameters["PREDID"].Value = p.PatternMemberID;
                        break;
                    case "O":
                        SelectCommand.CommandText = $"SELECT TRIPLEFLAVOR, CONTEXT, SUBJECT, PREDICATE, OBJECT FROM {ConnectionBuilder.UserID}.QUADRUPLES WHERE OBJECTID = :OBJID AND TRIPLEFLAVOR = :TFV";
                        SelectCommand.Parameters.Clear();
                        SelectCommand.Parameters.Add(new OracleParameter("OBJID", OracleDbType.Int64));
                        SelectCommand.Parameters.Add(new OracleParameter("TFV", OracleDbType.Int32));
                        SelectCommand.Parameters["OBJID"].Value = o.PatternMemberID;
                        SelectCommand.Parameters["TFV"].Value = (int)RDFModelEnums.RDFTripleFlavors.SPO;
                        break;
                    case "L":
                        SelectCommand.CommandText = $"SELECT TRIPLEFLAVOR, CONTEXT, SUBJECT, PREDICATE, OBJECT FROM {ConnectionBuilder.UserID}.QUADRUPLES WHERE OBJECTID = :OBJID AND TRIPLEFLAVOR = :TFV";
                        SelectCommand.Parameters.Clear();
                        SelectCommand.Parameters.Add(new OracleParameter("OBJID", OracleDbType.Int64));
                        SelectCommand.Parameters.Add(new OracleParameter("TFV", OracleDbType.Int32));
                        SelectCommand.Parameters["OBJID"].Value = l.PatternMemberID;
                        SelectCommand.Parameters["TFV"].Value = (int)RDFModelEnums.RDFTripleFlavors.SPL;
                        break;
                    case "CS":
                        SelectCommand.CommandText = $"SELECT TRIPLEFLAVOR, CONTEXT, SUBJECT, PREDICATE, OBJECT FROM {ConnectionBuilder.UserID}.QUADRUPLES WHERE CONTEXTID = :CTXID AND SUBJECTID = :SUBJID";
                        SelectCommand.Parameters.Clear();
                        SelectCommand.Parameters.Add(new OracleParameter("CTXID", OracleDbType.Int64));
                        SelectCommand.Parameters.Add(new OracleParameter("SUBJID", OracleDbType.Int64));
                        SelectCommand.Parameters["CTXID"].Value = c.PatternMemberID;
                        SelectCommand.Parameters["SUBJID"].Value = s.PatternMemberID;
                        break;
                    case "CP":
                        SelectCommand.CommandText = $"SELECT TRIPLEFLAVOR, CONTEXT, SUBJECT, PREDICATE, OBJECT FROM {ConnectionBuilder.UserID}.QUADRUPLES WHERE CONTEXTID = :CTXID AND PREDICATEID = :PREDID";
                        SelectCommand.Parameters.Clear();
                        SelectCommand.Parameters.Add(new OracleParameter("CTXID", OracleDbType.Int64));
                        SelectCommand.Parameters.Add(new OracleParameter("PREDID", OracleDbType.Int64));
                        SelectCommand.Parameters["CTXID"].Value = c.PatternMemberID;
                        SelectCommand.Parameters["PREDID"].Value = p.PatternMemberID;
                        break;
                    case "CO":
                        SelectCommand.CommandText = $"SELECT TRIPLEFLAVOR, CONTEXT, SUBJECT, PREDICATE, OBJECT FROM {ConnectionBuilder.UserID}.QUADRUPLES WHERE CONTEXTID = :CTXID AND OBJECTID = :OBJID AND TRIPLEFLAVOR = :TFV";
                        SelectCommand.Parameters.Clear();
                        SelectCommand.Parameters.Add(new OracleParameter("CTXID", OracleDbType.Int64));
                        SelectCommand.Parameters.Add(new OracleParameter("OBJID", OracleDbType.Int64));
                        SelectCommand.Parameters.Add(new OracleParameter("TFV", OracleDbType.Int32));
                        SelectCommand.Parameters["CTXID"].Value = c.PatternMemberID;
                        SelectCommand.Parameters["OBJID"].Value = o.PatternMemberID;
                        SelectCommand.Parameters["TFV"].Value = (int)RDFModelEnums.RDFTripleFlavors.SPO;
                        break;
                    case "CL":
                        SelectCommand.CommandText = $"SELECT TRIPLEFLAVOR, CONTEXT, SUBJECT, PREDICATE, OBJECT FROM {ConnectionBuilder.UserID}.QUADRUPLES WHERE CONTEXTID = :CTXID AND OBJECTID = :OBJID AND TRIPLEFLAVOR = :TFV";
                        SelectCommand.Parameters.Clear();
                        SelectCommand.Parameters.Add(new OracleParameter("CTXID", OracleDbType.Int64));
                        SelectCommand.Parameters.Add(new OracleParameter("OBJID", OracleDbType.Int64));
                        SelectCommand.Parameters.Add(new OracleParameter("TFV", OracleDbType.Int32));
                        SelectCommand.Parameters["CTXID"].Value = c.PatternMemberID;
                        SelectCommand.Parameters["OBJID"].Value = l.PatternMemberID;
                        SelectCommand.Parameters["TFV"].Value = (int)RDFModelEnums.RDFTripleFlavors.SPL;
                        break;
                    case "CSP":
                        SelectCommand.CommandText = $"SELECT TRIPLEFLAVOR, CONTEXT, SUBJECT, PREDICATE, OBJECT FROM {ConnectionBuilder.UserID}.QUADRUPLES WHERE CONTEXTID = :CTXID AND SUBJECTID = :SUBJID AND PREDICATEID = :PREDID";
                        SelectCommand.Parameters.Clear();
                        SelectCommand.Parameters.Add(new OracleParameter("CTXID", OracleDbType.Int64));
                        SelectCommand.Parameters.Add(new OracleParameter("SUBJID", OracleDbType.Int64));
                        SelectCommand.Parameters.Add(new OracleParameter("PREDID", OracleDbType.Int64));
                        SelectCommand.Parameters["CTXID"].Value = c.PatternMemberID;
                        SelectCommand.Parameters["SUBJID"].Value = s.PatternMemberID;
                        SelectCommand.Parameters["PREDID"].Value = p.PatternMemberID;
                        break;
                    case "CSO":
                        SelectCommand.CommandText = $"SELECT TRIPLEFLAVOR, CONTEXT, SUBJECT, PREDICATE, OBJECT FROM {ConnectionBuilder.UserID}.QUADRUPLES WHERE CONTEXTID = :CTXID AND SUBJECTID = :SUBJID AND OBJECTID = :OBJID AND TRIPLEFLAVOR = :TFV";
                        SelectCommand.Parameters.Clear();
                        SelectCommand.Parameters.Add(new OracleParameter("CTXID", OracleDbType.Int64));
                        SelectCommand.Parameters.Add(new OracleParameter("SUBJID", OracleDbType.Int64));
                        SelectCommand.Parameters.Add(new OracleParameter("OBJID", OracleDbType.Int64));
                        SelectCommand.Parameters.Add(new OracleParameter("TFV", OracleDbType.Int32));
                        SelectCommand.Parameters["CTXID"].Value = c.PatternMemberID;
                        SelectCommand.Parameters["SUBJID"].Value = s.PatternMemberID;
                        SelectCommand.Parameters["OBJID"].Value = o.PatternMemberID;
                        SelectCommand.Parameters["TFV"].Value = (int)RDFModelEnums.RDFTripleFlavors.SPO;
                        break;
                    case "CSL":
                        SelectCommand.CommandText = $"SELECT TRIPLEFLAVOR, CONTEXT, SUBJECT, PREDICATE, OBJECT FROM {ConnectionBuilder.UserID}.QUADRUPLES WHERE CONTEXTID = :CTXID AND SUBJECTID = :SUBJID AND OBJECTID = :OBJID AND TRIPLEFLAVOR = :TFV";
                        SelectCommand.Parameters.Clear();
                        SelectCommand.Parameters.Add(new OracleParameter("CTXID", OracleDbType.Int64));
                        SelectCommand.Parameters.Add(new OracleParameter("SUBJID", OracleDbType.Int64));
                        SelectCommand.Parameters.Add(new OracleParameter("OBJID", OracleDbType.Int64));
                        SelectCommand.Parameters.Add(new OracleParameter("TFV", OracleDbType.Int32));
                        SelectCommand.Parameters["CTXID"].Value = c.PatternMemberID;
                        SelectCommand.Parameters["SUBJID"].Value = s.PatternMemberID;
                        SelectCommand.Parameters["OBJID"].Value = l.PatternMemberID;
                        SelectCommand.Parameters["TFV"].Value = (int)RDFModelEnums.RDFTripleFlavors.SPL;
                        break;
                    case "CPO":
                        SelectCommand.CommandText = $"SELECT TripleFlavor, Context, Subject, Predicate, Object FROM {ConnectionBuilder.UserID}.QUADRUPLES WHERE CONTEXTID = :CTXID AND \"PREDID\" = :PREDID AND \"OBJID\" = :OBJID AND TRIPLEFLAVOR = :TFV";
                        SelectCommand.Parameters.Clear();
                        SelectCommand.Parameters.Add(new OracleParameter("CTXID", OracleDbType.Int64));
                        SelectCommand.Parameters.Add(new OracleParameter("PREDID", OracleDbType.Int64));
                        SelectCommand.Parameters.Add(new OracleParameter("OBJID", OracleDbType.Int64));
                        SelectCommand.Parameters.Add(new OracleParameter("TFV", OracleDbType.Int32));
                        SelectCommand.Parameters["CTXID"].Value = c.PatternMemberID;
                        SelectCommand.Parameters["PREDID"].Value = p.PatternMemberID;
                        SelectCommand.Parameters["OBJID"].Value = o.PatternMemberID;
                        SelectCommand.Parameters["TFV"].Value = (int)RDFModelEnums.RDFTripleFlavors.SPO;
                        break;
                    case "CPL":
                        SelectCommand.CommandText = $"SELECT TRIPLEFLAVOR, CONTEXT, SUBJECT, PREDICATE, OBJECT FROM {ConnectionBuilder.UserID}.QUADRUPLES WHERE CONTEXTID = :CTXID AND PREDICATEID = :PREDID AND OBJECTID = :OBJID AND TRIPLEFLAVOR = :TFV";
                        SelectCommand.Parameters.Clear();
                        SelectCommand.Parameters.Add(new OracleParameter("CTXID", OracleDbType.Int64));
                        SelectCommand.Parameters.Add(new OracleParameter("PREDID", OracleDbType.Int64));
                        SelectCommand.Parameters.Add(new OracleParameter("OBJID", OracleDbType.Int64));
                        SelectCommand.Parameters.Add(new OracleParameter("TFV", OracleDbType.Int32));
                        SelectCommand.Parameters["CTXID"].Value = c.PatternMemberID;
                        SelectCommand.Parameters["PREDID"].Value = p.PatternMemberID;
                        SelectCommand.Parameters["OBJID"].Value = l.PatternMemberID;
                        SelectCommand.Parameters["TFV"].Value = (int)RDFModelEnums.RDFTripleFlavors.SPL;
                        break;
                    case "CSPO":
                        SelectCommand.CommandText = $"SELECT TRIPLEFLAVOR, CONTEXT, SUBJECT, PREDICATE, OBJECT FROM {ConnectionBuilder.UserID}.QUADRUPLES WHERE CONTEXTID = :CTXID AND SUBJECTID = :SUBJID AND PREDICATEID = :PREDID AND OBJECTID = :OBJID AND TRIPLEFLAVOR = :TFV";
                        SelectCommand.Parameters.Clear();
                        SelectCommand.Parameters.Add(new OracleParameter("CTXID", OracleDbType.Int64));
                        SelectCommand.Parameters.Add(new OracleParameter("SUBJID", OracleDbType.Int64));
                        SelectCommand.Parameters.Add(new OracleParameter("PREDID", OracleDbType.Int64));
                        SelectCommand.Parameters.Add(new OracleParameter("OBJID", OracleDbType.Int64));
                        SelectCommand.Parameters.Add(new OracleParameter("TFV", OracleDbType.Int32));
                        SelectCommand.Parameters["CTXID"].Value = c.PatternMemberID;
                        SelectCommand.Parameters["SUBJID"].Value = s.PatternMemberID;
                        SelectCommand.Parameters["PREDID"].Value = p.PatternMemberID;
                        SelectCommand.Parameters["OBJID"].Value = o.PatternMemberID;
                        SelectCommand.Parameters["TFV"].Value = (int)RDFModelEnums.RDFTripleFlavors.SPO;
                        break;
                    case "CSPL":
                        SelectCommand.CommandText = $"SELECT TRIPLEFLAVOR, CONTEXT, SUBJECT, PREDICATE, OBJECT FROM {ConnectionBuilder.UserID}.QUADRUPLES WHERE CONTEXTID = :CTXID AND SUBJECTID = :SUBJID AND PREDICATEID = :PREDID AND OBJECTID = :OBJID AND TRIPLEFLAVOR = :TFV";
                        SelectCommand.Parameters.Clear();
                        SelectCommand.Parameters.Add(new OracleParameter("CTXID", OracleDbType.Int64));
                        SelectCommand.Parameters.Add(new OracleParameter("SUBJID", OracleDbType.Int64));
                        SelectCommand.Parameters.Add(new OracleParameter("PREDID", OracleDbType.Int64));
                        SelectCommand.Parameters.Add(new OracleParameter("OBJID", OracleDbType.Int64));
                        SelectCommand.Parameters.Add(new OracleParameter("TFV", OracleDbType.Int32));
                        SelectCommand.Parameters["CTXID"].Value = c.PatternMemberID;
                        SelectCommand.Parameters["SUBJID"].Value = s.PatternMemberID;
                        SelectCommand.Parameters["PREDID"].Value = p.PatternMemberID;
                        SelectCommand.Parameters["OBJID"].Value = l.PatternMemberID;
                        SelectCommand.Parameters["TFV"].Value = (int)RDFModelEnums.RDFTripleFlavors.SPL;
                        break;
                    case "SP":
                        SelectCommand.CommandText = $"SELECT TRIPLEFLAVOR, CONTEXT, SUBJECT, PREDICATE, OBJECT FROM {ConnectionBuilder.UserID}.QUADRUPLES WHERE SUBJECTID = :SUBJID AND PREDICATEID = :PREDID";
                        SelectCommand.Parameters.Clear();
                        SelectCommand.Parameters.Add(new OracleParameter("SUBJID", OracleDbType.Int64));
                        SelectCommand.Parameters.Add(new OracleParameter("PREDID", OracleDbType.Int64));
                        SelectCommand.Parameters["SUBJID"].Value = s.PatternMemberID;
                        SelectCommand.Parameters["PREDID"].Value = p.PatternMemberID;
                        break;
                    case "SO":
                        SelectCommand.CommandText = $"SELECT TRIPLEFLAVOR, CONTEXT, SUBJECT, PREDICATE, OBJECT FROM {ConnectionBuilder.UserID}.QUADRUPLES WHERE SUBJECTID = :SUBJID AND OBJECTID = :OBJID AND TRIPLEFLAVOR = :TFV";
                        SelectCommand.Parameters.Clear();
                        SelectCommand.Parameters.Add(new OracleParameter("SUBJID", OracleDbType.Int64));
                        SelectCommand.Parameters.Add(new OracleParameter("OBJID", OracleDbType.Int64));
                        SelectCommand.Parameters.Add(new OracleParameter("TFV", OracleDbType.Int32));
                        SelectCommand.Parameters["SUBJID"].Value = s.PatternMemberID;
                        SelectCommand.Parameters["OBJID"].Value = o.PatternMemberID;
                        SelectCommand.Parameters["TFV"].Value = (int)RDFModelEnums.RDFTripleFlavors.SPO;
                        break;
                    case "SL":
                        SelectCommand.CommandText = $"SELECT TRIPLEFLAVOR, CONTEXT, SUBJECT, PREDICATE, OBJECT FROM {ConnectionBuilder.UserID}.QUADRUPLES WHERE SUBJECTID = :SUBJID AND OBJECTID = :OBJID AND TRIPLEFLAVOR = :TFV";
                        SelectCommand.Parameters.Clear();
                        SelectCommand.Parameters.Add(new OracleParameter("SUBJID", OracleDbType.Int64));
                        SelectCommand.Parameters.Add(new OracleParameter("OBJID", OracleDbType.Int64));
                        SelectCommand.Parameters.Add(new OracleParameter("TFV", OracleDbType.Int32));
                        SelectCommand.Parameters["SUBJID"].Value = s.PatternMemberID;
                        SelectCommand.Parameters["OBJID"].Value = l.PatternMemberID;
                        SelectCommand.Parameters["TFV"].Value = (int)RDFModelEnums.RDFTripleFlavors.SPL;
                        break;
                    case "SPO":
                        SelectCommand.CommandText = $"SELECT TRIPLEFLAVOR, CONTEXT, SUBJECT, PREDICATE, OBJECT FROM {ConnectionBuilder.UserID}.QUADRUPLES WHERE SUBJECTID = :SUBJID AND PREDICATEID = :PREDID AND OBJECTID = :OBJID AND TRIPLEFLAVOR = :TFV";
                        SelectCommand.Parameters.Clear();
                        SelectCommand.Parameters.Add(new OracleParameter("SUBJID", OracleDbType.Int64));
                        SelectCommand.Parameters.Add(new OracleParameter("PREDID", OracleDbType.Int64));
                        SelectCommand.Parameters.Add(new OracleParameter("OBJID", OracleDbType.Int64));
                        SelectCommand.Parameters.Add(new OracleParameter("TFV", OracleDbType.Int32));
                        SelectCommand.Parameters["SUBJID"].Value = s.PatternMemberID;
                        SelectCommand.Parameters["PREDID"].Value = p.PatternMemberID;
                        SelectCommand.Parameters["OBJID"].Value = o.PatternMemberID;
                        SelectCommand.Parameters["TFV"].Value = (int)RDFModelEnums.RDFTripleFlavors.SPO;
                        break;
                    case "SPL":
                        SelectCommand.CommandText = $"SELECT TRIPLEFLAVOR, CONTEXT, SUBJECT, PREDICATE, OBJECT FROM {ConnectionBuilder.UserID}.QUADRUPLES WHERE SUBJECTID = :SUBJID AND PREDICATEID = :PREDID AND OBJECTID = :OBJID AND TRIPLEFLAVOR = :TFV";
                        SelectCommand.Parameters.Clear();
                        SelectCommand.Parameters.Add(new OracleParameter("SUBJID", OracleDbType.Int64));
                        SelectCommand.Parameters.Add(new OracleParameter("PREDID", OracleDbType.Int64));
                        SelectCommand.Parameters.Add(new OracleParameter("OBJID", OracleDbType.Int64));
                        SelectCommand.Parameters.Add(new OracleParameter("TFV", OracleDbType.Int32));
                        SelectCommand.Parameters["SUBJID"].Value = s.PatternMemberID;
                        SelectCommand.Parameters["PREDID"].Value = p.PatternMemberID;
                        SelectCommand.Parameters["OBJID"].Value = l.PatternMemberID;
                        SelectCommand.Parameters["TFV"].Value = (int)RDFModelEnums.RDFTripleFlavors.SPL;
                        break;
                    case "PO":
                        SelectCommand.CommandText = $"SELECT TRIPLEFLAVOR, CONTEXT, SUBJECT, PREDICATE, OBJECT FROM {ConnectionBuilder.UserID}.QUADRUPLES WHERE PREDICATEID = :PREDID AND OBJECTID = :OBJID AND TRIPLEFLAVOR = :TFV";
                        SelectCommand.Parameters.Clear();
                        SelectCommand.Parameters.Add(new OracleParameter("PREDID", OracleDbType.Int64));
                        SelectCommand.Parameters.Add(new OracleParameter("OBJID", OracleDbType.Int64));
                        SelectCommand.Parameters.Add(new OracleParameter("TFV", OracleDbType.Int32));
                        SelectCommand.Parameters["PREDID"].Value = p.PatternMemberID;
                        SelectCommand.Parameters["OBJID"].Value = o.PatternMemberID;
                        SelectCommand.Parameters["TFV"].Value = (int)RDFModelEnums.RDFTripleFlavors.SPO;
                        break;
                    case "PL":
                        SelectCommand.CommandText = $"SELECT TRIPLEFLAVOR, CONTEXT, SUBJECT, PREDICATE, OBJECT FROM {ConnectionBuilder.UserID}.QUADRUPLES WHERE PREDICATEID = :PREDID AND OBJECTID = :OBJID AND TRIPLEFLAVOR = :TFV";
                        SelectCommand.Parameters.Clear();
                        SelectCommand.Parameters.Add(new OracleParameter("PREDID", OracleDbType.Int64));
                        SelectCommand.Parameters.Add(new OracleParameter("OBJID", OracleDbType.Int64));
                        SelectCommand.Parameters.Add(new OracleParameter("TFV", OracleDbType.Int32));
                        SelectCommand.Parameters["PREDID"].Value = p.PatternMemberID;
                        SelectCommand.Parameters["OBJID"].Value = l.PatternMemberID;
                        SelectCommand.Parameters["TFV"].Value = (int)RDFModelEnums.RDFTripleFlavors.SPL;
                        break;
                    //SELECT *
                    default:
                        SelectCommand.CommandText = $"SELECT TRIPLEFLAVOR, CONTEXT, SUBJECT, PREDICATE, OBJECT FROM {ConnectionBuilder.UserID}.QUADRUPLES";
                        SelectCommand.Parameters.Clear();
                        break;
                }

                //Open connection
                EnsureConnectionIsOpen();

                //Execute command
                using (OracleDataReader quadruples = SelectCommand.ExecuteReader(CommandBehavior.Default))
                {
                    while (quadruples.Read())
                        result.Add(RDFStoreUtilities.ParseQuadruple(quadruples));
                }

                //Close connection
                Connection.Close();
            }
            catch (Exception ex)
            {
                //Close connection
                Connection.Close();

                //Propagate exception
                throw new RDFStoreException("Cannot read data from Firebird store because: " + ex.Message, ex);
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
            try
            {
                //Open connection
                EnsureConnectionIsOpen();

                //Create command
                SelectCommand.CommandText = $"SELECT COUNT(*) FROM {ConnectionBuilder.UserID}.QUADRUPLES";
                SelectCommand.Parameters.Clear();

                //Execute command
                long result = long.Parse(SelectCommand.ExecuteScalar().ToString());

                //Close connection
                Connection.Close();

                //Return the quadruples count
                return  result;
            }
            catch
            {
                //Close connection
                Connection.Close();

                //Return the quadruples count (-1 to indicate error)
                return -1;
            }
        }
        #endregion

        #region Optimize
        /// <summary>
        /// Optimizes "Quadruples" table of Oracle store
        /// </summary>
        public void Optimize()
        {
            try
            {
                //Open connection
                EnsureConnectionIsOpen();

                //Create command
                OracleCommand optimizeCommand = new OracleCommand($"ALTER INDEX {ConnectionBuilder.UserID}.IDX_CONTEXTID REBUILD", Connection);
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

                //Close connection
                Connection.Close();
            }
            catch (Exception ex)
            {
                //Close connection
                Connection.Close();

                //Propagate exception
                throw new RDFStoreException("Cannot optimize Oracle store because: " + ex.Message, ex);
            }
        }
        #endregion
        
        #region Utilities
        private void EnsureConnectionIsOpen()
        {
            switch (Connection.State)
            {
                case ConnectionState.Closed:
                    Connection.Open();
                    break;

                case ConnectionState.Broken:
                case ConnectionState.Connecting:
                    Connection.Close();
                    Connection.Open();
                    break;
            }
        }
        #endregion

        #endregion
    }
}