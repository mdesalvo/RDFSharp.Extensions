/*
   Copyright 2012-2024 Marco De Salvo

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
    public class RDFOracleStore : RDFStore, IDisposable
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
        public Task<long> QuadruplesCountAsync 
			=> GetQuadruplesCountAsync(); 

        /// <summary>
        /// Connection to the Oracle database
        /// </summary>
        internal OracleConnection Connection { get; set; }

        /// <summary>
        /// Utility for getting fields of the connection
        /// </summary>
        internal OracleConnectionStringBuilder ConnectionBuilder { get; set; }

        /// <summary>
        /// Command to execute SELECT queries on the Oracle database
        /// </summary>
        internal OracleCommand SelectCommand { get; set; }

        /// <summary>
        /// Command to execute INSERT queries on the Oracle database
        /// </summary>
        internal OracleCommand InsertCommand { get; set; }

        /// <summary>
        /// Command to execute DELETE queries on the Oracle database
        /// </summary>
        internal OracleCommand DeleteCommand { get; set; }

        /// <summary>
        /// Flag indicating that the Oracle store instance has already been disposed
        /// </summary>
        internal bool Disposed { get; set; }
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
            StoreType = "ORACLE";
            Connection = new OracleConnection(oracleConnectionString);
            ConnectionBuilder = new OracleConnectionStringBuilder(Connection.ConnectionString);
            SelectCommand = new OracleCommand() { Connection = Connection, CommandTimeout = oracleStoreOptions.SelectTimeout };
            DeleteCommand = new OracleCommand() { Connection = Connection, CommandTimeout = oracleStoreOptions.DeleteTimeout };
            InsertCommand = new OracleCommand() { Connection = Connection, CommandTimeout = oracleStoreOptions.InsertTimeout };
            StoreID = RDFModelUtilities.CreateHash(ToString());
            Disposed = false;

            //Perform initial diagnostics
            InitializeStore();
        }
        
        /// <summary>
        /// Destroys the Oracle store instance
        /// </summary>
        ~RDFOracleStore() => Dispose(false);
        #endregion

        #region Interfaces
        /// <summary>
        /// Gives the string representation of the Oracle store 
        /// </summary>
        public override string ToString()
            => string.Concat(base.ToString(), "|SERVER=", Connection.DataSource, ";DATABASE=", Connection.Database);

        /// <summary>
        /// Disposes the Oracle store instance 
        /// </summary>
        public void Dispose()
        {
            Dispose(true);
            GC.SuppressFinalize(this);
        }

        /// <summary>
        /// Disposes the Oracle store instance  (business logic of resources disposal)
        /// </summary>
        protected virtual void Dispose(bool disposing)
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
        /// Merges the given graph into the store within a single transaction, avoiding duplicate insertions
        /// </summary>
        public override RDFStore MergeGraph(RDFGraph graph)
        {
            if (graph != null)
            {
                RDFContext graphCtx = new RDFContext(graph.Context);

                //Create command
                InsertCommand.CommandText = "INSERT INTO \"" + ConnectionBuilder.UserID + "\".\"QUADRUPLES\"(\"QUADRUPLEID\", \"TRIPLEFLAVOR\", \"CONTEXT\", \"CONTEXTID\", \"SUBJECT\", \"SUBJECTID\", \"PREDICATE\", \"PREDICATEID\", \"OBJECT\", \"OBJECTID\") SELECT :QID, :TFV, :CTX, :CTXID, :SUBJ, :SUBJID, :PRED, :PREDID, :OBJ, :OBJID FROM DUAL WHERE NOT EXISTS(SELECT \"QUADRUPLEID\" FROM \"" + ConnectionBuilder.UserID + "\".\"QUADRUPLES\" WHERE \"QUADRUPLEID\" = :QID)";
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
                    Connection.Open();

                    //Prepare command
                    InsertCommand.Prepare();

                    //Open transaction
                    InsertCommand.Transaction = Connection.BeginTransaction();

                    //Iterate triples
                    foreach (RDFTriple triple in graph)
                    {
                        //Valorize parameters
                        InsertCommand.Parameters["QID"].Value = RDFModelUtilities.CreateHash(string.Concat(graphCtx, " ", triple.Subject, " ", triple.Predicate, " ", triple.Object));
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
        /// Adds the given quadruple to the store, avoiding duplicate insertions
        /// </summary>
        public override RDFStore AddQuadruple(RDFQuadruple quadruple)
        {
            if (quadruple != null)
            {
                //Create command
                InsertCommand.CommandText = "INSERT INTO \"" + ConnectionBuilder.UserID + "\".\"QUADRUPLES\"(\"QUADRUPLEID\", \"TRIPLEFLAVOR\", \"CONTEXT\", \"CONTEXTID\", \"SUBJECT\", \"SUBJECTID\", \"PREDICATE\", \"PREDICATEID\", \"OBJECT\", \"OBJECTID\") SELECT :QID, :TFV, :CTX, :CTXID, :SUBJ, :SUBJID, :PRED, :PREDID, :OBJ, :OBJID FROM DUAL WHERE NOT EXISTS(SELECT \"QUADRUPLEID\" FROM \"" + ConnectionBuilder.UserID + "\".\"QUADRUPLES\" WHERE \"QUADRUPLEID\" = :QID)";
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
                    Connection.Open();

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
                    InsertCommand.Transaction.Rollback();

                    //Close connection
                    Connection.Close();

                    //Propagate exception
                    throw new RDFStoreException("Cannot insert data into Oracle store because: " + ex.Message, ex);
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
        {
            if (quadruple != null)
            {
                //Create command
                DeleteCommand.CommandText = "DELETE FROM \"" + ConnectionBuilder.UserID + "\".\"QUADRUPLES\" WHERE \"QUADRUPLEID\" = :QID";
                DeleteCommand.Parameters.Clear();
                DeleteCommand.Parameters.Add(new OracleParameter("QID", OracleDbType.Int64));

                //Valorize parameters
                DeleteCommand.Parameters["QID"].Value = quadruple.QuadrupleID;

                try
                {
                    //Open connection
                    Connection.Open();

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
        /// Removes the quadruples with the given context
        /// </summary>
        public override RDFStore RemoveQuadruplesByContext(RDFContext contextResource)
        {
            if (contextResource != null)
            {
                //Create command
                DeleteCommand.CommandText = "DELETE FROM \"" + ConnectionBuilder.UserID + "\".\"QUADRUPLES\" WHERE \"CONTEXTID\" = :CTXID";
                DeleteCommand.Parameters.Clear();
                DeleteCommand.Parameters.Add(new OracleParameter("CTXID", OracleDbType.Int64));

                //Valorize parameters
                DeleteCommand.Parameters["CTXID"].Value = contextResource.PatternMemberID;

                try
                {
                    //Open connection
                    Connection.Open();

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
        /// Removes the quadruples with the given subject
        /// </summary>
        public override RDFStore RemoveQuadruplesBySubject(RDFResource subjectResource)
        {
            if (subjectResource != null)
            {
                //Create command
                DeleteCommand.CommandText = "DELETE FROM \"" + ConnectionBuilder.UserID + "\".\"QUADRUPLES\" WHERE \"SUBJECTID\" = :SUBJID";
                DeleteCommand.Parameters.Clear();
                DeleteCommand.Parameters.Add(new OracleParameter("SUBJID", OracleDbType.Int64));

                //Valorize parameters
                DeleteCommand.Parameters["SUBJID"].Value = subjectResource.PatternMemberID;

                try
                {
                    //Open connection
                    Connection.Open();

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
        /// Removes the quadruples with the given predicate
        /// </summary>
        public override RDFStore RemoveQuadruplesByPredicate(RDFResource predicateResource)
        {
            if (predicateResource != null)
            {
                //Create command
                DeleteCommand.CommandText = "DELETE FROM \"" + ConnectionBuilder.UserID + "\".\"QUADRUPLES\" WHERE \"PREDICATEID\" = :PREDID";
                DeleteCommand.Parameters.Clear();
                DeleteCommand.Parameters.Add(new OracleParameter("PREDID", OracleDbType.Int64));

                //Valorize parameters
                DeleteCommand.Parameters["PREDID"].Value = predicateResource.PatternMemberID;

                try
                {
                    //Open connection
                    Connection.Open();

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
        /// Removes the quadruples with the given resource as object
        /// </summary>
        public override RDFStore RemoveQuadruplesByObject(RDFResource objectResource)
        {
            if (objectResource != null)
            {
                //Create command
                DeleteCommand.CommandText = "DELETE FROM \"" + ConnectionBuilder.UserID + "\".\"QUADRUPLES\" WHERE \"OBJECTID\" = :OBJID AND \"TRIPLEFLAVOR\" = :TFV";
                DeleteCommand.Parameters.Clear();
                DeleteCommand.Parameters.Add(new OracleParameter("OBJID", OracleDbType.Int64));
                DeleteCommand.Parameters.Add(new OracleParameter("TFV", OracleDbType.Int32));

                //Valorize parameters
                DeleteCommand.Parameters["OBJID"].Value = objectResource.PatternMemberID;
                DeleteCommand.Parameters["TFV"].Value = (int)RDFModelEnums.RDFTripleFlavors.SPO;

                try
                {
                    //Open connection
                    Connection.Open();

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
        /// Removes the quadruples with the given literal as object
        /// </summary>
        public override RDFStore RemoveQuadruplesByLiteral(RDFLiteral literalObject)
        {
            if (literalObject != null)
            {
                //Create command
                DeleteCommand.CommandText = "DELETE FROM \"" + ConnectionBuilder.UserID + "\".\"QUADRUPLES\" WHERE \"OBJECTID\" = :OBJID AND \"TRIPLEFLAVOR\" = :TFV";
                DeleteCommand.Parameters.Clear();
                DeleteCommand.Parameters.Add(new OracleParameter("OBJID", OracleDbType.Int64));
                DeleteCommand.Parameters.Add(new OracleParameter("TFV", OracleDbType.Int32));

                //Valorize parameters
                DeleteCommand.Parameters["OBJID"].Value = literalObject.PatternMemberID;
                DeleteCommand.Parameters["TFV"].Value = (int)RDFModelEnums.RDFTripleFlavors.SPL;

                try
                {
                    //Open connection
                    Connection.Open();

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
        /// Removes the quadruples with the given context and subject
        /// </summary>
        public override RDFStore RemoveQuadruplesByContextSubject(RDFContext contextResource, RDFResource subjectResource)
        {
            if (contextResource != null && subjectResource != null)
            {
                //Create command
                DeleteCommand.CommandText = "DELETE FROM \"" + ConnectionBuilder.UserID + "\".\"QUADRUPLES\" WHERE \"CONTEXTID\" = :CTXID AND \"SUBJECTID\" = :SUBJID";
                DeleteCommand.Parameters.Clear();
                DeleteCommand.Parameters.Add(new OracleParameter("CTXID", OracleDbType.Int64));
                DeleteCommand.Parameters.Add(new OracleParameter("SUBJID", OracleDbType.Int64));

                //Valorize parameters
                DeleteCommand.Parameters["CTXID"].Value = contextResource.PatternMemberID;
                DeleteCommand.Parameters["SUBJID"].Value = subjectResource.PatternMemberID;

                try
                {
                    //Open connection
                    Connection.Open();

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
        /// Removes the quadruples with the given context and predicate
        /// </summary>
        public override RDFStore RemoveQuadruplesByContextPredicate(RDFContext contextResource, RDFResource predicateResource)
        {
            if (contextResource != null && predicateResource != null)
            {
                //Create command
                DeleteCommand.CommandText = "DELETE FROM \"" + ConnectionBuilder.UserID + "\".\"QUADRUPLES\" WHERE \"CONTEXTID\" = :CTXID AND \"PREDICATEID\" = :PREDID";
                DeleteCommand.Parameters.Clear();
                DeleteCommand.Parameters.Add(new OracleParameter("CTXID", OracleDbType.Int64));
                DeleteCommand.Parameters.Add(new OracleParameter("PREDID", OracleDbType.Int64));

                //Valorize parameters
                DeleteCommand.Parameters["CTXID"].Value = contextResource.PatternMemberID;
                DeleteCommand.Parameters["PREDID"].Value = predicateResource.PatternMemberID;

                try
                {
                    //Open connection
                    Connection.Open();

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
        /// Removes the quadruples with the given context and object
        /// </summary>
        public override RDFStore RemoveQuadruplesByContextObject(RDFContext contextResource, RDFResource objectResource)
        {
            if (contextResource != null && objectResource != null)
            {
                //Create command
                DeleteCommand.CommandText = "DELETE FROM \"" + ConnectionBuilder.UserID + "\".\"QUADRUPLES\" WHERE \"CONTEXTID\" = :CTXID AND \"OBJECTID\" = :OBJID AND \"TRIPLEFLAVOR\" = :TFV";
                DeleteCommand.Parameters.Clear();
                DeleteCommand.Parameters.Add(new OracleParameter("CTXID", OracleDbType.Int64));
                DeleteCommand.Parameters.Add(new OracleParameter("OBJID", OracleDbType.Int64));
                DeleteCommand.Parameters.Add(new OracleParameter("TFV", OracleDbType.Int32));

                //Valorize parameters
                DeleteCommand.Parameters["CTXID"].Value = contextResource.PatternMemberID;
                DeleteCommand.Parameters["OBJID"].Value = objectResource.PatternMemberID;
                DeleteCommand.Parameters["TFV"].Value = RDFModelEnums.RDFTripleFlavors.SPO;

                try
                {
                    //Open connection
                    Connection.Open();

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
        /// Removes the quadruples with the given context and literal
        /// </summary>
        public override RDFStore RemoveQuadruplesByContextLiteral(RDFContext contextResource, RDFLiteral objectLiteral)
        {
            if (contextResource != null && objectLiteral != null)
            {
                //Create command
                DeleteCommand.CommandText = "DELETE FROM \"" + ConnectionBuilder.UserID + "\".\"QUADRUPLES\" WHERE \"CONTEXTID\" = :CTXID AND \"OBJECTID\" = :OBJID AND \"TRIPLEFLAVOR\" = :TFV";
                DeleteCommand.Parameters.Clear();
                DeleteCommand.Parameters.Add(new OracleParameter("CTXID", OracleDbType.Int64));
                DeleteCommand.Parameters.Add(new OracleParameter("OBJID", OracleDbType.Int64));
                DeleteCommand.Parameters.Add(new OracleParameter("TFV", OracleDbType.Int32));

                //Valorize parameters
                DeleteCommand.Parameters["CTXID"].Value = contextResource.PatternMemberID;
                DeleteCommand.Parameters["OBJID"].Value = objectLiteral.PatternMemberID;
                DeleteCommand.Parameters["TFV"].Value = RDFModelEnums.RDFTripleFlavors.SPL;

                try
                {
                    //Open connection
                    Connection.Open();

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
        /// Removes the quadruples with the given context, subject and predicate
        /// </summary>
        public override RDFStore RemoveQuadruplesByContextSubjectPredicate(RDFContext contextResource, RDFResource subjectResource, RDFResource predicateResource)
        {
            if (contextResource != null && subjectResource != null && predicateResource != null)
            {
                //Create command
                DeleteCommand.CommandText = "DELETE FROM \"" + ConnectionBuilder.UserID + "\".\"QUADRUPLES\" WHERE \"CONTEXTID\" = :CTXID AND \"SUBJECTID\" = :SUBJID AND \"PREDICATEID\" = :PREDID";
                DeleteCommand.Parameters.Clear();
                DeleteCommand.Parameters.Add(new OracleParameter("CTXID", OracleDbType.Int64));
                DeleteCommand.Parameters.Add(new OracleParameter("SUBJID", OracleDbType.Int64));
                DeleteCommand.Parameters.Add(new OracleParameter("PREDID", OracleDbType.Int64));

                //Valorize parameters
                DeleteCommand.Parameters["CTXID"].Value = contextResource.PatternMemberID;
                DeleteCommand.Parameters["SUBJID"].Value = subjectResource.PatternMemberID;
                DeleteCommand.Parameters["PREDID"].Value = predicateResource.PatternMemberID;

                try
                {
                    //Open connection
                    Connection.Open();

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
        /// Removes the quadruples with the given context, subject and object
        /// </summary>
        public override RDFStore RemoveQuadruplesByContextSubjectObject(RDFContext contextResource, RDFResource subjectResource, RDFResource objectResource)
        {
            if (contextResource != null && subjectResource != null && objectResource != null)
            {
                //Create command
                DeleteCommand.CommandText = "DELETE FROM \"" + ConnectionBuilder.UserID + "\".\"QUADRUPLES\" WHERE \"CONTEXTID\" = :CTXID AND \"SUBJECTID\" = :SUBJID AND \"OBJECTID\" = :OBJID AND \"TRIPLEFLAVOR\" = :TFV";
                DeleteCommand.Parameters.Clear();
                DeleteCommand.Parameters.Add(new OracleParameter("CTXID", OracleDbType.Int64));
                DeleteCommand.Parameters.Add(new OracleParameter("SUBJID", OracleDbType.Int64));
                DeleteCommand.Parameters.Add(new OracleParameter("OBJID", OracleDbType.Int64));
                DeleteCommand.Parameters.Add(new OracleParameter("TFV", OracleDbType.Int32));

                //Valorize parameters
                DeleteCommand.Parameters["CTXID"].Value = contextResource.PatternMemberID;
                DeleteCommand.Parameters["SUBJID"].Value = subjectResource.PatternMemberID;
                DeleteCommand.Parameters["OBJID"].Value = objectResource.PatternMemberID;
                DeleteCommand.Parameters["TFV"].Value = RDFModelEnums.RDFTripleFlavors.SPO;

                try
                {
                    //Open connection
                    Connection.Open();

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
        /// Removes the quadruples with the given context, subject and literal
        /// </summary>
        public override RDFStore RemoveQuadruplesByContextSubjectLiteral(RDFContext contextResource, RDFResource subjectResource, RDFLiteral objectLiteral)
        {
            if (contextResource != null && subjectResource != null && objectLiteral != null)
            {
                //Create command
                DeleteCommand.CommandText = "DELETE FROM \"" + ConnectionBuilder.UserID + "\".\"QUADRUPLES\" WHERE \"CONTEXTID\" = :CTXID AND \"SUBJECTID\" = :SUBJID AND \"OBJECTID\" = :OBJID AND \"TRIPLEFLAVOR\" = :TFV";
                DeleteCommand.Parameters.Clear();
                DeleteCommand.Parameters.Add(new OracleParameter("CTXID", OracleDbType.Int64));
                DeleteCommand.Parameters.Add(new OracleParameter("SUBJID", OracleDbType.Int64));
                DeleteCommand.Parameters.Add(new OracleParameter("OBJID", OracleDbType.Int64));
                DeleteCommand.Parameters.Add(new OracleParameter("TFV", OracleDbType.Int32));

                //Valorize parameters
                DeleteCommand.Parameters["CTXID"].Value = contextResource.PatternMemberID;
                DeleteCommand.Parameters["SUBJID"].Value = subjectResource.PatternMemberID;
                DeleteCommand.Parameters["OBJID"].Value = objectLiteral.PatternMemberID;
                DeleteCommand.Parameters["TFV"].Value = RDFModelEnums.RDFTripleFlavors.SPL;

                try
                {
                    //Open connection
                    Connection.Open();

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
        /// Removes the quadruples with the given context, predicate and object
        /// </summary>
        public override RDFStore RemoveQuadruplesByContextPredicateObject(RDFContext contextResource, RDFResource predicateResource, RDFResource objectResource)
        {
            if (contextResource != null && predicateResource != null && objectResource != null)
            {
                //Create command
                DeleteCommand.CommandText = "DELETE FROM \"" + ConnectionBuilder.UserID + "\".\"QUADRUPLES\" WHERE \"CONTEXTID\" = :CTXID AND \"PREDICATEID\" = :PREDID AND \"OBJECTID\" = :OBJID AND \"TRIPLEFLAVOR\" = :TFV";
                DeleteCommand.Parameters.Clear();
                DeleteCommand.Parameters.Add(new OracleParameter("CTXID", OracleDbType.Int64));
                DeleteCommand.Parameters.Add(new OracleParameter("PREDID", OracleDbType.Int64));
                DeleteCommand.Parameters.Add(new OracleParameter("OBJID", OracleDbType.Int64));
                DeleteCommand.Parameters.Add(new OracleParameter("TFV", OracleDbType.Int32));

                //Valorize parameters
                DeleteCommand.Parameters["CTXID"].Value = contextResource.PatternMemberID;
                DeleteCommand.Parameters["PREDID"].Value = predicateResource.PatternMemberID;
                DeleteCommand.Parameters["OBJID"].Value = objectResource.PatternMemberID;
                DeleteCommand.Parameters["TFV"].Value = RDFModelEnums.RDFTripleFlavors.SPO;

                try
                {
                    //Open connection
                    Connection.Open();

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
        /// Removes the quadruples with the given context, predicate and literal
        /// </summary>
        public override RDFStore RemoveQuadruplesByContextPredicateLiteral(RDFContext contextResource, RDFResource predicateResource, RDFLiteral objectLiteral)
        {
            if (contextResource != null && predicateResource != null && objectLiteral != null)
            {
                //Create command
                DeleteCommand.CommandText = "DELETE FROM \"" + ConnectionBuilder.UserID + "\".\"QUADRUPLES\" WHERE \"CONTEXTID\" = :CTXID AND \"PREDICATEID\" = :PREDID AND \"OBJECTID\" = :OBJID AND \"TRIPLEFLAVOR\" = :TFV";
                DeleteCommand.Parameters.Clear();
                DeleteCommand.Parameters.Add(new OracleParameter("CTXID", OracleDbType.Int64));
                DeleteCommand.Parameters.Add(new OracleParameter("PREDID", OracleDbType.Int64));
                DeleteCommand.Parameters.Add(new OracleParameter("OBJID", OracleDbType.Int64));
                DeleteCommand.Parameters.Add(new OracleParameter("TFV", OracleDbType.Int32));

                //Valorize parameters
                DeleteCommand.Parameters["CTXID"].Value = contextResource.PatternMemberID;
                DeleteCommand.Parameters["PREDID"].Value = predicateResource.PatternMemberID;
                DeleteCommand.Parameters["OBJID"].Value = objectLiteral.PatternMemberID;
                DeleteCommand.Parameters["TFV"].Value = RDFModelEnums.RDFTripleFlavors.SPL;

                try
                {
                    //Open connection
                    Connection.Open();

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
        /// Removes the quadruples with the given subject and predicate
        /// </summary>
        public override RDFStore RemoveQuadruplesBySubjectPredicate(RDFResource subjectResource, RDFResource predicateResource)
        {
            if (subjectResource != null && predicateResource != null)
            {
                //Create command
                DeleteCommand.CommandText = "DELETE FROM \"" + ConnectionBuilder.UserID + "\".\"QUADRUPLES\" WHERE \"SUBJECTID\" = :SUBJID AND \"PREDICATEID\" = :PREDID";
                DeleteCommand.Parameters.Clear();
                DeleteCommand.Parameters.Add(new OracleParameter("SUBJID", OracleDbType.Int64));
                DeleteCommand.Parameters.Add(new OracleParameter("PREDID", OracleDbType.Int64));

                //Valorize parameters
                DeleteCommand.Parameters["SUBJID"].Value = subjectResource.PatternMemberID;
                DeleteCommand.Parameters["PREDID"].Value = predicateResource.PatternMemberID;

                try
                {
                    //Open connection
                    Connection.Open();

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
        /// Removes the quadruples with the given subject and object
        /// </summary>
        public override RDFStore RemoveQuadruplesBySubjectObject(RDFResource subjectResource, RDFResource objectResource)
        {
            if (subjectResource != null && objectResource != null)
            {
                //Create command
                DeleteCommand.CommandText = "DELETE FROM \"" + ConnectionBuilder.UserID + "\".\"QUADRUPLES\" WHERE \"SUBJECTID\" = :SUBJID AND \"OBJECTID\" = :OBJID AND \"TRIPLEFLAVOR\" = :TFV";
                DeleteCommand.Parameters.Clear();
                DeleteCommand.Parameters.Add(new OracleParameter("SUBJID", OracleDbType.Int64));
                DeleteCommand.Parameters.Add(new OracleParameter("OBJID", OracleDbType.Int64));
                DeleteCommand.Parameters.Add(new OracleParameter("TFV", OracleDbType.Int32));

                //Valorize parameters
                DeleteCommand.Parameters["SUBJID"].Value = subjectResource.PatternMemberID;
                DeleteCommand.Parameters["OBJID"].Value = objectResource.PatternMemberID;
                DeleteCommand.Parameters["TFV"].Value = RDFModelEnums.RDFTripleFlavors.SPO;

                try
                {
                    //Open connection
                    Connection.Open();

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
        /// Removes the quadruples with the given subject and literal
        /// </summary>
        public override RDFStore RemoveQuadruplesBySubjectLiteral(RDFResource subjectResource, RDFLiteral objectLiteral)
        {
            if (subjectResource != null && objectLiteral != null)
            {
                //Create command
                DeleteCommand.CommandText = "DELETE FROM \"" + ConnectionBuilder.UserID + "\".\"QUADRUPLES\" WHERE \"SUBJECTID\" = :SUBJID AND \"OBJECTID\" = :OBJID AND \"TRIPLEFLAVOR\" = :TFV";
                DeleteCommand.Parameters.Clear();
                DeleteCommand.Parameters.Add(new OracleParameter("SUBJID", OracleDbType.Int64));
                DeleteCommand.Parameters.Add(new OracleParameter("OBJID", OracleDbType.Int64));
                DeleteCommand.Parameters.Add(new OracleParameter("TFV", OracleDbType.Int32));

                //Valorize parameters
                DeleteCommand.Parameters["SUBJID"].Value = subjectResource.PatternMemberID;
                DeleteCommand.Parameters["OBJID"].Value = objectLiteral.PatternMemberID;
                DeleteCommand.Parameters["TFV"].Value = RDFModelEnums.RDFTripleFlavors.SPL;

                try
                {
                    //Open connection
                    Connection.Open();

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
        /// Removes the quadruples with the given predicate and object
        /// </summary>
        public override RDFStore RemoveQuadruplesByPredicateObject(RDFResource predicateResource, RDFResource objectResource)
        {
            if (predicateResource != null && objectResource != null)
            {
                //Create command
                DeleteCommand.CommandText = "DELETE FROM \"" + ConnectionBuilder.UserID + "\".\"QUADRUPLES\" WHERE \"PREDICATEID\" = :PREDID AND \"OBJECTID\" = :OBJID AND \"TRIPLEFLAVOR\" = :TFV";
                DeleteCommand.Parameters.Clear();
                DeleteCommand.Parameters.Add(new OracleParameter("PREDID", OracleDbType.Int64));
                DeleteCommand.Parameters.Add(new OracleParameter("OBJID", OracleDbType.Int64));
                DeleteCommand.Parameters.Add(new OracleParameter("TFV", OracleDbType.Int32));

                //Valorize parameters
                DeleteCommand.Parameters["PREDID"].Value = predicateResource.PatternMemberID;
                DeleteCommand.Parameters["OBJID"].Value = objectResource.PatternMemberID;
                DeleteCommand.Parameters["TFV"].Value = RDFModelEnums.RDFTripleFlavors.SPO;

                try
                {
                    //Open connection
                    Connection.Open();

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
        /// Removes the quadruples with the given predicate and literal
        /// </summary>
        public override RDFStore RemoveQuadruplesByPredicateLiteral(RDFResource predicateResource, RDFLiteral objectLiteral)
        {
            if (predicateResource != null && objectLiteral != null)
            {
                //Create command
                DeleteCommand.CommandText = "DELETE FROM \"" + ConnectionBuilder.UserID + "\".\"QUADRUPLES\" WHERE \"PREDICATEID\" = :PREDID AND \"OBJECTID\" = :OBJID AND \"TRIPLEFLAVOR\" = :TFV";
                DeleteCommand.Parameters.Clear();
                DeleteCommand.Parameters.Add(new OracleParameter("PREDID", OracleDbType.Int64));
                DeleteCommand.Parameters.Add(new OracleParameter("OBJID", OracleDbType.Int64));
                DeleteCommand.Parameters.Add(new OracleParameter("TFV", OracleDbType.Int32));

                //Valorize parameters
                DeleteCommand.Parameters["PREDID"].Value = predicateResource.PatternMemberID;
                DeleteCommand.Parameters["OBJID"].Value = objectLiteral.PatternMemberID;
                DeleteCommand.Parameters["TFV"].Value = RDFModelEnums.RDFTripleFlavors.SPL;

                try
                {
                    //Open connection
                    Connection.Open();

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
        /// Clears the quadruples of the store
        /// </summary>
        public override void ClearQuadruples()
        {
            //Create command
            DeleteCommand.CommandText = "DELETE FROM \"" + ConnectionBuilder.UserID + "\".\"QUADRUPLES\"";
            DeleteCommand.Parameters.Clear();

            try
            {
                //Open connection
                Connection.Open();

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
                DeleteCommand.Transaction.Rollback();

                //Close connection
                Connection.Close();

                //Propagate exception
                throw new RDFStoreException("Cannot delete data from Oracle store because: " + ex.Message, ex);
            }
        }
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
            SelectCommand.CommandText = "SELECT CASE WHEN EXISTS (SELECT 1 FROM \"" + ConnectionBuilder.UserID + "\".\"QUADRUPLES\" WHERE \"QUADRUPLEID\" = :QID) THEN 1 ELSE 0 END AS REC_EXISTS FROM DUAL;";
            SelectCommand.Parameters.Clear();
            SelectCommand.Parameters.Add(new OracleParameter("QID", OracleDbType.Int64));

            //Valorize parameters
            SelectCommand.Parameters["QID"].Value = quadruple.QuadrupleID;

            //Prepare and execute command
            try
            {
                //Open connection
                Connection.Open();

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
        /// Gets a memory store containing quadruples satisfying the given pattern
        /// </summary>
        public override RDFMemoryStore SelectQuadruples(RDFContext ctx, RDFResource subj, RDFResource pred, RDFResource obj, RDFLiteral lit)
        {
            RDFMemoryStore result = new RDFMemoryStore();
            StringBuilder queryFilters = new StringBuilder();

            //Filter by Context
            if (ctx != null)
                queryFilters.Append('C');

            //Filter by Subject
            if (subj != null)
                queryFilters.Append('S');

            //Filter by Predicate
            if (pred != null)
                queryFilters.Append('P');

            //Filter by Object
            if (obj != null)
                queryFilters.Append('O');

            //Filter by Literal
            if (lit != null)
                queryFilters.Append('L');

            //Intersect the filters
            switch (queryFilters.ToString())
            {
                case "C":
                    //C->->->
                    SelectCommand.CommandText = "SELECT \"TRIPLEFLAVOR\", \"CONTEXT\", \"SUBJECT\", \"PREDICATE\", \"OBJECT\" FROM \"" + ConnectionBuilder.UserID + "\".\"QUADRUPLES\" WHERE \"CONTEXTID\" = :CTXID";
                    SelectCommand.Parameters.Clear();
                    SelectCommand.Parameters.Add(new OracleParameter("CTXID", OracleDbType.Int64));
                    SelectCommand.Parameters["CTXID"].Value = ctx.PatternMemberID;
                    break;
                case "S":
                    //->S->->
                    SelectCommand.CommandText = "SELECT \"TRIPLEFLAVOR\", \"CONTEXT\", \"SUBJECT\", \"PREDICATE\", \"OBJECT\" FROM \"" + ConnectionBuilder.UserID + "\".\"QUADRUPLES\" WHERE \"SUBJECTID\" = :SUBJID";
                    SelectCommand.Parameters.Clear();
                    SelectCommand.Parameters.Add(new OracleParameter("SUBJID", OracleDbType.Int64));
                    SelectCommand.Parameters["SUBJID"].Value = subj.PatternMemberID;
                    break;
                case "P":
                    //->->P->
                    SelectCommand.CommandText = "SELECT \"TRIPLEFLAVOR\", \"CONTEXT\", \"SUBJECT\", \"PREDICATE\", \"OBJECT\" FROM \"" + ConnectionBuilder.UserID + "\".\"QUADRUPLES\" WHERE \"PREDICATEID\" = :PREDID";
                    SelectCommand.Parameters.Clear();
                    SelectCommand.Parameters.Add(new OracleParameter("PREDID", OracleDbType.Int64));
                    SelectCommand.Parameters["PREDID"].Value = pred.PatternMemberID;
                    break;
                case "O":
                    //->->->O
                    SelectCommand.CommandText = "SELECT \"TRIPLEFLAVOR\", \"CONTEXT\", \"SUBJECT\", \"PREDICATE\", \"OBJECT\" FROM \"" + ConnectionBuilder.UserID + "\".\"QUADRUPLES\" WHERE \"OBJECTID\" = :OBJID AND \"TRIPLEFLAVOR\" = :TFV";
                    SelectCommand.Parameters.Clear();
                    SelectCommand.Parameters.Add(new OracleParameter("OBJID", OracleDbType.Int64));
                    SelectCommand.Parameters.Add(new OracleParameter("TFV", OracleDbType.Int32));
                    SelectCommand.Parameters["OBJID"].Value = obj.PatternMemberID;
                    SelectCommand.Parameters["TFV"].Value = (int)RDFModelEnums.RDFTripleFlavors.SPO;
                    break;
                case "L":
                    //->->->L
                    SelectCommand.CommandText = "SELECT \"TRIPLEFLAVOR\", \"CONTEXT\", \"SUBJECT\", \"PREDICATE\", \"OBJECT\" FROM \"" + ConnectionBuilder.UserID + "\".\"QUADRUPLES\" WHERE \"OBJECTID\" = :OBJID AND \"TRIPLEFLAVOR\" = :TFV";
                    SelectCommand.Parameters.Clear();
                    SelectCommand.Parameters.Add(new OracleParameter("OBJID", OracleDbType.Int64));
                    SelectCommand.Parameters.Add(new OracleParameter("TFV", OracleDbType.Int32));
                    SelectCommand.Parameters["OBJID"].Value = lit.PatternMemberID;
                    SelectCommand.Parameters["TFV"].Value = (int)RDFModelEnums.RDFTripleFlavors.SPL;
                    break;
                case "CS":
                    //C->S->->
                    SelectCommand.CommandText = "SELECT \"TRIPLEFLAVOR\", \"CONTEXT\", \"SUBJECT\", \"PREDICATE\", \"OBJECT\" FROM \"" + ConnectionBuilder.UserID + "\".\"QUADRUPLES\" WHERE \"CONTEXTID\" = :CTXID AND \"SUBJECTID\" = :SUBJID";
                    SelectCommand.Parameters.Clear();
                    SelectCommand.Parameters.Add(new OracleParameter("CTXID", OracleDbType.Int64));
                    SelectCommand.Parameters.Add(new OracleParameter("SUBJID", OracleDbType.Int64));
                    SelectCommand.Parameters["CTXID"].Value = ctx.PatternMemberID;
                    SelectCommand.Parameters["SUBJID"].Value = subj.PatternMemberID;
                    break;
                case "CP":
                    //C->->P->
                    SelectCommand.CommandText = "SELECT \"TRIPLEFLAVOR\", \"CONTEXT\", \"SUBJECT\", \"PREDICATE\", \"OBJECT\" FROM \"" + ConnectionBuilder.UserID + "\".\"QUADRUPLES\" WHERE \"CONTEXTID\" = :CTXID AND \"PREDICATEID\" = :PREDID";
                    SelectCommand.Parameters.Clear();
                    SelectCommand.Parameters.Add(new OracleParameter("CTXID", OracleDbType.Int64));
                    SelectCommand.Parameters.Add(new OracleParameter("PREDID", OracleDbType.Int64));
                    SelectCommand.Parameters["CTXID"].Value = ctx.PatternMemberID;
                    SelectCommand.Parameters["PREDID"].Value = pred.PatternMemberID;
                    break;
                case "CO":
                    //C->->->O
                    SelectCommand.CommandText = "SELECT \"TRIPLEFLAVOR\", \"CONTEXT\", \"SUBJECT\", \"PREDICATE\", \"OBJECT\" FROM \"" + ConnectionBuilder.UserID + "\".\"QUADRUPLES\" WHERE \"CONTEXTID\" = :CTXID AND \"OBJECTID\" = :OBJID AND \"TRIPLEFLAVOR\" = :TFV";
                    SelectCommand.Parameters.Clear();
                    SelectCommand.Parameters.Add(new OracleParameter("CTXID", OracleDbType.Int64));
                    SelectCommand.Parameters.Add(new OracleParameter("OBJID", OracleDbType.Int64));
                    SelectCommand.Parameters.Add(new OracleParameter("TFV", OracleDbType.Int32));
                    SelectCommand.Parameters["CTXID"].Value = ctx.PatternMemberID;
                    SelectCommand.Parameters["OBJID"].Value = obj.PatternMemberID;
                    SelectCommand.Parameters["TFV"].Value = (int)RDFModelEnums.RDFTripleFlavors.SPO;
                    break;
                case "CL":
                    //C->->->L
                    SelectCommand.CommandText = "SELECT \"TRIPLEFLAVOR\", \"CONTEXT\", \"SUBJECT\", \"PREDICATE\", \"OBJECT\" FROM \"" + ConnectionBuilder.UserID + "\".\"QUADRUPLES\" WHERE \"CONTEXTID\" = :CTXID AND \"OBJECTID\" = :OBJID AND \"TRIPLEFLAVOR\" = :TFV";
                    SelectCommand.Parameters.Clear();
                    SelectCommand.Parameters.Add(new OracleParameter("CTXID", OracleDbType.Int64));
                    SelectCommand.Parameters.Add(new OracleParameter("OBJID", OracleDbType.Int64));
                    SelectCommand.Parameters.Add(new OracleParameter("TFV", OracleDbType.Int32));
                    SelectCommand.Parameters["CTXID"].Value = ctx.PatternMemberID;
                    SelectCommand.Parameters["OBJID"].Value = lit.PatternMemberID;
                    SelectCommand.Parameters["TFV"].Value = (int)RDFModelEnums.RDFTripleFlavors.SPL;
                    break;
                case "CSP":
                    //C->S->P->
                    SelectCommand.CommandText = "SELECT \"TRIPLEFLAVOR\", \"CONTEXT\", \"SUBJECT\", \"PREDICATE\", \"OBJECT\" FROM \"" + ConnectionBuilder.UserID + "\".\"QUADRUPLES\" WHERE \"CONTEXTID\" = :CTXID AND \"SUBJECTID\" = :SUBJID AND \"PREDICATEID\" = :PREDID";
                    SelectCommand.Parameters.Clear();
                    SelectCommand.Parameters.Add(new OracleParameter("CTXID", OracleDbType.Int64));
                    SelectCommand.Parameters.Add(new OracleParameter("SUBJID", OracleDbType.Int64));
                    SelectCommand.Parameters.Add(new OracleParameter("PREDID", OracleDbType.Int64));
                    SelectCommand.Parameters["CTXID"].Value = ctx.PatternMemberID;
                    SelectCommand.Parameters["SUBJID"].Value = subj.PatternMemberID;
                    SelectCommand.Parameters["PREDID"].Value = pred.PatternMemberID;
                    break;
                case "CSO":
                    //C->S->->O
                    SelectCommand.CommandText = "SELECT \"TRIPLEFLAVOR\", \"CONTEXT\", \"SUBJECT\", \"PREDICATE\", \"OBJECT\" FROM \"" + ConnectionBuilder.UserID + "\".\"QUADRUPLES\" WHERE \"CONTEXTID\" = :CTXID AND \"SUBJECTID\" = :SUBJID AND \"OBJECTID\" = :OBJID AND \"TRIPLEFLAVOR\" = :TFV";
                    SelectCommand.Parameters.Clear();
                    SelectCommand.Parameters.Add(new OracleParameter("CTXID", OracleDbType.Int64));
                    SelectCommand.Parameters.Add(new OracleParameter("SUBJID", OracleDbType.Int64));
                    SelectCommand.Parameters.Add(new OracleParameter("OBJID", OracleDbType.Int64));
                    SelectCommand.Parameters.Add(new OracleParameter("TFV", OracleDbType.Int32));
                    SelectCommand.Parameters["CTXID"].Value = ctx.PatternMemberID;
                    SelectCommand.Parameters["SUBJID"].Value = subj.PatternMemberID;
                    SelectCommand.Parameters["OBJID"].Value = obj.PatternMemberID;
                    SelectCommand.Parameters["TFV"].Value = (int)RDFModelEnums.RDFTripleFlavors.SPO;
                    break;
                case "CSL":
                    //C->S->->L
                    SelectCommand.CommandText = "SELECT \"TRIPLEFLAVOR\", \"CONTEXT\", \"SUBJECT\", \"PREDICATE\", \"OBJECT\" FROM \"" + ConnectionBuilder.UserID + "\".\"QUADRUPLES\" WHERE \"CONTEXTID\" = :CTXID AND \"SUBJECTID\" = :SUBJID AND \"OBJECTID\" = :OBJID AND \"TRIPLEFLAVOR\" = :TFV";
                    SelectCommand.Parameters.Clear();
                    SelectCommand.Parameters.Add(new OracleParameter("CTXID", OracleDbType.Int64));
                    SelectCommand.Parameters.Add(new OracleParameter("SUBJID", OracleDbType.Int64));
                    SelectCommand.Parameters.Add(new OracleParameter("OBJID", OracleDbType.Int64));
                    SelectCommand.Parameters.Add(new OracleParameter("TFV", OracleDbType.Int32));
                    SelectCommand.Parameters["CTXID"].Value = ctx.PatternMemberID;
                    SelectCommand.Parameters["SUBJID"].Value = subj.PatternMemberID;
                    SelectCommand.Parameters["OBJID"].Value = lit.PatternMemberID;
                    SelectCommand.Parameters["TFV"].Value = (int)RDFModelEnums.RDFTripleFlavors.SPL;
                    break;
                case "CPO":
                    //C->->P->O
                    SelectCommand.CommandText = "SELECT \"TRIPLEFLAVOR\", \"CONTEXT\", \"SUBJECT\", \"PREDICATE\", \"OBJECT\" FROM \"" + ConnectionBuilder.UserID + "\".\"QUADRUPLES\" WHERE \"CONTEXTID\" = :CTXID AND \"PREDICATEID\" = :PREDID AND \"OBJECTID\" = :OBJID AND \"TRIPLEFLAVOR\" = :TFV";
                    SelectCommand.Parameters.Clear();
                    SelectCommand.Parameters.Add(new OracleParameter("CTXID", OracleDbType.Int64));
                    SelectCommand.Parameters.Add(new OracleParameter("PREDID", OracleDbType.Int64));
                    SelectCommand.Parameters.Add(new OracleParameter("OBJID", OracleDbType.Int64));
                    SelectCommand.Parameters.Add(new OracleParameter("TFV", OracleDbType.Int32));
                    SelectCommand.Parameters["CTXID"].Value = ctx.PatternMemberID;
                    SelectCommand.Parameters["PREDID"].Value = pred.PatternMemberID;
                    SelectCommand.Parameters["OBJID"].Value = obj.PatternMemberID;
                    SelectCommand.Parameters["TFV"].Value = (int)RDFModelEnums.RDFTripleFlavors.SPO;
                    break;
                case "CPL":
                    //C->->P->L
                    SelectCommand.CommandText = "SELECT \"TRIPLEFLAVOR\", \"CONTEXT\", \"SUBJECT\", \"PREDICATE\", \"OBJECT\" FROM \"" + ConnectionBuilder.UserID + "\".\"QUADRUPLES\" WHERE \"CONTEXTID\" = :CTXID AND \"PREDICATEID\" = :PREDID AND \"OBJECTID\" = :OBJID AND \"TRIPLEFLAVOR\" = :TFV";
                    SelectCommand.Parameters.Clear();
                    SelectCommand.Parameters.Add(new OracleParameter("CTXID", OracleDbType.Int64));
                    SelectCommand.Parameters.Add(new OracleParameter("PREDID", OracleDbType.Int64));
                    SelectCommand.Parameters.Add(new OracleParameter("OBJID", OracleDbType.Int64));
                    SelectCommand.Parameters.Add(new OracleParameter("TFV", OracleDbType.Int32));
                    SelectCommand.Parameters["CTXID"].Value = ctx.PatternMemberID;
                    SelectCommand.Parameters["PREDID"].Value = pred.PatternMemberID;
                    SelectCommand.Parameters["OBJID"].Value = lit.PatternMemberID;
                    SelectCommand.Parameters["TFV"].Value = (int)RDFModelEnums.RDFTripleFlavors.SPL;
                    break;
                case "CSPO":
                    //C->S->P->O
                    SelectCommand.CommandText = "SELECT \"TRIPLEFLAVOR\", \"CONTEXT\", \"SUBJECT\", \"PREDICATE\", \"OBJECT\" FROM \"" + ConnectionBuilder.UserID + "\".\"QUADRUPLES\" WHERE \"CONTEXTID\" = :CTXID AND \"SUBJECTID\" = :SUBJID AND \"PREDICATEID\" = :PREDID AND \"OBJECTID\" = :OBJID AND \"TRIPLEFLAVOR\" = :TFV";
                    SelectCommand.Parameters.Clear();
                    SelectCommand.Parameters.Add(new OracleParameter("CTXID", OracleDbType.Int64));
                    SelectCommand.Parameters.Add(new OracleParameter("SUBJID", OracleDbType.Int64));
                    SelectCommand.Parameters.Add(new OracleParameter("PREDID", OracleDbType.Int64));
                    SelectCommand.Parameters.Add(new OracleParameter("OBJID", OracleDbType.Int64));
                    SelectCommand.Parameters.Add(new OracleParameter("TFV", OracleDbType.Int32));
                    SelectCommand.Parameters["CTXID"].Value = ctx.PatternMemberID;
                    SelectCommand.Parameters["SUBJID"].Value = subj.PatternMemberID;
                    SelectCommand.Parameters["PREDID"].Value = pred.PatternMemberID;
                    SelectCommand.Parameters["OBJID"].Value = obj.PatternMemberID;
                    SelectCommand.Parameters["TFV"].Value = (int)RDFModelEnums.RDFTripleFlavors.SPO;
                    break;
                case "CSPL":
                    //C->S->P->L
                    SelectCommand.CommandText = "SELECT \"TRIPLEFLAVOR\", \"CONTEXT\", \"SUBJECT\", \"PREDICATE\", \"OBJECT\" FROM \"" + ConnectionBuilder.UserID + "\".\"QUADRUPLES\" WHERE \"CONTEXTID\" = :CTXID AND \"SUBJECTID\" = :SUBJID AND \"PREDICATEID\" = :PREDID AND \"OBJECTID\" = :OBJID AND \"TRIPLEFLAVOR\" = :TFV";
                    SelectCommand.Parameters.Clear();
                    SelectCommand.Parameters.Add(new OracleParameter("CTXID", OracleDbType.Int64));
                    SelectCommand.Parameters.Add(new OracleParameter("SUBJID", OracleDbType.Int64));
                    SelectCommand.Parameters.Add(new OracleParameter("PREDID", OracleDbType.Int64));
                    SelectCommand.Parameters.Add(new OracleParameter("OBJID", OracleDbType.Int64));
                    SelectCommand.Parameters.Add(new OracleParameter("TFV", OracleDbType.Int32));
                    SelectCommand.Parameters["CTXID"].Value = ctx.PatternMemberID;
                    SelectCommand.Parameters["SUBJID"].Value = subj.PatternMemberID;
                    SelectCommand.Parameters["PREDID"].Value = pred.PatternMemberID;
                    SelectCommand.Parameters["OBJID"].Value = lit.PatternMemberID;
                    SelectCommand.Parameters["TFV"].Value = (int)RDFModelEnums.RDFTripleFlavors.SPL;
                    break;
                case "SP":
                    //->S->P->
                    SelectCommand.CommandText = "SELECT \"TRIPLEFLAVOR\", \"CONTEXT\", \"SUBJECT\", \"PREDICATE\", \"OBJECT\" FROM \"" + ConnectionBuilder.UserID + "\".\"QUADRUPLES\" WHERE \"SUBJECTID\" = :SUBJID AND \"PREDICATEID\" = :PREDID";
                    SelectCommand.Parameters.Clear();
                    SelectCommand.Parameters.Add(new OracleParameter("SUBJID", OracleDbType.Int64));
                    SelectCommand.Parameters.Add(new OracleParameter("PREDID", OracleDbType.Int64));
                    SelectCommand.Parameters["SUBJID"].Value = subj.PatternMemberID;
                    SelectCommand.Parameters["PREDID"].Value = pred.PatternMemberID;
                    break;
                case "SO":
                    //->S->->O
                    SelectCommand.CommandText = "SELECT \"TRIPLEFLAVOR\", \"CONTEXT\", \"SUBJECT\", \"PREDICATE\", \"OBJECT\" FROM \"" + ConnectionBuilder.UserID + "\".\"QUADRUPLES\" WHERE \"SUBJECTID\" = :SUBJID AND \"OBJECTID\" = :OBJID AND \"TRIPLEFLAVOR\" = :TFV";
                    SelectCommand.Parameters.Clear();
                    SelectCommand.Parameters.Add(new OracleParameter("SUBJID", OracleDbType.Int64));
                    SelectCommand.Parameters.Add(new OracleParameter("OBJID", OracleDbType.Int64));
                    SelectCommand.Parameters.Add(new OracleParameter("TFV", OracleDbType.Int32));
                    SelectCommand.Parameters["SUBJID"].Value = subj.PatternMemberID;
                    SelectCommand.Parameters["OBJID"].Value = obj.PatternMemberID;
                    SelectCommand.Parameters["TFV"].Value = (int)RDFModelEnums.RDFTripleFlavors.SPO;
                    break;
                case "SL":
                    //->S->->L
                    SelectCommand.CommandText = "SELECT \"TRIPLEFLAVOR\", \"CONTEXT\", \"SUBJECT\", \"PREDICATE\", \"OBJECT\" FROM \"" + ConnectionBuilder.UserID + "\".\"QUADRUPLES\" WHERE \"SUBJECTID\" = :SUBJID AND \"OBJECTID\" = :OBJID AND \"TRIPLEFLAVOR\" = :TFV";
                    SelectCommand.Parameters.Clear();
                    SelectCommand.Parameters.Add(new OracleParameter("SUBJID", OracleDbType.Int64));
                    SelectCommand.Parameters.Add(new OracleParameter("OBJID", OracleDbType.Int64));
                    SelectCommand.Parameters.Add(new OracleParameter("TFV", OracleDbType.Int32));
                    SelectCommand.Parameters["SUBJID"].Value = subj.PatternMemberID;
                    SelectCommand.Parameters["OBJID"].Value = lit.PatternMemberID;
                    SelectCommand.Parameters["TFV"].Value = (int)RDFModelEnums.RDFTripleFlavors.SPL;
                    break;
                case "PO":
                    //->->P->O
                    SelectCommand.CommandText = "SELECT \"TRIPLEFLAVOR\", \"CONTEXT\", \"SUBJECT\", \"PREDICATE\", \"OBJECT\" FROM \"" + ConnectionBuilder.UserID + "\".\"QUADRUPLES\" WHERE \"PREDICATEID\" = :PREDID AND \"OBJECTID\" = :OBJID AND \"TRIPLEFLAVOR\" = :TFV";
                    SelectCommand.Parameters.Clear();
                    SelectCommand.Parameters.Add(new OracleParameter("PREDID", OracleDbType.Int64));
                    SelectCommand.Parameters.Add(new OracleParameter("OBJID", OracleDbType.Int64));
                    SelectCommand.Parameters.Add(new OracleParameter("TFV", OracleDbType.Int32));
                    SelectCommand.Parameters["PREDID"].Value = pred.PatternMemberID;
                    SelectCommand.Parameters["OBJID"].Value = obj.PatternMemberID;
                    SelectCommand.Parameters["TFV"].Value = (int)RDFModelEnums.RDFTripleFlavors.SPO;
                    break;
                case "PL":
                    //->->P->L
                    SelectCommand.CommandText = "SELECT \"TRIPLEFLAVOR\", \"CONTEXT\", \"SUBJECT\", \"PREDICATE\", \"OBJECT\" FROM \"" + ConnectionBuilder.UserID + "\".\"QUADRUPLES\" WHERE \"PREDICATEID\" = :PREDID AND \"OBJECTID\" = :OBJID AND \"TRIPLEFLAVOR\" = :TFV";
                    SelectCommand.Parameters.Clear();
                    SelectCommand.Parameters.Add(new OracleParameter("PREDID", OracleDbType.Int64));
                    SelectCommand.Parameters.Add(new OracleParameter("OBJID", OracleDbType.Int64));
                    SelectCommand.Parameters.Add(new OracleParameter("TFV", OracleDbType.Int32));
                    SelectCommand.Parameters["PREDID"].Value = pred.PatternMemberID;
                    SelectCommand.Parameters["OBJID"].Value = lit.PatternMemberID;
                    SelectCommand.Parameters["TFV"].Value = (int)RDFModelEnums.RDFTripleFlavors.SPL;
                    break;
                case "SPO":
                    //->S->P->O
                    SelectCommand.CommandText = "SELECT \"TRIPLEFLAVOR\", \"CONTEXT\", \"SUBJECT\", \"PREDICATE\", \"OBJECT\" FROM \"" + ConnectionBuilder.UserID + "\".\"QUADRUPLES\" WHERE \"SUBJECTID\" = :SUBJID AND \"PREDICATEID\" = :PREDID AND \"OBJECTID\" = :OBJID AND \"TRIPLEFLAVOR\" = :TFV";
                    SelectCommand.Parameters.Clear();
                    SelectCommand.Parameters.Add(new OracleParameter("SUBJID", OracleDbType.Int64));
                    SelectCommand.Parameters.Add(new OracleParameter("PREDID", OracleDbType.Int64));
                    SelectCommand.Parameters.Add(new OracleParameter("OBJID", OracleDbType.Int64));
                    SelectCommand.Parameters.Add(new OracleParameter("TFV", OracleDbType.Int32));
                    SelectCommand.Parameters["SUBJID"].Value = subj.PatternMemberID;
                    SelectCommand.Parameters["PREDID"].Value = pred.PatternMemberID;
                    SelectCommand.Parameters["OBJID"].Value = obj.PatternMemberID;
                    SelectCommand.Parameters["TFV"].Value = (int)RDFModelEnums.RDFTripleFlavors.SPO;
                    break;
                case "SPL":
                    //->S->P->L
                    SelectCommand.CommandText = "SELECT \"TRIPLEFLAVOR\", \"CONTEXT\", \"SUBJECT\", \"PREDICATE\", \"OBJECT\" FROM \"" + ConnectionBuilder.UserID + "\".\"QUADRUPLES\" WHERE \"SUBJECTID\" = :SUBJID AND \"PREDICATEID\" = :PREDID AND \"OBJECTID\" = :OBJID AND \"TRIPLEFLAVOR\" = :TFV";
                    SelectCommand.Parameters.Clear();
                    SelectCommand.Parameters.Add(new OracleParameter("SUBJID", OracleDbType.Int64));
                    SelectCommand.Parameters.Add(new OracleParameter("PREDID", OracleDbType.Int64));
                    SelectCommand.Parameters.Add(new OracleParameter("OBJID", OracleDbType.Int64));
                    SelectCommand.Parameters.Add(new OracleParameter("TFV", OracleDbType.Int32));
                    SelectCommand.Parameters["SUBJID"].Value = subj.PatternMemberID;
                    SelectCommand.Parameters["PREDID"].Value = pred.PatternMemberID;
                    SelectCommand.Parameters["OBJID"].Value = lit.PatternMemberID;
                    SelectCommand.Parameters["TFV"].Value = (int)RDFModelEnums.RDFTripleFlavors.SPL;
                    break;
                default:
                    //->->->
                    SelectCommand.CommandText = "SELECT \"TRIPLEFLAVOR\", \"CONTEXT\", \"SUBJECT\", \"PREDICATE\", \"OBJECT\" FROM \"" + ConnectionBuilder.UserID + "\".\"QUADRUPLES\"";
                    SelectCommand.Parameters.Clear();
                    break;
            }

            //Prepare and execute command
            try
            {
                //Open connection
                Connection.Open();

                //Prepare command
                SelectCommand.Prepare();

                //Execute command
                using (OracleDataReader quadruples = SelectCommand.ExecuteReader())
                {
                    while (quadruples.Read())
                        result.AddQuadruple(RDFStoreUtilities.ParseQuadruple(quadruples));
                }

                //Close connection
                Connection.Close();
            }
            catch (Exception ex)
            {
                //Close connection
                Connection.Close();

                //Propagate exception
                throw new RDFStoreException("Cannot read data from Oracle store because: " + ex.Message, ex);
            }

            return result;
        }
        
        /// <summary>
        /// Counts the Oracle database quadruples
        /// </summary>
        private long GetQuadruplesCount()
        {
            try
            {
                //Open connection
                Connection.Open();

                //Create command
                SelectCommand.CommandText = "SELECT COUNT(*) FROM \"" + ConnectionBuilder.UserID + "\".\"QUADRUPLES\"";
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

		/// <summary>
        /// Asynchronously counts the Oracle database quadruples
        /// </summary>
        private Task<long> GetQuadruplesCountAsync()
			=> Task.Run(() => GetQuadruplesCount()); //Just a wrapper because Oracle doesn't provide ExecuteScalarAsync override
        #endregion

        #region Diagnostics
        /// <summary>
        /// Performs the preliminary diagnostics controls on the underlying Oracle database
        /// </summary>
        private RDFStoreEnums.RDFStoreSQLErrors Diagnostics()
        {
            try
            {
                //Open connection
                Connection.Open();

                //Create command
                SelectCommand.CommandText = "SELECT COUNT(*) FROM ALL_OBJECTS WHERE OBJECT_TYPE = 'TABLE' AND OBJECT_NAME = 'QUADRUPLES'";
                SelectCommand.Parameters.Clear();

                //Execute command
                int result = int.Parse(SelectCommand.ExecuteScalar().ToString());

                //Close connection
                Connection.Close();

                //Return the diagnostics state
                return result == 0 ? RDFStoreEnums.RDFStoreSQLErrors.QuadruplesTableNotFound
                                   : RDFStoreEnums.RDFStoreSQLErrors.NoErrors;
            }
            catch
            {
                //Close connection
                Connection.Close();

                //Return the diagnostics state
                return RDFStoreEnums.RDFStoreSQLErrors.InvalidDataSource;
            }
        }

        /// <summary>
        /// Initializes the underlying Oracle database
        /// </summary>
        private void InitializeStore()
        {
            RDFStoreEnums.RDFStoreSQLErrors check = Diagnostics();

            //Prepare the database if diagnostics has not found the "Quadruples" table
            if (check == RDFStoreEnums.RDFStoreSQLErrors.QuadruplesTableNotFound)
            {
                try
                {
                    //Open connection
                    Connection.Open();

                    //Create & Execute command
                    OracleCommand createCommand = new OracleCommand("CREATE TABLE \"" + ConnectionBuilder.UserID + "\".\"QUADRUPLES\"(\"QUADRUPLEID\" NUMBER(19, 0) NOT NULL ENABLE,\"TRIPLEFLAVOR\" NUMBER(10, 0) NOT NULL ENABLE,\"CONTEXTID\" NUMBER(19, 0) NOT NULL ENABLE,\"CONTEXT\" VARCHAR2(1000) NOT NULL ENABLE,\"SUBJECTID\" NUMBER(19, 0) NOT NULL ENABLE,\"SUBJECT\" VARCHAR2(1000) NOT NULL ENABLE,\"PREDICATEID\" NUMBER(19, 0) NOT NULL ENABLE,\"PREDICATE\" VARCHAR2(1000) NOT NULL ENABLE,\"OBJECTID\" NUMBER(19, 0) NOT NULL ENABLE,\"OBJECT\" VARCHAR2(1000) NOT NULL ENABLE,PRIMARY KEY(\"QUADRUPLEID\") ENABLE)", Connection) { CommandTimeout = 120 };
                    createCommand.ExecuteNonQuery();
                    createCommand.CommandText = "CREATE INDEX \"" + ConnectionBuilder.UserID + "\".\"IDX_CONTEXTID\" ON \"QUADRUPLES\"(\"CONTEXTID\")";
                    createCommand.ExecuteNonQuery();
                    createCommand.CommandText = "CREATE INDEX \"" + ConnectionBuilder.UserID + "\".\"IDX_SUBJECTID\" ON \"QUADRUPLES\"(\"SUBJECTID\")";
                    createCommand.ExecuteNonQuery();
                    createCommand.CommandText = "CREATE INDEX \"" + ConnectionBuilder.UserID + "\".\"IDX_PREDICATEID\" ON \"QUADRUPLES\"(\"PREDICATEID\")";
                    createCommand.ExecuteNonQuery();
                    createCommand.CommandText = "CREATE INDEX \"" + ConnectionBuilder.UserID + "\".\"IDX_OBJECTID\" ON \"QUADRUPLES\"(\"OBJECTID\",\"TRIPLEFLAVOR\")";
                    createCommand.ExecuteNonQuery();
                    createCommand.CommandText = "CREATE INDEX \"" + ConnectionBuilder.UserID + "\".\"IDX_SUBJECTID_PREDICATEID\" ON \"QUADRUPLES\"(\"SUBJECTID\",\"PREDICATEID\")";
                    createCommand.ExecuteNonQuery();
                    createCommand.CommandText = "CREATE INDEX \"" + ConnectionBuilder.UserID + "\".\"IDX_SUBJECTID_OBJECTID\" ON \"QUADRUPLES\"(\"SUBJECTID\",\"OBJECTID\",\"TRIPLEFLAVOR\")";
                    createCommand.ExecuteNonQuery();
                    createCommand.CommandText = "CREATE INDEX \"" + ConnectionBuilder.UserID + "\".\"IDX_PREDICATEID_OBJECTID\" ON \"QUADRUPLES\"(\"PREDICATEID\",\"OBJECTID\",\"TRIPLEFLAVOR\")";
                    createCommand.ExecuteNonQuery();

                    //Close connection
                    Connection.Close();
                }
                catch (Exception ex)
                {
                    //Close connection
                    Connection.Close();

                    //Propagate exception
                    throw new RDFStoreException("Cannot prepare Oracle store because: " + ex.Message, ex);
                }
            }

            //Otherwise, an exception must be thrown because it has not been possible to connect to the instance/database
            else if (check == RDFStoreEnums.RDFStoreSQLErrors.InvalidDataSource)
                throw new RDFStoreException("Cannot prepare Oracle store because: unable to connect to the server instance or to open the selected database.");
        }
        #endregion

        #region Optimize
        /// <summary>
        /// Executes a special command to optimize Oracle store
        /// </summary>
        public void OptimizeStore()
        {
            try
            {
                //Open connection
                Connection.Open();

                //Create & Execute command
                OracleCommand optimizeCommand = new OracleCommand("ALTER INDEX \"" + ConnectionBuilder.UserID + "\".\"IDX_CONTEXTID\" REBUILD", Connection) { CommandTimeout = 120 };
                optimizeCommand.ExecuteNonQuery();
                optimizeCommand.CommandText = "ALTER INDEX \"" + ConnectionBuilder.UserID + "\".\"IDX_SUBJECTID\" REBUILD";
                optimizeCommand.ExecuteNonQuery();
                optimizeCommand.CommandText = "ALTER INDEX \"" + ConnectionBuilder.UserID + "\".\"IDX_PREDICATEID\" REBUILD";
                optimizeCommand.ExecuteNonQuery();
                optimizeCommand.CommandText = "ALTER INDEX \"" + ConnectionBuilder.UserID + "\".\"IDX_OBJECTID\" REBUILD";
                optimizeCommand.ExecuteNonQuery();
                optimizeCommand.CommandText = "ALTER INDEX \"" + ConnectionBuilder.UserID + "\".\"IDX_SUBJECTID_PREDICATEID\" REBUILD";
                optimizeCommand.ExecuteNonQuery();
                optimizeCommand.CommandText = "ALTER INDEX \"" + ConnectionBuilder.UserID + "\".\"IDX_SUBJECTID_OBJECTID\" REBUILD";
                optimizeCommand.ExecuteNonQuery();
                optimizeCommand.CommandText = "ALTER INDEX \"" + ConnectionBuilder.UserID + "\".\"IDX_PREDICATEID_OBJECTID\" REBUILD";
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

        #endregion
    }

    /// <summary>
    /// RDFOracleStoreOptions is a collector of options for customizing the default behaviour of an Oracle store
    /// </summary>
    public class RDFOracleStoreOptions
    {
        #region Properties
        /// <summary>
        /// Timeout in seconds for SELECT queries executed on the Oracle store (default: 120)
        /// </summary>
        public int SelectTimeout { get; set; } = 120;

        /// <summary>
        /// Timeout in seconds for DELETE queries executed on the Oracle store (default: 120)
        /// </summary>
        public int DeleteTimeout { get; set; } = 120;

        /// <summary>
        /// Timeout in seconds for INSERT queries executed on the Oracle store (default: 120)
        /// </summary>
        public int InsertTimeout { get; set; } = 120;
        #endregion
    }
}