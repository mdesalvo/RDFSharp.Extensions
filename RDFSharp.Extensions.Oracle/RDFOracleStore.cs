﻿/*
   Copyright 2012-2022 Marco De Salvo

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
        /// Connection to the Oracle database
        /// </summary>
        internal OracleConnection Connection { get; set; }

        /// <summary>
        /// Utility for getting fields of the connection
        /// </summary>
        internal OracleConnectionStringBuilder ConnectionBuilder { get; set; }
        
        /// <summary>
        /// Flag indicating that the Oracle store instance has already been disposed
        /// </summary>
        internal bool Disposed { get; set; }
        #endregion

        #region Ctors
        /// <summary>
        /// Default-ctor to build an Oracle store instance
        /// </summary>
        public RDFOracleStore(string oracleConnectionString)
        {
            if (string.IsNullOrEmpty(oracleConnectionString))
            	throw new RDFStoreException("Cannot connect to Oracle store because: given \"oracleConnectionString\" parameter is null or empty.");
            
            //Initialize store structures
            StoreType = "ORACLE";
            Connection = new OracleConnection(oracleConnectionString);
            ConnectionBuilder = new OracleConnectionStringBuilder(Connection.ConnectionString);
            StoreID = RDFModelUtilities.CreateHash(ToString());
            Disposed = false;

            //Perform initial diagnostics
            PrepareStore();
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
                Connection?.Dispose();
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
                OracleCommand command = new OracleCommand("INSERT INTO \"" + ConnectionBuilder.UserID + "\".\"QUADRUPLES\"(\"QUADRUPLEID\", \"TRIPLEFLAVOR\", \"CONTEXT\", \"CONTEXTID\", \"SUBJECT\", \"SUBJECTID\", \"PREDICATE\", \"PREDICATEID\", \"OBJECT\", \"OBJECTID\") SELECT :QID, :TFV, :CTX, :CTXID, :SUBJ, :SUBJID, :PRED, :PREDID, :OBJ, :OBJID FROM DUAL WHERE NOT EXISTS(SELECT \"QUADRUPLEID\" FROM \"" + ConnectionBuilder.UserID + "\".\"QUADRUPLES\" WHERE \"QUADRUPLEID\" = :QID)", Connection);
                command.Parameters.Add(new OracleParameter("QID", OracleDbType.Int64));
                command.Parameters.Add(new OracleParameter("TFV", OracleDbType.Int32));
                command.Parameters.Add(new OracleParameter("CTX", OracleDbType.Varchar2, 1000));
                command.Parameters.Add(new OracleParameter("CTXID", OracleDbType.Int64));
                command.Parameters.Add(new OracleParameter("SUBJ", OracleDbType.Varchar2, 1000));
                command.Parameters.Add(new OracleParameter("SUBJID", OracleDbType.Int64));
                command.Parameters.Add(new OracleParameter("PRED", OracleDbType.Varchar2, 1000));
                command.Parameters.Add(new OracleParameter("PREDID", OracleDbType.Int64));
                command.Parameters.Add(new OracleParameter("OBJ", OracleDbType.Varchar2, 1000));
                command.Parameters.Add(new OracleParameter("OBJID", OracleDbType.Int64));

                try
                {
                    //Open connection
                    Connection.Open();

                    //Prepare command
                    command.Prepare();

                    //Open transaction
                    command.Transaction = Connection.BeginTransaction();

                    //Iterate triples
                    foreach (var triple in graph)
                    {
                        //Valorize parameters
                        command.Parameters["QID"].Value = RDFModelUtilities.CreateHash(string.Concat(graphCtx, " ", triple.Subject, " ", triple.Predicate, " ", triple.Object));
                        command.Parameters["TFV"].Value = (int)triple.TripleFlavor;
                        command.Parameters["CTX"].Value = graphCtx.ToString();
                        command.Parameters["CTXID"].Value = graphCtx.PatternMemberID;
                        command.Parameters["SUBJ"].Value = triple.Subject.ToString();
                        command.Parameters["SUBJID"].Value = triple.Subject.PatternMemberID;
                        command.Parameters["PRED"].Value = triple.Predicate.ToString();
                        command.Parameters["PREDID"].Value = triple.Predicate.PatternMemberID;
                        command.Parameters["OBJ"].Value = triple.Object.ToString();
                        command.Parameters["OBJID"].Value = triple.Object.PatternMemberID;

                        //Execute command
                        command.ExecuteNonQuery();
                    }

                    //Close transaction
                    command.Transaction.Commit();

                    //Close connection
                    Connection.Close();
                }
                catch (Exception ex)
                {
                    //Rollback transaction
                    command.Transaction.Rollback();

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
                OracleCommand command = new OracleCommand("INSERT INTO \"" + ConnectionBuilder.UserID + "\".\"QUADRUPLES\"(\"QUADRUPLEID\", \"TRIPLEFLAVOR\", \"CONTEXT\", \"CONTEXTID\", \"SUBJECT\", \"SUBJECTID\", \"PREDICATE\", \"PREDICATEID\", \"OBJECT\", \"OBJECTID\") SELECT :QID, :TFV, :CTX, :CTXID, :SUBJ, :SUBJID, :PRED, :PREDID, :OBJ, :OBJID FROM DUAL WHERE NOT EXISTS(SELECT \"QUADRUPLEID\" FROM \"" + ConnectionBuilder.UserID + "\".\"QUADRUPLES\" WHERE \"QUADRUPLEID\" = :QID)", Connection);
                command.Parameters.Add(new OracleParameter("QID", OracleDbType.Int64));
                command.Parameters.Add(new OracleParameter("TFV", OracleDbType.Int32));
                command.Parameters.Add(new OracleParameter("CTX", OracleDbType.Varchar2, 1000));
                command.Parameters.Add(new OracleParameter("CTXID", OracleDbType.Int64));
                command.Parameters.Add(new OracleParameter("SUBJ", OracleDbType.Varchar2, 1000));
                command.Parameters.Add(new OracleParameter("SUBJID", OracleDbType.Int64));
                command.Parameters.Add(new OracleParameter("PRED", OracleDbType.Varchar2, 1000));
                command.Parameters.Add(new OracleParameter("PREDID", OracleDbType.Int64));
                command.Parameters.Add(new OracleParameter("OBJ", OracleDbType.Varchar2, 1000));
                command.Parameters.Add(new OracleParameter("OBJID", OracleDbType.Int64));

                //Valorize parameters
                command.Parameters["QID"].Value = quadruple.QuadrupleID;
                command.Parameters["TFV"].Value = (int)quadruple.TripleFlavor;
                command.Parameters["CTX"].Value = quadruple.Context.ToString();
                command.Parameters["CTXID"].Value = quadruple.Context.PatternMemberID;
                command.Parameters["SUBJ"].Value = quadruple.Subject.ToString();
                command.Parameters["SUBJID"].Value = quadruple.Subject.PatternMemberID;
                command.Parameters["PRED"].Value = quadruple.Predicate.ToString();
                command.Parameters["PREDID"].Value = quadruple.Predicate.PatternMemberID;
                command.Parameters["OBJ"].Value = quadruple.Object.ToString();
                command.Parameters["OBJID"].Value = quadruple.Object.PatternMemberID;

                try
                {
                    //Open connection
                    Connection.Open();

                    //Prepare command
                    command.Prepare();

                    //Open transaction
                    command.Transaction = Connection.BeginTransaction();

                    //Execute command
                    command.ExecuteNonQuery();

                    //Close transaction
                    command.Transaction.Commit();

                    //Close connection
                    Connection.Close();
                }
                catch (Exception ex)
                {
                    //Rollback transaction
                    command.Transaction.Rollback();

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
                OracleCommand command = new OracleCommand("DELETE FROM \"" + ConnectionBuilder.UserID + "\".\"QUADRUPLES\" WHERE \"QUADRUPLEID\" = :QID", Connection);
                command.Parameters.Add(new OracleParameter("QID", OracleDbType.Int64));

                //Valorize parameters
                command.Parameters["QID"].Value = quadruple.QuadrupleID;

                try
                {
                    //Open connection
                    Connection.Open();

                    //Prepare command
                    command.Prepare();

                    //Open transaction
                    command.Transaction = Connection.BeginTransaction();

                    //Execute command
                    command.ExecuteNonQuery();

                    //Close transaction
                    command.Transaction.Commit();

                    //Close connection
                    Connection.Close();
                }
                catch (Exception ex)
                {
                    //Rollback transaction
                    command.Transaction.Rollback();

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
                OracleCommand command = new OracleCommand("DELETE FROM \"" + ConnectionBuilder.UserID + "\".\"QUADRUPLES\" WHERE \"CONTEXTID\" = :CTXID", Connection);
                command.Parameters.Add(new OracleParameter("CTXID", OracleDbType.Int64));

                //Valorize parameters
                command.Parameters["CTXID"].Value = contextResource.PatternMemberID;

                try
                {
                    //Open connection
                    Connection.Open();

                    //Prepare command
                    command.Prepare();

                    //Open transaction
                    command.Transaction = Connection.BeginTransaction();

                    //Execute command
                    command.ExecuteNonQuery();

                    //Close transaction
                    command.Transaction.Commit();

                    //Close connection
                    Connection.Close();
                }
                catch (Exception ex)
                {
                    //Rollback transaction
                    command.Transaction.Rollback();

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
                OracleCommand command = new OracleCommand("DELETE FROM \"" + ConnectionBuilder.UserID + "\".\"QUADRUPLES\" WHERE \"SUBJECTID\" = :SUBJID", Connection);
                command.Parameters.Add(new OracleParameter("SUBJID", OracleDbType.Int64));

                //Valorize parameters
                command.Parameters["SUBJID"].Value = subjectResource.PatternMemberID;

                try
                {
                    //Open connection
                    Connection.Open();

                    //Prepare command
                    command.Prepare();

                    //Open transaction
                    command.Transaction = Connection.BeginTransaction();

                    //Execute command
                    command.ExecuteNonQuery();

                    //Close transaction
                    command.Transaction.Commit();

                    //Close connection
                    Connection.Close();
                }
                catch (Exception ex)
                {
                    //Rollback transaction
                    command.Transaction.Rollback();

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
                OracleCommand command = new OracleCommand("DELETE FROM \"" + ConnectionBuilder.UserID + "\".\"QUADRUPLES\" WHERE \"PREDICATEID\" = :PREDID", Connection);
                command.Parameters.Add(new OracleParameter("PREDID", OracleDbType.Int64));

                //Valorize parameters
                command.Parameters["PREDID"].Value = predicateResource.PatternMemberID;

                try
                {
                    //Open connection
                    Connection.Open();

                    //Prepare command
                    command.Prepare();

                    //Open transaction
                    command.Transaction = Connection.BeginTransaction();

                    //Execute command
                    command.ExecuteNonQuery();

                    //Close transaction
                    command.Transaction.Commit();

                    //Close connection
                    Connection.Close();
                }
                catch (Exception ex)
                {
                    //Rollback transaction
                    command.Transaction.Rollback();

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
                OracleCommand command = new OracleCommand("DELETE FROM \"" + ConnectionBuilder.UserID + "\".\"QUADRUPLES\" WHERE \"OBJECTID\" = :OBJID AND \"TRIPLEFLAVOR\" = :TFV", Connection);
                command.Parameters.Add(new OracleParameter("OBJID", OracleDbType.Int64));
                command.Parameters.Add(new OracleParameter("TFV", OracleDbType.Int32));

                //Valorize parameters
                command.Parameters["OBJID"].Value = objectResource.PatternMemberID;
                command.Parameters["TFV"].Value = (int)RDFModelEnums.RDFTripleFlavors.SPO;

                try
                {
                    //Open connection
                    Connection.Open();

                    //Prepare command
                    command.Prepare();

                    //Open transaction
                    command.Transaction = Connection.BeginTransaction();

                    //Execute command
                    command.ExecuteNonQuery();

                    //Close transaction
                    command.Transaction.Commit();

                    //Close connection
                    Connection.Close();
                }
                catch (Exception ex)
                {
                    //Rollback transaction
                    command.Transaction.Rollback();

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
                OracleCommand command = new OracleCommand("DELETE FROM \"" + ConnectionBuilder.UserID + "\".\"QUADRUPLES\" WHERE \"OBJECTID\" = :OBJID AND \"TRIPLEFLAVOR\" = :TFV", Connection);
                command.Parameters.Add(new OracleParameter("OBJID", OracleDbType.Int64));
                command.Parameters.Add(new OracleParameter("TFV", OracleDbType.Int32));

                //Valorize parameters
                command.Parameters["OBJID"].Value = literalObject.PatternMemberID;
                command.Parameters["TFV"].Value = (int)RDFModelEnums.RDFTripleFlavors.SPL;

                try
                {
                    //Open connection
                    Connection.Open();

                    //Prepare command
                    command.Prepare();

                    //Open transaction
                    command.Transaction = Connection.BeginTransaction();

                    //Execute command
                    command.ExecuteNonQuery();

                    //Close transaction
                    command.Transaction.Commit();

                    //Close connection
                    Connection.Close();
                }
                catch (Exception ex)
                {
                    //Rollback transaction
                    command.Transaction.Rollback();

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
                OracleCommand command = new OracleCommand("DELETE FROM \"" + ConnectionBuilder.UserID + "\".\"QUADRUPLES\" WHERE \"CONTEXTID\" = :CTXID AND \"SUBJECTID\" = :SUBJID", Connection);
                command.Parameters.Add(new OracleParameter("CTXID", OracleDbType.Int64));
                command.Parameters.Add(new OracleParameter("SUBJID", OracleDbType.Int64));

                //Valorize parameters
                command.Parameters["CTXID"].Value = contextResource.PatternMemberID;
                command.Parameters["SUBJID"].Value = subjectResource.PatternMemberID;

                try
                {
                    //Open connection
                    Connection.Open();

                    //Prepare command
                    command.Prepare();

                    //Open transaction
                    command.Transaction = Connection.BeginTransaction();

                    //Execute command
                    command.ExecuteNonQuery();

                    //Close transaction
                    command.Transaction.Commit();

                    //Close connection
                    Connection.Close();
                }
                catch (Exception ex)
                {
                    //Rollback transaction
                    command.Transaction.Rollback();

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
                OracleCommand command = new OracleCommand("DELETE FROM \"" + ConnectionBuilder.UserID + "\".\"QUADRUPLES\" WHERE \"CONTEXTID\" = :CTXID AND \"PREDICATEID\" = :PREDID", Connection);
                command.Parameters.Add(new OracleParameter("CTXID", OracleDbType.Int64));
                command.Parameters.Add(new OracleParameter("PREDID", OracleDbType.Int64));

                //Valorize parameters
                command.Parameters["CTXID"].Value = contextResource.PatternMemberID;
                command.Parameters["PREDID"].Value = predicateResource.PatternMemberID;

                try
                {
                    //Open connection
                    Connection.Open();

                    //Prepare command
                    command.Prepare();

                    //Open transaction
                    command.Transaction = Connection.BeginTransaction();

                    //Execute command
                    command.ExecuteNonQuery();

                    //Close transaction
                    command.Transaction.Commit();

                    //Close connection
                    Connection.Close();
                }
                catch (Exception ex)
                {
                    //Rollback transaction
                    command.Transaction.Rollback();

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
                OracleCommand command = new OracleCommand("DELETE FROM \"" + ConnectionBuilder.UserID + "\".\"QUADRUPLES\" WHERE \"CONTEXTID\" = :CTXID AND \"OBJECTID\" = :OBJID AND \"TRIPLEFLAVOR\" = :TFV", Connection);
                command.Parameters.Add(new OracleParameter("CTXID", OracleDbType.Int64));
                command.Parameters.Add(new OracleParameter("OBJID", OracleDbType.Int64));
                command.Parameters.Add(new OracleParameter("TFV", OracleDbType.Int32));

                //Valorize parameters
                command.Parameters["CTXID"].Value = contextResource.PatternMemberID;
                command.Parameters["OBJID"].Value = objectResource.PatternMemberID;
                command.Parameters["TFV"].Value = RDFModelEnums.RDFTripleFlavors.SPO;

                try
                {
                    //Open connection
                    Connection.Open();

                    //Prepare command
                    command.Prepare();

                    //Open transaction
                    command.Transaction = Connection.BeginTransaction();

                    //Execute command
                    command.ExecuteNonQuery();

                    //Close transaction
                    command.Transaction.Commit();

                    //Close connection
                    Connection.Close();
                }
                catch (Exception ex)
                {
                    //Rollback transaction
                    command.Transaction.Rollback();

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
                OracleCommand command = new OracleCommand("DELETE FROM \"" + ConnectionBuilder.UserID + "\".\"QUADRUPLES\" WHERE \"CONTEXTID\" = :CTXID AND \"OBJECTID\" = :OBJID AND \"TRIPLEFLAVOR\" = :TFV", Connection);
                command.Parameters.Add(new OracleParameter("CTXID", OracleDbType.Int64));
                command.Parameters.Add(new OracleParameter("OBJID", OracleDbType.Int64));
                command.Parameters.Add(new OracleParameter("TFV", OracleDbType.Int32));

                //Valorize parameters
                command.Parameters["CTXID"].Value = contextResource.PatternMemberID;
                command.Parameters["OBJID"].Value = objectLiteral.PatternMemberID;
                command.Parameters["TFV"].Value = RDFModelEnums.RDFTripleFlavors.SPL;

                try
                {
                    //Open connection
                    Connection.Open();

                    //Prepare command
                    command.Prepare();

                    //Open transaction
                    command.Transaction = Connection.BeginTransaction();

                    //Execute command
                    command.ExecuteNonQuery();

                    //Close transaction
                    command.Transaction.Commit();

                    //Close connection
                    Connection.Close();
                }
                catch (Exception ex)
                {
                    //Rollback transaction
                    command.Transaction.Rollback();

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
                OracleCommand command = new OracleCommand("DELETE FROM \"" + ConnectionBuilder.UserID + "\".\"QUADRUPLES\" WHERE \"CONTEXTID\" = :CTXID AND \"SUBJECTID\" = :SUBJID AND \"PREDICATEID\" = :PREDID", Connection);
                command.Parameters.Add(new OracleParameter("CTXID", OracleDbType.Int64));
                command.Parameters.Add(new OracleParameter("SUBJID", OracleDbType.Int64));
                command.Parameters.Add(new OracleParameter("PREDID", OracleDbType.Int64));

                //Valorize parameters
                command.Parameters["CTXID"].Value = contextResource.PatternMemberID;
                command.Parameters["SUBJID"].Value = subjectResource.PatternMemberID;
                command.Parameters["PREDID"].Value = predicateResource.PatternMemberID;

                try
                {
                    //Open connection
                    Connection.Open();

                    //Prepare command
                    command.Prepare();

                    //Open transaction
                    command.Transaction = Connection.BeginTransaction();

                    //Execute command
                    command.ExecuteNonQuery();

                    //Close transaction
                    command.Transaction.Commit();

                    //Close connection
                    Connection.Close();
                }
                catch (Exception ex)
                {
                    //Rollback transaction
                    command.Transaction.Rollback();

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
                OracleCommand command = new OracleCommand("DELETE FROM \"" + ConnectionBuilder.UserID + "\".\"QUADRUPLES\" WHERE \"CONTEXTID\" = :CTXID AND \"SUBJECTID\" = :SUBJID AND \"OBJECTID\" = :OBJID AND \"TRIPLEFLAVOR\" = :TFV", Connection);
                command.Parameters.Add(new OracleParameter("CTXID", OracleDbType.Int64));
                command.Parameters.Add(new OracleParameter("SUBJID", OracleDbType.Int64));
                command.Parameters.Add(new OracleParameter("OBJID", OracleDbType.Int64));
                command.Parameters.Add(new OracleParameter("TFV", OracleDbType.Int32));

                //Valorize parameters
                command.Parameters["CTXID"].Value = contextResource.PatternMemberID;
                command.Parameters["SUBJID"].Value = subjectResource.PatternMemberID;
                command.Parameters["OBJID"].Value = objectResource.PatternMemberID;
                command.Parameters["TFV"].Value = RDFModelEnums.RDFTripleFlavors.SPO;

                try
                {
                    //Open connection
                    Connection.Open();

                    //Prepare command
                    command.Prepare();

                    //Open transaction
                    command.Transaction = Connection.BeginTransaction();

                    //Execute command
                    command.ExecuteNonQuery();

                    //Close transaction
                    command.Transaction.Commit();

                    //Close connection
                    Connection.Close();
                }
                catch (Exception ex)
                {
                    //Rollback transaction
                    command.Transaction.Rollback();

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
                OracleCommand command = new OracleCommand("DELETE FROM \"" + ConnectionBuilder.UserID + "\".\"QUADRUPLES\" WHERE \"CONTEXTID\" = :CTXID AND \"SUBJECTID\" = :SUBJID AND \"OBJECTID\" = :OBJID AND \"TRIPLEFLAVOR\" = :TFV", Connection);
                command.Parameters.Add(new OracleParameter("CTXID", OracleDbType.Int64));
                command.Parameters.Add(new OracleParameter("SUBJID", OracleDbType.Int64));
                command.Parameters.Add(new OracleParameter("OBJID", OracleDbType.Int64));
                command.Parameters.Add(new OracleParameter("TFV", OracleDbType.Int32));

                //Valorize parameters
                command.Parameters["CTXID"].Value = contextResource.PatternMemberID;
                command.Parameters["SUBJID"].Value = subjectResource.PatternMemberID;
                command.Parameters["OBJID"].Value = objectLiteral.PatternMemberID;
                command.Parameters["TFV"].Value = RDFModelEnums.RDFTripleFlavors.SPL;

                try
                {
                    //Open connection
                    Connection.Open();

                    //Prepare command
                    command.Prepare();

                    //Open transaction
                    command.Transaction = Connection.BeginTransaction();

                    //Execute command
                    command.ExecuteNonQuery();

                    //Close transaction
                    command.Transaction.Commit();

                    //Close connection
                    Connection.Close();
                }
                catch (Exception ex)
                {
                    //Rollback transaction
                    command.Transaction.Rollback();

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
                OracleCommand command = new OracleCommand("DELETE FROM \"" + ConnectionBuilder.UserID + "\".\"QUADRUPLES\" WHERE \"CONTEXTID\" = :CTXID AND \"PREDICATEID\" = :PREDID AND \"OBJECTID\" = :OBJID AND \"TRIPLEFLAVOR\" = :TFV", Connection);
                command.Parameters.Add(new OracleParameter("CTXID", OracleDbType.Int64));
                command.Parameters.Add(new OracleParameter("PREDID", OracleDbType.Int64));
                command.Parameters.Add(new OracleParameter("OBJID", OracleDbType.Int64));
                command.Parameters.Add(new OracleParameter("TFV", OracleDbType.Int32));

                //Valorize parameters
                command.Parameters["CTXID"].Value = contextResource.PatternMemberID;
                command.Parameters["PREDID"].Value = predicateResource.PatternMemberID;
                command.Parameters["OBJID"].Value = objectResource.PatternMemberID;
                command.Parameters["TFV"].Value = RDFModelEnums.RDFTripleFlavors.SPO;

                try
                {
                    //Open connection
                    Connection.Open();

                    //Prepare command
                    command.Prepare();

                    //Open transaction
                    command.Transaction = Connection.BeginTransaction();

                    //Execute command
                    command.ExecuteNonQuery();

                    //Close transaction
                    command.Transaction.Commit();

                    //Close connection
                    Connection.Close();
                }
                catch (Exception ex)
                {
                    //Rollback transaction
                    command.Transaction.Rollback();

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
                OracleCommand command = new OracleCommand("DELETE FROM \"" + ConnectionBuilder.UserID + "\".\"QUADRUPLES\" WHERE \"CONTEXTID\" = :CTXID AND \"PREDICATEID\" = :PREDID AND \"OBJECTID\" = :OBJID AND \"TRIPLEFLAVOR\" = :TFV", Connection);
                command.Parameters.Add(new OracleParameter("CTXID", OracleDbType.Int64));
                command.Parameters.Add(new OracleParameter("PREDID", OracleDbType.Int64));
                command.Parameters.Add(new OracleParameter("OBJID", OracleDbType.Int64));
                command.Parameters.Add(new OracleParameter("TFV", OracleDbType.Int32));

                //Valorize parameters
                command.Parameters["CTXID"].Value = contextResource.PatternMemberID;
                command.Parameters["PREDID"].Value = predicateResource.PatternMemberID;
                command.Parameters["OBJID"].Value = objectLiteral.PatternMemberID;
                command.Parameters["TFV"].Value = RDFModelEnums.RDFTripleFlavors.SPL;

                try
                {
                    //Open connection
                    Connection.Open();

                    //Prepare command
                    command.Prepare();

                    //Open transaction
                    command.Transaction = Connection.BeginTransaction();

                    //Execute command
                    command.ExecuteNonQuery();

                    //Close transaction
                    command.Transaction.Commit();

                    //Close connection
                    Connection.Close();
                }
                catch (Exception ex)
                {
                    //Rollback transaction
                    command.Transaction.Rollback();

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
                OracleCommand command = new OracleCommand("DELETE FROM \"" + ConnectionBuilder.UserID + "\".\"QUADRUPLES\" WHERE \"SUBJECTID\" = :SUBJID AND \"PREDICATEID\" = :PREDID", Connection);
                command.Parameters.Add(new OracleParameter("SUBJID", OracleDbType.Int64));
                command.Parameters.Add(new OracleParameter("PREDID", OracleDbType.Int64));

                //Valorize parameters
                command.Parameters["SUBJID"].Value = subjectResource.PatternMemberID;
                command.Parameters["PREDID"].Value = predicateResource.PatternMemberID;

                try
                {
                    //Open connection
                    Connection.Open();

                    //Prepare command
                    command.Prepare();

                    //Open transaction
                    command.Transaction = Connection.BeginTransaction();

                    //Execute command
                    command.ExecuteNonQuery();

                    //Close transaction
                    command.Transaction.Commit();

                    //Close connection
                    Connection.Close();
                }
                catch (Exception ex)
                {
                    //Rollback transaction
                    command.Transaction.Rollback();

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
                OracleCommand command = new OracleCommand("DELETE FROM \"" + ConnectionBuilder.UserID + "\".\"QUADRUPLES\" WHERE \"SUBJECTID\" = :SUBJID AND \"OBJECTID\" = :OBJID AND \"TRIPLEFLAVOR\" = :TFV", Connection);
                command.Parameters.Add(new OracleParameter("SUBJID", OracleDbType.Int64));
                command.Parameters.Add(new OracleParameter("OBJID", OracleDbType.Int64));
                command.Parameters.Add(new OracleParameter("TFV", OracleDbType.Int32));

                //Valorize parameters
                command.Parameters["SUBJID"].Value = subjectResource.PatternMemberID;
                command.Parameters["OBJID"].Value = objectResource.PatternMemberID;
                command.Parameters["TFV"].Value = RDFModelEnums.RDFTripleFlavors.SPO;

                try
                {
                    //Open connection
                    Connection.Open();

                    //Prepare command
                    command.Prepare();

                    //Open transaction
                    command.Transaction = Connection.BeginTransaction();

                    //Execute command
                    command.ExecuteNonQuery();

                    //Close transaction
                    command.Transaction.Commit();

                    //Close connection
                    Connection.Close();
                }
                catch (Exception ex)
                {
                    //Rollback transaction
                    command.Transaction.Rollback();

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
                OracleCommand command = new OracleCommand("DELETE FROM \"" + ConnectionBuilder.UserID + "\".\"QUADRUPLES\" WHERE \"SUBJECTID\" = :SUBJID AND \"OBJECTID\" = :OBJID AND \"TRIPLEFLAVOR\" = :TFV", Connection);
                command.Parameters.Add(new OracleParameter("SUBJID", OracleDbType.Int64));
                command.Parameters.Add(new OracleParameter("OBJID", OracleDbType.Int64));
                command.Parameters.Add(new OracleParameter("TFV", OracleDbType.Int32));

                //Valorize parameters
                command.Parameters["SUBJID"].Value = subjectResource.PatternMemberID;
                command.Parameters["OBJID"].Value = objectLiteral.PatternMemberID;
                command.Parameters["TFV"].Value = RDFModelEnums.RDFTripleFlavors.SPL;

                try
                {
                    //Open connection
                    Connection.Open();

                    //Prepare command
                    command.Prepare();

                    //Open transaction
                    command.Transaction = Connection.BeginTransaction();

                    //Execute command
                    command.ExecuteNonQuery();

                    //Close transaction
                    command.Transaction.Commit();

                    //Close connection
                    Connection.Close();
                }
                catch (Exception ex)
                {
                    //Rollback transaction
                    command.Transaction.Rollback();

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
                OracleCommand command = new OracleCommand("DELETE FROM \"" + ConnectionBuilder.UserID + "\".\"QUADRUPLES\" WHERE \"PREDICATEID\" = :PREDID AND \"OBJECTID\" = :OBJID AND \"TRIPLEFLAVOR\" = :TFV", Connection);
                command.Parameters.Add(new OracleParameter("PREDID", OracleDbType.Int64));
                command.Parameters.Add(new OracleParameter("OBJID", OracleDbType.Int64));
                command.Parameters.Add(new OracleParameter("TFV", OracleDbType.Int32));

                //Valorize parameters
                command.Parameters["PREDID"].Value = predicateResource.PatternMemberID;
                command.Parameters["OBJID"].Value = objectResource.PatternMemberID;
                command.Parameters["TFV"].Value = RDFModelEnums.RDFTripleFlavors.SPO;

                try
                {
                    //Open connection
                    Connection.Open();

                    //Prepare command
                    command.Prepare();

                    //Open transaction
                    command.Transaction = Connection.BeginTransaction();

                    //Execute command
                    command.ExecuteNonQuery();

                    //Close transaction
                    command.Transaction.Commit();

                    //Close connection
                    Connection.Close();
                }
                catch (Exception ex)
                {
                    //Rollback transaction
                    command.Transaction.Rollback();

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
                OracleCommand command = new OracleCommand("DELETE FROM \"" + ConnectionBuilder.UserID + "\".\"QUADRUPLES\" WHERE \"PREDICATEID\" = :PREDID AND \"OBJECTID\" = :OBJID AND \"TRIPLEFLAVOR\" = :TFV", Connection);
                command.Parameters.Add(new OracleParameter("PREDID", OracleDbType.Int64));
                command.Parameters.Add(new OracleParameter("OBJID", OracleDbType.Int64));
                command.Parameters.Add(new OracleParameter("TFV", OracleDbType.Int32));

                //Valorize parameters
                command.Parameters["PREDID"].Value = predicateResource.PatternMemberID;
                command.Parameters["OBJID"].Value = objectLiteral.PatternMemberID;
                command.Parameters["TFV"].Value = RDFModelEnums.RDFTripleFlavors.SPL;

                try
                {
                    //Open connection
                    Connection.Open();

                    //Prepare command
                    command.Prepare();

                    //Open transaction
                    command.Transaction = Connection.BeginTransaction();

                    //Execute command
                    command.ExecuteNonQuery();

                    //Close transaction
                    command.Transaction.Commit();

                    //Close connection
                    Connection.Close();
                }
                catch (Exception ex)
                {
                    //Rollback transaction
                    command.Transaction.Rollback();

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
            OracleCommand command = new OracleCommand("DELETE FROM \"" + ConnectionBuilder.UserID + "\".\"QUADRUPLES\"", Connection);

            try
            {
                //Open connection
                Connection.Open();

                //Prepare command
                command.Prepare();

                //Open transaction
                command.Transaction = Connection.BeginTransaction();

                //Execute command
                command.ExecuteNonQuery();

                //Close transaction
                command.Transaction.Commit();

                //Close connection
                Connection.Close();
            }
            catch (Exception ex)
            {
                //Rollback transaction
                command.Transaction.Rollback();

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
            OracleCommand command = new OracleCommand("SELECT CASE WHEN EXISTS (SELECT 1 FROM \"" + ConnectionBuilder.UserID + "\".\"QUADRUPLES\" WHERE \"QUADRUPLEID\" = :QID) THEN 1 ELSE 0 END AS REC_EXISTS FROM DUAL;", Connection);
            command.Parameters.Add(new OracleParameter("QID", OracleDbType.Int64));            

            //Valorize parameters
            command.Parameters["QID"].Value = quadruple.QuadrupleID;

            //Prepare and execute command
            try
            {
                //Open connection
                Connection.Open();

                //Prepare command
                command.Prepare();

                //Execute command
                int result = int.Parse(command.ExecuteScalar().ToString());

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
        internal override RDFMemoryStore SelectQuadruples(RDFContext ctx, RDFResource subj, RDFResource pred, RDFResource obj, RDFLiteral lit)
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
            OracleCommand command = null;
            switch (queryFilters.ToString())
            {
                case "C":
                    //C->->->
                    command = new OracleCommand("SELECT \"TRIPLEFLAVOR\", \"CONTEXT\", \"SUBJECT\", \"PREDICATE\", \"OBJECT\" FROM \"" + ConnectionBuilder.UserID + "\".\"QUADRUPLES\" WHERE \"CONTEXTID\" = :CTXID", Connection);
                    command.Parameters.Add(new OracleParameter("CTXID", OracleDbType.Int64));
                    command.Parameters["CTXID"].Value = ctx.PatternMemberID;
                    break;
                case "S":
                    //->S->->
                    command = new OracleCommand("SELECT \"TRIPLEFLAVOR\", \"CONTEXT\", \"SUBJECT\", \"PREDICATE\", \"OBJECT\" FROM \"" + ConnectionBuilder.UserID + "\".\"QUADRUPLES\" WHERE \"SUBJECTID\" = :SUBJID", Connection);
                    command.Parameters.Add(new OracleParameter("SUBJID", OracleDbType.Int64));
                    command.Parameters["SUBJID"].Value = subj.PatternMemberID;
                    break;
                case "P":
                    //->->P->
                    command = new OracleCommand("SELECT \"TRIPLEFLAVOR\", \"CONTEXT\", \"SUBJECT\", \"PREDICATE\", \"OBJECT\" FROM \"" + ConnectionBuilder.UserID + "\".\"QUADRUPLES\" WHERE \"PREDICATEID\" = :PREDID", Connection);
                    command.Parameters.Add(new OracleParameter("PREDID", OracleDbType.Int64));
                    command.Parameters["PREDID"].Value = pred.PatternMemberID;
                    break;
                case "O":
                    //->->->O
                    command = new OracleCommand("SELECT \"TRIPLEFLAVOR\", \"CONTEXT\", \"SUBJECT\", \"PREDICATE\", \"OBJECT\" FROM \"" + ConnectionBuilder.UserID + "\".\"QUADRUPLES\" WHERE \"OBJECTID\" = :OBJID AND \"TRIPLEFLAVOR\" = :TFV", Connection);
                    command.Parameters.Add(new OracleParameter("OBJID", OracleDbType.Int64));
                    command.Parameters.Add(new OracleParameter("TFV", OracleDbType.Int32));
                    command.Parameters["OBJID"].Value = obj.PatternMemberID;
                    command.Parameters["TFV"].Value = (int)RDFModelEnums.RDFTripleFlavors.SPO;
                    break;
                case "L":
                    //->->->L
                    command = new OracleCommand("SELECT \"TRIPLEFLAVOR\", \"CONTEXT\", \"SUBJECT\", \"PREDICATE\", \"OBJECT\" FROM \"" + ConnectionBuilder.UserID + "\".\"QUADRUPLES\" WHERE \"OBJECTID\" = :OBJID AND \"TRIPLEFLAVOR\" = :TFV", Connection);
                    command.Parameters.Add(new OracleParameter("OBJID", OracleDbType.Int64));
                    command.Parameters.Add(new OracleParameter("TFV", OracleDbType.Int32));
                    command.Parameters["OBJID"].Value = lit.PatternMemberID;
                    command.Parameters["TFV"].Value = (int)RDFModelEnums.RDFTripleFlavors.SPL;
                    break;
                case "CS":
                    //C->S->->
                    command = new OracleCommand("SELECT \"TRIPLEFLAVOR\", \"CONTEXT\", \"SUBJECT\", \"PREDICATE\", \"OBJECT\" FROM \"" + ConnectionBuilder.UserID + "\".\"QUADRUPLES\" WHERE \"CONTEXTID\" = :CTXID AND \"SUBJECTID\" = :SUBJID", Connection);
                    command.Parameters.Add(new OracleParameter("CTXID", OracleDbType.Int64));
                    command.Parameters.Add(new OracleParameter("SUBJID", OracleDbType.Int64));
                    command.Parameters["CTXID"].Value = ctx.PatternMemberID;
                    command.Parameters["SUBJID"].Value = subj.PatternMemberID;
                    break;
                case "CP":
                    //C->->P->
                    command = new OracleCommand("SELECT \"TRIPLEFLAVOR\", \"CONTEXT\", \"SUBJECT\", \"PREDICATE\", \"OBJECT\" FROM \"" + ConnectionBuilder.UserID + "\".\"QUADRUPLES\" WHERE \"CONTEXTID\" = :CTXID AND \"PREDICATEID\" = :PREDID", Connection);
                    command.Parameters.Add(new OracleParameter("CTXID", OracleDbType.Int64));
                    command.Parameters.Add(new OracleParameter("PREDID", OracleDbType.Int64));
                    command.Parameters["CTXID"].Value = ctx.PatternMemberID;
                    command.Parameters["PREDID"].Value = pred.PatternMemberID;
                    break;
                case "CO":
                    //C->->->O
                    command = new OracleCommand("SELECT \"TRIPLEFLAVOR\", \"CONTEXT\", \"SUBJECT\", \"PREDICATE\", \"OBJECT\" FROM \"" + ConnectionBuilder.UserID + "\".\"QUADRUPLES\" WHERE \"CONTEXTID\" = :CTXID AND \"OBJECTID\" = :OBJID AND \"TRIPLEFLAVOR\" = :TFV", Connection);
                    command.Parameters.Add(new OracleParameter("CTXID", OracleDbType.Int64));
                    command.Parameters.Add(new OracleParameter("OBJID", OracleDbType.Int64));
                    command.Parameters.Add(new OracleParameter("TFV", OracleDbType.Int32));
                    command.Parameters["CTXID"].Value = ctx.PatternMemberID;
                    command.Parameters["OBJID"].Value = obj.PatternMemberID;
                    command.Parameters["TFV"].Value = (int)RDFModelEnums.RDFTripleFlavors.SPO;
                    break;
                case "CL":
                    //C->->->L
                    command = new OracleCommand("SELECT \"TRIPLEFLAVOR\", \"CONTEXT\", \"SUBJECT\", \"PREDICATE\", \"OBJECT\" FROM \"" + ConnectionBuilder.UserID + "\".\"QUADRUPLES\" WHERE \"CONTEXTID\" = :CTXID AND \"OBJECTID\" = :OBJID AND \"TRIPLEFLAVOR\" = :TFV", Connection);
                    command.Parameters.Add(new OracleParameter("CTXID", OracleDbType.Int64));
                    command.Parameters.Add(new OracleParameter("OBJID", OracleDbType.Int64));
                    command.Parameters.Add(new OracleParameter("TFV", OracleDbType.Int32));
                    command.Parameters["CTXID"].Value = ctx.PatternMemberID;
                    command.Parameters["OBJID"].Value = lit.PatternMemberID;
                    command.Parameters["TFV"].Value = (int)RDFModelEnums.RDFTripleFlavors.SPL;
                    break;
                case "CSP":
                    //C->S->P->
                    command = new OracleCommand("SELECT \"TRIPLEFLAVOR\", \"CONTEXT\", \"SUBJECT\", \"PREDICATE\", \"OBJECT\" FROM \"" + ConnectionBuilder.UserID + "\".\"QUADRUPLES\" WHERE \"CONTEXTID\" = :CTXID AND \"SUBJECTID\" = :SUBJID AND \"PREDICATEID\" = :PREDID", Connection);
                    command.Parameters.Add(new OracleParameter("CTXID", OracleDbType.Int64));
                    command.Parameters.Add(new OracleParameter("SUBJID", OracleDbType.Int64));
                    command.Parameters.Add(new OracleParameter("PREDID", OracleDbType.Int64));
                    command.Parameters["CTXID"].Value = ctx.PatternMemberID;
                    command.Parameters["SUBJID"].Value = subj.PatternMemberID;
                    command.Parameters["PREDID"].Value = pred.PatternMemberID;
                    break;
                case "CSO":
                    //C->S->->O
                    command = new OracleCommand("SELECT \"TRIPLEFLAVOR\", \"CONTEXT\", \"SUBJECT\", \"PREDICATE\", \"OBJECT\" FROM \"" + ConnectionBuilder.UserID + "\".\"QUADRUPLES\" WHERE \"CONTEXTID\" = :CTXID AND \"SUBJECTID\" = :SUBJID AND \"OBJECTID\" = :OBJID AND \"TRIPLEFLAVOR\" = :TFV", Connection);
                    command.Parameters.Add(new OracleParameter("CTXID", OracleDbType.Int64));
                    command.Parameters.Add(new OracleParameter("SUBJID", OracleDbType.Int64));
                    command.Parameters.Add(new OracleParameter("OBJID", OracleDbType.Int64));
                    command.Parameters.Add(new OracleParameter("TFV", OracleDbType.Int32));
                    command.Parameters["CTXID"].Value = ctx.PatternMemberID;
                    command.Parameters["SUBJID"].Value = subj.PatternMemberID;
                    command.Parameters["OBJID"].Value = obj.PatternMemberID;
                    command.Parameters["TFV"].Value = (int)RDFModelEnums.RDFTripleFlavors.SPO;
                    break;
                case "CSL":
                    //C->S->->L
                    command = new OracleCommand("SELECT \"TRIPLEFLAVOR\", \"CONTEXT\", \"SUBJECT\", \"PREDICATE\", \"OBJECT\" FROM \"" + ConnectionBuilder.UserID + "\".\"QUADRUPLES\" WHERE \"CONTEXTID\" = :CTXID AND \"SUBJECTID\" = :SUBJID AND \"OBJECTID\" = :OBJID AND \"TRIPLEFLAVOR\" = :TFV", Connection);
                    command.Parameters.Add(new OracleParameter("CTXID", OracleDbType.Int64));
                    command.Parameters.Add(new OracleParameter("SUBJID", OracleDbType.Int64));
                    command.Parameters.Add(new OracleParameter("OBJID", OracleDbType.Int64));
                    command.Parameters.Add(new OracleParameter("TFV", OracleDbType.Int32));
                    command.Parameters["CTXID"].Value = ctx.PatternMemberID;
                    command.Parameters["SUBJID"].Value = subj.PatternMemberID;
                    command.Parameters["OBJID"].Value = lit.PatternMemberID;
                    command.Parameters["TFV"].Value = (int)RDFModelEnums.RDFTripleFlavors.SPL;
                    break;
                case "CPO":
                    //C->->P->O
                    command = new OracleCommand("SELECT \"TRIPLEFLAVOR\", \"CONTEXT\", \"SUBJECT\", \"PREDICATE\", \"OBJECT\" FROM \"" + ConnectionBuilder.UserID + "\".\"QUADRUPLES\" WHERE \"CONTEXTID\" = :CTXID AND \"PREDICATEID\" = :PREDID AND \"OBJECTID\" = :OBJID AND \"TRIPLEFLAVOR\" = :TFV", Connection);
                    command.Parameters.Add(new OracleParameter("CTXID", OracleDbType.Int64));
                    command.Parameters.Add(new OracleParameter("PREDID", OracleDbType.Int64));
                    command.Parameters.Add(new OracleParameter("OBJID", OracleDbType.Int64));
                    command.Parameters.Add(new OracleParameter("TFV", OracleDbType.Int32));
                    command.Parameters["CTXID"].Value = ctx.PatternMemberID;
                    command.Parameters["PREDID"].Value = pred.PatternMemberID;
                    command.Parameters["OBJID"].Value = obj.PatternMemberID;
                    command.Parameters["TFV"].Value = (int)RDFModelEnums.RDFTripleFlavors.SPO;
                    break;
                case "CPL":
                    //C->->P->L
                    command = new OracleCommand("SELECT \"TRIPLEFLAVOR\", \"CONTEXT\", \"SUBJECT\", \"PREDICATE\", \"OBJECT\" FROM \"" + ConnectionBuilder.UserID + "\".\"QUADRUPLES\" WHERE \"CONTEXTID\" = :CTXID AND \"PREDICATEID\" = :PREDID AND \"OBJECTID\" = :OBJID AND \"TRIPLEFLAVOR\" = :TFV", Connection);
                    command.Parameters.Add(new OracleParameter("CTXID", OracleDbType.Int64));
                    command.Parameters.Add(new OracleParameter("PREDID", OracleDbType.Int64));
                    command.Parameters.Add(new OracleParameter("OBJID", OracleDbType.Int64));
                    command.Parameters.Add(new OracleParameter("TFV", OracleDbType.Int32));
                    command.Parameters["CTXID"].Value = ctx.PatternMemberID;
                    command.Parameters["PREDID"].Value = pred.PatternMemberID;
                    command.Parameters["OBJID"].Value = lit.PatternMemberID;
                    command.Parameters["TFV"].Value = (int)RDFModelEnums.RDFTripleFlavors.SPL;
                    break;
                case "CSPO":
                    //C->S->P->O
                    command = new OracleCommand("SELECT \"TRIPLEFLAVOR\", \"CONTEXT\", \"SUBJECT\", \"PREDICATE\", \"OBJECT\" FROM \"" + ConnectionBuilder.UserID + "\".\"QUADRUPLES\" WHERE \"CONTEXTID\" = :CTXID AND \"SUBJECTID\" = :SUBJID AND \"PREDICATEID\" = :PREDID AND \"OBJECTID\" = :OBJID AND \"TRIPLEFLAVOR\" = :TFV", Connection);
                    command.Parameters.Add(new OracleParameter("CTXID", OracleDbType.Int64));
                    command.Parameters.Add(new OracleParameter("SUBJID", OracleDbType.Int64));
                    command.Parameters.Add(new OracleParameter("PREDID", OracleDbType.Int64));
                    command.Parameters.Add(new OracleParameter("OBJID", OracleDbType.Int64));
                    command.Parameters.Add(new OracleParameter("TFV", OracleDbType.Int32));
                    command.Parameters["CTXID"].Value = ctx.PatternMemberID;
                    command.Parameters["SUBJID"].Value = subj.PatternMemberID;
                    command.Parameters["PREDID"].Value = pred.PatternMemberID;
                    command.Parameters["OBJID"].Value = obj.PatternMemberID;
                    command.Parameters["TFV"].Value = (int)RDFModelEnums.RDFTripleFlavors.SPO;
                    break;
                case "CSPL":
                    //C->S->P->L
                    command = new OracleCommand("SELECT \"TRIPLEFLAVOR\", \"CONTEXT\", \"SUBJECT\", \"PREDICATE\", \"OBJECT\" FROM \"" + ConnectionBuilder.UserID + "\".\"QUADRUPLES\" WHERE \"CONTEXTID\" = :CTXID AND \"SUBJECTID\" = :SUBJID AND \"PREDICATEID\" = :PREDID AND \"OBJECTID\" = :OBJID AND \"TRIPLEFLAVOR\" = :TFV", Connection);
                    command.Parameters.Add(new OracleParameter("CTXID", OracleDbType.Int64));
                    command.Parameters.Add(new OracleParameter("SUBJID", OracleDbType.Int64));
                    command.Parameters.Add(new OracleParameter("PREDID", OracleDbType.Int64));
                    command.Parameters.Add(new OracleParameter("OBJID", OracleDbType.Int64));
                    command.Parameters.Add(new OracleParameter("TFV", OracleDbType.Int32));
                    command.Parameters["CTXID"].Value = ctx.PatternMemberID;
                    command.Parameters["SUBJID"].Value = subj.PatternMemberID;
                    command.Parameters["PREDID"].Value = pred.PatternMemberID;
                    command.Parameters["OBJID"].Value = lit.PatternMemberID;
                    command.Parameters["TFV"].Value = (int)RDFModelEnums.RDFTripleFlavors.SPL;
                    break;
                case "SP":
                    //->S->P->
                    command = new OracleCommand("SELECT \"TRIPLEFLAVOR\", \"CONTEXT\", \"SUBJECT\", \"PREDICATE\", \"OBJECT\" FROM \"" + ConnectionBuilder.UserID + "\".\"QUADRUPLES\" WHERE \"SUBJECTID\" = :SUBJID AND \"PREDICATEID\" = :PREDID", Connection);
                    command.Parameters.Add(new OracleParameter("SUBJID", OracleDbType.Int64));
                    command.Parameters.Add(new OracleParameter("PREDID", OracleDbType.Int64));
                    command.Parameters["SUBJID"].Value = subj.PatternMemberID;
                    command.Parameters["PREDID"].Value = pred.PatternMemberID;
                    break;
                case "SO":
                    //->S->->O
                    command = new OracleCommand("SELECT \"TRIPLEFLAVOR\", \"CONTEXT\", \"SUBJECT\", \"PREDICATE\", \"OBJECT\" FROM \"" + ConnectionBuilder.UserID + "\".\"QUADRUPLES\" WHERE \"SUBJECTID\" = :SUBJID AND \"OBJECTID\" = :OBJID AND \"TRIPLEFLAVOR\" = :TFV", Connection);
                    command.Parameters.Add(new OracleParameter("SUBJID", OracleDbType.Int64));
                    command.Parameters.Add(new OracleParameter("OBJID", OracleDbType.Int64));
                    command.Parameters.Add(new OracleParameter("TFV", OracleDbType.Int32));
                    command.Parameters["SUBJID"].Value = subj.PatternMemberID;
                    command.Parameters["OBJID"].Value = obj.PatternMemberID;
                    command.Parameters["TFV"].Value = (int)RDFModelEnums.RDFTripleFlavors.SPO;
                    break;
                case "SL":
                    //->S->->L
                    command = new OracleCommand("SELECT \"TRIPLEFLAVOR\", \"CONTEXT\", \"SUBJECT\", \"PREDICATE\", \"OBJECT\" FROM \"" + ConnectionBuilder.UserID + "\".\"QUADRUPLES\" WHERE \"SUBJECTID\" = :SUBJID AND \"OBJECTID\" = :OBJID AND \"TRIPLEFLAVOR\" = :TFV", Connection);
                    command.Parameters.Add(new OracleParameter("SUBJID", OracleDbType.Int64));
                    command.Parameters.Add(new OracleParameter("OBJID", OracleDbType.Int64));
                    command.Parameters.Add(new OracleParameter("TFV", OracleDbType.Int32));
                    command.Parameters["SUBJID"].Value = subj.PatternMemberID;
                    command.Parameters["OBJID"].Value = lit.PatternMemberID;
                    command.Parameters["TFV"].Value = (int)RDFModelEnums.RDFTripleFlavors.SPL;
                    break;
                case "PO":
                    //->->P->O
                    command = new OracleCommand("SELECT \"TRIPLEFLAVOR\", \"CONTEXT\", \"SUBJECT\", \"PREDICATE\", \"OBJECT\" FROM \"" + ConnectionBuilder.UserID + "\".\"QUADRUPLES\" WHERE \"PREDICATEID\" = :PREDID AND \"OBJECTID\" = :OBJID AND \"TRIPLEFLAVOR\" = :TFV", Connection);
                    command.Parameters.Add(new OracleParameter("PREDID", OracleDbType.Int64));
                    command.Parameters.Add(new OracleParameter("OBJID", OracleDbType.Int64));
                    command.Parameters.Add(new OracleParameter("TFV", OracleDbType.Int32));
                    command.Parameters["PREDID"].Value = pred.PatternMemberID;
                    command.Parameters["OBJID"].Value = obj.PatternMemberID;
                    command.Parameters["TFV"].Value = (int)RDFModelEnums.RDFTripleFlavors.SPO;
                    break;
                case "PL":
                    //->->P->L
                    command = new OracleCommand("SELECT \"TRIPLEFLAVOR\", \"CONTEXT\", \"SUBJECT\", \"PREDICATE\", \"OBJECT\" FROM \"" + ConnectionBuilder.UserID + "\".\"QUADRUPLES\" WHERE \"PREDICATEID\" = :PREDID AND \"OBJECTID\" = :OBJID AND \"TRIPLEFLAVOR\" = :TFV", Connection);
                    command.Parameters.Add(new OracleParameter("PREDID", OracleDbType.Int64));
                    command.Parameters.Add(new OracleParameter("OBJID", OracleDbType.Int64));
                    command.Parameters.Add(new OracleParameter("TFV", OracleDbType.Int32));
                    command.Parameters["PREDID"].Value = pred.PatternMemberID;
                    command.Parameters["OBJID"].Value = lit.PatternMemberID;
                    command.Parameters["TFV"].Value = (int)RDFModelEnums.RDFTripleFlavors.SPL;
                    break;
                case "SPO":
                    //->S->P->O
                    command = new OracleCommand("SELECT \"TRIPLEFLAVOR\", \"CONTEXT\", \"SUBJECT\", \"PREDICATE\", \"OBJECT\" FROM \"" + ConnectionBuilder.UserID + "\".\"QUADRUPLES\" WHERE \"SUBJECTID\" = :SUBJID AND \"PREDICATEID\" = :PREDID AND \"OBJECTID\" = :OBJID AND \"TRIPLEFLAVOR\" = :TFV", Connection);
                    command.Parameters.Add(new OracleParameter("SUBJID", OracleDbType.Int64));
                    command.Parameters.Add(new OracleParameter("PREDID", OracleDbType.Int64));
                    command.Parameters.Add(new OracleParameter("OBJID", OracleDbType.Int64));
                    command.Parameters.Add(new OracleParameter("TFV", OracleDbType.Int32));
                    command.Parameters["SUBJID"].Value = subj.PatternMemberID;
                    command.Parameters["PREDID"].Value = pred.PatternMemberID;
                    command.Parameters["OBJID"].Value = obj.PatternMemberID;
                    command.Parameters["TFV"].Value = (int)RDFModelEnums.RDFTripleFlavors.SPO;
                    break;
                case "SPL":
                    //->S->P->L
                    command = new OracleCommand("SELECT \"TRIPLEFLAVOR\", \"CONTEXT\", \"SUBJECT\", \"PREDICATE\", \"OBJECT\" FROM \"" + ConnectionBuilder.UserID + "\".\"QUADRUPLES\" WHERE \"SUBJECTID\" = :SUBJID AND \"PREDICATEID\" = :PREDID AND \"OBJECTID\" = :OBJID AND \"TRIPLEFLAVOR\" = :TFV", Connection);
                    command.Parameters.Add(new OracleParameter("SUBJID", OracleDbType.Int64));
                    command.Parameters.Add(new OracleParameter("PREDID", OracleDbType.Int64));
                    command.Parameters.Add(new OracleParameter("OBJID", OracleDbType.Int64));
                    command.Parameters.Add(new OracleParameter("TFV", OracleDbType.Int32));
                    command.Parameters["SUBJID"].Value = subj.PatternMemberID;
                    command.Parameters["PREDID"].Value = pred.PatternMemberID;
                    command.Parameters["OBJID"].Value = lit.PatternMemberID;
                    command.Parameters["TFV"].Value = (int)RDFModelEnums.RDFTripleFlavors.SPL;
                    break;
                default:
                    //->->->
                    command = new OracleCommand("SELECT \"TRIPLEFLAVOR\", \"CONTEXT\", \"SUBJECT\", \"PREDICATE\", \"OBJECT\" FROM \"" + ConnectionBuilder.UserID + "\".\"QUADRUPLES\"", Connection);
                    break;
            }

            //Prepare and execute command
            try
            {
                //Open connection
                Connection.Open();

                //Prepare command
                command.Prepare();
                command.CommandTimeout = 180;

                //Execute command
                using (OracleDataReader quadruples = command.ExecuteReader())
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
                OracleCommand command = new OracleCommand("SELECT COUNT(*) FROM ALL_OBJECTS WHERE OBJECT_TYPE IN ('TABLE', 'VIEW') AND OBJECT_NAME = 'QUADRUPLES'", Connection);

                //Execute command
                int result = int.Parse(command.ExecuteScalar().ToString());

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
        /// Prepares the underlying Oracle database
        /// </summary>
        private void PrepareStore()
        {
            RDFStoreEnums.RDFStoreSQLErrors check = Diagnostics();

            //Prepare the database only if diagnostics has detected the missing of "QUADRUPLES" table in the store
            if (check == RDFStoreEnums.RDFStoreSQLErrors.QuadruplesTableNotFound)
            {
                try
                {
                    //Open connection
                    Connection.Open();

                    //Create & Execute command
                    OracleCommand command = new OracleCommand("CREATE TABLE \"" + ConnectionBuilder.UserID + "\".\"QUADRUPLES\"(\"QUADRUPLEID\" NUMBER(19, 0) NOT NULL ENABLE,\"TRIPLEFLAVOR\" NUMBER(10, 0) NOT NULL ENABLE,\"CONTEXTID\" NUMBER(19, 0) NOT NULL ENABLE,\"CONTEXT\" VARCHAR2(1000) NOT NULL ENABLE,\"SUBJECTID\" NUMBER(19, 0) NOT NULL ENABLE,\"SUBJECT\" VARCHAR2(1000) NOT NULL ENABLE,\"PREDICATEID\" NUMBER(19, 0) NOT NULL ENABLE,\"PREDICATE\" VARCHAR2(1000) NOT NULL ENABLE,\"OBJECTID\" NUMBER(19, 0) NOT NULL ENABLE,\"OBJECT\" VARCHAR2(1000) NOT NULL ENABLE,PRIMARY KEY(\"QUADRUPLEID\") ENABLE)", Connection);
                    command.ExecuteNonQuery();
                    command.CommandText = "CREATE INDEX \"" + ConnectionBuilder.UserID + "\".\"IDX_CONTEXTID\"             ON \"QUADRUPLES\"(\"CONTEXTID\")";
                    command.ExecuteNonQuery();
                    command.CommandText = "CREATE INDEX \"" + ConnectionBuilder.UserID + "\".\"IDX_SUBJECTID\"             ON \"QUADRUPLES\"(\"SUBJECTID\")";
                    command.ExecuteNonQuery();
                    command.CommandText = "CREATE INDEX \"" + ConnectionBuilder.UserID + "\".\"IDX_PREDICATEID\"           ON \"QUADRUPLES\"(\"PREDICATEID\")";
                    command.ExecuteNonQuery();
                    command.CommandText = "CREATE INDEX \"" + ConnectionBuilder.UserID + "\".\"IDX_OBJECTID\"              ON \"QUADRUPLES\"(\"OBJECTID\",\"TRIPLEFLAVOR\")";
                    command.ExecuteNonQuery();
                    command.CommandText = "CREATE INDEX \"" + ConnectionBuilder.UserID + "\".\"IDX_SUBJECTID_PREDICATEID\" ON \"QUADRUPLES\"(\"SUBJECTID\",\"PREDICATEID\")";
                    command.ExecuteNonQuery();
                    command.CommandText = "CREATE INDEX \"" + ConnectionBuilder.UserID + "\".\"IDX_SUBJECTID_OBJECTID\"    ON \"QUADRUPLES\"(\"SUBJECTID\",\"OBJECTID\",\"TRIPLEFLAVOR\")";
                    command.ExecuteNonQuery();
                    command.CommandText = "CREATE INDEX \"" + ConnectionBuilder.UserID + "\".\"IDX_PREDICATEID_OBJECTID\"  ON \"QUADRUPLES\"(\"PREDICATEID\",\"OBJECTID\",\"TRIPLEFLAVOR\")";
                    command.ExecuteNonQuery();

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
                OracleCommand command = new OracleCommand("ALTER INDEX \"" + ConnectionBuilder.UserID + "\".\"IDX_CONTEXTID\" REBUILD", Connection);
                command.ExecuteNonQuery();
                command.CommandText = "ALTER INDEX \"" + ConnectionBuilder.UserID + "\".\"IDX_SUBJECTID\" REBUILD";
                command.ExecuteNonQuery();
                command.CommandText = "ALTER INDEX \"" + ConnectionBuilder.UserID + "\".\"IDX_PREDICATEID\" REBUILD";
                command.ExecuteNonQuery();
                command.CommandText = "ALTER INDEX \"" + ConnectionBuilder.UserID + "\".\"IDX_OBJECTID\" REBUILD";
                command.ExecuteNonQuery();
                command.CommandText = "ALTER INDEX \"" + ConnectionBuilder.UserID + "\".\"IDX_SUBJECTID_PREDICATEID\" REBUILD";
                command.ExecuteNonQuery();
                command.CommandText = "ALTER INDEX \"" + ConnectionBuilder.UserID + "\".\"IDX_SUBJECTID_OBJECTID\" REBUILD";
                command.ExecuteNonQuery();
                command.CommandText = "ALTER INDEX \"" + ConnectionBuilder.UserID + "\".\"IDX_PREDICATEID_OBJECTID\" REBUILD";
                command.ExecuteNonQuery();

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
}