﻿/*
   Copyright 2012-2020 Marco De Salvo

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
using MySql.Data.MySqlClient;
using RDFSharp.Model;

namespace RDFSharp.Store
{

    /// <summary>
    /// RDFMySQLStore represents a store backed on MySQL engine
    /// </summary>
    public class RDFMySQLStore : RDFStore, IDisposable
    {
        #region Properties
        /// <summary>
        /// Connection to the MySQL database
        /// </summary>
        private MySqlConnection Connection { get; set; }
        
        /// <summary>
        /// Flag indicating that the MySQL store instance has already been disposed
        /// </summary>
        private bool Disposed { get; set; }
        #endregion

        #region Ctors
        /// <summary>
        /// Default-ctor to build a MySQL store instance
        /// </summary>
        public RDFMySQLStore(string mysqlConnectionString)
        {
            if (string.IsNullOrEmpty(mysqlConnectionString))
                throw new RDFStoreException("Cannot connect to MySQL store because: given \"mysqlConnectionString\" parameter is null or empty.");

            //Initialize store structures
            this.StoreType = "MYSQL";
            this.Connection = new MySqlConnection(mysqlConnectionString);
            this.StoreID = RDFModelUtilities.CreateHash(this.ToString());
            this.Disposed = false;

            //Perform initial diagnostics
            this.PrepareStore();
        }

        /// <summary>
        /// Destroys the MySQL store instance
        /// </summary>
        ~RDFMySQLStore() => this.Dispose(false);
        #endregion

        #region Interfaces
        /// <summary>
        /// Gives the string representation of the MySQL store 
        /// </summary>
        public override string ToString()
            => string.Concat(base.ToString(), "|SERVER=", this.Connection.DataSource, ";DATABASE=", this.Connection.Database);

        /// <summary>
        /// Disposes the MySQL store instance 
        /// </summary>
        public void Dispose()
        {
            this.Dispose(true);
            GC.SuppressFinalize(this);
        }

        /// <summary>
        /// Disposes the MySQL store instance  (business logic of resources disposal)
        /// </summary>
        protected virtual void Dispose(bool disposing)
        {
            if (this.Disposed)
                return;

            if (disposing)
            {
                this.Connection?.Dispose();
                this.Connection = null;
            }

            this.Disposed = true;
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
                MySqlCommand command = new MySqlCommand("INSERT IGNORE INTO Quadruples(QuadrupleID, TripleFlavor, Context, ContextID, Subject, SubjectID, Predicate, PredicateID, Object, ObjectID) VALUES (@QID, @TFV, @CTX, @CTXID, @SUBJ, @SUBJID, @PRED, @PREDID, @OBJ, @OBJID)", this.Connection);
                command.Parameters.Add(new MySqlParameter("QID", MySqlDbType.Int64));
                command.Parameters.Add(new MySqlParameter("TFV", MySqlDbType.Int32));
                command.Parameters.Add(new MySqlParameter("CTX", MySqlDbType.VarChar, 1000));
                command.Parameters.Add(new MySqlParameter("CTXID", MySqlDbType.Int64));
                command.Parameters.Add(new MySqlParameter("SUBJ", MySqlDbType.VarChar, 1000));
                command.Parameters.Add(new MySqlParameter("SUBJID", MySqlDbType.Int64));
                command.Parameters.Add(new MySqlParameter("PRED", MySqlDbType.VarChar, 1000));
                command.Parameters.Add(new MySqlParameter("PREDID", MySqlDbType.Int64));
                command.Parameters.Add(new MySqlParameter("OBJ", MySqlDbType.VarChar, 1000));
                command.Parameters.Add(new MySqlParameter("OBJID", MySqlDbType.Int64));

                try
                {
                    //Open connection
                    this.Connection.Open();

                    //Prepare command
                    command.Prepare();

                    //Open transaction
                    command.Transaction = this.Connection.BeginTransaction();

                    //Iterate triples
                    foreach (var triple in graph)
                    {
                        //Valorize parameters
                        command.Parameters["QID"].Value = RDFModelUtilities.CreateHash(string.Concat(graphCtx, " ", triple.Subject, " ", triple.Predicate, " ", triple.Object));
                        command.Parameters["TFV"].Value = triple.TripleFlavor;
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
                    this.Connection.Close();
                }
                catch (Exception ex)
                {
                    //Rollback transaction
                    command.Transaction.Rollback();

                    //Close connection
                    this.Connection.Close();

                    //Propagate exception
                    throw new RDFStoreException("Cannot insert data into MySQL store because: " + ex.Message, ex);
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
                MySqlCommand command = new MySqlCommand("INSERT IGNORE INTO Quadruples(QuadrupleID, TripleFlavor, Context, ContextID, Subject, SubjectID, Predicate, PredicateID, Object, ObjectID) VALUES (@QID, @TFV, @CTX, @CTXID, @SUBJ, @SUBJID, @PRED, @PREDID, @OBJ, @OBJID)", this.Connection);
                command.Parameters.Add(new MySqlParameter("QID", MySqlDbType.Int64));
                command.Parameters.Add(new MySqlParameter("TFV", MySqlDbType.Int32));
                command.Parameters.Add(new MySqlParameter("CTX", MySqlDbType.VarChar, 1000));
                command.Parameters.Add(new MySqlParameter("CTXID", MySqlDbType.Int64));
                command.Parameters.Add(new MySqlParameter("SUBJ", MySqlDbType.VarChar, 1000));
                command.Parameters.Add(new MySqlParameter("SUBJID", MySqlDbType.Int64));
                command.Parameters.Add(new MySqlParameter("PRED", MySqlDbType.VarChar, 1000));
                command.Parameters.Add(new MySqlParameter("PREDID", MySqlDbType.Int64));
                command.Parameters.Add(new MySqlParameter("OBJ", MySqlDbType.VarChar, 1000));
                command.Parameters.Add(new MySqlParameter("OBJID", MySqlDbType.Int64));

                //Valorize parameters
                command.Parameters["QID"].Value = quadruple.QuadrupleID;
                command.Parameters["TFV"].Value = quadruple.TripleFlavor;
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
                    this.Connection.Open();

                    //Prepare command
                    command.Prepare();

                    //Open transaction
                    command.Transaction = this.Connection.BeginTransaction();

                    //Execute command
                    command.ExecuteNonQuery();

                    //Close transaction
                    command.Transaction.Commit();

                    //Close connection
                    this.Connection.Close();
                }
                catch (Exception ex)
                {
                    //Rollback transaction
                    command.Transaction.Rollback();

                    //Close connection
                    this.Connection.Close();

                    //Propagate exception
                    throw new RDFStoreException("Cannot insert data into MySQL store because: " + ex.Message, ex);
                }
            }
            return this;
        }
        #endregion

        #region Remove
        /// <summary>
        /// Removes the given quadruples from the store
        /// </summary>
        public override RDFStore RemoveQuadruple(RDFQuadruple quadruple)
        {
            if (quadruple != null)
            {
                //Create command
                MySqlCommand command = new MySqlCommand("DELETE FROM Quadruples WHERE QuadrupleID = @QID", this.Connection);
                command.Parameters.Add(new MySqlParameter("QID", MySqlDbType.Int64));

                //Valorize parameters
                command.Parameters["QID"].Value = quadruple.QuadrupleID;

                try
                {
                    //Open connection
                    this.Connection.Open();

                    //Prepare command
                    command.Prepare();

                    //Open transaction
                    command.Transaction = this.Connection.BeginTransaction();

                    //Execute command
                    command.ExecuteNonQuery();

                    //Close transaction
                    command.Transaction.Commit();

                    //Close connection
                    this.Connection.Close();
                }
                catch (Exception ex)
                {
                    //Rollback transaction
                    command.Transaction.Rollback();

                    //Close connection
                    this.Connection.Close();

                    //Propagate exception
                    throw new RDFStoreException("Cannot delete data from MySQL store because: " + ex.Message, ex);
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
                MySqlCommand command = new MySqlCommand("DELETE FROM Quadruples WHERE ContextID = @CTXID", this.Connection);
                command.Parameters.Add(new MySqlParameter("CTXID", MySqlDbType.Int64));

                //Valorize parameters
                command.Parameters["CTXID"].Value = contextResource.PatternMemberID;

                try
                {
                    //Open connection
                    this.Connection.Open();

                    //Prepare command
                    command.Prepare();

                    //Open transaction
                    command.Transaction = this.Connection.BeginTransaction();

                    //Execute command
                    command.ExecuteNonQuery();

                    //Close transaction
                    command.Transaction.Commit();

                    //Close connection
                    this.Connection.Close();
                }
                catch (Exception ex)
                {
                    //Rollback transaction
                    command.Transaction.Rollback();

                    //Close connection
                    this.Connection.Close();

                    //Propagate exception
                    throw new RDFStoreException("Cannot delete data from MySQL store because: " + ex.Message, ex);
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
                MySqlCommand command = new MySqlCommand("DELETE FROM Quadruples WHERE SubjectID = @SUBJID", this.Connection);
                command.Parameters.Add(new MySqlParameter("SUBJID", MySqlDbType.Int64));

                //Valorize parameters
                command.Parameters["SUBJID"].Value = subjectResource.PatternMemberID;

                try
                {
                    //Open connection
                    this.Connection.Open();

                    //Prepare command
                    command.Prepare();

                    //Open transaction
                    command.Transaction = this.Connection.BeginTransaction();

                    //Execute command
                    command.ExecuteNonQuery();

                    //Close transaction
                    command.Transaction.Commit();

                    //Close connection
                    this.Connection.Close();
                }
                catch (Exception ex)
                {
                    //Rollback transaction
                    command.Transaction.Rollback();

                    //Close connection
                    this.Connection.Close();

                    //Propagate exception
                    throw new RDFStoreException("Cannot delete data from MySQL store because: " + ex.Message, ex);
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
                MySqlCommand command = new MySqlCommand("DELETE FROM Quadruples WHERE PredicateID = @PREDID", this.Connection);
                command.Parameters.Add(new MySqlParameter("PREDID", MySqlDbType.Int64));

                //Valorize parameters
                command.Parameters["PREDID"].Value = predicateResource.PatternMemberID;

                try
                {
                    //Open connection
                    this.Connection.Open();

                    //Prepare command
                    command.Prepare();

                    //Open transaction
                    command.Transaction = this.Connection.BeginTransaction();

                    //Execute command
                    command.ExecuteNonQuery();

                    //Close transaction
                    command.Transaction.Commit();

                    //Close connection
                    this.Connection.Close();
                }
                catch (Exception ex)
                {
                    //Rollback transaction
                    command.Transaction.Rollback();

                    //Close connection
                    this.Connection.Close();

                    //Propagate exception
                    throw new RDFStoreException("Cannot delete data from MySQL store because: " + ex.Message, ex);
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
                MySqlCommand command = new MySqlCommand("DELETE FROM Quadruples WHERE ObjectID = @OBJID AND TripleFlavor = @TFV", this.Connection);
                command.Parameters.Add(new MySqlParameter("OBJID", MySqlDbType.Int64));
                command.Parameters.Add(new MySqlParameter("TFV", MySqlDbType.Int32));

                //Valorize parameters
                command.Parameters["OBJID"].Value = objectResource.PatternMemberID;
                command.Parameters["TFV"].Value = RDFModelEnums.RDFTripleFlavors.SPO;

                try
                {
                    //Open connection
                    this.Connection.Open();

                    //Prepare command
                    command.Prepare();

                    //Open transaction
                    command.Transaction = this.Connection.BeginTransaction();

                    //Execute command
                    command.ExecuteNonQuery();

                    //Close transaction
                    command.Transaction.Commit();

                    //Close connection
                    this.Connection.Close();
                }
                catch (Exception ex)
                {
                    //Rollback transaction
                    command.Transaction.Rollback();

                    //Close connection
                    this.Connection.Close();

                    //Propagate exception
                    throw new RDFStoreException("Cannot delete data from MySQL store because: " + ex.Message, ex);
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
                MySqlCommand command = new MySqlCommand("DELETE FROM Quadruples WHERE ObjectID = @OBJID AND TripleFlavor = @TFV", this.Connection);
                command.Parameters.Add(new MySqlParameter("OBJID", MySqlDbType.Int64));
                command.Parameters.Add(new MySqlParameter("TFV", MySqlDbType.Int32));

                //Valorize parameters
                command.Parameters["OBJID"].Value = literalObject.PatternMemberID;
                command.Parameters["TFV"].Value = RDFModelEnums.RDFTripleFlavors.SPL;

                try
                {
                    //Open connection
                    this.Connection.Open();

                    //Prepare command
                    command.Prepare();

                    //Open transaction
                    command.Transaction = this.Connection.BeginTransaction();

                    //Execute command
                    command.ExecuteNonQuery();

                    //Close transaction
                    command.Transaction.Commit();

                    //Close connection
                    this.Connection.Close();
                }
                catch (Exception ex)
                {
                    //Rollback transaction
                    command.Transaction.Rollback();

                    //Close connection
                    this.Connection.Close();

                    //Propagate exception
                    throw new RDFStoreException("Cannot delete data from MySQL store because: " + ex.Message, ex);
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
                MySqlCommand command = new MySqlCommand("DELETE FROM Quadruples WHERE ContextID = @CTXID AND SubjectID = @SUBJID", this.Connection);
                command.Parameters.Add(new MySqlParameter("CTXID", MySqlDbType.Int64));
                command.Parameters.Add(new MySqlParameter("SUBJID", MySqlDbType.Int64));

                //Valorize parameters
                command.Parameters["CTXID"].Value = contextResource.PatternMemberID;
                command.Parameters["SUBJID"].Value = subjectResource.PatternMemberID;

                try
                {
                    //Open connection
                    this.Connection.Open();

                    //Prepare command
                    command.Prepare();

                    //Open transaction
                    command.Transaction = this.Connection.BeginTransaction();

                    //Execute command
                    command.ExecuteNonQuery();

                    //Close transaction
                    command.Transaction.Commit();

                    //Close connection
                    this.Connection.Close();
                }
                catch (Exception ex)
                {
                    //Rollback transaction
                    command.Transaction.Rollback();

                    //Close connection
                    this.Connection.Close();

                    //Propagate exception
                    throw new RDFStoreException("Cannot delete data from MySQL store because: " + ex.Message, ex);
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
                MySqlCommand command = new MySqlCommand("DELETE FROM Quadruples WHERE ContextID = @CTXID AND PredicateID = @PREDID", this.Connection);
                command.Parameters.Add(new MySqlParameter("CTXID", MySqlDbType.Int64));
                command.Parameters.Add(new MySqlParameter("PREDID", MySqlDbType.Int64));

                //Valorize parameters
                command.Parameters["CTXID"].Value = contextResource.PatternMemberID;
                command.Parameters["PREDID"].Value = predicateResource.PatternMemberID;

                try
                {
                    //Open connection
                    this.Connection.Open();

                    //Prepare command
                    command.Prepare();

                    //Open transaction
                    command.Transaction = this.Connection.BeginTransaction();

                    //Execute command
                    command.ExecuteNonQuery();

                    //Close transaction
                    command.Transaction.Commit();

                    //Close connection
                    this.Connection.Close();
                }
                catch (Exception ex)
                {
                    //Rollback transaction
                    command.Transaction.Rollback();

                    //Close connection
                    this.Connection.Close();

                    //Propagate exception
                    throw new RDFStoreException("Cannot delete data from MySQL store because: " + ex.Message, ex);
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
                MySqlCommand command = new MySqlCommand("DELETE FROM Quadruples WHERE ContextID = @CTXID AND ObjectID = @OBJID AND TripleFlavor = @TFV", this.Connection);
                command.Parameters.Add(new MySqlParameter("CTXID", MySqlDbType.Int64));
                command.Parameters.Add(new MySqlParameter("OBJID", MySqlDbType.Int64));
                command.Parameters.Add(new MySqlParameter("TFV", MySqlDbType.Int32));

                //Valorize parameters
                command.Parameters["CTXID"].Value = contextResource.PatternMemberID;
                command.Parameters["OBJID"].Value = objectResource.PatternMemberID;
                command.Parameters["TFV"].Value = RDFModelEnums.RDFTripleFlavors.SPO;

                try
                {
                    //Open connection
                    this.Connection.Open();

                    //Prepare command
                    command.Prepare();

                    //Open transaction
                    command.Transaction = this.Connection.BeginTransaction();

                    //Execute command
                    command.ExecuteNonQuery();

                    //Close transaction
                    command.Transaction.Commit();

                    //Close connection
                    this.Connection.Close();
                }
                catch (Exception ex)
                {
                    //Rollback transaction
                    command.Transaction.Rollback();

                    //Close connection
                    this.Connection.Close();

                    //Propagate exception
                    throw new RDFStoreException("Cannot delete data from MySQL store because: " + ex.Message, ex);
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
                MySqlCommand command = new MySqlCommand("DELETE FROM Quadruples WHERE ContextID = @CTXID AND ObjectID = @OBJID AND TripleFlavor = @TFV", this.Connection);
                command.Parameters.Add(new MySqlParameter("CTXID", MySqlDbType.Int64));
                command.Parameters.Add(new MySqlParameter("OBJID", MySqlDbType.Int64));
                command.Parameters.Add(new MySqlParameter("TFV", MySqlDbType.Int32));

                //Valorize parameters
                command.Parameters["CTXID"].Value = contextResource.PatternMemberID;
                command.Parameters["OBJID"].Value = objectLiteral.PatternMemberID;
                command.Parameters["TFV"].Value = RDFModelEnums.RDFTripleFlavors.SPL;

                try
                {
                    //Open connection
                    this.Connection.Open();

                    //Prepare command
                    command.Prepare();

                    //Open transaction
                    command.Transaction = this.Connection.BeginTransaction();

                    //Execute command
                    command.ExecuteNonQuery();

                    //Close transaction
                    command.Transaction.Commit();

                    //Close connection
                    this.Connection.Close();
                }
                catch (Exception ex)
                {
                    //Rollback transaction
                    command.Transaction.Rollback();

                    //Close connection
                    this.Connection.Close();

                    //Propagate exception
                    throw new RDFStoreException("Cannot delete data from MySQL store because: " + ex.Message, ex);
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
                MySqlCommand command = new MySqlCommand("DELETE FROM Quadruples WHERE ContextID = @CTXID AND SubjectID = @SUBJID AND PredicateID = @PREDID", this.Connection);
                command.Parameters.Add(new MySqlParameter("CTXID", MySqlDbType.Int64));
                command.Parameters.Add(new MySqlParameter("SUBJID", MySqlDbType.Int64));
                command.Parameters.Add(new MySqlParameter("PREDID", MySqlDbType.Int64));

                //Valorize parameters
                command.Parameters["CTXID"].Value = contextResource.PatternMemberID;
                command.Parameters["SUBJID"].Value = subjectResource.PatternMemberID;
                command.Parameters["PREDID"].Value = predicateResource.PatternMemberID;

                try
                {
                    //Open connection
                    this.Connection.Open();

                    //Prepare command
                    command.Prepare();

                    //Open transaction
                    command.Transaction = this.Connection.BeginTransaction();

                    //Execute command
                    command.ExecuteNonQuery();

                    //Close transaction
                    command.Transaction.Commit();

                    //Close connection
                    this.Connection.Close();
                }
                catch (Exception ex)
                {
                    //Rollback transaction
                    command.Transaction.Rollback();

                    //Close connection
                    this.Connection.Close();

                    //Propagate exception
                    throw new RDFStoreException("Cannot delete data from MySQL store because: " + ex.Message, ex);
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
                MySqlCommand command = new MySqlCommand("DELETE FROM Quadruples WHERE ContextID = @CTXID AND SubjectID = @SUBJID AND ObjectID = @OBJID AND TripleFlavor = @TFV", this.Connection);
                command.Parameters.Add(new MySqlParameter("CTXID", MySqlDbType.Int64));
                command.Parameters.Add(new MySqlParameter("SUBJID", MySqlDbType.Int64));
                command.Parameters.Add(new MySqlParameter("OBJID", MySqlDbType.Int64));
                command.Parameters.Add(new MySqlParameter("TFV", MySqlDbType.Int32));

                //Valorize parameters
                command.Parameters["CTXID"].Value = contextResource.PatternMemberID;
                command.Parameters["SUBJID"].Value = subjectResource.PatternMemberID;
                command.Parameters["OBJID"].Value = objectResource.PatternMemberID;
                command.Parameters["TFV"].Value = RDFModelEnums.RDFTripleFlavors.SPO;

                try
                {
                    //Open connection
                    this.Connection.Open();

                    //Prepare command
                    command.Prepare();

                    //Open transaction
                    command.Transaction = this.Connection.BeginTransaction();

                    //Execute command
                    command.ExecuteNonQuery();

                    //Close transaction
                    command.Transaction.Commit();

                    //Close connection
                    this.Connection.Close();
                }
                catch (Exception ex)
                {
                    //Rollback transaction
                    command.Transaction.Rollback();

                    //Close connection
                    this.Connection.Close();

                    //Propagate exception
                    throw new RDFStoreException("Cannot delete data from MySQL store because: " + ex.Message, ex);
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
                MySqlCommand command = new MySqlCommand("DELETE FROM Quadruples WHERE ContextID = @CTXID AND SubjectID = @SUBJID AND ObjectID = @OBJID AND TripleFlavor = @TFV", this.Connection);
                command.Parameters.Add(new MySqlParameter("CTXID", MySqlDbType.Int64));
                command.Parameters.Add(new MySqlParameter("SUBJID", MySqlDbType.Int64));
                command.Parameters.Add(new MySqlParameter("OBJID", MySqlDbType.Int64));
                command.Parameters.Add(new MySqlParameter("TFV", MySqlDbType.Int32));

                //Valorize parameters
                command.Parameters["CTXID"].Value = contextResource.PatternMemberID;
                command.Parameters["SUBJID"].Value = subjectResource.PatternMemberID;
                command.Parameters["OBJID"].Value = objectLiteral.PatternMemberID;
                command.Parameters["TFV"].Value = RDFModelEnums.RDFTripleFlavors.SPL;

                try
                {
                    //Open connection
                    this.Connection.Open();

                    //Prepare command
                    command.Prepare();

                    //Open transaction
                    command.Transaction = this.Connection.BeginTransaction();

                    //Execute command
                    command.ExecuteNonQuery();

                    //Close transaction
                    command.Transaction.Commit();

                    //Close connection
                    this.Connection.Close();
                }
                catch (Exception ex)
                {
                    //Rollback transaction
                    command.Transaction.Rollback();

                    //Close connection
                    this.Connection.Close();

                    //Propagate exception
                    throw new RDFStoreException("Cannot delete data from MySQL store because: " + ex.Message, ex);
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
                MySqlCommand command = new MySqlCommand("DELETE FROM Quadruples WHERE ContextID = @CTXID AND PredicateID = @PREDID AND ObjectID = @OBJID AND TripleFlavor = @TFV", this.Connection);
                command.Parameters.Add(new MySqlParameter("CTXID", MySqlDbType.Int64));
                command.Parameters.Add(new MySqlParameter("PREDID", MySqlDbType.Int64));
                command.Parameters.Add(new MySqlParameter("OBJID", MySqlDbType.Int64));
                command.Parameters.Add(new MySqlParameter("TFV", MySqlDbType.Int32));

                //Valorize parameters
                command.Parameters["CTXID"].Value = contextResource.PatternMemberID;
                command.Parameters["PREDID"].Value = predicateResource.PatternMemberID;
                command.Parameters["OBJID"].Value = objectResource.PatternMemberID;
                command.Parameters["TFV"].Value = RDFModelEnums.RDFTripleFlavors.SPO;

                try
                {
                    //Open connection
                    this.Connection.Open();

                    //Prepare command
                    command.Prepare();

                    //Open transaction
                    command.Transaction = this.Connection.BeginTransaction();

                    //Execute command
                    command.ExecuteNonQuery();

                    //Close transaction
                    command.Transaction.Commit();

                    //Close connection
                    this.Connection.Close();
                }
                catch (Exception ex)
                {
                    //Rollback transaction
                    command.Transaction.Rollback();

                    //Close connection
                    this.Connection.Close();

                    //Propagate exception
                    throw new RDFStoreException("Cannot delete data from MySQL store because: " + ex.Message, ex);
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
                MySqlCommand command = new MySqlCommand("DELETE FROM Quadruples WHERE ContextID = @CTXID AND PredicateID = @PREDID AND ObjectID = @OBJID AND TripleFlavor = @TFV", this.Connection);
                command.Parameters.Add(new MySqlParameter("CTXID", MySqlDbType.Int64));
                command.Parameters.Add(new MySqlParameter("PREDID", MySqlDbType.Int64));
                command.Parameters.Add(new MySqlParameter("OBJID", MySqlDbType.Int64));
                command.Parameters.Add(new MySqlParameter("TFV", MySqlDbType.Int32));

                //Valorize parameters
                command.Parameters["CTXID"].Value = contextResource.PatternMemberID;
                command.Parameters["PREDID"].Value = predicateResource.PatternMemberID;
                command.Parameters["OBJID"].Value = objectLiteral.PatternMemberID;
                command.Parameters["TFV"].Value = RDFModelEnums.RDFTripleFlavors.SPL;

                try
                {
                    //Open connection
                    this.Connection.Open();

                    //Prepare command
                    command.Prepare();

                    //Open transaction
                    command.Transaction = this.Connection.BeginTransaction();

                    //Execute command
                    command.ExecuteNonQuery();

                    //Close transaction
                    command.Transaction.Commit();

                    //Close connection
                    this.Connection.Close();
                }
                catch (Exception ex)
                {
                    //Rollback transaction
                    command.Transaction.Rollback();

                    //Close connection
                    this.Connection.Close();

                    //Propagate exception
                    throw new RDFStoreException("Cannot delete data from MySQL store because: " + ex.Message, ex);
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
                MySqlCommand command = new MySqlCommand("DELETE FROM Quadruples WHERE SubjectID = @SUBJID AND PredicateID = @PREDID", this.Connection);
                command.Parameters.Add(new MySqlParameter("SUBJID", MySqlDbType.Int64));
                command.Parameters.Add(new MySqlParameter("PREDID", MySqlDbType.Int64));

                //Valorize parameters
                command.Parameters["SUBJID"].Value = subjectResource.PatternMemberID;
                command.Parameters["PREDID"].Value = predicateResource.PatternMemberID;

                try
                {
                    //Open connection
                    this.Connection.Open();

                    //Prepare command
                    command.Prepare();

                    //Open transaction
                    command.Transaction = this.Connection.BeginTransaction();

                    //Execute command
                    command.ExecuteNonQuery();

                    //Close transaction
                    command.Transaction.Commit();

                    //Close connection
                    this.Connection.Close();
                }
                catch (Exception ex)
                {
                    //Rollback transaction
                    command.Transaction.Rollback();

                    //Close connection
                    this.Connection.Close();

                    //Propagate exception
                    throw new RDFStoreException("Cannot delete data from MySQL store because: " + ex.Message, ex);
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
                MySqlCommand command = new MySqlCommand("DELETE FROM Quadruples WHERE SubjectID = @SUBJID AND ObjectID = @OBJID AND TripleFlavor = @TFV", this.Connection);
                command.Parameters.Add(new MySqlParameter("SUBJID", MySqlDbType.Int64));
                command.Parameters.Add(new MySqlParameter("OBJID", MySqlDbType.Int64));
                command.Parameters.Add(new MySqlParameter("TFV", MySqlDbType.Int32));

                //Valorize parameters
                command.Parameters["SUBJID"].Value = subjectResource.PatternMemberID;
                command.Parameters["OBJID"].Value = objectResource.PatternMemberID;
                command.Parameters["TFV"].Value = RDFModelEnums.RDFTripleFlavors.SPO;

                try
                {
                    //Open connection
                    this.Connection.Open();

                    //Prepare command
                    command.Prepare();

                    //Open transaction
                    command.Transaction = this.Connection.BeginTransaction();

                    //Execute command
                    command.ExecuteNonQuery();

                    //Close transaction
                    command.Transaction.Commit();

                    //Close connection
                    this.Connection.Close();
                }
                catch (Exception ex)
                {
                    //Rollback transaction
                    command.Transaction.Rollback();

                    //Close connection
                    this.Connection.Close();

                    //Propagate exception
                    throw new RDFStoreException("Cannot delete data from MySQL store because: " + ex.Message, ex);
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
                MySqlCommand command = new MySqlCommand("DELETE FROM Quadruples WHERE SubjectID = @SUBJID AND ObjectID = @OBJID AND TripleFlavor = @TFV", this.Connection);
                command.Parameters.Add(new MySqlParameter("SUBJID", MySqlDbType.Int64));
                command.Parameters.Add(new MySqlParameter("OBJID", MySqlDbType.Int64));
                command.Parameters.Add(new MySqlParameter("TFV", MySqlDbType.Int32));

                //Valorize parameters
                command.Parameters["SUBJID"].Value = subjectResource.PatternMemberID;
                command.Parameters["OBJID"].Value = objectLiteral.PatternMemberID;
                command.Parameters["TFV"].Value = RDFModelEnums.RDFTripleFlavors.SPL;

                try
                {
                    //Open connection
                    this.Connection.Open();

                    //Prepare command
                    command.Prepare();

                    //Open transaction
                    command.Transaction = this.Connection.BeginTransaction();

                    //Execute command
                    command.ExecuteNonQuery();

                    //Close transaction
                    command.Transaction.Commit();

                    //Close connection
                    this.Connection.Close();
                }
                catch (Exception ex)
                {
                    //Rollback transaction
                    command.Transaction.Rollback();

                    //Close connection
                    this.Connection.Close();

                    //Propagate exception
                    throw new RDFStoreException("Cannot delete data from MySQL store because: " + ex.Message, ex);
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
                MySqlCommand command = new MySqlCommand("DELETE FROM Quadruples WHERE PredicateID = @PREDID AND ObjectID = @OBJID AND TripleFlavor = @TFV", this.Connection);
                command.Parameters.Add(new MySqlParameter("PREDID", MySqlDbType.Int64));
                command.Parameters.Add(new MySqlParameter("OBJID", MySqlDbType.Int64));
                command.Parameters.Add(new MySqlParameter("TFV", MySqlDbType.Int32));

                //Valorize parameters
                command.Parameters["PREDID"].Value = predicateResource.PatternMemberID;
                command.Parameters["OBJID"].Value = objectResource.PatternMemberID;
                command.Parameters["TFV"].Value = RDFModelEnums.RDFTripleFlavors.SPO;

                try
                {
                    //Open connection
                    this.Connection.Open();

                    //Prepare command
                    command.Prepare();

                    //Open transaction
                    command.Transaction = this.Connection.BeginTransaction();

                    //Execute command
                    command.ExecuteNonQuery();

                    //Close transaction
                    command.Transaction.Commit();

                    //Close connection
                    this.Connection.Close();
                }
                catch (Exception ex)
                {
                    //Rollback transaction
                    command.Transaction.Rollback();

                    //Close connection
                    this.Connection.Close();

                    //Propagate exception
                    throw new RDFStoreException("Cannot delete data from MySQL store because: " + ex.Message, ex);
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
                MySqlCommand command = new MySqlCommand("DELETE FROM Quadruples WHERE PredicateID = @PREDID AND ObjectID = @OBJID AND TripleFlavor = @TFV", this.Connection);
                command.Parameters.Add(new MySqlParameter("PREDID", MySqlDbType.Int64));
                command.Parameters.Add(new MySqlParameter("OBJID", MySqlDbType.Int64));
                command.Parameters.Add(new MySqlParameter("TFV", MySqlDbType.Int32));

                //Valorize parameters
                command.Parameters["PREDID"].Value = predicateResource.PatternMemberID;
                command.Parameters["OBJID"].Value = objectLiteral.PatternMemberID;
                command.Parameters["TFV"].Value = RDFModelEnums.RDFTripleFlavors.SPL;

                try
                {
                    //Open connection
                    this.Connection.Open();

                    //Prepare command
                    command.Prepare();

                    //Open transaction
                    command.Transaction = this.Connection.BeginTransaction();

                    //Execute command
                    command.ExecuteNonQuery();

                    //Close transaction
                    command.Transaction.Commit();

                    //Close connection
                    this.Connection.Close();
                }
                catch (Exception ex)
                {
                    //Rollback transaction
                    command.Transaction.Rollback();

                    //Close connection
                    this.Connection.Close();

                    //Propagate exception
                    throw new RDFStoreException("Cannot delete data from MySQL store because: " + ex.Message, ex);
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
            MySqlCommand command = new MySqlCommand("DELETE FROM Quadruples", this.Connection);

            try
            {
                //Open connection
                this.Connection.Open();

                //Prepare command
                command.Prepare();

                //Open transaction
                command.Transaction = this.Connection.BeginTransaction();

                //Execute command
                command.ExecuteNonQuery();

                //Close transaction
                command.Transaction.Commit();

                //Close connection
                this.Connection.Close();
            }
            catch (Exception ex)
            {
                //Rollback transaction
                command.Transaction.Rollback();

                //Close connection
                this.Connection.Close();

                //Propagate exception
                throw new RDFStoreException("Cannot delete data from MySQL store because: " + ex.Message, ex);
            }
        }
        #endregion

        #region Select
        /// <summary>
        /// Gets a memory store containing quadruples satisfying the given pattern
        /// </summary>
        internal override RDFMemoryStore SelectQuadruples(RDFContext ctx, RDFResource subj, RDFResource pred, RDFResource obj, RDFLiteral lit)
        {
            RDFMemoryStore result = new RDFMemoryStore();
            MySqlCommand command = null;

            //Intersect the filters
            if (ctx != null)
            {
                if (subj != null)
                {
                    if (pred != null)
                    {
                        if (obj != null)
                        {
                            //C->S->P->O
                            command = new MySqlCommand("SELECT TripleFlavor, Context, Subject, Predicate, Object FROM Quadruples WHERE ContextID = @CTXID AND SubjectID = @SUBJID AND PredicateID = @PREDID AND ObjectID = @OBJID AND TripleFlavor = @TFV", this.Connection);
                            command.Parameters.Add(new MySqlParameter("TFV", MySqlDbType.Int32));
                            command.Parameters.Add(new MySqlParameter("CTXID", MySqlDbType.Int64));
                            command.Parameters.Add(new MySqlParameter("SUBJID", MySqlDbType.Int64));
                            command.Parameters.Add(new MySqlParameter("PREDID", MySqlDbType.Int64));
                            command.Parameters.Add(new MySqlParameter("OBJID", MySqlDbType.Int64));
                            command.Parameters["TFV"].Value = RDFModelEnums.RDFTripleFlavors.SPO;
                            command.Parameters["CTXID"].Value = ctx.PatternMemberID;
                            command.Parameters["SUBJID"].Value = subj.PatternMemberID;
                            command.Parameters["PREDID"].Value = pred.PatternMemberID;
                            command.Parameters["OBJID"].Value = obj.PatternMemberID;
                        }
                        else
                        {
                            if (lit != null)
                            {
                                //C->S->P->L
                                command = new MySqlCommand("SELECT TripleFlavor, Context, Subject, Predicate, Object FROM Quadruples WHERE ContextID = @CTXID AND SubjectID = @SUBJID AND PredicateID = @PREDID AND ObjectID = @OBJID AND TripleFlavor = @TFV", this.Connection);
                                command.Parameters.Add(new MySqlParameter("TFV", MySqlDbType.Int32));
                                command.Parameters.Add(new MySqlParameter("CTXID", MySqlDbType.Int64));
                                command.Parameters.Add(new MySqlParameter("SUBJID", MySqlDbType.Int64));
                                command.Parameters.Add(new MySqlParameter("PREDID", MySqlDbType.Int64));
                                command.Parameters.Add(new MySqlParameter("OBJID", MySqlDbType.Int64));
                                command.Parameters["TFV"].Value = RDFModelEnums.RDFTripleFlavors.SPL;
                                command.Parameters["CTXID"].Value = ctx.PatternMemberID;
                                command.Parameters["SUBJID"].Value = subj.PatternMemberID;
                                command.Parameters["PREDID"].Value = pred.PatternMemberID;
                                command.Parameters["OBJID"].Value = lit.PatternMemberID;
                            }
                            else
                            {
                                //C->S->P->
                                command = new MySqlCommand("SELECT TripleFlavor, Context, Subject, Predicate, Object FROM Quadruples WHERE ContextID = @CTXID AND SubjectID = @SUBJID AND PredicateID = @PREDID", this.Connection);
                                command.Parameters.Add(new MySqlParameter("CTXID", MySqlDbType.Int64));
                                command.Parameters.Add(new MySqlParameter("SUBJID", MySqlDbType.Int64));
                                command.Parameters.Add(new MySqlParameter("PREDID", MySqlDbType.Int64));
                                command.Parameters["CTXID"].Value = ctx.PatternMemberID;
                                command.Parameters["SUBJID"].Value = subj.PatternMemberID;
                                command.Parameters["PREDID"].Value = pred.PatternMemberID;
                            }
                        }
                    }
                    else
                    {
                        if (obj != null)
                        {
                            //C->S->->O
                            command = new MySqlCommand("SELECT TripleFlavor, Context, Subject, Predicate, Object FROM Quadruples WHERE ContextID = @CTXID AND SubjectID = @SUBJID AND ObjectID = @OBJID AND TripleFlavor = @TFV", this.Connection);
                            command.Parameters.Add(new MySqlParameter("TFV", MySqlDbType.Int32));
                            command.Parameters.Add(new MySqlParameter("CTXID", MySqlDbType.Int64));
                            command.Parameters.Add(new MySqlParameter("SUBJID", MySqlDbType.Int64));
                            command.Parameters.Add(new MySqlParameter("OBJID", MySqlDbType.Int64));
                            command.Parameters["TFV"].Value = RDFModelEnums.RDFTripleFlavors.SPO;
                            command.Parameters["CTXID"].Value = ctx.PatternMemberID;
                            command.Parameters["SUBJID"].Value = subj.PatternMemberID;
                            command.Parameters["OBJID"].Value = obj.PatternMemberID;
                        }
                        else
                        {
                            if (lit != null)
                            {
                                //C->S->->L
                                command = new MySqlCommand("SELECT TripleFlavor, Context, Subject, Predicate, Object FROM Quadruples WHERE ContextID = @CTXID AND SubjectID = @SUBJID AND ObjectID = @OBJID AND TripleFlavor = @TFV", this.Connection);
                                command.Parameters.Add(new MySqlParameter("TFV", MySqlDbType.Int32));
                                command.Parameters.Add(new MySqlParameter("CTXID", MySqlDbType.Int64));
                                command.Parameters.Add(new MySqlParameter("SUBJID", MySqlDbType.Int64));
                                command.Parameters.Add(new MySqlParameter("OBJID", MySqlDbType.Int64));
                                command.Parameters["TFV"].Value = RDFModelEnums.RDFTripleFlavors.SPL;
                                command.Parameters["CTXID"].Value = ctx.PatternMemberID;
                                command.Parameters["SUBJID"].Value = subj.PatternMemberID;
                                command.Parameters["OBJID"].Value = lit.PatternMemberID;
                            }
                            else
                            {
                                //C->S->->
                                command = new MySqlCommand("SELECT TripleFlavor, Context, Subject, Predicate, Object FROM Quadruples WHERE ContextID = @CTXID AND SubjectID = @SUBJID", this.Connection);
                                command.Parameters.Add(new MySqlParameter("CTXID", MySqlDbType.Int64));
                                command.Parameters.Add(new MySqlParameter("SUBJID", MySqlDbType.Int64));
                                command.Parameters["CTXID"].Value = ctx.PatternMemberID;
                                command.Parameters["SUBJID"].Value = subj.PatternMemberID;
                            }
                        }
                    }
                }
                else
                {
                    if (pred != null)
                    {
                        if (obj != null)
                        {
                            //C->->P->O
                            command = new MySqlCommand("SELECT TripleFlavor, Context, Subject, Predicate, Object FROM Quadruples WHERE ContextID = @CTXID AND PredicateID = @PREDID AND ObjectID = @OBJID AND TripleFlavor = @TFV", this.Connection);
                            command.Parameters.Add(new MySqlParameter("TFV", MySqlDbType.Int32));
                            command.Parameters.Add(new MySqlParameter("CTXID", MySqlDbType.Int64));
                            command.Parameters.Add(new MySqlParameter("PREDID", MySqlDbType.Int64));
                            command.Parameters.Add(new MySqlParameter("OBJID", MySqlDbType.Int64));
                            command.Parameters["TFV"].Value = RDFModelEnums.RDFTripleFlavors.SPO;
                            command.Parameters["CTXID"].Value = ctx.PatternMemberID;
                            command.Parameters["PREDID"].Value = pred.PatternMemberID;
                            command.Parameters["OBJID"].Value = obj.PatternMemberID;
                        }
                        else
                        {
                            if (lit != null)
                            {
                                //C->->P->L
                                command = new MySqlCommand("SELECT TripleFlavor, Context, Subject, Predicate, Object FROM Quadruples WHERE ContextID = @CTXID AND PredicateID = @PREDID AND ObjectID = @OBJID AND TripleFlavor = @TFV", this.Connection);
                                command.Parameters.Add(new MySqlParameter("TFV", MySqlDbType.Int32));
                                command.Parameters.Add(new MySqlParameter("CTXID", MySqlDbType.Int64));
                                command.Parameters.Add(new MySqlParameter("PREDID", MySqlDbType.Int64));
                                command.Parameters.Add(new MySqlParameter("OBJID", MySqlDbType.Int64));
                                command.Parameters["TFV"].Value = RDFModelEnums.RDFTripleFlavors.SPL;
                                command.Parameters["CTXID"].Value = ctx.PatternMemberID;
                                command.Parameters["PREDID"].Value = pred.PatternMemberID;
                                command.Parameters["OBJID"].Value = lit.PatternMemberID;
                            }
                            else
                            {
                                //C->->P->
                                command = new MySqlCommand("SELECT TripleFlavor, Context, Subject, Predicate, Object FROM Quadruples WHERE ContextID = @CTXID AND PredicateID = @PREDID", this.Connection);
                                command.Parameters.Add(new MySqlParameter("CTXID", MySqlDbType.Int64));
                                command.Parameters.Add(new MySqlParameter("PREDID", MySqlDbType.Int64));
                                command.Parameters["CTXID"].Value = ctx.PatternMemberID;
                                command.Parameters["PREDID"].Value = pred.PatternMemberID;
                            }
                        }
                    }
                    else
                    {
                        if (obj != null)
                        {
                            //C->->->O
                            command = new MySqlCommand("SELECT TripleFlavor, Context, Subject, Predicate, Object FROM Quadruples WHERE ContextID = @CTXID AND ObjectID = @OBJID AND TripleFlavor = @TFV", this.Connection);
                            command.Parameters.Add(new MySqlParameter("TFV", MySqlDbType.Int32));
                            command.Parameters.Add(new MySqlParameter("CTXID", MySqlDbType.Int64));
                            command.Parameters.Add(new MySqlParameter("OBJID", MySqlDbType.Int64));
                            command.Parameters["TFV"].Value = RDFModelEnums.RDFTripleFlavors.SPO;
                            command.Parameters["CTXID"].Value = ctx.PatternMemberID;
                            command.Parameters["OBJID"].Value = obj.PatternMemberID;
                        }
                        else
                        {
                            if (lit != null)
                            {
                                //C->->->L
                                command = new MySqlCommand("SELECT TripleFlavor, Context, Subject, Predicate, Object FROM Quadruples WHERE ContextID = @CTXID AND ObjectID = @OBJID AND TripleFlavor = @TFV", this.Connection);
                                command.Parameters.Add(new MySqlParameter("TFV", MySqlDbType.Int32));
                                command.Parameters.Add(new MySqlParameter("CTXID", MySqlDbType.Int64));
                                command.Parameters.Add(new MySqlParameter("OBJID", MySqlDbType.Int64));
                                command.Parameters["TFV"].Value = RDFModelEnums.RDFTripleFlavors.SPL;
                                command.Parameters["CTXID"].Value = ctx.PatternMemberID;
                                command.Parameters["OBJID"].Value = lit.PatternMemberID;
                            }
                            else
                            {
                                //C->->->
                                command = new MySqlCommand("SELECT TripleFlavor, Context, Subject, Predicate, Object FROM Quadruples WHERE ContextID = @CTXID", this.Connection);
                                command.Parameters.Add(new MySqlParameter("CTXID", MySqlDbType.Int64));
                                command.Parameters["CTXID"].Value = ctx.PatternMemberID;
                            }
                        }
                    }
                }
            }
            else
            {
                if (subj != null)
                {
                    if (pred != null)
                    {
                        if (obj != null)
                        {
                            //->S->P->O
                            command = new MySqlCommand("SELECT TripleFlavor, Context, Subject, Predicate, Object FROM Quadruples WHERE SubjectID = @SUBJID AND PredicateID = @PREDID AND ObjectID = @OBJID AND TripleFlavor = @TFV", this.Connection);
                            command.Parameters.Add(new MySqlParameter("TFV", MySqlDbType.Int32));
                            command.Parameters.Add(new MySqlParameter("SUBJID", MySqlDbType.Int64));
                            command.Parameters.Add(new MySqlParameter("PREDID", MySqlDbType.Int64));
                            command.Parameters.Add(new MySqlParameter("OBJID", MySqlDbType.Int64));
                            command.Parameters["TFV"].Value = RDFModelEnums.RDFTripleFlavors.SPO;
                            command.Parameters["SUBJID"].Value = subj.PatternMemberID;
                            command.Parameters["PREDID"].Value = pred.PatternMemberID;
                            command.Parameters["OBJID"].Value = obj.PatternMemberID;
                        }
                        else
                        {
                            if (lit != null)
                            {
                                //->S->P->L
                                command = new MySqlCommand("SELECT TripleFlavor, Context, Subject, Predicate, Object FROM Quadruples WHERE SubjectID = @SUBJID AND PredicateID = @PREDID AND ObjectID = @OBJID AND TripleFlavor = @TFV", this.Connection);
                                command.Parameters.Add(new MySqlParameter("TFV", MySqlDbType.Int32));
                                command.Parameters.Add(new MySqlParameter("SUBJID", MySqlDbType.Int64));
                                command.Parameters.Add(new MySqlParameter("PREDID", MySqlDbType.Int64));
                                command.Parameters.Add(new MySqlParameter("OBJID", MySqlDbType.Int64));
                                command.Parameters["TFV"].Value = RDFModelEnums.RDFTripleFlavors.SPL;
                                command.Parameters["SUBJID"].Value = subj.PatternMemberID;
                                command.Parameters["PREDID"].Value = pred.PatternMemberID;
                                command.Parameters["OBJID"].Value = lit.PatternMemberID;
                            }
                            else
                            {
                                //->S->P->
                                command = new MySqlCommand("SELECT TripleFlavor, Context, Subject, Predicate, Object FROM Quadruples WHERE SubjectID = @SUBJID AND PredicateID = @PREDID", this.Connection);
                                command.Parameters.Add(new MySqlParameter("SUBJID", MySqlDbType.Int64));
                                command.Parameters.Add(new MySqlParameter("PREDID", MySqlDbType.Int64));
                                command.Parameters["SUBJID"].Value = subj.PatternMemberID;
                                command.Parameters["PREDID"].Value = pred.PatternMemberID;
                            }
                        }
                    }
                    else
                    {
                        if (obj != null)
                        {
                            //->S->->O
                            command = new MySqlCommand("SELECT TripleFlavor, Context, Subject, Predicate, Object FROM Quadruples WHERE SubjectID = @SUBJID AND ObjectID = @OBJID AND TripleFlavor = @TFV", this.Connection);
                            command.Parameters.Add(new MySqlParameter("TFV", MySqlDbType.Int32));
                            command.Parameters.Add(new MySqlParameter("SUBJID", MySqlDbType.Int64));
                            command.Parameters.Add(new MySqlParameter("OBJID", MySqlDbType.Int64));
                            command.Parameters["TFV"].Value = RDFModelEnums.RDFTripleFlavors.SPO;
                            command.Parameters["SUBJID"].Value = subj.PatternMemberID;
                            command.Parameters["OBJID"].Value = obj.PatternMemberID;
                        }
                        else
                        {
                            if (lit != null)
                            {
                                //->S->->L
                                command = new MySqlCommand("SELECT TripleFlavor, Context, Subject, Predicate, Object FROM Quadruples WHERE SubjectID = @SUBJID AND ObjectID = @OBJID AND TripleFlavor = @TFV", this.Connection);
                                command.Parameters.Add(new MySqlParameter("TFV", MySqlDbType.Int32));
                                command.Parameters.Add(new MySqlParameter("SUBJID", MySqlDbType.Int64));
                                command.Parameters.Add(new MySqlParameter("OBJID", MySqlDbType.Int64));
                                command.Parameters["TFV"].Value = RDFModelEnums.RDFTripleFlavors.SPL;
                                command.Parameters["SUBJID"].Value = subj.PatternMemberID;
                                command.Parameters["OBJID"].Value = lit.PatternMemberID;
                            }
                            else
                            {
                                //->S->->
                                command = new MySqlCommand("SELECT TripleFlavor, Context, Subject, Predicate, Object FROM Quadruples WHERE SubjectID = @SUBJID", this.Connection);
                                command.Parameters.Add(new MySqlParameter("SUBJID", MySqlDbType.Int64));
                                command.Parameters["SUBJID"].Value = subj.PatternMemberID;
                            }
                        }
                    }
                }
                else
                {
                    if (pred != null)
                    {
                        if (obj != null)
                        {
                            //->->P->O
                            command = new MySqlCommand("SELECT TripleFlavor, Context, Subject, Predicate, Object FROM Quadruples WHERE PredicateID = @PREDID AND ObjectID = @OBJID AND TripleFlavor = @TFV", this.Connection);
                            command.Parameters.Add(new MySqlParameter("TFV", MySqlDbType.Int32));
                            command.Parameters.Add(new MySqlParameter("PREDID", MySqlDbType.Int64));
                            command.Parameters.Add(new MySqlParameter("OBJID", MySqlDbType.Int64));
                            command.Parameters["TFV"].Value = RDFModelEnums.RDFTripleFlavors.SPO;
                            command.Parameters["PREDID"].Value = pred.PatternMemberID;
                            command.Parameters["OBJID"].Value = obj.PatternMemberID;
                        }
                        else
                        {
                            if (lit != null)
                            {
                                //->->P->L
                                command = new MySqlCommand("SELECT TripleFlavor, Context, Subject, Predicate, Object FROM Quadruples WHERE PredicateID = @PREDID AND ObjectID = @OBJID AND TripleFlavor = @TFV", this.Connection);
                                command.Parameters.Add(new MySqlParameter("TFV", MySqlDbType.Int32));
                                command.Parameters.Add(new MySqlParameter("PREDID", MySqlDbType.Int64));
                                command.Parameters.Add(new MySqlParameter("OBJID", MySqlDbType.Int64));
                                command.Parameters["TFV"].Value = RDFModelEnums.RDFTripleFlavors.SPL;
                                command.Parameters["PREDID"].Value = pred.PatternMemberID;
                                command.Parameters["OBJID"].Value = lit.PatternMemberID;
                            }
                            else
                            {
                                //->->P->
                                command = new MySqlCommand("SELECT TripleFlavor, Context, Subject, Predicate, Object FROM Quadruples WHERE PredicateID = @PREDID", this.Connection);
                                command.Parameters.Add(new MySqlParameter("PREDID", MySqlDbType.Int64));
                                command.Parameters["PREDID"].Value = pred.PatternMemberID;
                            }
                        }
                    }
                    else
                    {
                        if (obj != null)
                        {
                            //->->->O
                            command = new MySqlCommand("SELECT TripleFlavor, Context, Subject, Predicate, Object FROM Quadruples WHERE ObjectID = @OBJID AND TripleFlavor = @TFV", this.Connection);
                            command.Parameters.Add(new MySqlParameter("TFV", MySqlDbType.Int32));
                            command.Parameters.Add(new MySqlParameter("OBJID", MySqlDbType.Int64));
                            command.Parameters["TFV"].Value = RDFModelEnums.RDFTripleFlavors.SPO;
                            command.Parameters["OBJID"].Value = obj.PatternMemberID;
                        }
                        else
                        {
                            if (lit != null)
                            {
                                //->->->L
                                command = new MySqlCommand("SELECT TripleFlavor, Context, Subject, Predicate, Object FROM Quadruples WHERE ObjectID = @OBJID AND TripleFlavor = @TFV", this.Connection);
                                command.Parameters.Add(new MySqlParameter("TFV", MySqlDbType.Int32));
                                command.Parameters.Add(new MySqlParameter("OBJID", MySqlDbType.Int64));
                                command.Parameters["TFV"].Value = RDFModelEnums.RDFTripleFlavors.SPL;
                                command.Parameters["OBJID"].Value = lit.PatternMemberID;
                            }
                            else
                            {
                                //->->->
                                command = new MySqlCommand("SELECT TripleFlavor, Context, Subject, Predicate, Object FROM Quadruples", this.Connection);
                            }
                        }
                    }
                }
            }

            //Prepare and execute command
            try
            {
                //Open connection
                this.Connection.Open();

                //Prepare command
                command.Prepare();

                //Set command timeout (3min)
                command.CommandTimeout = 180;

                //Execute command
                using (var quadruples = command.ExecuteReader())
                {
                    if (quadruples.HasRows)
                    {
                        while (quadruples.Read())
                        {
                            result.AddQuadruple(RDFStoreUtilities.ParseQuadruple(quadruples));
                        }
                    }
                }

                //Close connection
                this.Connection.Close();
            }
            catch (Exception ex)
            {
                //Close connection
                this.Connection.Close();

                //Propagate exception
                throw new RDFStoreException("Cannot read data from MySQL store because: " + ex.Message, ex);
            }

            return result;
        }
        #endregion

        #region Diagnostics
        /// <summary>
        /// Performs the preliminary diagnostics controls on the underlying MySQL database
        /// </summary>
        private RDFStoreEnums.RDFStoreSQLErrors Diagnostics()
        {
            try
            {
                //Open connection
                this.Connection.Open();

                //Create command
                MySqlCommand command = new MySqlCommand("SELECT COUNT(*) FROM information_schema.tables WHERE table_schema = '" + this.Connection.Database + "' AND table_name = 'Quadruples'", this.Connection);

                //Execute command
                int result = Int32.Parse(command.ExecuteScalar().ToString());

                //Close connection
                this.Connection.Close();

                //Return the diagnostics state
                if (result == 0)
                    return RDFStoreEnums.RDFStoreSQLErrors.QuadruplesTableNotFound;
                else
                    return RDFStoreEnums.RDFStoreSQLErrors.NoErrors;
            }
            catch
            {
                //Close connection
                this.Connection.Close();

                //Return the diagnostics state
                return RDFStoreEnums.RDFStoreSQLErrors.InvalidDataSource;
            }
        }

        /// <summary>
        /// Prepares the underlying MySQL database
        /// </summary>
        private void PrepareStore()
        {
            RDFStoreEnums.RDFStoreSQLErrors check = this.Diagnostics();

            //Prepare the database only if diagnostics has detected the missing of "Quadruples" table in the store
            if (check == RDFStoreEnums.RDFStoreSQLErrors.QuadruplesTableNotFound)
            {
                try
                {
                    //Open connection
                    this.Connection.Open();

                    //Create & Execute command
                    MySqlCommand command = new MySqlCommand("CREATE TABLE Quadruples (QuadrupleID BIGINT NOT NULL PRIMARY KEY, TripleFlavor INT NOT NULL, Context VARCHAR(1000) NOT NULL, ContextID BIGINT NOT NULL, Subject VARCHAR(1000) NOT NULL, SubjectID BIGINT NOT NULL, Predicate VARCHAR(1000) NOT NULL, PredicateID BIGINT NOT NULL, Object VARCHAR(1000) NOT NULL, ObjectID BIGINT NOT NULL) ENGINE=InnoDB;ALTER TABLE Quadruples ADD INDEX IDX_ContextID(ContextID);ALTER TABLE Quadruples ADD INDEX IDX_SubjectID(SubjectID);ALTER TABLE Quadruples ADD INDEX IDX_PredicateID(PredicateID);ALTER TABLE Quadruples ADD INDEX IDX_ObjectID(ObjectID,TripleFlavor);ALTER TABLE Quadruples ADD INDEX IDX_SubjectID_PredicateID(SubjectID,PredicateID);ALTER TABLE Quadruples ADD INDEX IDX_SubjectID_ObjectID(SubjectID,ObjectID,TripleFlavor);ALTER TABLE Quadruples ADD INDEX IDX_PredicateID_ObjectID(PredicateID,ObjectID,TripleFlavor);", this.Connection);
                    command.ExecuteNonQuery();

                    //Close connection
                    this.Connection.Close();
                }
                catch (Exception ex)
                {
                    //Close connection
                    this.Connection.Close();

                    //Propagate exception
                    throw new RDFStoreException("Cannot prepare MySQL store because: " + ex.Message, ex);
                }
            }

            //Otherwise, an exception must be thrown because it has not been possible to connect to the instance/database
            else if (check == RDFStoreEnums.RDFStoreSQLErrors.InvalidDataSource)
            {
                throw new RDFStoreException("Cannot prepare MySQL store because: unable to connect to the server instance or to open the selected database.");
            }
        }
        #endregion

        #region Optimize
        /// <summary>
        /// Executes a special command to optimize MySQL store
        /// </summary>
        public void OptimizeStore()
        {
            try
            {
                //Open connection
                this.Connection.Open();

                //Create command
                MySqlCommand command = new MySqlCommand("OPTIMIZE TABLE Quadruples", this.Connection);
                command.CommandTimeout = 120;

                //Execute command
                command.ExecuteNonQuery();

                //Close connection
                this.Connection.Close();
            }
            catch (Exception ex)
            {
                //Close connection
                this.Connection.Close();

                //Propagate exception
                throw new RDFStoreException("Cannot optimize MySQL store because: " + ex.Message, ex);
            }
        }
        #endregion

        #endregion
    }

}