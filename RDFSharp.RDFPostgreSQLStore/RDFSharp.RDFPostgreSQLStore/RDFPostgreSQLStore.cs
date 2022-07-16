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
using Npgsql;
using NpgsqlTypes;
using RDFSharp.Model;

namespace RDFSharp.Store
{
    /// <summary>
    /// RDFPostgreSQLStore represents a store backed on PostgreSQL engine
    /// </summary>
    public class RDFPostgreSQLStore : RDFStore, IDisposable
    {
        #region Properties
        /// <summary>
        /// Connection to the PostgreSQL database
        /// </summary>
        internal NpgsqlConnection Connection { get; set; }
        
        /// <summary>
        /// Flag indicating that the PostgreSQL store instance has already been disposed
        /// </summary>
        internal bool Disposed { get; set; }
        #endregion

        #region Ctors
        /// <summary>
        /// Default-ctor to build a PostgreSQL store instance
        /// </summary>
        public RDFPostgreSQLStore(string pgsqlConnectionString)
        {
            if (string.IsNullOrEmpty(pgsqlConnectionString))
            	throw new RDFStoreException("Cannot connect to PostgreSQL store because: given \"pgsqlConnectionString\" parameter is null or empty.");

            //Initialize store structures
            StoreType = "POSTGRESQL";
            Connection = new NpgsqlConnection(pgsqlConnectionString);
            StoreID = RDFModelUtilities.CreateHash(ToString());
            Disposed = false;

            //Perform initial diagnostics
            PrepareStore();
        }

        /// <summary>
        /// Destroys the PostgreSQL store instance
        /// </summary>
        ~RDFPostgreSQLStore() => Dispose(false);
        #endregion

        #region Interfaces
        /// <summary>
        /// Gives the string representation of the PostgreSQL store 
        /// </summary>
        public override string ToString()
            => string.Concat(base.ToString(), "|SERVER=", Connection.DataSource, ";DATABASE=", Connection.Database);

        /// <summary>
        /// Disposes the PostgreSQL store instance 
        /// </summary>
        public void Dispose()
        {
            Dispose(true);
            GC.SuppressFinalize(this);
        }

        /// <summary>
        /// Disposes the PostgreSQL store instance  (business logic of resources disposal)
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
                NpgsqlCommand command = new NpgsqlCommand("INSERT INTO quadruples(quadrupleid, tripleflavor, context, contextid, subject, subjectid, predicate, predicateid, object, objectid) SELECT @QID, @TFV, @CTX, @CTXID, @SUBJ, @SUBJID, @PRED, @PREDID, @OBJ, @OBJID WHERE NOT EXISTS (SELECT 1 FROM quadruples WHERE quadrupleid = @QID)", Connection);
                command.Parameters.Add(new NpgsqlParameter("QID", NpgsqlDbType.Bigint));
                command.Parameters.Add(new NpgsqlParameter("TFV", NpgsqlDbType.Integer));
                command.Parameters.Add(new NpgsqlParameter("CTX", NpgsqlDbType.Varchar, 1000));
                command.Parameters.Add(new NpgsqlParameter("CTXID", NpgsqlDbType.Bigint));
                command.Parameters.Add(new NpgsqlParameter("SUBJ", NpgsqlDbType.Varchar, 1000));
                command.Parameters.Add(new NpgsqlParameter("SUBJID", NpgsqlDbType.Bigint));
                command.Parameters.Add(new NpgsqlParameter("PRED", NpgsqlDbType.Varchar, 1000));
                command.Parameters.Add(new NpgsqlParameter("PREDID", NpgsqlDbType.Bigint));
                command.Parameters.Add(new NpgsqlParameter("OBJ", NpgsqlDbType.Varchar, 1000));
                command.Parameters.Add(new NpgsqlParameter("OBJID", NpgsqlDbType.Bigint));

                try
                {
                    //Open connection
                    Connection.Open();

                    //Prepare command
                    command.Prepare();

                    //Open transaction
                    command.Transaction = Connection.BeginTransaction();

                    //Iterate triples
                    foreach (RDFTriple triple in graph)
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
                    throw new RDFStoreException("Cannot insert data into PostgreSQL store because: " + ex.Message, ex);
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
                NpgsqlCommand command = new NpgsqlCommand("INSERT INTO quadruples(quadrupleid, tripleflavor, context, contextid, subject, subjectid, predicate, predicateid, object, objectid) SELECT @QID, @TFV, @CTX, @CTXID, @SUBJ, @SUBJID, @PRED, @PREDID, @OBJ, @OBJID WHERE NOT EXISTS (SELECT 1 FROM quadruples WHERE quadrupleid = @QID)", Connection);
                command.Parameters.Add(new NpgsqlParameter("QID", NpgsqlDbType.Bigint));
                command.Parameters.Add(new NpgsqlParameter("TFV", NpgsqlDbType.Integer));
                command.Parameters.Add(new NpgsqlParameter("CTX", NpgsqlDbType.Varchar, 1000));
                command.Parameters.Add(new NpgsqlParameter("CTXID", NpgsqlDbType.Bigint));
                command.Parameters.Add(new NpgsqlParameter("SUBJ", NpgsqlDbType.Varchar, 1000));
                command.Parameters.Add(new NpgsqlParameter("SUBJID", NpgsqlDbType.Bigint));
                command.Parameters.Add(new NpgsqlParameter("PRED", NpgsqlDbType.Varchar, 1000));
                command.Parameters.Add(new NpgsqlParameter("PREDID", NpgsqlDbType.Bigint));
                command.Parameters.Add(new NpgsqlParameter("OBJ", NpgsqlDbType.Varchar, 1000));
                command.Parameters.Add(new NpgsqlParameter("OBJID", NpgsqlDbType.Bigint));

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
                    throw new RDFStoreException("Cannot insert data into PostgreSQL store because: " + ex.Message, ex);
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
                NpgsqlCommand command = new NpgsqlCommand("DELETE FROM quadruples WHERE quadrupleid = @QID", Connection);
                command.Parameters.Add(new NpgsqlParameter("QID", NpgsqlDbType.Bigint));

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
                    throw new RDFStoreException("Cannot delete data from PostgreSQL store because: " + ex.Message, ex);
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
                NpgsqlCommand command = new NpgsqlCommand("DELETE FROM quadruples WHERE contextid = @CTXID", Connection);
                command.Parameters.Add(new NpgsqlParameter("CTXID", NpgsqlDbType.Bigint));

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
                    throw new RDFStoreException("Cannot delete data from PostgreSQL store because: " + ex.Message, ex);
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
                NpgsqlCommand command = new NpgsqlCommand("DELETE FROM quadruples WHERE subjectid = @SUBJID", Connection);
                command.Parameters.Add(new NpgsqlParameter("SUBJID", NpgsqlDbType.Bigint));

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
                    throw new RDFStoreException("Cannot delete data from PostgreSQL store because: " + ex.Message, ex);
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
                NpgsqlCommand command = new NpgsqlCommand("DELETE FROM quadruples WHERE predicateid = @PREDID", Connection);
                command.Parameters.Add(new NpgsqlParameter("PREDID", NpgsqlDbType.Bigint));

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
                    throw new RDFStoreException("Cannot delete data from PostgreSQL store because: " + ex.Message, ex);
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
                NpgsqlCommand command = new NpgsqlCommand("DELETE FROM quadruples WHERE objectid = @OBJID AND tripleflavor = @TFV", Connection);
                command.Parameters.Add(new NpgsqlParameter("OBJID", NpgsqlDbType.Bigint));
                command.Parameters.Add(new NpgsqlParameter("TFV", NpgsqlDbType.Integer));

                //Valorize parameters
                command.Parameters["OBJID"].Value = objectResource.PatternMemberID;
                command.Parameters["TFV"].Value = (Int32)RDFModelEnums.RDFTripleFlavors.SPO;

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
                    throw new RDFStoreException("Cannot delete data from PostgreSQL store because: " + ex.Message, ex);
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
                NpgsqlCommand command = new NpgsqlCommand("DELETE FROM quadruples WHERE objectid = @OBJID AND tripleflavor = @TFV", Connection);
                command.Parameters.Add(new NpgsqlParameter("OBJID", NpgsqlDbType.Bigint));
                command.Parameters.Add(new NpgsqlParameter("TFV", NpgsqlDbType.Integer));

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
                    throw new RDFStoreException("Cannot delete data from PostgreSQL store because: " + ex.Message, ex);
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
                NpgsqlCommand command = new NpgsqlCommand("DELETE FROM quadruples WHERE contextid = @CTXID AND subjectid = @SUBJID", Connection);
                command.Parameters.Add(new NpgsqlParameter("CTXID", NpgsqlDbType.Bigint));
                command.Parameters.Add(new NpgsqlParameter("SUBJID", NpgsqlDbType.Bigint));

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
                    throw new RDFStoreException("Cannot delete data from PostgreSQL store because: " + ex.Message, ex);
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
                NpgsqlCommand command = new NpgsqlCommand("DELETE FROM quadruples WHERE contextid = @CTXID AND predicateid = @PREDID", Connection);
                command.Parameters.Add(new NpgsqlParameter("CTXID", NpgsqlDbType.Bigint));
                command.Parameters.Add(new NpgsqlParameter("PREDID", NpgsqlDbType.Bigint));

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
                    throw new RDFStoreException("Cannot delete data from PostgreSQL store because: " + ex.Message, ex);
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
                NpgsqlCommand command = new NpgsqlCommand("DELETE FROM quadruples WHERE contextid = @CTXID AND objectid = @OBJID AND tripleflavor = @TFV", Connection);
                command.Parameters.Add(new NpgsqlParameter("CTXID", NpgsqlDbType.Bigint));
                command.Parameters.Add(new NpgsqlParameter("OBJID", NpgsqlDbType.Bigint));
                command.Parameters.Add(new NpgsqlParameter("TFV", NpgsqlDbType.Integer));

                //Valorize parameters
                command.Parameters["CTXID"].Value = contextResource.PatternMemberID;
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
                    throw new RDFStoreException("Cannot delete data from PostgreSQL store because: " + ex.Message, ex);
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
                NpgsqlCommand command = new NpgsqlCommand("DELETE FROM quadruples WHERE contextid = @CTXID AND objectid = @OBJID AND tripleflavor = @TFV", Connection);
                command.Parameters.Add(new NpgsqlParameter("CTXID", NpgsqlDbType.Bigint));
                command.Parameters.Add(new NpgsqlParameter("OBJID", NpgsqlDbType.Bigint));
                command.Parameters.Add(new NpgsqlParameter("TFV", NpgsqlDbType.Integer));

                //Valorize parameters
                command.Parameters["CTXID"].Value = contextResource.PatternMemberID;
                command.Parameters["OBJID"].Value = objectLiteral.PatternMemberID;
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
                    throw new RDFStoreException("Cannot delete data from PostgreSQL store because: " + ex.Message, ex);
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
                NpgsqlCommand command = new NpgsqlCommand("DELETE FROM quadruples WHERE contextid = @CTXID AND subjectid = @SUBJID AND predicateid = @PREDID", Connection);
                command.Parameters.Add(new NpgsqlParameter("CTXID", NpgsqlDbType.Bigint));
                command.Parameters.Add(new NpgsqlParameter("SUBJID", NpgsqlDbType.Bigint));
                command.Parameters.Add(new NpgsqlParameter("PREDID", NpgsqlDbType.Bigint));

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
                    throw new RDFStoreException("Cannot delete data from PostgreSQL store because: " + ex.Message, ex);
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
                NpgsqlCommand command = new NpgsqlCommand("DELETE FROM quadruples WHERE contextid = @CTXID AND subjectid = @SUBJID AND objectid = @OBJID AND tripleflavor = @TFV", Connection);
                command.Parameters.Add(new NpgsqlParameter("CTXID", NpgsqlDbType.Bigint));
                command.Parameters.Add(new NpgsqlParameter("SUBJID", NpgsqlDbType.Bigint));
                command.Parameters.Add(new NpgsqlParameter("OBJID", NpgsqlDbType.Bigint));
                command.Parameters.Add(new NpgsqlParameter("TFV", NpgsqlDbType.Integer));

                //Valorize parameters
                command.Parameters["CTXID"].Value = contextResource.PatternMemberID;
                command.Parameters["SUBJID"].Value = subjectResource.PatternMemberID;
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
                    throw new RDFStoreException("Cannot delete data from PostgreSQL store because: " + ex.Message, ex);
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
                NpgsqlCommand command = new NpgsqlCommand("DELETE FROM quadruples WHERE contextid = @CTXID AND subjectid = @SUBJID AND objectid = @OBJID AND tripleflavor = @TFV", Connection);
                command.Parameters.Add(new NpgsqlParameter("CTXID", NpgsqlDbType.Bigint));
                command.Parameters.Add(new NpgsqlParameter("SUBJID", NpgsqlDbType.Bigint));
                command.Parameters.Add(new NpgsqlParameter("OBJID", NpgsqlDbType.Bigint));
                command.Parameters.Add(new NpgsqlParameter("TFV", NpgsqlDbType.Integer));

                //Valorize parameters
                command.Parameters["CTXID"].Value = contextResource.PatternMemberID;
                command.Parameters["SUBJID"].Value = subjectResource.PatternMemberID;
                command.Parameters["OBJID"].Value = objectLiteral.PatternMemberID;
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
                    throw new RDFStoreException("Cannot delete data from PostgreSQL store because: " + ex.Message, ex);
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
                NpgsqlCommand command = new NpgsqlCommand("DELETE FROM quadruples WHERE contextid = @CTXID AND predicateid = @PREDID AND objectid = @OBJID AND tripleflavor = @TFV", Connection);
                command.Parameters.Add(new NpgsqlParameter("CTXID", NpgsqlDbType.Bigint));
                command.Parameters.Add(new NpgsqlParameter("PREDID", NpgsqlDbType.Bigint));
                command.Parameters.Add(new NpgsqlParameter("OBJID", NpgsqlDbType.Bigint));
                command.Parameters.Add(new NpgsqlParameter("TFV", NpgsqlDbType.Integer));

                //Valorize parameters
                command.Parameters["CTXID"].Value = contextResource.PatternMemberID;
                command.Parameters["PREDID"].Value = predicateResource.PatternMemberID;
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
                    throw new RDFStoreException("Cannot delete data from PostgreSQL store because: " + ex.Message, ex);
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
                NpgsqlCommand command = new NpgsqlCommand("DELETE FROM quadruples WHERE contextid = @CTXID AND predicateid = @PREDID AND objectid = @OBJID AND tripleflavor = @TFV", Connection);
                command.Parameters.Add(new NpgsqlParameter("CTXID", NpgsqlDbType.Bigint));
                command.Parameters.Add(new NpgsqlParameter("PREDID", NpgsqlDbType.Bigint));
                command.Parameters.Add(new NpgsqlParameter("OBJID", NpgsqlDbType.Bigint));
                command.Parameters.Add(new NpgsqlParameter("TFV", NpgsqlDbType.Integer));

                //Valorize parameters
                command.Parameters["CTXID"].Value = contextResource.PatternMemberID;
                command.Parameters["PREDID"].Value = predicateResource.PatternMemberID;
                command.Parameters["OBJID"].Value = objectLiteral.PatternMemberID;
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
                    throw new RDFStoreException("Cannot delete data from PostgreSQL store because: " + ex.Message, ex);
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
                NpgsqlCommand command = new NpgsqlCommand("DELETE FROM quadruples WHERE subjectid = @SUBJID AND predicateid = @PREDID", Connection);
                command.Parameters.Add(new NpgsqlParameter("SUBJID", NpgsqlDbType.Bigint));
                command.Parameters.Add(new NpgsqlParameter("PREDID", NpgsqlDbType.Bigint));

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
                    throw new RDFStoreException("Cannot delete data from PostgreSQL store because: " + ex.Message, ex);
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
                NpgsqlCommand command = new NpgsqlCommand("DELETE FROM quadruples WHERE subjectid = @SUBJID AND objectid = @OBJID AND tripleflavor = @TFV", Connection);
                command.Parameters.Add(new NpgsqlParameter("SUBJID", NpgsqlDbType.Bigint));
                command.Parameters.Add(new NpgsqlParameter("OBJID", NpgsqlDbType.Bigint));
                command.Parameters.Add(new NpgsqlParameter("TFV", NpgsqlDbType.Integer));

                //Valorize parameters
                command.Parameters["SUBJID"].Value = subjectResource.PatternMemberID;
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
                    throw new RDFStoreException("Cannot delete data from PostgreSQL store because: " + ex.Message, ex);
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
                NpgsqlCommand command = new NpgsqlCommand("DELETE FROM quadruples WHERE subjectid = @SUBJID AND objectid = @OBJID AND tripleflavor = @TFV", Connection);
                command.Parameters.Add(new NpgsqlParameter("SUBJID", NpgsqlDbType.Bigint));
                command.Parameters.Add(new NpgsqlParameter("OBJID", NpgsqlDbType.Bigint));
                command.Parameters.Add(new NpgsqlParameter("TFV", NpgsqlDbType.Integer));

                //Valorize parameters
                command.Parameters["SUBJID"].Value = subjectResource.PatternMemberID;
                command.Parameters["OBJID"].Value = objectLiteral.PatternMemberID;
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
                    throw new RDFStoreException("Cannot delete data from PostgreSQL store because: " + ex.Message, ex);
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
                NpgsqlCommand command = new NpgsqlCommand("DELETE FROM quadruples WHERE predicateid = @PREDID AND objectid = @OBJID AND tripleflavor = @TFV", Connection);
                command.Parameters.Add(new NpgsqlParameter("PREDID", NpgsqlDbType.Bigint));
                command.Parameters.Add(new NpgsqlParameter("OBJID", NpgsqlDbType.Bigint));
                command.Parameters.Add(new NpgsqlParameter("TFV", NpgsqlDbType.Integer));

                //Valorize parameters
                command.Parameters["PREDID"].Value = predicateResource.PatternMemberID;
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
                    throw new RDFStoreException("Cannot delete data from PostgreSQL store because: " + ex.Message, ex);
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
                NpgsqlCommand command = new NpgsqlCommand("DELETE FROM quadruples WHERE predicateid = @PREDID AND objectid = @OBJID AND tripleflavor = @TFV", Connection);
                command.Parameters.Add(new NpgsqlParameter("PREDID", NpgsqlDbType.Bigint));
                command.Parameters.Add(new NpgsqlParameter("OBJID", NpgsqlDbType.Bigint));
                command.Parameters.Add(new NpgsqlParameter("TFV", NpgsqlDbType.Integer));

                //Valorize parameters
                command.Parameters["PREDID"].Value = predicateResource.PatternMemberID;
                command.Parameters["OBJID"].Value = objectLiteral.PatternMemberID;
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
                    throw new RDFStoreException("Cannot delete data from PostgreSQL store because: " + ex.Message, ex);
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
            NpgsqlCommand command = new NpgsqlCommand("DELETE FROM quadruples", Connection);

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
                throw new RDFStoreException("Cannot delete data from PostgreSQL store because: " + ex.Message, ex);
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
            NpgsqlCommand command = new NpgsqlCommand("SELECT COUNT(1) WHERE EXISTS(SELECT 1 FROM quadruples WHERE quadrupleid = @QID)", Connection);
            command.Parameters.Add(new NpgsqlParameter("QID", NpgsqlDbType.Bigint));            

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
                throw new RDFStoreException("Cannot read data from PostgreSQL store because: " + ex.Message, ex);
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
            NpgsqlCommand command = null;
            switch (queryFilters.ToString())
            {
                case "C":
                    //C->->->
                    command = new NpgsqlCommand("SELECT tripleflavor, context, subject, predicate, object FROM quadruples WHERE contextid = @CTXID", Connection);
                    command.Parameters.Add(new NpgsqlParameter("CTXID", NpgsqlDbType.Bigint));
                    command.Parameters["CTXID"].Value = ctx.PatternMemberID;
                    break;
                case "S":
                    //->S->->
                    command = new NpgsqlCommand("SELECT tripleflavor, context, subject, predicate, object FROM quadruples WHERE subjectid = @SUBJID", Connection);
                    command.Parameters.Add(new NpgsqlParameter("SUBJID", NpgsqlDbType.Bigint));
                    command.Parameters["SUBJID"].Value = subj.PatternMemberID;
                    break;
                case "P":
                    //->->P->
                    command = new NpgsqlCommand("SELECT tripleflavor, context, subject, predicate, object FROM quadruples WHERE predicateid = @PREDID", Connection);
                    command.Parameters.Add(new NpgsqlParameter("PREDID", NpgsqlDbType.Bigint));
                    command.Parameters["PREDID"].Value = pred.PatternMemberID;
                    break;
                case "O":
                    //->->->O
                    command = new NpgsqlCommand("SELECT tripleflavor, context, subject, predicate, object FROM quadruples WHERE objectid = @OBJID AND tripleflavor = @TFV", Connection);
                    command.Parameters.Add(new NpgsqlParameter("OBJID", NpgsqlDbType.Bigint));
                    command.Parameters.Add(new NpgsqlParameter("TFV", NpgsqlDbType.Integer));
                    command.Parameters["OBJID"].Value = obj.PatternMemberID;
                    command.Parameters["TFV"].Value = (Int32)RDFModelEnums.RDFTripleFlavors.SPO;
                    break;
                case "L":
                    //->->->L
                    command = new NpgsqlCommand("SELECT tripleflavor, context, subject, predicate, object FROM quadruples WHERE objectid = @OBJID AND tripleflavor = @TFV", Connection);
                    command.Parameters.Add(new NpgsqlParameter("OBJID", NpgsqlDbType.Bigint));
                    command.Parameters.Add(new NpgsqlParameter("TFV", NpgsqlDbType.Integer));
                    command.Parameters["OBJID"].Value = lit.PatternMemberID;
                    command.Parameters["TFV"].Value = (Int32)RDFModelEnums.RDFTripleFlavors.SPL;
                    break;
                case "CS":
                    //C->S->->
                    command = new NpgsqlCommand("SELECT tripleflavor, context, subject, predicate, object FROM quadruples WHERE contextid = @CTXID AND subjectid = @SUBJID", Connection);
                    command.Parameters.Add(new NpgsqlParameter("CTXID", NpgsqlDbType.Bigint));
                    command.Parameters.Add(new NpgsqlParameter("SUBJID", NpgsqlDbType.Bigint));
                    command.Parameters["CTXID"].Value = ctx.PatternMemberID;
                    command.Parameters["SUBJID"].Value = subj.PatternMemberID;
                    break;
                case "CP":
                    //C->->P->
                    command = new NpgsqlCommand("SELECT tripleflavor, context, subject, predicate, object FROM quadruples WHERE contextid = @CTXID AND predicateid = @PREDID", Connection);
                    command.Parameters.Add(new NpgsqlParameter("CTXID", NpgsqlDbType.Bigint));
                    command.Parameters.Add(new NpgsqlParameter("PREDID", NpgsqlDbType.Bigint));
                    command.Parameters["CTXID"].Value = ctx.PatternMemberID;
                    command.Parameters["PREDID"].Value = pred.PatternMemberID;
                    break;
                case "CO":
                    //C->->->O
                    command = new NpgsqlCommand("SELECT tripleflavor, context, subject, predicate, object FROM quadruples WHERE contextid = @CTXID AND objectid = @OBJID AND tripleflavor = @TFV", Connection);
                    command.Parameters.Add(new NpgsqlParameter("CTXID", NpgsqlDbType.Bigint));
                    command.Parameters.Add(new NpgsqlParameter("OBJID", NpgsqlDbType.Bigint));
                    command.Parameters.Add(new NpgsqlParameter("TFV", NpgsqlDbType.Integer));
                    command.Parameters["CTXID"].Value = ctx.PatternMemberID;
                    command.Parameters["OBJID"].Value = obj.PatternMemberID;
                    command.Parameters["TFV"].Value = (Int32)RDFModelEnums.RDFTripleFlavors.SPO;
                    break;
                case "CL":
                    //C->->->L
                    command = new NpgsqlCommand("SELECT tripleflavor, context, subject, predicate, object FROM quadruples WHERE contextid = @CTXID AND objectid = @OBJID AND tripleflavor = @TFV", Connection);
                    command.Parameters.Add(new NpgsqlParameter("CTXID", NpgsqlDbType.Bigint));
                    command.Parameters.Add(new NpgsqlParameter("OBJID", NpgsqlDbType.Bigint));
                    command.Parameters.Add(new NpgsqlParameter("TFV", NpgsqlDbType.Integer));
                    command.Parameters["CTXID"].Value = ctx.PatternMemberID;
                    command.Parameters["OBJID"].Value = lit.PatternMemberID;
                    command.Parameters["TFV"].Value = (Int32)RDFModelEnums.RDFTripleFlavors.SPL;
                    break;
                case "CSP":
                    //C->S->P->
                    command = new NpgsqlCommand("SELECT tripleflavor, context, subject, predicate, object FROM quadruples WHERE contextid = @CTXID AND subjectid = @SUBJID AND predicateid = @PREDID", Connection);
                    command.Parameters.Add(new NpgsqlParameter("CTXID", NpgsqlDbType.Bigint));
                    command.Parameters.Add(new NpgsqlParameter("SUBJID", NpgsqlDbType.Bigint));
                    command.Parameters.Add(new NpgsqlParameter("PREDID", NpgsqlDbType.Bigint));
                    command.Parameters["CTXID"].Value = ctx.PatternMemberID;
                    command.Parameters["SUBJID"].Value = subj.PatternMemberID;
                    command.Parameters["PREDID"].Value = pred.PatternMemberID;
                    break;
                case "CSO":
                    //C->S->->O
                    command = new NpgsqlCommand("SELECT tripleflavor, context, subject, predicate, object FROM quadruples WHERE contextid = @CTXID AND subjectid = @SUBJID AND objectid = @OBJID AND tripleflavor = @TFV", Connection);
                    command.Parameters.Add(new NpgsqlParameter("CTXID", NpgsqlDbType.Bigint));
                    command.Parameters.Add(new NpgsqlParameter("SUBJID", NpgsqlDbType.Bigint));
                    command.Parameters.Add(new NpgsqlParameter("OBJID", NpgsqlDbType.Bigint));
                    command.Parameters.Add(new NpgsqlParameter("TFV", NpgsqlDbType.Integer));
                    command.Parameters["CTXID"].Value = ctx.PatternMemberID;
                    command.Parameters["SUBJID"].Value = subj.PatternMemberID;
                    command.Parameters["OBJID"].Value = obj.PatternMemberID;
                    command.Parameters["TFV"].Value = (Int32)RDFModelEnums.RDFTripleFlavors.SPO;
                    break;
                case "CSL":
                    //C->S->->L
                    command = new NpgsqlCommand("SELECT tripleflavor, context, subject, predicate, object FROM quadruples WHERE contextid = @CTXID AND subjectid = @SUBJID AND objectid = @OBJID AND tripleflavor = @TFV", Connection);
                    command.Parameters.Add(new NpgsqlParameter("CTXID", NpgsqlDbType.Bigint));
                    command.Parameters.Add(new NpgsqlParameter("SUBJID", NpgsqlDbType.Bigint));
                    command.Parameters.Add(new NpgsqlParameter("OBJID", NpgsqlDbType.Bigint));
                    command.Parameters.Add(new NpgsqlParameter("TFV", NpgsqlDbType.Integer));
                    command.Parameters["CTXID"].Value = ctx.PatternMemberID;
                    command.Parameters["SUBJID"].Value = subj.PatternMemberID;
                    command.Parameters["OBJID"].Value = lit.PatternMemberID;
                    command.Parameters["TFV"].Value = (Int32)RDFModelEnums.RDFTripleFlavors.SPL;
                    break;
                case "CPO":
                    //C->->P->O
                    command = new NpgsqlCommand("SELECT tripleflavor, context, subject, predicate, object FROM quadruples WHERE contextid = @CTXID AND predicateid = @PREDID AND objectid = @OBJID AND tripleflavor = @TFV", Connection);
                    command.Parameters.Add(new NpgsqlParameter("CTXID", NpgsqlDbType.Bigint));
                    command.Parameters.Add(new NpgsqlParameter("PREDID", NpgsqlDbType.Bigint));
                    command.Parameters.Add(new NpgsqlParameter("OBJID", NpgsqlDbType.Bigint));
                    command.Parameters.Add(new NpgsqlParameter("TFV", NpgsqlDbType.Integer));
                    command.Parameters["CTXID"].Value = ctx.PatternMemberID;
                    command.Parameters["PREDID"].Value = pred.PatternMemberID;
                    command.Parameters["OBJID"].Value = obj.PatternMemberID;
                    command.Parameters["TFV"].Value = (Int32)RDFModelEnums.RDFTripleFlavors.SPO;
                    break;
                case "CPL":
                    //C->->P->L
                    command = new NpgsqlCommand("SELECT tripleflavor, context, subject, predicate, object FROM quadruples WHERE contextid = @CTXID AND predicateid = @PREDID AND objectid = @OBJID AND tripleflavor = @TFV", Connection);
                    command.Parameters.Add(new NpgsqlParameter("CTXID", NpgsqlDbType.Bigint));
                    command.Parameters.Add(new NpgsqlParameter("PREDID", NpgsqlDbType.Bigint));
                    command.Parameters.Add(new NpgsqlParameter("OBJID", NpgsqlDbType.Bigint));
                    command.Parameters.Add(new NpgsqlParameter("TFV", NpgsqlDbType.Integer));
                    command.Parameters["CTXID"].Value = ctx.PatternMemberID;
                    command.Parameters["PREDID"].Value = pred.PatternMemberID;
                    command.Parameters["OBJID"].Value = lit.PatternMemberID;
                    command.Parameters["TFV"].Value = (Int32)RDFModelEnums.RDFTripleFlavors.SPL;
                    break;
                case "CSPO":
                    //C->S->P->O
                    command = new NpgsqlCommand("SELECT tripleflavor, context, subject, predicate, object FROM quadruples WHERE contextid = @CTXID AND subjectid = @SUBJID AND predicateid = @PREDID AND objectid = @OBJID AND tripleflavor = @TFV", Connection);
                    command.Parameters.Add(new NpgsqlParameter("CTXID", NpgsqlDbType.Bigint));
                    command.Parameters.Add(new NpgsqlParameter("SUBJID", NpgsqlDbType.Bigint));
                    command.Parameters.Add(new NpgsqlParameter("PREDID", NpgsqlDbType.Bigint));
                    command.Parameters.Add(new NpgsqlParameter("OBJID", NpgsqlDbType.Bigint));
                    command.Parameters.Add(new NpgsqlParameter("TFV", NpgsqlDbType.Integer));
                    command.Parameters["CTXID"].Value = ctx.PatternMemberID;
                    command.Parameters["SUBJID"].Value = subj.PatternMemberID;
                    command.Parameters["PREDID"].Value = pred.PatternMemberID;
                    command.Parameters["OBJID"].Value = obj.PatternMemberID;
                    command.Parameters["TFV"].Value = (Int32)RDFModelEnums.RDFTripleFlavors.SPO;
                    break;
                case "CSPL":
                    //C->S->P->L
                    command = new NpgsqlCommand("SELECT tripleflavor, context, subject, predicate, object FROM quadruples WHERE contextid = @CTXID AND subjectid = @SUBJID AND predicateid = @PREDID AND objectid = @OBJID AND tripleflavor = @TFV", Connection);
                    command.Parameters.Add(new NpgsqlParameter("CTXID", NpgsqlDbType.Bigint));
                    command.Parameters.Add(new NpgsqlParameter("SUBJID", NpgsqlDbType.Bigint));
                    command.Parameters.Add(new NpgsqlParameter("PREDID", NpgsqlDbType.Bigint));
                    command.Parameters.Add(new NpgsqlParameter("OBJID", NpgsqlDbType.Bigint));
                    command.Parameters.Add(new NpgsqlParameter("TFV", NpgsqlDbType.Integer));
                    command.Parameters["CTXID"].Value = ctx.PatternMemberID;
                    command.Parameters["SUBJID"].Value = subj.PatternMemberID;
                    command.Parameters["PREDID"].Value = pred.PatternMemberID;
                    command.Parameters["OBJID"].Value = lit.PatternMemberID;
                    command.Parameters["TFV"].Value = (Int32)RDFModelEnums.RDFTripleFlavors.SPL;
                    break;
                case "SP":
                    //->S->P->
                    command = new NpgsqlCommand("SELECT tripleflavor, context, subject, predicate, object FROM quadruples WHERE subjectid = @SUBJID AND predicateid = @PREDID", Connection);
                    command.Parameters.Add(new NpgsqlParameter("SUBJID", NpgsqlDbType.Bigint));
                    command.Parameters.Add(new NpgsqlParameter("PREDID", NpgsqlDbType.Bigint));
                    command.Parameters["SUBJID"].Value = subj.PatternMemberID;
                    command.Parameters["PREDID"].Value = pred.PatternMemberID;
                    break;
                case "SO":
                    //->S->->O
                    command = new NpgsqlCommand("SELECT tripleflavor, context, subject, predicate, object FROM quadruples WHERE subjectid = @SUBJID AND objectid = @OBJID AND tripleflavor = @TFV", Connection);
                    command.Parameters.Add(new NpgsqlParameter("SUBJID", NpgsqlDbType.Bigint));
                    command.Parameters.Add(new NpgsqlParameter("OBJID", NpgsqlDbType.Bigint));
                    command.Parameters.Add(new NpgsqlParameter("TFV", NpgsqlDbType.Integer));
                    command.Parameters["SUBJID"].Value = subj.PatternMemberID;
                    command.Parameters["OBJID"].Value = obj.PatternMemberID;
                    command.Parameters["TFV"].Value = (Int32)RDFModelEnums.RDFTripleFlavors.SPO;
                    break;
                case "SL":
                    //->S->->L
                    command = new NpgsqlCommand("SELECT tripleflavor, context, subject, predicate, object FROM quadruples WHERE subjectid = @SUBJID AND objectid = @OBJID AND tripleflavor = @TFV", Connection);
                    command.Parameters.Add(new NpgsqlParameter("SUBJID", NpgsqlDbType.Bigint));
                    command.Parameters.Add(new NpgsqlParameter("OBJID", NpgsqlDbType.Bigint));
                    command.Parameters.Add(new NpgsqlParameter("TFV", NpgsqlDbType.Integer));
                    command.Parameters["SUBJID"].Value = subj.PatternMemberID;
                    command.Parameters["OBJID"].Value = lit.PatternMemberID;
                    command.Parameters["TFV"].Value = (Int32)RDFModelEnums.RDFTripleFlavors.SPL;
                    break;
                case "PO":
                    //->->P->O
                    command = new NpgsqlCommand("SELECT tripleflavor, context, subject, predicate, object FROM quadruples WHERE predicateid = @PREDID AND objectid = @OBJID AND tripleflavor = @TFV", Connection);
                    command.Parameters.Add(new NpgsqlParameter("PREDID", NpgsqlDbType.Bigint));
                    command.Parameters.Add(new NpgsqlParameter("OBJID", NpgsqlDbType.Bigint));
                    command.Parameters.Add(new NpgsqlParameter("TFV", NpgsqlDbType.Integer));
                    command.Parameters["PREDID"].Value = pred.PatternMemberID;
                    command.Parameters["OBJID"].Value = obj.PatternMemberID;
                    command.Parameters["TFV"].Value = (Int32)RDFModelEnums.RDFTripleFlavors.SPO;
                    break;
                case "PL":
                    //->->P->L
                    command = new NpgsqlCommand("SELECT tripleflavor, context, subject, predicate, object FROM quadruples WHERE predicateid = @PREDID AND objectid = @OBJID AND tripleflavor = @TFV", Connection);
                    command.Parameters.Add(new NpgsqlParameter("PREDID", NpgsqlDbType.Bigint));
                    command.Parameters.Add(new NpgsqlParameter("OBJID", NpgsqlDbType.Bigint));
                    command.Parameters.Add(new NpgsqlParameter("TFV", NpgsqlDbType.Integer));
                    command.Parameters["PREDID"].Value = pred.PatternMemberID;
                    command.Parameters["OBJID"].Value = lit.PatternMemberID;
                    command.Parameters["TFV"].Value = (Int32)RDFModelEnums.RDFTripleFlavors.SPL;
                    break;
                case "SPO":
                    //->S->P->O
                    command = new NpgsqlCommand("SELECT tripleflavor, context, subject, predicate, object FROM quadruples WHERE subjectid = @SUBJID AND predicateid = @PREDID AND objectid = @OBJID AND tripleflavor = @TFV", Connection);
                    command.Parameters.Add(new NpgsqlParameter("SUBJID", NpgsqlDbType.Bigint));
                    command.Parameters.Add(new NpgsqlParameter("PREDID", NpgsqlDbType.Bigint));
                    command.Parameters.Add(new NpgsqlParameter("OBJID", NpgsqlDbType.Bigint));
                    command.Parameters.Add(new NpgsqlParameter("TFV", NpgsqlDbType.Integer));
                    command.Parameters["SUBJID"].Value = subj.PatternMemberID;
                    command.Parameters["PREDID"].Value = pred.PatternMemberID;
                    command.Parameters["OBJID"].Value = obj.PatternMemberID;
                    command.Parameters["TFV"].Value = (Int32)RDFModelEnums.RDFTripleFlavors.SPO;
                    break;
                case "SPL":
                    //->S->P->L
                    command = new NpgsqlCommand("SELECT tripleflavor, context, subject, predicate, object FROM quadruples WHERE subjectid = @SUBJID AND predicateid = @PREDID AND objectid = @OBJID AND tripleflavor = @TFV", Connection);
                    command.Parameters.Add(new NpgsqlParameter("SUBJID", NpgsqlDbType.Bigint));
                    command.Parameters.Add(new NpgsqlParameter("PREDID", NpgsqlDbType.Bigint));
                    command.Parameters.Add(new NpgsqlParameter("OBJID", NpgsqlDbType.Bigint));
                    command.Parameters.Add(new NpgsqlParameter("TFV", NpgsqlDbType.Integer));
                    command.Parameters["SUBJID"].Value = subj.PatternMemberID;
                    command.Parameters["PREDID"].Value = pred.PatternMemberID;
                    command.Parameters["OBJID"].Value = lit.PatternMemberID;
                    command.Parameters["TFV"].Value = (Int32)RDFModelEnums.RDFTripleFlavors.SPL;
                    break;
                default:
                    //->->->
                    command = new NpgsqlCommand("SELECT tripleflavor, context, subject, predicate, object FROM quadruples", Connection);
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
                using (NpgsqlDataReader quadruples = command.ExecuteReader())
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
                throw new RDFStoreException("Cannot read data from PostgreSQL store because: " + ex.Message, ex);
            }

            return result;
        }
        #endregion

        #region Diagnostics
        /// <summary>
        /// Performs the preliminary diagnostics controls on the underlying PostgreSQL database
        /// </summary>
        private RDFStoreEnums.RDFStoreSQLErrors Diagnostics()
        {
            try
            {
                //Open connection
                Connection.Open();

                //Create command
                NpgsqlCommand command = new NpgsqlCommand("SELECT COUNT(*) FROM pg_catalog.pg_class c JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace WHERE n.nspname = 'public' AND c.relname = 'quadruples';", Connection);

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
        /// Prepares the underlying PostgreSQL database
        /// </summary>
        private void PrepareStore()
        {
            RDFStoreEnums.RDFStoreSQLErrors check = Diagnostics();

            //Prepare the database only if diagnostics has detected the missing of "Quadruples" table in the store
            if (check == RDFStoreEnums.RDFStoreSQLErrors.QuadruplesTableNotFound)
            {
                try
                {
                    //Open connection
                    Connection.Open();

                    //Create & Execute command
                    NpgsqlCommand command = new NpgsqlCommand("CREATE TABLE quadruples (\"quadrupleid\" BIGINT NOT NULL PRIMARY KEY, \"tripleflavor\" INTEGER NOT NULL, \"contextid\" bigint NOT NULL, \"context\" VARCHAR NOT NULL, \"subjectid\" BIGINT NOT NULL, \"subject\" VARCHAR NOT NULL, \"predicateid\" BIGINT NOT NULL, \"predicate\" VARCHAR NOT NULL, \"objectid\" BIGINT NOT NULL, \"object\" VARCHAR NOT NULL);CREATE INDEX \"idx_contextid\" ON quadruples USING btree (\"contextid\");CREATE INDEX \"idx_subjectid\" ON quadruples USING btree (\"subjectid\");CREATE INDEX \"idx_predicateid\" ON quadruples USING btree (\"predicateid\");CREATE INDEX \"idx_objectid\" ON quadruples USING btree (\"objectid\",\"tripleflavor\");CREATE INDEX \"idx_subjectid_predicateid\" ON quadruples USING btree (\"subjectid\",\"predicateid\");CREATE INDEX \"idx_subjectid_objectid\" ON quadruples USING btree (\"subjectid\",\"objectid\",\"tripleflavor\");CREATE INDEX \"idx_predicateid_objectid\" ON quadruples USING btree (\"predicateid\",\"objectid\",\"tripleflavor\");", Connection);
                    command.ExecuteNonQuery();

                    //Close connection
                    Connection.Close();
                }
                catch (Exception ex)
                {
                    //Close connection
                    Connection.Close();

                    //Propagate exception
                    throw new RDFStoreException("Cannot prepare PostgreSQL store because: " + ex.Message, ex);
                }
            }

            //Otherwise, an exception must be thrown because it has not been possible to connect to the instance/database
            else if (check == RDFStoreEnums.RDFStoreSQLErrors.InvalidDataSource)
                throw new RDFStoreException("Cannot prepare PostgreSQL store because: unable to connect to the server instance or to open the selected database.");
        }
        #endregion

        #region Optimize
        /// <summary>
        /// Executes a special command to optimize PostgreSQL store
        /// </summary>
        public void OptimizeStore()
        {
            try
            {
                //Open connection
                Connection.Open();

                //Create command
                NpgsqlCommand command = new NpgsqlCommand("VACUUM ANALYZE quadruples", Connection);

                //Execute command
                command.ExecuteNonQuery();

                //Close connection
                Connection.Close();
            }
            catch (Exception ex)
            {
                //Close connection
                Connection.Close();

                //Propagate exception
                throw new RDFStoreException("Cannot optimize PostgreSQL store because: " + ex.Message, ex);
            }
        }
        #endregion

        #endregion
    }
}