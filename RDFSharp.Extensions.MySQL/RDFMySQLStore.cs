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
using System.Text;
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
    public sealed class RDFMySQLStore : RDFStore, IDisposable
    {
        #region Properties
        /// <summary>
        /// Count of the MySQL database quadruples (-1 in case of errors)
        /// </summary>
        public override long QuadruplesCount 
            => GetQuadruplesCount();

        /// <summary>
        /// Asynchronous count of the MySQL database quadruples (-1 in case of errors)
        /// </summary>
        public Task<long> QuadruplesCountAsync 
            => GetQuadruplesCountAsync();

        /// <summary>
        /// Connection to the MySQL database
        /// </summary>
        private MySqlConnection Connection { get; set; }

        /// <summary>
        /// Command to execute SELECT queries on the MySQL database
        /// </summary>
        private MySqlCommand SelectCommand { get; set; }

        /// <summary>
        /// Command to execute INSERT queries on the MySQL database
        /// </summary>
        private MySqlCommand InsertCommand { get; set; }

        /// <summary>
        /// Command to execute DELETE queries on the MySQL database
        /// </summary>
        private MySqlCommand DeleteCommand { get; set; }

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
            if (mysqlStoreOptions == null)
                mysqlStoreOptions = new RDFMySQLStoreOptions();

            //Initialize store structures
            StoreType = "MYSQL";
            Connection = new MySqlConnection(mysqlConnectionString);
            SelectCommand = new MySqlCommand { Connection = Connection, CommandTimeout = mysqlStoreOptions.SelectTimeout };
            DeleteCommand = new MySqlCommand { Connection = Connection, CommandTimeout = mysqlStoreOptions.DeleteTimeout };
            InsertCommand = new MySqlCommand { Connection = Connection, CommandTimeout = mysqlStoreOptions.InsertTimeout };
            StoreID = RDFModelUtilities.CreateHash(ToString());
            Disposed = false;

            //Perform initial diagnostics
            InitializeStore();
        }

        /// <summary>
        /// Destroys the MySQL store instance
        /// </summary>
        ~RDFMySQLStore() => Dispose(false);
        #endregion

        #region Interfaces
        /// <summary>
        /// Gives the string representation of the MySQL store 
        /// </summary>
        public override string ToString()
            => string.Concat(base.ToString(), "|SERVER=", Connection.DataSource, ";DATABASE=", Connection.Database);

        /// <summary>
        /// Disposes the MySQL store instance 
        /// </summary>
        public void Dispose()
        {
            Dispose(true);
            GC.SuppressFinalize(this);
        }

        /// <summary>
        /// Disposes the MySQL store instance  (business logic of resources disposal)
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
        /// Merges the given graph into the store within a single transaction, avoiding duplicate insertions
        /// </summary>
        public override RDFStore MergeGraph(RDFGraph graph)
        {
            if (graph != null)
            {
                RDFContext graphCtx = new RDFContext(graph.Context);

                //Create command
                InsertCommand.CommandText = "INSERT IGNORE INTO Quadruples(QuadrupleID, TripleFlavor, Context, ContextID, Subject, SubjectID, Predicate, PredicateID, Object, ObjectID) VALUES (@QID, @TFV, @CTX, @CTXID, @SUBJ, @SUBJID, @PRED, @PREDID, @OBJ, @OBJID)";
                InsertCommand.Parameters.Clear();
                InsertCommand.Parameters.Add(new MySqlParameter("QID", MySqlDbType.Int64));
                InsertCommand.Parameters.Add(new MySqlParameter("TFV", MySqlDbType.Int32));
                InsertCommand.Parameters.Add(new MySqlParameter("CTX", MySqlDbType.VarChar, 1000));
                InsertCommand.Parameters.Add(new MySqlParameter("CTXID", MySqlDbType.Int64));
                InsertCommand.Parameters.Add(new MySqlParameter("SUBJ", MySqlDbType.VarChar, 1000));
                InsertCommand.Parameters.Add(new MySqlParameter("SUBJID", MySqlDbType.Int64));
                InsertCommand.Parameters.Add(new MySqlParameter("PRED", MySqlDbType.VarChar, 1000));
                InsertCommand.Parameters.Add(new MySqlParameter("PREDID", MySqlDbType.Int64));
                InsertCommand.Parameters.Add(new MySqlParameter("OBJ", MySqlDbType.VarChar, 1000));
                InsertCommand.Parameters.Add(new MySqlParameter("OBJID", MySqlDbType.Int64));

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
                InsertCommand.CommandText = "INSERT IGNORE INTO Quadruples(QuadrupleID, TripleFlavor, Context, ContextID, Subject, SubjectID, Predicate, PredicateID, Object, ObjectID) VALUES (@QID, @TFV, @CTX, @CTXID, @SUBJ, @SUBJID, @PRED, @PREDID, @OBJ, @OBJID)";
                InsertCommand.Parameters.Clear();
                InsertCommand.Parameters.Add(new MySqlParameter("QID", MySqlDbType.Int64));
                InsertCommand.Parameters.Add(new MySqlParameter("TFV", MySqlDbType.Int32));
                InsertCommand.Parameters.Add(new MySqlParameter("CTX", MySqlDbType.VarChar, 1000));
                InsertCommand.Parameters.Add(new MySqlParameter("CTXID", MySqlDbType.Int64));
                InsertCommand.Parameters.Add(new MySqlParameter("SUBJ", MySqlDbType.VarChar, 1000));
                InsertCommand.Parameters.Add(new MySqlParameter("SUBJID", MySqlDbType.Int64));
                InsertCommand.Parameters.Add(new MySqlParameter("PRED", MySqlDbType.VarChar, 1000));
                InsertCommand.Parameters.Add(new MySqlParameter("PREDID", MySqlDbType.Int64));
                InsertCommand.Parameters.Add(new MySqlParameter("OBJ", MySqlDbType.VarChar, 1000));
                InsertCommand.Parameters.Add(new MySqlParameter("OBJID", MySqlDbType.Int64));

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
                    throw new RDFStoreException("Cannot insert data into MySQL store because: " + ex.Message, ex);
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
                DeleteCommand.CommandText = "DELETE FROM Quadruples WHERE QuadrupleID = @QID";
                DeleteCommand.Parameters.Clear();
                DeleteCommand.Parameters.Add(new MySqlParameter("QID", MySqlDbType.Int64));

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
                DeleteCommand.CommandText = "DELETE FROM Quadruples WHERE ContextID = @CTXID";
                DeleteCommand.Parameters.Clear();
                DeleteCommand.Parameters.Add(new MySqlParameter("CTXID", MySqlDbType.Int64));

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
                DeleteCommand.CommandText = "DELETE FROM Quadruples WHERE SubjectID = @SUBJID";
                DeleteCommand.Parameters.Clear();
                DeleteCommand.Parameters.Add(new MySqlParameter("SUBJID", MySqlDbType.Int64));

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
                DeleteCommand.CommandText = "DELETE FROM Quadruples WHERE PredicateID = @PREDID";
                DeleteCommand.Parameters.Clear();
                DeleteCommand.Parameters.Add(new MySqlParameter("PREDID", MySqlDbType.Int64));

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
                DeleteCommand.CommandText = "DELETE FROM Quadruples WHERE ObjectID = @OBJID AND TripleFlavor = @TFV";
                DeleteCommand.Parameters.Clear();
                DeleteCommand.Parameters.Add(new MySqlParameter("OBJID", MySqlDbType.Int64));
                DeleteCommand.Parameters.Add(new MySqlParameter("TFV", MySqlDbType.Int32));

                //Valorize parameters
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
                DeleteCommand.CommandText = "DELETE FROM Quadruples WHERE ObjectID = @OBJID AND TripleFlavor = @TFV";
                DeleteCommand.Parameters.Clear();
                DeleteCommand.Parameters.Add(new MySqlParameter("OBJID", MySqlDbType.Int64));
                DeleteCommand.Parameters.Add(new MySqlParameter("TFV", MySqlDbType.Int32));

                //Valorize parameters
                DeleteCommand.Parameters["OBJID"].Value = literalObject.PatternMemberID;
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
                DeleteCommand.CommandText = "DELETE FROM Quadruples WHERE ContextID = @CTXID AND SubjectID = @SUBJID";
                DeleteCommand.Parameters.Clear();
                DeleteCommand.Parameters.Add(new MySqlParameter("CTXID", MySqlDbType.Int64));
                DeleteCommand.Parameters.Add(new MySqlParameter("SUBJID", MySqlDbType.Int64));

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
                DeleteCommand.CommandText = "DELETE FROM Quadruples WHERE ContextID = @CTXID AND PredicateID = @PREDID";
                DeleteCommand.Parameters.Clear();
                DeleteCommand.Parameters.Add(new MySqlParameter("CTXID", MySqlDbType.Int64));
                DeleteCommand.Parameters.Add(new MySqlParameter("PREDID", MySqlDbType.Int64));

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
                DeleteCommand.CommandText = "DELETE FROM Quadruples WHERE ContextID = @CTXID AND ObjectID = @OBJID AND TripleFlavor = @TFV";
                DeleteCommand.Parameters.Clear();
                DeleteCommand.Parameters.Add(new MySqlParameter("CTXID", MySqlDbType.Int64));
                DeleteCommand.Parameters.Add(new MySqlParameter("OBJID", MySqlDbType.Int64));
                DeleteCommand.Parameters.Add(new MySqlParameter("TFV", MySqlDbType.Int32));

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
                DeleteCommand.CommandText = "DELETE FROM Quadruples WHERE ContextID = @CTXID AND ObjectID = @OBJID AND TripleFlavor = @TFV";
                DeleteCommand.Parameters.Clear();
                DeleteCommand.Parameters.Add(new MySqlParameter("CTXID", MySqlDbType.Int64));
                DeleteCommand.Parameters.Add(new MySqlParameter("OBJID", MySqlDbType.Int64));
                DeleteCommand.Parameters.Add(new MySqlParameter("TFV", MySqlDbType.Int32));

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
                DeleteCommand.CommandText = "DELETE FROM Quadruples WHERE ContextID = @CTXID AND SubjectID = @SUBJID AND PredicateID = @PREDID";
                DeleteCommand.Parameters.Clear();
                DeleteCommand.Parameters.Add(new MySqlParameter("CTXID", MySqlDbType.Int64));
                DeleteCommand.Parameters.Add(new MySqlParameter("SUBJID", MySqlDbType.Int64));
                DeleteCommand.Parameters.Add(new MySqlParameter("PREDID", MySqlDbType.Int64));

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
                DeleteCommand.CommandText = "DELETE FROM Quadruples WHERE ContextID = @CTXID AND SubjectID = @SUBJID AND ObjectID = @OBJID AND TripleFlavor = @TFV";
                DeleteCommand.Parameters.Clear();
                DeleteCommand.Parameters.Add(new MySqlParameter("CTXID", MySqlDbType.Int64));
                DeleteCommand.Parameters.Add(new MySqlParameter("SUBJID", MySqlDbType.Int64));
                DeleteCommand.Parameters.Add(new MySqlParameter("OBJID", MySqlDbType.Int64));
                DeleteCommand.Parameters.Add(new MySqlParameter("TFV", MySqlDbType.Int32));

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
                DeleteCommand.CommandText = "DELETE FROM Quadruples WHERE ContextID = @CTXID AND SubjectID = @SUBJID AND ObjectID = @OBJID AND TripleFlavor = @TFV";
                DeleteCommand.Parameters.Clear();
                DeleteCommand.Parameters.Add(new MySqlParameter("CTXID", MySqlDbType.Int64));
                DeleteCommand.Parameters.Add(new MySqlParameter("SUBJID", MySqlDbType.Int64));
                DeleteCommand.Parameters.Add(new MySqlParameter("OBJID", MySqlDbType.Int64));
                DeleteCommand.Parameters.Add(new MySqlParameter("TFV", MySqlDbType.Int32));

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
                DeleteCommand.CommandText = "DELETE FROM Quadruples WHERE ContextID = @CTXID AND PredicateID = @PREDID AND ObjectID = @OBJID AND TripleFlavor = @TFV";
                DeleteCommand.Parameters.Clear();
                DeleteCommand.Parameters.Add(new MySqlParameter("CTXID", MySqlDbType.Int64));
                DeleteCommand.Parameters.Add(new MySqlParameter("PREDID", MySqlDbType.Int64));
                DeleteCommand.Parameters.Add(new MySqlParameter("OBJID", MySqlDbType.Int64));
                DeleteCommand.Parameters.Add(new MySqlParameter("TFV", MySqlDbType.Int32));

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
                DeleteCommand.CommandText = "DELETE FROM Quadruples WHERE ContextID = @CTXID AND PredicateID = @PREDID AND ObjectID = @OBJID AND TripleFlavor = @TFV";
                DeleteCommand.Parameters.Clear();
                DeleteCommand.Parameters.Add(new MySqlParameter("CTXID", MySqlDbType.Int64));
                DeleteCommand.Parameters.Add(new MySqlParameter("PREDID", MySqlDbType.Int64));
                DeleteCommand.Parameters.Add(new MySqlParameter("OBJID", MySqlDbType.Int64));
                DeleteCommand.Parameters.Add(new MySqlParameter("TFV", MySqlDbType.Int32));

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
                DeleteCommand.CommandText = "DELETE FROM Quadruples WHERE SubjectID = @SUBJID AND PredicateID = @PREDID";
                DeleteCommand.Parameters.Clear();
                DeleteCommand.Parameters.Add(new MySqlParameter("SUBJID", MySqlDbType.Int64));
                DeleteCommand.Parameters.Add(new MySqlParameter("PREDID", MySqlDbType.Int64));

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
                DeleteCommand.CommandText = "DELETE FROM Quadruples WHERE SubjectID = @SUBJID AND ObjectID = @OBJID AND TripleFlavor = @TFV";
                DeleteCommand.Parameters.Clear();
                DeleteCommand.Parameters.Add(new MySqlParameter("SUBJID", MySqlDbType.Int64));
                DeleteCommand.Parameters.Add(new MySqlParameter("OBJID", MySqlDbType.Int64));
                DeleteCommand.Parameters.Add(new MySqlParameter("TFV", MySqlDbType.Int32));

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
                DeleteCommand.CommandText = "DELETE FROM Quadruples WHERE SubjectID = @SUBJID AND ObjectID = @OBJID AND TripleFlavor = @TFV";
                DeleteCommand.Parameters.Clear();
                DeleteCommand.Parameters.Add(new MySqlParameter("SUBJID", MySqlDbType.Int64));
                DeleteCommand.Parameters.Add(new MySqlParameter("OBJID", MySqlDbType.Int64));
                DeleteCommand.Parameters.Add(new MySqlParameter("TFV", MySqlDbType.Int32));

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
                DeleteCommand.CommandText = "DELETE FROM Quadruples WHERE PredicateID = @PREDID AND ObjectID = @OBJID AND TripleFlavor = @TFV";
                DeleteCommand.Parameters.Clear();
                DeleteCommand.Parameters.Add(new MySqlParameter("PREDID", MySqlDbType.Int64));
                DeleteCommand.Parameters.Add(new MySqlParameter("OBJID", MySqlDbType.Int64));
                DeleteCommand.Parameters.Add(new MySqlParameter("TFV", MySqlDbType.Int32));

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
                DeleteCommand.CommandText = "DELETE FROM Quadruples WHERE PredicateID = @PREDID AND ObjectID = @OBJID AND TripleFlavor = @TFV";
                DeleteCommand.Parameters.Clear();
                DeleteCommand.Parameters.Add(new MySqlParameter("PREDID", MySqlDbType.Int64));
                DeleteCommand.Parameters.Add(new MySqlParameter("OBJID", MySqlDbType.Int64));
                DeleteCommand.Parameters.Add(new MySqlParameter("TFV", MySqlDbType.Int32));

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
            DeleteCommand.CommandText = "DELETE FROM Quadruples";
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
                throw new RDFStoreException("Cannot delete data from MySQL store because: " + ex.Message, ex);
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
            SelectCommand.CommandText = "SELECT COUNT(1) WHERE EXISTS(SELECT 1 FROM Quadruples WHERE QuadrupleID = @QID)";
            SelectCommand.Parameters.Clear();
            SelectCommand.Parameters.Add(new MySqlParameter("QID", MySqlDbType.Int64));

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
                throw new RDFStoreException("Cannot read data from MySQL store because: " + ex.Message, ex);
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
                    SelectCommand.CommandText = "SELECT TripleFlavor, Context, Subject, Predicate, Object FROM Quadruples WHERE ContextID = @CTXID";
                    SelectCommand.Parameters.Clear();
                    SelectCommand.Parameters.Add(new MySqlParameter("CTXID", MySqlDbType.Int64));
                    SelectCommand.Parameters["CTXID"].Value = ctx.PatternMemberID;
                    break;
                case "S":
                    //->S->->
                    SelectCommand.CommandText = "SELECT TripleFlavor, Context, Subject, Predicate, Object FROM Quadruples WHERE SubjectID = @SUBJID";
                    SelectCommand.Parameters.Clear();
                    SelectCommand.Parameters.Add(new MySqlParameter("SUBJID", MySqlDbType.Int64));
                    SelectCommand.Parameters["SUBJID"].Value = subj.PatternMemberID;
                    break;
                case "P":
                    //->->P->
                    SelectCommand.CommandText = "SELECT TripleFlavor, Context, Subject, Predicate, Object FROM Quadruples WHERE PredicateID = @PREDID";
                    SelectCommand.Parameters.Clear();
                    SelectCommand.Parameters.Add(new MySqlParameter("PREDID", MySqlDbType.Int64));
                    SelectCommand.Parameters["PREDID"].Value = pred.PatternMemberID;
                    break;
                case "O":
                    //->->->O
                    SelectCommand.CommandText = "SELECT TripleFlavor, Context, Subject, Predicate, Object FROM Quadruples WHERE ObjectID = @OBJID AND TripleFlavor = @TFV";
                    SelectCommand.Parameters.Clear();
                    SelectCommand.Parameters.Add(new MySqlParameter("OBJID", MySqlDbType.Int64));
                    SelectCommand.Parameters.Add(new MySqlParameter("TFV", MySqlDbType.Int32));
                    SelectCommand.Parameters["OBJID"].Value = obj.PatternMemberID;
                    SelectCommand.Parameters["TFV"].Value = RDFModelEnums.RDFTripleFlavors.SPO;
                    break;
                case "L":
                    //->->->L
                    SelectCommand.CommandText = "SELECT TripleFlavor, Context, Subject, Predicate, Object FROM Quadruples WHERE ObjectID = @OBJID AND TripleFlavor = @TFV";
                    SelectCommand.Parameters.Clear();
                    SelectCommand.Parameters.Add(new MySqlParameter("OBJID", MySqlDbType.Int64));
                    SelectCommand.Parameters.Add(new MySqlParameter("TFV", MySqlDbType.Int32));
                    SelectCommand.Parameters["OBJID"].Value = lit.PatternMemberID;
                    SelectCommand.Parameters["TFV"].Value = RDFModelEnums.RDFTripleFlavors.SPL;
                    break;
                case "CS":
                    //C->S->->
                    SelectCommand.CommandText = "SELECT TripleFlavor, Context, Subject, Predicate, Object FROM Quadruples WHERE ContextID = @CTXID AND SubjectID = @SUBJID";
                    SelectCommand.Parameters.Clear();
                    SelectCommand.Parameters.Add(new MySqlParameter("CTXID", MySqlDbType.Int64));
                    SelectCommand.Parameters.Add(new MySqlParameter("SUBJID", MySqlDbType.Int64));
                    SelectCommand.Parameters["CTXID"].Value = ctx.PatternMemberID;
                    SelectCommand.Parameters["SUBJID"].Value = subj.PatternMemberID;
                    break;
                case "CP":
                    //C->->P->
                    SelectCommand.CommandText = "SELECT TripleFlavor, Context, Subject, Predicate, Object FROM Quadruples WHERE ContextID = @CTXID AND PredicateID = @PREDID";
                    SelectCommand.Parameters.Clear();
                    SelectCommand.Parameters.Add(new MySqlParameter("CTXID", MySqlDbType.Int64));
                    SelectCommand.Parameters.Add(new MySqlParameter("PREDID", MySqlDbType.Int64));
                    SelectCommand.Parameters["CTXID"].Value = ctx.PatternMemberID;
                    SelectCommand.Parameters["PREDID"].Value = pred.PatternMemberID;
                    break;
                case "CO":
                    //C->->->O
                    SelectCommand.CommandText = "SELECT TripleFlavor, Context, Subject, Predicate, Object FROM Quadruples WHERE ContextID = @CTXID AND ObjectID = @OBJID AND TripleFlavor = @TFV";
                    SelectCommand.Parameters.Clear();
                    SelectCommand.Parameters.Add(new MySqlParameter("CTXID", MySqlDbType.Int64));
                    SelectCommand.Parameters.Add(new MySqlParameter("OBJID", MySqlDbType.Int64));
                    SelectCommand.Parameters.Add(new MySqlParameter("TFV", MySqlDbType.Int32));
                    SelectCommand.Parameters["CTXID"].Value = ctx.PatternMemberID;
                    SelectCommand.Parameters["OBJID"].Value = obj.PatternMemberID;
                    SelectCommand.Parameters["TFV"].Value = RDFModelEnums.RDFTripleFlavors.SPO;
                    break;
                case "CL":
                    //C->->->L
                    SelectCommand.CommandText = "SELECT TripleFlavor, Context, Subject, Predicate, Object FROM Quadruples WHERE ContextID = @CTXID AND ObjectID = @OBJID AND TripleFlavor = @TFV";
                    SelectCommand.Parameters.Clear();
                    SelectCommand.Parameters.Add(new MySqlParameter("CTXID", MySqlDbType.Int64));
                    SelectCommand.Parameters.Add(new MySqlParameter("OBJID", MySqlDbType.Int64));
                    SelectCommand.Parameters.Add(new MySqlParameter("TFV", MySqlDbType.Int32));
                    SelectCommand.Parameters["CTXID"].Value = ctx.PatternMemberID;
                    SelectCommand.Parameters["OBJID"].Value = lit.PatternMemberID;
                    SelectCommand.Parameters["TFV"].Value = RDFModelEnums.RDFTripleFlavors.SPL;
                    break;
                case "CSP":
                    //C->S->P->
                    SelectCommand.CommandText = "SELECT TripleFlavor, Context, Subject, Predicate, Object FROM Quadruples WHERE ContextID = @CTXID AND SubjectID = @SUBJID AND PredicateID = @PREDID";
                    SelectCommand.Parameters.Clear();
                    SelectCommand.Parameters.Add(new MySqlParameter("CTXID", MySqlDbType.Int64));
                    SelectCommand.Parameters.Add(new MySqlParameter("SUBJID", MySqlDbType.Int64));
                    SelectCommand.Parameters.Add(new MySqlParameter("PREDID", MySqlDbType.Int64));
                    SelectCommand.Parameters["CTXID"].Value = ctx.PatternMemberID;
                    SelectCommand.Parameters["SUBJID"].Value = subj.PatternMemberID;
                    SelectCommand.Parameters["PREDID"].Value = pred.PatternMemberID;
                    break;
                case "CSO":
                    //C->S->->O
                    SelectCommand.CommandText = "SELECT TripleFlavor, Context, Subject, Predicate, Object FROM Quadruples WHERE ContextID = @CTXID AND SubjectID = @SUBJID AND ObjectID = @OBJID AND TripleFlavor = @TFV";
                    SelectCommand.Parameters.Clear();
                    SelectCommand.Parameters.Add(new MySqlParameter("CTXID", MySqlDbType.Int64));
                    SelectCommand.Parameters.Add(new MySqlParameter("SUBJID", MySqlDbType.Int64));
                    SelectCommand.Parameters.Add(new MySqlParameter("OBJID", MySqlDbType.Int64));
                    SelectCommand.Parameters.Add(new MySqlParameter("TFV", MySqlDbType.Int32));
                    SelectCommand.Parameters["CTXID"].Value = ctx.PatternMemberID;
                    SelectCommand.Parameters["SUBJID"].Value = subj.PatternMemberID;
                    SelectCommand.Parameters["OBJID"].Value = obj.PatternMemberID;
                    SelectCommand.Parameters["TFV"].Value = RDFModelEnums.RDFTripleFlavors.SPO;
                    break;
                case "CSL":
                    //C->S->->L
                    SelectCommand.CommandText = "SELECT TripleFlavor, Context, Subject, Predicate, Object FROM Quadruples WHERE ContextID = @CTXID AND SubjectID = @SUBJID AND ObjectID = @OBJID AND TripleFlavor = @TFV";
                    SelectCommand.Parameters.Add(new MySqlParameter("CTXID", MySqlDbType.Int64));
                    SelectCommand.Parameters.Add(new MySqlParameter("SUBJID", MySqlDbType.Int64));
                    SelectCommand.Parameters.Add(new MySqlParameter("OBJID", MySqlDbType.Int64));
                    SelectCommand.Parameters.Add(new MySqlParameter("TFV", MySqlDbType.Int32));
                    SelectCommand.Parameters["CTXID"].Value = ctx.PatternMemberID;
                    SelectCommand.Parameters["SUBJID"].Value = subj.PatternMemberID;
                    SelectCommand.Parameters["OBJID"].Value = lit.PatternMemberID;
                    SelectCommand.Parameters["TFV"].Value = RDFModelEnums.RDFTripleFlavors.SPL;
                    break;
                case "CPO":
                    //C->->P->O
                    SelectCommand.CommandText = "SELECT TripleFlavor, Context, Subject, Predicate, Object FROM Quadruples WHERE ContextID = @CTXID AND PredicateID = @PREDID AND ObjectID = @OBJID AND TripleFlavor = @TFV";
                    SelectCommand.Parameters.Clear();
                    SelectCommand.Parameters.Add(new MySqlParameter("CTXID", MySqlDbType.Int64));
                    SelectCommand.Parameters.Add(new MySqlParameter("PREDID", MySqlDbType.Int64));
                    SelectCommand.Parameters.Add(new MySqlParameter("OBJID", MySqlDbType.Int64));
                    SelectCommand.Parameters.Add(new MySqlParameter("TFV", MySqlDbType.Int32));
                    SelectCommand.Parameters["CTXID"].Value = ctx.PatternMemberID;
                    SelectCommand.Parameters["PREDID"].Value = pred.PatternMemberID;
                    SelectCommand.Parameters["OBJID"].Value = obj.PatternMemberID;
                    SelectCommand.Parameters["TFV"].Value = RDFModelEnums.RDFTripleFlavors.SPO;
                    break;
                case "CPL":
                    //C->->P->L
                    SelectCommand.CommandText = "SELECT TripleFlavor, Context, Subject, Predicate, Object FROM Quadruples WHERE ContextID = @CTXID AND PredicateID = @PREDID AND ObjectID = @OBJID AND TripleFlavor = @TFV";
                    SelectCommand.Parameters.Clear();
                    SelectCommand.Parameters.Add(new MySqlParameter("CTXID", MySqlDbType.Int64));
                    SelectCommand.Parameters.Add(new MySqlParameter("PREDID", MySqlDbType.Int64));
                    SelectCommand.Parameters.Add(new MySqlParameter("OBJID", MySqlDbType.Int64));
                    SelectCommand.Parameters.Add(new MySqlParameter("TFV", MySqlDbType.Int32));
                    SelectCommand.Parameters["CTXID"].Value = ctx.PatternMemberID;
                    SelectCommand.Parameters["PREDID"].Value = pred.PatternMemberID;
                    SelectCommand.Parameters["OBJID"].Value = lit.PatternMemberID;
                    SelectCommand.Parameters["TFV"].Value = RDFModelEnums.RDFTripleFlavors.SPL;
                    break;
                case "CSPO":
                    //C->S->P->O
                    SelectCommand.CommandText = "SELECT TripleFlavor, Context, Subject, Predicate, Object FROM Quadruples WHERE ContextID = @CTXID AND SubjectID = @SUBJID AND PredicateID = @PREDID AND ObjectID = @OBJID AND TripleFlavor = @TFV";
                    SelectCommand.Parameters.Clear();
                    SelectCommand.Parameters.Add(new MySqlParameter("CTXID", MySqlDbType.Int64));
                    SelectCommand.Parameters.Add(new MySqlParameter("SUBJID", MySqlDbType.Int64));
                    SelectCommand.Parameters.Add(new MySqlParameter("PREDID", MySqlDbType.Int64));
                    SelectCommand.Parameters.Add(new MySqlParameter("OBJID", MySqlDbType.Int64));
                    SelectCommand.Parameters.Add(new MySqlParameter("TFV", MySqlDbType.Int32));
                    SelectCommand.Parameters["CTXID"].Value = ctx.PatternMemberID;
                    SelectCommand.Parameters["SUBJID"].Value = subj.PatternMemberID;
                    SelectCommand.Parameters["PREDID"].Value = pred.PatternMemberID;
                    SelectCommand.Parameters["OBJID"].Value = obj.PatternMemberID;
                    SelectCommand.Parameters["TFV"].Value = RDFModelEnums.RDFTripleFlavors.SPO;
                    break;
                case "CSPL":
                    //C->S->P->L
                    SelectCommand.CommandText = "SELECT TripleFlavor, Context, Subject, Predicate, Object FROM Quadruples WHERE ContextID = @CTXID AND SubjectID = @SUBJID AND PredicateID = @PREDID AND ObjectID = @OBJID AND TripleFlavor = @TFV";
                    SelectCommand.Parameters.Clear();
                    SelectCommand.Parameters.Add(new MySqlParameter("CTXID", MySqlDbType.Int64));
                    SelectCommand.Parameters.Add(new MySqlParameter("SUBJID", MySqlDbType.Int64));
                    SelectCommand.Parameters.Add(new MySqlParameter("PREDID", MySqlDbType.Int64));
                    SelectCommand.Parameters.Add(new MySqlParameter("OBJID", MySqlDbType.Int64));
                    SelectCommand.Parameters.Add(new MySqlParameter("TFV", MySqlDbType.Int32));
                    SelectCommand.Parameters["CTXID"].Value = ctx.PatternMemberID;
                    SelectCommand.Parameters["SUBJID"].Value = subj.PatternMemberID;
                    SelectCommand.Parameters["PREDID"].Value = pred.PatternMemberID;
                    SelectCommand.Parameters["OBJID"].Value = lit.PatternMemberID;
                    SelectCommand.Parameters["TFV"].Value = RDFModelEnums.RDFTripleFlavors.SPL;
                    break;
                case "SP":
                    //->S->P->
                    SelectCommand.CommandText = "SELECT TripleFlavor, Context, Subject, Predicate, Object FROM Quadruples WHERE SubjectID = @SUBJID AND PredicateID = @PREDID";
                    SelectCommand.Parameters.Clear();
                    SelectCommand.Parameters.Add(new MySqlParameter("SUBJID", MySqlDbType.Int64));
                    SelectCommand.Parameters.Add(new MySqlParameter("PREDID", MySqlDbType.Int64));
                    SelectCommand.Parameters["SUBJID"].Value = subj.PatternMemberID;
                    SelectCommand.Parameters["PREDID"].Value = pred.PatternMemberID;
                    break;
                case "SO":
                    //->S->->O
                    SelectCommand.CommandText = "SELECT TripleFlavor, Context, Subject, Predicate, Object FROM Quadruples WHERE SubjectID = @SUBJID AND ObjectID = @OBJID AND TripleFlavor = @TFV";
                    SelectCommand.Parameters.Clear();
                    SelectCommand.Parameters.Add(new MySqlParameter("SUBJID", MySqlDbType.Int64));
                    SelectCommand.Parameters.Add(new MySqlParameter("OBJID", MySqlDbType.Int64));
                    SelectCommand.Parameters.Add(new MySqlParameter("TFV", MySqlDbType.Int32));
                    SelectCommand.Parameters["SUBJID"].Value = subj.PatternMemberID;
                    SelectCommand.Parameters["OBJID"].Value = obj.PatternMemberID;
                    SelectCommand.Parameters["TFV"].Value = RDFModelEnums.RDFTripleFlavors.SPO;
                    break;
                case "SL":
                    //->S->->L
                    SelectCommand.CommandText = "SELECT TripleFlavor, Context, Subject, Predicate, Object FROM Quadruples WHERE SubjectID = @SUBJID AND ObjectID = @OBJID AND TripleFlavor = @TFV";
                    SelectCommand.Parameters.Clear();
                    SelectCommand.Parameters.Add(new MySqlParameter("SUBJID", MySqlDbType.Int64));
                    SelectCommand.Parameters.Add(new MySqlParameter("OBJID", MySqlDbType.Int64));
                    SelectCommand.Parameters.Add(new MySqlParameter("TFV", MySqlDbType.Int32));
                    SelectCommand.Parameters["SUBJID"].Value = subj.PatternMemberID;
                    SelectCommand.Parameters["OBJID"].Value = lit.PatternMemberID;
                    SelectCommand.Parameters["TFV"].Value = RDFModelEnums.RDFTripleFlavors.SPL;
                    break;
                case "PO":
                    //->->P->O
                    SelectCommand.CommandText = "SELECT TripleFlavor, Context, Subject, Predicate, Object FROM Quadruples WHERE PredicateID = @PREDID AND ObjectID = @OBJID AND TripleFlavor = @TFV";
                    SelectCommand.Parameters.Clear();
                    SelectCommand.Parameters.Add(new MySqlParameter("PREDID", MySqlDbType.Int64));
                    SelectCommand.Parameters.Add(new MySqlParameter("OBJID", MySqlDbType.Int64));
                    SelectCommand.Parameters.Add(new MySqlParameter("TFV", MySqlDbType.Int32));
                    SelectCommand.Parameters["PREDID"].Value = pred.PatternMemberID;
                    SelectCommand.Parameters["OBJID"].Value = obj.PatternMemberID;
                    SelectCommand.Parameters["TFV"].Value = RDFModelEnums.RDFTripleFlavors.SPO;
                    break;
                case "PL":
                    //->->P->L
                    SelectCommand.CommandText = "SELECT TripleFlavor, Context, Subject, Predicate, Object FROM Quadruples WHERE PredicateID = @PREDID AND ObjectID = @OBJID AND TripleFlavor = @TFV";
                    SelectCommand.Parameters.Clear();
                    SelectCommand.Parameters.Add(new MySqlParameter("PREDID", MySqlDbType.Int64));
                    SelectCommand.Parameters.Add(new MySqlParameter("OBJID", MySqlDbType.Int64));
                    SelectCommand.Parameters.Add(new MySqlParameter("TFV", MySqlDbType.Int32));
                    SelectCommand.Parameters["PREDID"].Value = pred.PatternMemberID;
                    SelectCommand.Parameters["OBJID"].Value = lit.PatternMemberID;
                    SelectCommand.Parameters["TFV"].Value = RDFModelEnums.RDFTripleFlavors.SPL;
                    break;
                case "SPO":
                    //->S->P->O
                    SelectCommand.CommandText = "SELECT TripleFlavor, Context, Subject, Predicate, Object FROM Quadruples WHERE SubjectID = @SUBJID AND PredicateID = @PREDID AND ObjectID = @OBJID AND TripleFlavor = @TFV";
                    SelectCommand.Parameters.Clear();
                    SelectCommand.Parameters.Add(new MySqlParameter("SUBJID", MySqlDbType.Int64));
                    SelectCommand.Parameters.Add(new MySqlParameter("PREDID", MySqlDbType.Int64));
                    SelectCommand.Parameters.Add(new MySqlParameter("OBJID", MySqlDbType.Int64));
                    SelectCommand.Parameters.Add(new MySqlParameter("TFV", MySqlDbType.Int32));
                    SelectCommand.Parameters["SUBJID"].Value = subj.PatternMemberID;
                    SelectCommand.Parameters["PREDID"].Value = pred.PatternMemberID;
                    SelectCommand.Parameters["OBJID"].Value = obj.PatternMemberID;
                    SelectCommand.Parameters["TFV"].Value = RDFModelEnums.RDFTripleFlavors.SPO;
                    break;
                case "SPL":
                    //->S->P->L
                    SelectCommand.CommandText = "SELECT TripleFlavor, Context, Subject, Predicate, Object FROM Quadruples WHERE SubjectID = @SUBJID AND PredicateID = @PREDID AND ObjectID = @OBJID AND TripleFlavor = @TFV";
                    SelectCommand.Parameters.Clear();
                    SelectCommand.Parameters.Add(new MySqlParameter("SUBJID", MySqlDbType.Int64));
                    SelectCommand.Parameters.Add(new MySqlParameter("PREDID", MySqlDbType.Int64));
                    SelectCommand.Parameters.Add(new MySqlParameter("OBJID", MySqlDbType.Int64));
                    SelectCommand.Parameters.Add(new MySqlParameter("TFV", MySqlDbType.Int32));
                    SelectCommand.Parameters["SUBJID"].Value = subj.PatternMemberID;
                    SelectCommand.Parameters["PREDID"].Value = pred.PatternMemberID;
                    SelectCommand.Parameters["OBJID"].Value = lit.PatternMemberID;
                    SelectCommand.Parameters["TFV"].Value = RDFModelEnums.RDFTripleFlavors.SPL;
                    break;
                default:
                    //->->->
                    SelectCommand.CommandText = "SELECT TripleFlavor, Context, Subject, Predicate, Object FROM Quadruples";
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
                using (MySqlDataReader quadruples = SelectCommand.ExecuteReader())
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
                throw new RDFStoreException("Cannot read data from MySQL store because: " + ex.Message, ex);
            }

            return result;
        }
        
        /// <summary>
        /// Counts the MySQL database quadruples
        /// </summary>
        private long GetQuadruplesCount()
        {
            try
            {
                //Open connection
                Connection.Open();

                //Create command
                SelectCommand.CommandText = "SELECT COUNT(*) FROM Quadruples";
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
        /// Asynchronously counts the MySQL database quadruples
        /// </summary>
        private async Task<long> GetQuadruplesCountAsync()
        {
            try
            {
                //Open connection
                Connection.Open();

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

        #region Diagnostics
        /// <summary>
        /// Performs the preliminary diagnostics controls on the underlying MySQL database
        /// </summary>
        private RDFStoreEnums.RDFStoreSQLErrors Diagnostics()
        {
            try
            {
                //Open connection
                Connection.Open();

                //Create command
                SelectCommand.CommandText = "SELECT COUNT(*) FROM information_schema.tables WHERE table_schema = '" + Connection.Database + "' AND table_name = 'Quadruples'";
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
        /// Initializes the underlying MySQL database
        /// </summary>
        private void InitializeStore()
        {
            switch (Diagnostics())
            {
                //Prepare the database if diagnostics has not found the "Quadruples" table
                case RDFStoreEnums.RDFStoreSQLErrors.QuadruplesTableNotFound:
                    try
                    {
                        //Open connection
                        Connection.Open();

                        //Create & Execute command
                        MySqlCommand createCommand = new MySqlCommand("CREATE TABLE Quadruples (QuadrupleID BIGINT NOT NULL PRIMARY KEY, TripleFlavor INT NOT NULL, Context VARCHAR(1000) NOT NULL, ContextID BIGINT NOT NULL, Subject VARCHAR(1000) NOT NULL, SubjectID BIGINT NOT NULL, Predicate VARCHAR(1000) NOT NULL, PredicateID BIGINT NOT NULL, Object VARCHAR(1000) NOT NULL, ObjectID BIGINT NOT NULL) ENGINE=InnoDB;ALTER TABLE Quadruples ADD INDEX IDX_ContextID(ContextID);ALTER TABLE Quadruples ADD INDEX IDX_SubjectID(SubjectID);ALTER TABLE Quadruples ADD INDEX IDX_PredicateID(PredicateID);ALTER TABLE Quadruples ADD INDEX IDX_ObjectID(ObjectID,TripleFlavor);ALTER TABLE Quadruples ADD INDEX IDX_SubjectID_PredicateID(SubjectID,PredicateID);ALTER TABLE Quadruples ADD INDEX IDX_SubjectID_ObjectID(SubjectID,ObjectID,TripleFlavor);ALTER TABLE Quadruples ADD INDEX IDX_PredicateID_ObjectID(PredicateID,ObjectID,TripleFlavor);", Connection) { CommandTimeout = 120 };
                        createCommand.ExecuteNonQuery();

                        //Close connection
                        Connection.Close();
                    }
                    catch (Exception ex)
                    {
                        //Close connection
                        Connection.Close();

                        //Propagate exception
                        throw new RDFStoreException("Cannot prepare MySQL store because: " + ex.Message, ex);
                    }

                    break;
                //Otherwise, an exception must be thrown because it has not been possible to connect to the instance/database
                case RDFStoreEnums.RDFStoreSQLErrors.InvalidDataSource:
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
                Connection.Open();

                //Create command
                MySqlCommand optimizeCommand = new MySqlCommand("OPTIMIZE TABLE Quadruples", Connection) { CommandTimeout = 120 };

                //Execute command
                optimizeCommand.ExecuteNonQuery();

                //Close connection
                Connection.Close();
            }
            catch (Exception ex)
            {
                //Close connection
                Connection.Close();

                //Propagate exception
                throw new RDFStoreException("Cannot optimize MySQL store because: " + ex.Message, ex);
            }
        }
        #endregion

        #endregion
    }

    /// <summary>
    /// RDFMySQLStoreOptions is a collector of options for customizing the default behaviour of a MySQL store
    /// </summary>
    public class RDFMySQLStoreOptions
    {
        #region Properties
        /// <summary>
        /// Timeout in seconds for SELECT queries executed on the MySQL store (default: 120)
        /// </summary>
        public int SelectTimeout { get; set; } = 120;

        /// <summary>
        /// Timeout in seconds for DELETE queries executed on the MySQL store (default: 120)
        /// </summary>
        public int DeleteTimeout { get; set; } = 120;

        /// <summary>
        /// Timeout in seconds for INSERT queries executed on the MySQL store (default: 120)
        /// </summary>
        public int InsertTimeout { get; set; } = 120;
        #endregion
    }
}