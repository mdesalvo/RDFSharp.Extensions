﻿/*
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
using System.IO;
using System.Reflection;
using System.Text;
using Microsoft.Data.Sqlite;
using RDFSharp.Model;
using RDFSharp.Store;
using System.Threading.Tasks;

namespace RDFSharp.Extensions.SQLite
{
    /// <summary>
    /// RDFSQLiteStore represents a store backed on SQLite engine
    /// </summary>
    public sealed class RDFSQLiteStore : RDFStore, IDisposable
    {
        #region Properties
        /// <summary>
        /// Count of the SQLite database quadruples (-1 in case of errors)
        /// </summary>
        public override long QuadruplesCount
            => GetQuadruplesCount();

        /// <summary>
        /// Asynchronous count of the SQLite database quadruples (-1 in case of errors)
        /// </summary>
        public Task<long> QuadruplesCountAsync
            => GetQuadruplesCountAsync();

        /// <summary>
        /// Connection to the SQLite database
        /// </summary>
        private SqliteConnection Connection { get; set; }

        /// <summary>
        /// Command to execute SELECT queries on the SQLite database
        /// </summary>
        private SqliteCommand SelectCommand { get; set; }

        /// <summary>
        /// Command to execute INSERT queries on the SQLite database
        /// </summary>
        private SqliteCommand InsertCommand { get; set; }

        /// <summary>
        /// Command to execute DELETE queries on the SQLite database
        /// </summary>
        private SqliteCommand DeleteCommand { get; set; }

        /// <summary>
        /// Flag indicating that the SQLite store instance has already been disposed
        /// </summary>
        private bool Disposed { get; set; }
        #endregion

        #region Ctors
        /// <summary>
        /// Default-ctor to build a SQLite store instance (with eventual options)
        /// </summary>
        public RDFSQLiteStore(string sqliteDatabasePath, RDFSQLiteStoreOptions sqliteStoreOptions=null)
        {
            #region Guards
            if (string.IsNullOrWhiteSpace(sqliteDatabasePath))
                throw new RDFStoreException("Cannot connect to SQLite store because: given \"sqliteDatabasePath\" parameter is null or empty.");
            #endregion

            //Initialize options
            if (sqliteStoreOptions == null)
                sqliteStoreOptions = new RDFSQLiteStoreOptions();

            //Initialize store
            StoreType = "SQLITE";
            Connection = new SqliteConnection("Data Source=" + sqliteDatabasePath + ";");
            SelectCommand = new SqliteCommand { Connection = Connection, CommandTimeout = sqliteStoreOptions.SelectTimeout };
            DeleteCommand = new SqliteCommand { Connection = Connection, CommandTimeout = sqliteStoreOptions.DeleteTimeout };
            InsertCommand = new SqliteCommand { Connection = Connection, CommandTimeout = sqliteStoreOptions.InsertTimeout };
            StoreID = RDFModelUtilities.CreateHash(ToString());
            Disposed = false;

            try
            {
                //File does not exist => clone store template
                if (!File.Exists(sqliteDatabasePath))
                {
                    Assembly sqlite = Assembly.GetExecutingAssembly();
                    using (Stream templateDB = sqlite.GetManifestResourceStream("RDFSharp.Extensions.SQLite.Template.SQLite.db"))
                    {
                        using (FileStream targetDB = new FileStream(sqliteDatabasePath, FileMode.Create, FileAccess.ReadWrite))
                            templateDB.CopyTo(targetDB);
                    }
                }

                //File exists => execute diagnostics and initialization
                else
                {
                    InitializeStore();
                }
            }
            catch (Exception ex)
            {
                throw new RDFStoreException("Cannot create SQLite store because: " + ex.Message, ex);
            }
        }

        /// <summary>
        /// Destroys the SQLite store instance
        /// </summary>
        ~RDFSQLiteStore() => Dispose(false);
        #endregion

        #region Interfaces
        /// <summary>
        /// Gives the string representation of the SQLite store
        /// </summary>
        public override string ToString()
            => $"{base.ToString()}|SERVER={Connection.DataSource};DATABASE={Connection.Database}";

        /// <summary>
        /// Disposes the SQLite store instance
        /// </summary>
        public void Dispose()
        {
            Dispose(true);
            GC.SuppressFinalize(this);
        }

        /// <summary>
        /// Disposes the SQLite store instance  (business logic of resources disposal)
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
        /// Merges the given graph into the store, avoiding duplicate insertions
        /// </summary>
        public override RDFStore MergeGraph(RDFGraph graph)
        {
            if (graph != null)
            {
                RDFContext graphCtx = new RDFContext(graph.Context);

                //Create command
                InsertCommand.CommandText = "INSERT OR IGNORE INTO Quadruples(QuadrupleID, TripleFlavor, Context, ContextID, Subject, SubjectID, Predicate, PredicateID, Object, ObjectID) VALUES (@QID, @TFV, @CTX, @CTXID, @SUBJ, @SUBJID, @PRED, @PREDID, @OBJ, @OBJID)";
                InsertCommand.Parameters.Clear();
                InsertCommand.Parameters.Add(new SqliteParameter("QID", SqliteType.Integer));
                InsertCommand.Parameters.Add(new SqliteParameter("TFV", SqliteType.Integer));
                InsertCommand.Parameters.Add(new SqliteParameter("CTX", SqliteType.Text, 1000));
                InsertCommand.Parameters.Add(new SqliteParameter("CTXID", SqliteType.Integer));
                InsertCommand.Parameters.Add(new SqliteParameter("SUBJ", SqliteType.Text, 1000));
                InsertCommand.Parameters.Add(new SqliteParameter("SUBJID", SqliteType.Integer));
                InsertCommand.Parameters.Add(new SqliteParameter("PRED", SqliteType.Text, 1000));
                InsertCommand.Parameters.Add(new SqliteParameter("PREDID", SqliteType.Integer));
                InsertCommand.Parameters.Add(new SqliteParameter("OBJ", SqliteType.Text, 1000));
                InsertCommand.Parameters.Add(new SqliteParameter("OBJID", SqliteType.Integer));

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
                    InsertCommand.Transaction?.Rollback();

                    //Close connection
                    Connection.Close();

                    //Propagate exception
                    throw new RDFStoreException("Cannot insert data into SQLite store because: " + ex.Message, ex);
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
                InsertCommand.CommandText = "INSERT OR IGNORE INTO Quadruples(QuadrupleID, TripleFlavor, Context, ContextID, Subject, SubjectID, Predicate, PredicateID, Object, ObjectID) VALUES (@QID, @TFV, @CTX, @CTXID, @SUBJ, @SUBJID, @PRED, @PREDID, @OBJ, @OBJID)";
                InsertCommand.Parameters.Clear();
                InsertCommand.Parameters.Add(new SqliteParameter("QID", SqliteType.Integer));
                InsertCommand.Parameters.Add(new SqliteParameter("TFV", SqliteType.Integer));
                InsertCommand.Parameters.Add(new SqliteParameter("CTX", SqliteType.Text, 1000));
                InsertCommand.Parameters.Add(new SqliteParameter("CTXID", SqliteType.Integer));
                InsertCommand.Parameters.Add(new SqliteParameter("SUBJ", SqliteType.Text, 1000));
                InsertCommand.Parameters.Add(new SqliteParameter("SUBJID", SqliteType.Integer));
                InsertCommand.Parameters.Add(new SqliteParameter("PRED", SqliteType.Text, 1000));
                InsertCommand.Parameters.Add(new SqliteParameter("PREDID", SqliteType.Integer));
                InsertCommand.Parameters.Add(new SqliteParameter("OBJ", SqliteType.Text, 1000));
                InsertCommand.Parameters.Add(new SqliteParameter("OBJID", SqliteType.Integer));

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
                    InsertCommand.Transaction?.Rollback();

                    //Close connection
                    Connection.Close();

                    //Propagate exception
                    throw new RDFStoreException("Cannot insert data into SQLite store because: " + ex.Message, ex);
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
                DeleteCommand.Parameters.Add(new SqliteParameter("QID", SqliteType.Integer));

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
                    DeleteCommand.Transaction?.Rollback();

                    //Close connection
                    Connection.Close();

                    //Propagate exception
                    throw new RDFStoreException("Cannot delete data from SQLite store because: " + ex.Message, ex);
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
                DeleteCommand.Parameters.Add(new SqliteParameter("CTXID", SqliteType.Integer));

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
                    DeleteCommand.Transaction?.Rollback();

                    //Close connection
                    Connection.Close();

                    //Propagate exception
                    throw new RDFStoreException("Cannot delete data from SQLite store because: " + ex.Message, ex);
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
                DeleteCommand.Parameters.Add(new SqliteParameter("SUBJID", SqliteType.Integer));

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
                    DeleteCommand.Transaction?.Rollback();

                    //Close connection
                    Connection.Close();

                    //Propagate exception
                    throw new RDFStoreException("Cannot delete data from SQLite store because: " + ex.Message, ex);
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
                DeleteCommand.Parameters.Add(new SqliteParameter("PREDID", SqliteType.Integer));

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
                    DeleteCommand.Transaction?.Rollback();

                    //Close connection
                    Connection.Close();

                    //Propagate exception
                    throw new RDFStoreException("Cannot delete data from SQLite store because: " + ex.Message, ex);
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
                DeleteCommand.Parameters.Add(new SqliteParameter("OBJID", SqliteType.Integer));
                DeleteCommand.Parameters.Add(new SqliteParameter("TFV", SqliteType.Integer));

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
                    DeleteCommand.Transaction?.Rollback();

                    //Close connection
                    Connection.Close();

                    //Propagate exception
                    throw new RDFStoreException("Cannot delete data from SQLite store because: " + ex.Message, ex);
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
                DeleteCommand.Parameters.Add(new SqliteParameter("OBJID", SqliteType.Integer));
                DeleteCommand.Parameters.Add(new SqliteParameter("TFV", SqliteType.Integer));

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
                    DeleteCommand.Transaction?.Rollback();

                    //Close connection
                    Connection.Close();

                    //Propagate exception
                    throw new RDFStoreException("Cannot delete data from SQLite store because: " + ex.Message, ex);
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
                DeleteCommand.Parameters.Add(new SqliteParameter("CTXID", SqliteType.Integer));
                DeleteCommand.Parameters.Add(new SqliteParameter("SUBJID", SqliteType.Integer));

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
                    DeleteCommand.Transaction?.Rollback();

                    //Close connection
                    Connection.Close();

                    //Propagate exception
                    throw new RDFStoreException("Cannot delete data from SQLite store because: " + ex.Message, ex);
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
                DeleteCommand.Parameters.Add(new SqliteParameter("CTXID", SqliteType.Integer));
                DeleteCommand.Parameters.Add(new SqliteParameter("PREDID", SqliteType.Integer));

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
                    DeleteCommand.Transaction?.Rollback();

                    //Close connection
                    Connection.Close();

                    //Propagate exception
                    throw new RDFStoreException("Cannot delete data from SQLite store because: " + ex.Message, ex);
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
                DeleteCommand.Parameters.Add(new SqliteParameter("CTXID", SqliteType.Integer));
                DeleteCommand.Parameters.Add(new SqliteParameter("OBJID", SqliteType.Integer));
                DeleteCommand.Parameters.Add(new SqliteParameter("TFV", SqliteType.Integer));

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
                    DeleteCommand.Transaction?.Rollback();

                    //Close connection
                    Connection.Close();

                    //Propagate exception
                    throw new RDFStoreException("Cannot delete data from SQLite store because: " + ex.Message, ex);
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
                DeleteCommand.Parameters.Add(new SqliteParameter("CTXID", SqliteType.Integer));
                DeleteCommand.Parameters.Add(new SqliteParameter("OBJID", SqliteType.Integer));
                DeleteCommand.Parameters.Add(new SqliteParameter("TFV", SqliteType.Integer));

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
                    DeleteCommand.Transaction?.Rollback();

                    //Close connection
                    Connection.Close();

                    //Propagate exception
                    throw new RDFStoreException("Cannot delete data from SQLite store because: " + ex.Message, ex);
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
                DeleteCommand.Parameters.Add(new SqliteParameter("CTXID", SqliteType.Integer));
                DeleteCommand.Parameters.Add(new SqliteParameter("SUBJID", SqliteType.Integer));
                DeleteCommand.Parameters.Add(new SqliteParameter("PREDID", SqliteType.Integer));

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
                    DeleteCommand.Transaction?.Rollback();

                    //Close connection
                    Connection.Close();

                    //Propagate exception
                    throw new RDFStoreException("Cannot delete data from SQLite store because: " + ex.Message, ex);
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
                DeleteCommand.Parameters.Add(new SqliteParameter("CTXID", SqliteType.Integer));
                DeleteCommand.Parameters.Add(new SqliteParameter("SUBJID", SqliteType.Integer));
                DeleteCommand.Parameters.Add(new SqliteParameter("OBJID", SqliteType.Integer));
                DeleteCommand.Parameters.Add(new SqliteParameter("TFV", SqliteType.Integer));

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
                    DeleteCommand.Transaction?.Rollback();

                    //Close connection
                    Connection.Close();

                    //Propagate exception
                    throw new RDFStoreException("Cannot delete data from SQLite store because: " + ex.Message, ex);
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
                DeleteCommand.Parameters.Add(new SqliteParameter("CTXID", SqliteType.Integer));
                DeleteCommand.Parameters.Add(new SqliteParameter("SUBJID", SqliteType.Integer));
                DeleteCommand.Parameters.Add(new SqliteParameter("OBJID", SqliteType.Integer));
                DeleteCommand.Parameters.Add(new SqliteParameter("TFV", SqliteType.Integer));

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
                    DeleteCommand.Transaction?.Rollback();

                    //Close connection
                    Connection.Close();

                    //Propagate exception
                    throw new RDFStoreException("Cannot delete data from SQLite store because: " + ex.Message, ex);
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
                DeleteCommand.Parameters.Add(new SqliteParameter("CTXID", SqliteType.Integer));
                DeleteCommand.Parameters.Add(new SqliteParameter("PREDID", SqliteType.Integer));
                DeleteCommand.Parameters.Add(new SqliteParameter("OBJID", SqliteType.Integer));
                DeleteCommand.Parameters.Add(new SqliteParameter("TFV", SqliteType.Integer));

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
                    DeleteCommand.Transaction?.Rollback();

                    //Close connection
                    Connection.Close();

                    //Propagate exception
                    throw new RDFStoreException("Cannot delete data from SQLite store because: " + ex.Message, ex);
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
                DeleteCommand.Parameters.Add(new SqliteParameter("CTXID", SqliteType.Integer));
                DeleteCommand.Parameters.Add(new SqliteParameter("PREDID", SqliteType.Integer));
                DeleteCommand.Parameters.Add(new SqliteParameter("OBJID", SqliteType.Integer));
                DeleteCommand.Parameters.Add(new SqliteParameter("TFV", SqliteType.Integer));

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
                    DeleteCommand.Transaction?.Rollback();

                    //Close connection
                    Connection.Close();

                    //Propagate exception
                    throw new RDFStoreException("Cannot delete data from SQLite store because: " + ex.Message, ex);
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
                DeleteCommand.Parameters.Add(new SqliteParameter("SUBJID", SqliteType.Integer));
                DeleteCommand.Parameters.Add(new SqliteParameter("PREDID", SqliteType.Integer));

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
                    DeleteCommand.Transaction?.Rollback();

                    //Close connection
                    Connection.Close();

                    //Propagate exception
                    throw new RDFStoreException("Cannot delete data from SQLite store because: " + ex.Message, ex);
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
                DeleteCommand.Parameters.Add(new SqliteParameter("SUBJID", SqliteType.Integer));
                DeleteCommand.Parameters.Add(new SqliteParameter("OBJID", SqliteType.Integer));
                DeleteCommand.Parameters.Add(new SqliteParameter("TFV", SqliteType.Integer));

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
                    DeleteCommand.Transaction?.Rollback();

                    //Close connection
                    Connection.Close();

                    //Propagate exception
                    throw new RDFStoreException("Cannot delete data from SQLite store because: " + ex.Message, ex);
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
                DeleteCommand.Parameters.Add(new SqliteParameter("SUBJID", SqliteType.Integer));
                DeleteCommand.Parameters.Add(new SqliteParameter("OBJID", SqliteType.Integer));
                DeleteCommand.Parameters.Add(new SqliteParameter("TFV", SqliteType.Integer));

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
                    DeleteCommand.Transaction?.Rollback();

                    //Close connection
                    Connection.Close();

                    //Propagate exception
                    throw new RDFStoreException("Cannot delete data from SQLite store because: " + ex.Message, ex);
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
                DeleteCommand.Parameters.Add(new SqliteParameter("PREDID", SqliteType.Integer));
                DeleteCommand.Parameters.Add(new SqliteParameter("OBJID", SqliteType.Integer));
                DeleteCommand.Parameters.Add(new SqliteParameter("TFV", SqliteType.Integer));

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
                    DeleteCommand.Transaction?.Rollback();

                    //Close connection
                    Connection.Close();

                    //Propagate exception
                    throw new RDFStoreException("Cannot delete data from SQLite store because: " + ex.Message, ex);
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
                DeleteCommand.Parameters.Add(new SqliteParameter("PREDID", SqliteType.Integer));
                DeleteCommand.Parameters.Add(new SqliteParameter("OBJID", SqliteType.Integer));
                DeleteCommand.Parameters.Add(new SqliteParameter("TFV", SqliteType.Integer));

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
                    DeleteCommand.Transaction?.Rollback();

                    //Close connection
                    Connection.Close();

                    //Propagate exception
                    throw new RDFStoreException("Cannot delete data from SQLite store because: " + ex.Message, ex);
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
                DeleteCommand.Transaction?.Rollback();

                //Close connection
                Connection.Close();

                //Propagate exception
                throw new RDFStoreException("Cannot delete data from SQLite store because: " + ex.Message, ex);
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
            SelectCommand.CommandText = "SELECT EXISTS(SELECT 1 FROM Quadruples WHERE QuadrupleID = @QID)";
            SelectCommand.Parameters.Clear();
            SelectCommand.Parameters.Add(new SqliteParameter("QID", SqliteType.Integer));

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
                throw new RDFStoreException("Cannot read data from SQLite store because: " + ex.Message, ex);
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
                    SelectCommand.Parameters.Add(new SqliteParameter("CTXID", SqliteType.Integer));
                    SelectCommand.Parameters["CTXID"].Value = ctx.PatternMemberID;
                    break;
                case "S":
                    //->S->->
                    SelectCommand.CommandText = "SELECT TripleFlavor, Context, Subject, Predicate, Object FROM Quadruples WHERE SubjectID = @SUBJID";
                    SelectCommand.Parameters.Clear();
                    SelectCommand.Parameters.Add(new SqliteParameter("SUBJID", SqliteType.Integer));
                    SelectCommand.Parameters["SUBJID"].Value = subj.PatternMemberID;
                    break;
                case "P":
                    //->->P->
                    SelectCommand.CommandText = "SELECT TripleFlavor, Context, Subject, Predicate, Object FROM Quadruples WHERE PredicateID = @PREDID";
                    SelectCommand.Parameters.Clear();
                    SelectCommand.Parameters.Add(new SqliteParameter("PREDID", SqliteType.Integer));
                    SelectCommand.Parameters["PREDID"].Value = pred.PatternMemberID;
                    break;
                case "O":
                    //->->->O
                    SelectCommand.CommandText = "SELECT TripleFlavor, Context, Subject, Predicate, Object FROM Quadruples WHERE ObjectID = @OBJID AND TripleFlavor = @TFV";
                    SelectCommand.Parameters.Clear();
                    SelectCommand.Parameters.Add(new SqliteParameter("OBJID", SqliteType.Integer));
                    SelectCommand.Parameters.Add(new SqliteParameter("TFV", SqliteType.Integer));
                    SelectCommand.Parameters["OBJID"].Value = obj.PatternMemberID;
                    SelectCommand.Parameters["TFV"].Value = RDFModelEnums.RDFTripleFlavors.SPO;
                    break;
                case "L":
                    //->->->L
                    SelectCommand.CommandText = "SELECT TripleFlavor, Context, Subject, Predicate, Object FROM Quadruples WHERE ObjectID = @OBJID AND TripleFlavor = @TFV";
                    SelectCommand.Parameters.Clear();
                    SelectCommand.Parameters.Add(new SqliteParameter("OBJID", SqliteType.Integer));
                    SelectCommand.Parameters.Add(new SqliteParameter("TFV", SqliteType.Integer));
                    SelectCommand.Parameters["OBJID"].Value = lit.PatternMemberID;
                    SelectCommand.Parameters["TFV"].Value = RDFModelEnums.RDFTripleFlavors.SPL;
                    break;
                case "CS":
                    //C->S->->
                    SelectCommand.CommandText = "SELECT TripleFlavor, Context, Subject, Predicate, Object FROM Quadruples WHERE ContextID = @CTXID AND SubjectID = @SUBJID";
                    SelectCommand.Parameters.Clear();
                    SelectCommand.Parameters.Add(new SqliteParameter("CTXID", SqliteType.Integer));
                    SelectCommand.Parameters.Add(new SqliteParameter("SUBJID", SqliteType.Integer));
                    SelectCommand.Parameters["CTXID"].Value = ctx.PatternMemberID;
                    SelectCommand.Parameters["SUBJID"].Value = subj.PatternMemberID;
                    break;
                case "CP":
                    //C->->P->
                    SelectCommand.CommandText = "SELECT TripleFlavor, Context, Subject, Predicate, Object FROM Quadruples WHERE ContextID = @CTXID AND PredicateID = @PREDID";
                    SelectCommand.Parameters.Clear();
                    SelectCommand.Parameters.Add(new SqliteParameter("CTXID", SqliteType.Integer));
                    SelectCommand.Parameters.Add(new SqliteParameter("PREDID", SqliteType.Integer));
                    SelectCommand.Parameters["CTXID"].Value = ctx.PatternMemberID;
                    SelectCommand.Parameters["PREDID"].Value = pred.PatternMemberID;
                    break;
                case "CO":
                    //C->->->O
                    SelectCommand.CommandText = "SELECT TripleFlavor, Context, Subject, Predicate, Object FROM Quadruples WHERE ContextID = @CTXID AND ObjectID = @OBJID AND TripleFlavor = @TFV";
                    SelectCommand.Parameters.Clear();
                    SelectCommand.Parameters.Add(new SqliteParameter("CTXID", SqliteType.Integer));
                    SelectCommand.Parameters.Add(new SqliteParameter("OBJID", SqliteType.Integer));
                    SelectCommand.Parameters.Add(new SqliteParameter("TFV", SqliteType.Integer));
                    SelectCommand.Parameters["CTXID"].Value = ctx.PatternMemberID;
                    SelectCommand.Parameters["OBJID"].Value = obj.PatternMemberID;
                    SelectCommand.Parameters["TFV"].Value = RDFModelEnums.RDFTripleFlavors.SPO;
                    break;
                case "CL":
                    //C->->->L
                    SelectCommand.CommandText = "SELECT TripleFlavor, Context, Subject, Predicate, Object FROM Quadruples WHERE ContextID = @CTXID AND ObjectID = @OBJID AND TripleFlavor = @TFV";
                    SelectCommand.Parameters.Clear();
                    SelectCommand.Parameters.Add(new SqliteParameter("CTXID", SqliteType.Integer));
                    SelectCommand.Parameters.Add(new SqliteParameter("OBJID", SqliteType.Integer));
                    SelectCommand.Parameters.Add(new SqliteParameter("TFV", SqliteType.Integer));
                    SelectCommand.Parameters["CTXID"].Value = ctx.PatternMemberID;
                    SelectCommand.Parameters["OBJID"].Value = lit.PatternMemberID;
                    SelectCommand.Parameters["TFV"].Value = RDFModelEnums.RDFTripleFlavors.SPL;
                    break;
                case "CSP":
                    //C->S->P->
                    SelectCommand.CommandText = "SELECT TripleFlavor, Context, Subject, Predicate, Object FROM Quadruples WHERE ContextID = @CTXID AND SubjectID = @SUBJID AND PredicateID = @PREDID";
                    SelectCommand.Parameters.Clear();
                    SelectCommand.Parameters.Add(new SqliteParameter("CTXID", SqliteType.Integer));
                    SelectCommand.Parameters.Add(new SqliteParameter("SUBJID", SqliteType.Integer));
                    SelectCommand.Parameters.Add(new SqliteParameter("PREDID", SqliteType.Integer));
                    SelectCommand.Parameters["CTXID"].Value = ctx.PatternMemberID;
                    SelectCommand.Parameters["SUBJID"].Value = subj.PatternMemberID;
                    SelectCommand.Parameters["PREDID"].Value = pred.PatternMemberID;
                    break;
                case "CSO":
                    //C->S->->O
                    SelectCommand.CommandText = "SELECT TripleFlavor, Context, Subject, Predicate, Object FROM Quadruples WHERE ContextID = @CTXID AND SubjectID = @SUBJID AND ObjectID = @OBJID AND TripleFlavor = @TFV";
                    SelectCommand.Parameters.Clear();
                    SelectCommand.Parameters.Add(new SqliteParameter("CTXID", SqliteType.Integer));
                    SelectCommand.Parameters.Add(new SqliteParameter("SUBJID", SqliteType.Integer));
                    SelectCommand.Parameters.Add(new SqliteParameter("OBJID", SqliteType.Integer));
                    SelectCommand.Parameters.Add(new SqliteParameter("TFV", SqliteType.Integer));
                    SelectCommand.Parameters["CTXID"].Value = ctx.PatternMemberID;
                    SelectCommand.Parameters["SUBJID"].Value = subj.PatternMemberID;
                    SelectCommand.Parameters["OBJID"].Value = obj.PatternMemberID;
                    SelectCommand.Parameters["TFV"].Value = RDFModelEnums.RDFTripleFlavors.SPO;
                    break;
                case "CSL":
                    //C->S->->L
                    SelectCommand.CommandText = "SELECT TripleFlavor, Context, Subject, Predicate, Object FROM Quadruples WHERE ContextID = @CTXID AND SubjectID = @SUBJID AND ObjectID = @OBJID AND TripleFlavor = @TFV";
                    SelectCommand.Parameters.Clear();
                    SelectCommand.Parameters.Add(new SqliteParameter("CTXID", SqliteType.Integer));
                    SelectCommand.Parameters.Add(new SqliteParameter("SUBJID", SqliteType.Integer));
                    SelectCommand.Parameters.Add(new SqliteParameter("OBJID", SqliteType.Integer));
                    SelectCommand.Parameters.Add(new SqliteParameter("TFV", SqliteType.Integer));
                    SelectCommand.Parameters["CTXID"].Value = ctx.PatternMemberID;
                    SelectCommand.Parameters["SUBJID"].Value = subj.PatternMemberID;
                    SelectCommand.Parameters["OBJID"].Value = lit.PatternMemberID;
                    SelectCommand.Parameters["TFV"].Value = RDFModelEnums.RDFTripleFlavors.SPL;
                    break;
                case "CPO":
                    //C->->P->O
                    SelectCommand.CommandText = "SELECT TripleFlavor, Context, Subject, Predicate, Object FROM Quadruples WHERE ContextID = @CTXID AND PredicateID = @PREDID AND ObjectID = @OBJID AND TripleFlavor = @TFV";
                    SelectCommand.Parameters.Clear();
                    SelectCommand.Parameters.Add(new SqliteParameter("CTXID", SqliteType.Integer));
                    SelectCommand.Parameters.Add(new SqliteParameter("PREDID", SqliteType.Integer));
                    SelectCommand.Parameters.Add(new SqliteParameter("OBJID", SqliteType.Integer));
                    SelectCommand.Parameters.Add(new SqliteParameter("TFV", SqliteType.Integer));
                    SelectCommand.Parameters["CTXID"].Value = ctx.PatternMemberID;
                    SelectCommand.Parameters["PREDID"].Value = pred.PatternMemberID;
                    SelectCommand.Parameters["OBJID"].Value = obj.PatternMemberID;
                    SelectCommand.Parameters["TFV"].Value = RDFModelEnums.RDFTripleFlavors.SPO;
                    break;
                case "CPL":
                    //C->->P->L
                    SelectCommand.CommandText = "SELECT TripleFlavor, Context, Subject, Predicate, Object FROM Quadruples WHERE ContextID = @CTXID AND PredicateID = @PREDID AND ObjectID = @OBJID AND TripleFlavor = @TFV";
                    SelectCommand.Parameters.Clear();
                    SelectCommand.Parameters.Add(new SqliteParameter("CTXID", SqliteType.Integer));
                    SelectCommand.Parameters.Add(new SqliteParameter("PREDID", SqliteType.Integer));
                    SelectCommand.Parameters.Add(new SqliteParameter("OBJID", SqliteType.Integer));
                    SelectCommand.Parameters.Add(new SqliteParameter("TFV", SqliteType.Integer));
                    SelectCommand.Parameters["CTXID"].Value = ctx.PatternMemberID;
                    SelectCommand.Parameters["PREDID"].Value = pred.PatternMemberID;
                    SelectCommand.Parameters["OBJID"].Value = lit.PatternMemberID;
                    SelectCommand.Parameters["TFV"].Value = RDFModelEnums.RDFTripleFlavors.SPL;
                    break;
                case "CSPO":
                    //C->S->P->O
                    SelectCommand.CommandText = "SELECT TripleFlavor, Context, Subject, Predicate, Object FROM Quadruples WHERE ContextID = @CTXID AND SubjectID = @SUBJID AND PredicateID = @PREDID AND ObjectID = @OBJID AND TripleFlavor = @TFV";
                    SelectCommand.Parameters.Clear();
                    SelectCommand.Parameters.Add(new SqliteParameter("CTXID", SqliteType.Integer));
                    SelectCommand.Parameters.Add(new SqliteParameter("SUBJID", SqliteType.Integer));
                    SelectCommand.Parameters.Add(new SqliteParameter("PREDID", SqliteType.Integer));
                    SelectCommand.Parameters.Add(new SqliteParameter("OBJID", SqliteType.Integer));
                    SelectCommand.Parameters.Add(new SqliteParameter("TFV", SqliteType.Integer));
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
                    SelectCommand.Parameters.Add(new SqliteParameter("CTXID", SqliteType.Integer));
                    SelectCommand.Parameters.Add(new SqliteParameter("SUBJID", SqliteType.Integer));
                    SelectCommand.Parameters.Add(new SqliteParameter("PREDID", SqliteType.Integer));
                    SelectCommand.Parameters.Add(new SqliteParameter("OBJID", SqliteType.Integer));
                    SelectCommand.Parameters.Add(new SqliteParameter("TFV", SqliteType.Integer));
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
                    SelectCommand.Parameters.Add(new SqliteParameter("SUBJID", SqliteType.Integer));
                    SelectCommand.Parameters.Add(new SqliteParameter("PREDID", SqliteType.Integer));
                    SelectCommand.Parameters["SUBJID"].Value = subj.PatternMemberID;
                    SelectCommand.Parameters["PREDID"].Value = pred.PatternMemberID;
                    break;
                case "SO":
                    //->S->->O
                    SelectCommand.CommandText = "SELECT TripleFlavor, Context, Subject, Predicate, Object FROM Quadruples WHERE SubjectID = @SUBJID AND ObjectID = @OBJID AND TripleFlavor = @TFV";
                    SelectCommand.Parameters.Clear();
                    SelectCommand.Parameters.Add(new SqliteParameter("SUBJID", SqliteType.Integer));
                    SelectCommand.Parameters.Add(new SqliteParameter("OBJID", SqliteType.Integer));
                    SelectCommand.Parameters.Add(new SqliteParameter("TFV", SqliteType.Integer));
                    SelectCommand.Parameters["SUBJID"].Value = subj.PatternMemberID;
                    SelectCommand.Parameters["OBJID"].Value = obj.PatternMemberID;
                    SelectCommand.Parameters["TFV"].Value = RDFModelEnums.RDFTripleFlavors.SPO;
                    break;
                case "SL":
                    //->S->->L
                    SelectCommand.CommandText = "SELECT TripleFlavor, Context, Subject, Predicate, Object FROM Quadruples WHERE SubjectID = @SUBJID AND ObjectID = @OBJID AND TripleFlavor = @TFV";
                    SelectCommand.Parameters.Clear();
                    SelectCommand.Parameters.Add(new SqliteParameter("SUBJID", SqliteType.Integer));
                    SelectCommand.Parameters.Add(new SqliteParameter("OBJID", SqliteType.Integer));
                    SelectCommand.Parameters.Add(new SqliteParameter("TFV", SqliteType.Integer));
                    SelectCommand.Parameters["SUBJID"].Value = subj.PatternMemberID;
                    SelectCommand.Parameters["OBJID"].Value = lit.PatternMemberID;
                    SelectCommand.Parameters["TFV"].Value = RDFModelEnums.RDFTripleFlavors.SPL;
                    break;
                case "PO":
                    //->->P->O
                    SelectCommand.CommandText = "SELECT TripleFlavor, Context, Subject, Predicate, Object FROM Quadruples WHERE PredicateID = @PREDID AND ObjectID = @OBJID AND TripleFlavor = @TFV";
                    SelectCommand.Parameters.Clear();
                    SelectCommand.Parameters.Add(new SqliteParameter("PREDID", SqliteType.Integer));
                    SelectCommand.Parameters.Add(new SqliteParameter("OBJID", SqliteType.Integer));
                    SelectCommand.Parameters.Add(new SqliteParameter("TFV", SqliteType.Integer));
                    SelectCommand.Parameters["PREDID"].Value = pred.PatternMemberID;
                    SelectCommand.Parameters["OBJID"].Value = obj.PatternMemberID;
                    SelectCommand.Parameters["TFV"].Value = RDFModelEnums.RDFTripleFlavors.SPO;
                    break;
                case "PL":
                    //->->P->L
                    SelectCommand.CommandText = "SELECT TripleFlavor, Context, Subject, Predicate, Object FROM Quadruples WHERE PredicateID = @PREDID AND ObjectID = @OBJID AND TripleFlavor = @TFV";
                    SelectCommand.Parameters.Clear();
                    SelectCommand.Parameters.Add(new SqliteParameter("PREDID", SqliteType.Integer));
                    SelectCommand.Parameters.Add(new SqliteParameter("OBJID", SqliteType.Integer));
                    SelectCommand.Parameters.Add(new SqliteParameter("TFV", SqliteType.Integer));
                    SelectCommand.Parameters["PREDID"].Value = pred.PatternMemberID;
                    SelectCommand.Parameters["OBJID"].Value = lit.PatternMemberID;
                    SelectCommand.Parameters["TFV"].Value = RDFModelEnums.RDFTripleFlavors.SPL;
                    break;
                case "SPO":
                    //->S->P->O
                    SelectCommand.CommandText = "SELECT TripleFlavor, Context, Subject, Predicate, Object FROM Quadruples WHERE SubjectID = @SUBJID AND PredicateID = @PREDID AND ObjectID = @OBJID AND TripleFlavor = @TFV";
                    SelectCommand.Parameters.Clear();
                    SelectCommand.Parameters.Add(new SqliteParameter("SUBJID", SqliteType.Integer));
                    SelectCommand.Parameters.Add(new SqliteParameter("PREDID", SqliteType.Integer));
                    SelectCommand.Parameters.Add(new SqliteParameter("OBJID", SqliteType.Integer));
                    SelectCommand.Parameters.Add(new SqliteParameter("TFV", SqliteType.Integer));
                    SelectCommand.Parameters["SUBJID"].Value = subj.PatternMemberID;
                    SelectCommand.Parameters["PREDID"].Value = pred.PatternMemberID;
                    SelectCommand.Parameters["OBJID"].Value = obj.PatternMemberID;
                    SelectCommand.Parameters["TFV"].Value = RDFModelEnums.RDFTripleFlavors.SPO;
                    break;
                case "SPL":
                    //->S->P->L
                    SelectCommand.CommandText = "SELECT TripleFlavor, Context, Subject, Predicate, Object FROM Quadruples WHERE SubjectID = @SUBJID AND PredicateID = @PREDID AND ObjectID = @OBJID AND TripleFlavor = @TFV";
                    SelectCommand.Parameters.Clear();
                    SelectCommand.Parameters.Add(new SqliteParameter("SUBJID", SqliteType.Integer));
                    SelectCommand.Parameters.Add(new SqliteParameter("PREDID", SqliteType.Integer));
                    SelectCommand.Parameters.Add(new SqliteParameter("OBJID", SqliteType.Integer));
                    SelectCommand.Parameters.Add(new SqliteParameter("TFV", SqliteType.Integer));
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
                using (SqliteDataReader quadruples = SelectCommand.ExecuteReader())
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
                throw new RDFStoreException("Cannot read data from SQLite store because: " + ex.Message, ex);
            }

            return result;
        }

        /// <summary>
        /// Counts the SQLite database quadruples
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
        /// Asynchronously counts the SQLite database quadruples
        /// </summary>
        private Task<long> GetQuadruplesCountAsync()
            => Task.Run(GetQuadruplesCount); //Just a wrapper because SQLite doesn't provide ExecuteScalarAsync override
        #endregion

        #region Diagnostics
        /// <summary>
        /// Performs the preliminary diagnostics controls on the underlying SQLite database
        /// </summary>
        private RDFStoreEnums.RDFStoreSQLErrors Diagnostics()
        {
            try
            {
                //Open connection
                Connection.Open();

                //Create command
                SelectCommand.CommandText = "SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name='Quadruples'";
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
        /// Initializes the underlying SQLite database
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

                        //Create command
                        SqliteCommand createCommand = new SqliteCommand("CREATE TABLE Quadruples (QuadrupleID INTEGER NOT NULL PRIMARY KEY, TripleFlavor INTEGER NOT NULL, Context VARCHAR(1000) NOT NULL, ContextID INTEGER NOT NULL, Subject VARCHAR(1000) NOT NULL, SubjectID INTEGER NOT NULL, Predicate VARCHAR(1000) NOT NULL, PredicateID INTEGER NOT NULL, Object VARCHAR(1000) NOT NULL, ObjectID INTEGER NOT NULL);CREATE INDEX IDX_ContextID ON Quadruples (ContextID);CREATE INDEX IDX_SubjectID ON Quadruples (SubjectID);CREATE INDEX IDX_PredicateID ON Quadruples (PredicateID);CREATE INDEX IDX_ObjectID ON Quadruples (ObjectID,TripleFlavor);CREATE INDEX IDX_SubjectID_PredicateID ON Quadruples (SubjectID,PredicateID);CREATE INDEX IDX_SubjectID_ObjectID ON Quadruples (SubjectID,ObjectID,TripleFlavor);CREATE INDEX IDX_PredicateID_ObjectID ON Quadruples (PredicateID,ObjectID,TripleFlavor);", Connection) { CommandTimeout = 120 };

                        //Execute command
                        createCommand.ExecuteNonQuery();

                        //Close connection
                        Connection.Close();
                    }
                    catch (Exception ex)
                    {
                        //Close connection
                        Connection.Close();

                        //Propagate exception
                        throw new RDFStoreException("Cannot initialize SQLite store because: " + ex.Message, ex);
                    }

                    break;
                //Otherwise, an exception must be thrown because it has not been possible to connect to the database
                case RDFStoreEnums.RDFStoreSQLErrors.InvalidDataSource:
                    throw new RDFStoreException("Cannot initialize SQLite store because: unable to open the database.");
            }
        }
        #endregion

        #region Optimize
        /// <summary>
        /// Executes a special command to optimize SQLite store
        /// </summary>
        public void OptimizeStore()
        {
            try
            {
                //Open connection
                Connection.Open();

                //Create command
                SqliteCommand optimizeCommand = new SqliteCommand("VACUUM", Connection) { CommandTimeout = 120 };

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
                throw new RDFStoreException("Cannot optimize SQLite store because: " + ex.Message, ex);
            }
        }
        #endregion

        #endregion
    }

    /// <summary>
    /// RDFSQLiteStoreOptions is a collector of options for customizing the default behaviour of a SQLite store
    /// </summary>
    public class RDFSQLiteStoreOptions
    {
        #region Properties
        /// <summary>
        /// Timeout in seconds for SELECT queries executed on the SQLite store (default: 120)
        /// </summary>
        public int SelectTimeout { get; set; } = 120;

        /// <summary>
        /// Timeout in seconds for DELETE queries executed on the SQLite store (default: 120)
        /// </summary>
        public int DeleteTimeout { get; set; } = 120;

        /// <summary>
        /// Timeout in seconds for INSERT queries executed on the SQLite store (default: 120)
        /// </summary>
        public int InsertTimeout { get; set; } = 120;
        #endregion
    }
}