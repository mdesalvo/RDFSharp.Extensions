/*
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
using System.IO;
using System.Reflection;
using System.Text;
using FirebirdSql.Data.FirebirdClient;
using RDFSharp.Model;

namespace RDFSharp.Store
{
    /// <summary>
    /// RDFFirebirdStoreEnums represents a collector for all the enumerations used by RDFFirebirdStore class
    /// </summary>
    public static class RDFFirebirdStoreEnums
    {
        /// <summary>
        /// RDFFirebirdVersion represents an enumeration for supported version of Firebird
        /// </summary>
        public enum RDFFirebirdVersion
        {
            /// <summary>
            /// Firebird 2 (ODS=11)
            /// </summary>
            Firebird2 = 11,
            /// <summary>
            /// Firebird 3 (ODS=12)
            /// </summary>
            Firebird3 = 12,
            /// <summary>
            /// Firebird 4 (ODS=13)
            /// </summary>
            Firebird4 = 13
        }
    }

    /// <summary>
    /// RDFFirebirdStore represents a store backed on Firebird engine
    /// </summary>
    public class RDFFirebirdStore : RDFStore, IDisposable
    {
        #region Properties
        /// <summary>
        /// Connection to the Firebird database
        /// </summary>
        internal FbConnection Connection { get; set; }

        /// <summary>
        /// Flag indicating that the Firebird store instance has already been disposed
        /// </summary>
        internal bool Disposed { get; set; }
        #endregion

        #region Ctors
        /// <summary>
        /// Default-ctor to build a Firebird store instance using the given Firebird version
        /// </summary>
        public RDFFirebirdStore(string firebirdConnectionString, RDFFirebirdStoreEnums.RDFFirebirdVersion fbVersion = RDFFirebirdStoreEnums.RDFFirebirdVersion.Firebird2)
        {
            if (string.IsNullOrEmpty(firebirdConnectionString))
                throw new RDFStoreException("Cannot connect to Firebird store because: given \"firebirdConnectionString\" parameter is null or empty.");

            //Initialize store structures
            StoreType = "FIREBIRD";
            Connection = new FbConnection(firebirdConnectionString);
            StoreID = RDFModelUtilities.CreateHash(ToString());
            Disposed = false;

            //Clone internal store template
            if (!File.Exists(Connection.Database))
            {
                try
                {
                    Assembly firebird = Assembly.GetExecutingAssembly();
                    using (Stream templateDB = firebird.GetManifestResourceStream("RDFSharp.Store.Template.RDFFirebirdTemplateODS" + (int)fbVersion + ".fdb"))
                    {
                        using (FileStream targetDB = new FileStream(Connection.Database, FileMode.Create, FileAccess.ReadWrite))
                            templateDB.CopyTo(targetDB);
                    }
                }
                catch (Exception ex)
                {
                    throw new RDFStoreException("Cannot create Firebird store because: " + ex.Message, ex);
                }
            }

            //Perform initial diagnostics
            else
                PrepareStore();
        }

        /// <summary>
        /// Destroys the Firebird store instance
        /// </summary>
        ~RDFFirebirdStore() => Dispose(false);
        #endregion

        #region Interfaces
        /// <summary>
        /// Gives the string representation of the Firebird store 
        /// </summary>
        public override string ToString()
            => string.Concat(base.ToString(), "|SERVER=", Connection.DataSource, ";DATABASE=", Connection.Database);

        /// <summary>
        /// Disposes the Firebird store instance 
        /// </summary>
        public void Dispose()
        {
            Dispose(true);
            GC.SuppressFinalize(this);
        }

        /// <summary>
        /// Disposes the Firebird store instance  (business logic of resources disposal)
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
                FbCommand command = new FbCommand("UPDATE OR INSERT INTO Quadruples (QuadrupleID, TripleFlavor, Context, ContextID, Subject, SubjectID, Predicate, PredicateID, Object, ObjectID) VALUES (@QID, @TFV, @CTX, @CTXID, @SUBJ, @SUBJID, @PRED, @PREDID, @OBJ, @OBJID) MATCHING (QuadrupleID)", Connection);
                command.Parameters.Add(new FbParameter("QID", FbDbType.BigInt));
                command.Parameters.Add(new FbParameter("TFV", FbDbType.Integer));
                command.Parameters.Add(new FbParameter("CTX", FbDbType.VarChar, 1000));
                command.Parameters.Add(new FbParameter("CTXID", FbDbType.BigInt));
                command.Parameters.Add(new FbParameter("SUBJ", FbDbType.VarChar, 1000));
                command.Parameters.Add(new FbParameter("SUBJID", FbDbType.BigInt));
                command.Parameters.Add(new FbParameter("PRED", FbDbType.VarChar, 1000));
                command.Parameters.Add(new FbParameter("PREDID", FbDbType.BigInt));
                command.Parameters.Add(new FbParameter("OBJ", FbDbType.VarChar, 1000));
                command.Parameters.Add(new FbParameter("OBJID", FbDbType.BigInt));

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
                    Connection.Close();
                }
                catch (Exception ex)
                {
                    //Rollback transaction
                    command.Transaction.Rollback();

                    //Close connection
                    Connection.Close();

                    //Propagate exception
                    throw new RDFStoreException("Cannot insert data into Firebird store because: " + ex.Message, ex);
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
                FbCommand command = new FbCommand("UPDATE OR INSERT INTO Quadruples (QuadrupleID, TripleFlavor, Context, ContextID, Subject, SubjectID, Predicate, PredicateID, Object, ObjectID) VALUES (@QID, @TFV, @CTX, @CTXID, @SUBJ, @SUBJID, @PRED, @PREDID, @OBJ, @OBJID) MATCHING (QuadrupleID)", Connection);
                command.Parameters.Add(new FbParameter("QID", FbDbType.BigInt));
                command.Parameters.Add(new FbParameter("TFV", FbDbType.Integer));
                command.Parameters.Add(new FbParameter("CTX", FbDbType.VarChar, 1000));
                command.Parameters.Add(new FbParameter("CTXID", FbDbType.BigInt));
                command.Parameters.Add(new FbParameter("SUBJ", FbDbType.VarChar, 1000));
                command.Parameters.Add(new FbParameter("SUBJID", FbDbType.BigInt));
                command.Parameters.Add(new FbParameter("PRED", FbDbType.VarChar, 1000));
                command.Parameters.Add(new FbParameter("PREDID", FbDbType.BigInt));
                command.Parameters.Add(new FbParameter("OBJ", FbDbType.VarChar, 1000));
                command.Parameters.Add(new FbParameter("OBJID", FbDbType.BigInt));

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
                    throw new RDFStoreException("Cannot insert data into Firebird store because: " + ex.Message, ex);
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
                FbCommand command = new FbCommand("DELETE FROM Quadruples WHERE QuadrupleID = @QID", Connection);
                command.Parameters.Add(new FbParameter("QID", FbDbType.BigInt));

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
                    throw new RDFStoreException("Cannot delete data from Firebird store because: " + ex.Message, ex);
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
                FbCommand command = new FbCommand("DELETE FROM Quadruples WHERE ContextID = @CTXID", Connection);
                command.Parameters.Add(new FbParameter("CTXID", FbDbType.BigInt));

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
                    throw new RDFStoreException("Cannot delete data from Firebird store because: " + ex.Message, ex);
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
                FbCommand command = new FbCommand("DELETE FROM Quadruples WHERE SubjectID = @SUBJID", Connection);
                command.Parameters.Add(new FbParameter("SUBJID", FbDbType.BigInt));

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
                    throw new RDFStoreException("Cannot delete data from Firebird store because: " + ex.Message, ex);
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
                FbCommand command = new FbCommand("DELETE FROM Quadruples WHERE PredicateID = @PREDID", Connection);
                command.Parameters.Add(new FbParameter("PREDID", FbDbType.BigInt));

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
                    throw new RDFStoreException("Cannot delete data from Firebird store because: " + ex.Message, ex);
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
                FbCommand command = new FbCommand("DELETE FROM Quadruples WHERE ObjectID = @OBJID AND TripleFlavor = @TFV", Connection);
                command.Parameters.Add(new FbParameter("OBJID", FbDbType.BigInt));
                command.Parameters.Add(new FbParameter("TFV", FbDbType.Integer));

                //Valorize parameters
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
                    throw new RDFStoreException("Cannot delete data from Firebird store because: " + ex.Message, ex);
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
                FbCommand command = new FbCommand("DELETE FROM Quadruples WHERE ObjectID = @OBJID AND TripleFlavor = @TFV", Connection);
                command.Parameters.Add(new FbParameter("OBJID", FbDbType.BigInt));
                command.Parameters.Add(new FbParameter("TFV", FbDbType.Integer));

                //Valorize parameters
                command.Parameters["OBJID"].Value = literalObject.PatternMemberID;
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
                    throw new RDFStoreException("Cannot delete data from Firebird store because: " + ex.Message, ex);
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
                FbCommand command = new FbCommand("DELETE FROM Quadruples WHERE ContextID = @CTXID AND SubjectID = @SUBJID", Connection);
                command.Parameters.Add(new FbParameter("CTXID", FbDbType.BigInt));
                command.Parameters.Add(new FbParameter("SUBJID", FbDbType.BigInt));

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
                    throw new RDFStoreException("Cannot delete data from Firebird store because: " + ex.Message, ex);
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
                FbCommand command = new FbCommand("DELETE FROM Quadruples WHERE ContextID = @CTXID AND PredicateID = @PREDID", Connection);
                command.Parameters.Add(new FbParameter("CTXID", FbDbType.BigInt));
                command.Parameters.Add(new FbParameter("PREDID", FbDbType.BigInt));

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
                    throw new RDFStoreException("Cannot delete data from Firebird store because: " + ex.Message, ex);
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
                FbCommand command = new FbCommand("DELETE FROM Quadruples WHERE ContextID = @CTXID AND ObjectID = @OBJID AND TripleFlavor = @TFV", Connection);
                command.Parameters.Add(new FbParameter("CTXID", FbDbType.BigInt));
                command.Parameters.Add(new FbParameter("OBJID", FbDbType.BigInt));
                command.Parameters.Add(new FbParameter("TFV", FbDbType.Integer));

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
                    throw new RDFStoreException("Cannot delete data from Firebird store because: " + ex.Message, ex);
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
                FbCommand command = new FbCommand("DELETE FROM Quadruples WHERE ContextID = @CTXID AND ObjectID = @OBJID AND TripleFlavor = @TFV", Connection);
                command.Parameters.Add(new FbParameter("CTXID", FbDbType.BigInt));
                command.Parameters.Add(new FbParameter("OBJID", FbDbType.BigInt));
                command.Parameters.Add(new FbParameter("TFV", FbDbType.Integer));

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
                    throw new RDFStoreException("Cannot delete data from Firebird store because: " + ex.Message, ex);
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
                FbCommand command = new FbCommand("DELETE FROM Quadruples WHERE ContextID = @CTXID AND SubjectID = @SUBJID AND PredicateID = @PREDID", Connection);
                command.Parameters.Add(new FbParameter("CTXID", FbDbType.BigInt));
                command.Parameters.Add(new FbParameter("SUBJID", FbDbType.BigInt));
                command.Parameters.Add(new FbParameter("PREDID", FbDbType.BigInt));

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
                    throw new RDFStoreException("Cannot delete data from Firebird store because: " + ex.Message, ex);
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
                FbCommand command = new FbCommand("DELETE FROM Quadruples WHERE ContextID = @CTXID AND SubjectID = @SUBJID AND ObjectID = @OBJID AND TripleFlavor = @TFV", Connection);
                command.Parameters.Add(new FbParameter("CTXID", FbDbType.BigInt));
                command.Parameters.Add(new FbParameter("SUBJID", FbDbType.BigInt));
                command.Parameters.Add(new FbParameter("OBJID", FbDbType.BigInt));
                command.Parameters.Add(new FbParameter("TFV", FbDbType.Integer));

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
                    throw new RDFStoreException("Cannot delete data from Firebird store because: " + ex.Message, ex);
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
                FbCommand command = new FbCommand("DELETE FROM Quadruples WHERE ContextID = @CTXID AND SubjectID = @SUBJID AND ObjectID = @OBJID AND TripleFlavor = @TFV", Connection);
                command.Parameters.Add(new FbParameter("CTXID", FbDbType.BigInt));
                command.Parameters.Add(new FbParameter("SUBJID", FbDbType.BigInt));
                command.Parameters.Add(new FbParameter("OBJID", FbDbType.BigInt));
                command.Parameters.Add(new FbParameter("TFV", FbDbType.Integer));

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
                    throw new RDFStoreException("Cannot delete data from Firebird store because: " + ex.Message, ex);
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
                FbCommand command = new FbCommand("DELETE FROM Quadruples WHERE ContextID = @CTXID AND PredicateID = @PREDID AND ObjectID = @OBJID AND TripleFlavor = @TFV", Connection);
                command.Parameters.Add(new FbParameter("CTXID", FbDbType.BigInt));
                command.Parameters.Add(new FbParameter("PREDID", FbDbType.BigInt));
                command.Parameters.Add(new FbParameter("OBJID", FbDbType.BigInt));
                command.Parameters.Add(new FbParameter("TFV", FbDbType.Integer));

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
                    throw new RDFStoreException("Cannot delete data from Firebird store because: " + ex.Message, ex);
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
                FbCommand command = new FbCommand("DELETE FROM Quadruples WHERE ContextID = @CTXID AND PredicateID = @PREDID AND ObjectID = @OBJID AND TripleFlavor = @TFV", Connection);
                command.Parameters.Add(new FbParameter("CTXID", FbDbType.BigInt));
                command.Parameters.Add(new FbParameter("PREDID", FbDbType.BigInt));
                command.Parameters.Add(new FbParameter("OBJID", FbDbType.BigInt));
                command.Parameters.Add(new FbParameter("TFV", FbDbType.Integer));

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
                    throw new RDFStoreException("Cannot delete data from Firebird store because: " + ex.Message, ex);
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
                FbCommand command = new FbCommand("DELETE FROM Quadruples WHERE SubjectID = @SUBJID AND PredicateID = @PREDID", Connection);
                command.Parameters.Add(new FbParameter("SUBJID", FbDbType.BigInt));
                command.Parameters.Add(new FbParameter("PREDID", FbDbType.BigInt));

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
                    throw new RDFStoreException("Cannot delete data from Firebird store because: " + ex.Message, ex);
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
                FbCommand command = new FbCommand("DELETE FROM Quadruples WHERE SubjectID = @SUBJID AND ObjectID = @OBJID AND TripleFlavor = @TFV", Connection);
                command.Parameters.Add(new FbParameter("SUBJID", FbDbType.BigInt));
                command.Parameters.Add(new FbParameter("OBJID", FbDbType.BigInt));
                command.Parameters.Add(new FbParameter("TFV", FbDbType.Integer));

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
                    throw new RDFStoreException("Cannot delete data from Firebird store because: " + ex.Message, ex);
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
                FbCommand command = new FbCommand("DELETE FROM Quadruples WHERE SubjectID = @SUBJID AND ObjectID = @OBJID AND TripleFlavor = @TFV", Connection);
                command.Parameters.Add(new FbParameter("SUBJID", FbDbType.BigInt));
                command.Parameters.Add(new FbParameter("OBJID", FbDbType.BigInt));
                command.Parameters.Add(new FbParameter("TFV", FbDbType.Integer));

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
                    throw new RDFStoreException("Cannot delete data from Firebird store because: " + ex.Message, ex);
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
                FbCommand command = new FbCommand("DELETE FROM Quadruples WHERE PredicateID = @PREDID AND ObjectID = @OBJID AND TripleFlavor = @TFV", Connection);
                command.Parameters.Add(new FbParameter("PREDID", FbDbType.BigInt));
                command.Parameters.Add(new FbParameter("OBJID", FbDbType.BigInt));
                command.Parameters.Add(new FbParameter("TFV", FbDbType.Integer));

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
                    throw new RDFStoreException("Cannot delete data from Firebird store because: " + ex.Message, ex);
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
                FbCommand command = new FbCommand("DELETE FROM Quadruples WHERE PredicateID = @PREDID AND ObjectID = @OBJID AND TripleFlavor = @TFV", Connection);
                command.Parameters.Add(new FbParameter("PREDID", FbDbType.BigInt));
                command.Parameters.Add(new FbParameter("OBJID", FbDbType.BigInt));
                command.Parameters.Add(new FbParameter("TFV", FbDbType.Integer));

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
                    throw new RDFStoreException("Cannot delete data from Firebird store because: " + ex.Message, ex);
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
            FbCommand command = new FbCommand("DELETE FROM Quadruples", Connection);

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
                throw new RDFStoreException("Cannot delete data from Firebird store because: " + ex.Message, ex);
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
            FbCommand command = new FbCommand("SELECT EXISTS(SELECT 1 FROM Quadruples WHERE QuadrupleID = @QID)", Connection);
            command.Parameters.Add(new FbParameter("QID", FbDbType.Integer));            

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
                bool result = bool.Parse(command.ExecuteScalar().ToString());                

                //Close connection
                Connection.Close();

                //Give result
                return result;
            }
            catch (Exception ex)
            {
                //Close connection
                Connection.Close();

                //Propagate exception
                throw new RDFStoreException("Cannot read data from Firebird store because: " + ex.Message, ex);
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
            FbCommand command = null;
            switch (queryFilters.ToString())
            {
                case "C":
                    //C->->->
                    command = new FbCommand("SELECT TripleFlavor, Context, Subject, Predicate, Object FROM Quadruples WHERE ContextID = @CTXID", Connection);
                    command.Parameters.Add(new FbParameter("CTXID", FbDbType.Integer));
                    command.Parameters["CTXID"].Value = ctx.PatternMemberID;
                    break;
                case "S":
                    //->S->->
                    command = new FbCommand("SELECT TripleFlavor, Context, Subject, Predicate, Object FROM Quadruples WHERE SubjectID = @SUBJID", Connection);
                    command.Parameters.Add(new FbParameter("SUBJID", FbDbType.Integer));
                    command.Parameters["SUBJID"].Value = subj.PatternMemberID;
                    break;
                case "P":
                    //->->P->
                    command = new FbCommand("SELECT TripleFlavor, Context, Subject, Predicate, Object FROM Quadruples WHERE PredicateID = @PREDID", Connection);
                    command.Parameters.Add(new FbParameter("PREDID", FbDbType.Integer));
                    command.Parameters["PREDID"].Value = pred.PatternMemberID;
                    break;
                case "O":
                    //->->->O
                    command = new FbCommand("SELECT TripleFlavor, Context, Subject, Predicate, Object FROM Quadruples WHERE ObjectID = @OBJID AND TripleFlavor = @TFV", Connection);
                    command.Parameters.Add(new FbParameter("OBJID", FbDbType.Integer));
                    command.Parameters.Add(new FbParameter("TFV", FbDbType.Integer));                    
                    command.Parameters["OBJID"].Value = obj.PatternMemberID;
                    command.Parameters["TFV"].Value = RDFModelEnums.RDFTripleFlavors.SPO;
                    break;
                case "L":
                    //->->->L
                    command = new FbCommand("SELECT TripleFlavor, Context, Subject, Predicate, Object FROM Quadruples WHERE ObjectID = @OBJID AND TripleFlavor = @TFV", Connection);
                    command.Parameters.Add(new FbParameter("OBJID", FbDbType.Integer));
                    command.Parameters.Add(new FbParameter("TFV", FbDbType.Integer));
                    command.Parameters["OBJID"].Value = lit.PatternMemberID;
                    command.Parameters["TFV"].Value = RDFModelEnums.RDFTripleFlavors.SPL;                    
                    break;
                case "CS":
                    //C->S->->
                    command = new FbCommand("SELECT TripleFlavor, Context, Subject, Predicate, Object FROM Quadruples WHERE ContextID = @CTXID AND SubjectID = @SUBJID", Connection);
                    command.Parameters.Add(new FbParameter("CTXID", FbDbType.Integer));
                    command.Parameters.Add(new FbParameter("SUBJID", FbDbType.Integer));
                    command.Parameters["CTXID"].Value = ctx.PatternMemberID;
                    command.Parameters["SUBJID"].Value = subj.PatternMemberID;
                    break;
                case "CP":
                    //C->->P->
                    command = new FbCommand("SELECT TripleFlavor, Context, Subject, Predicate, Object FROM Quadruples WHERE ContextID = @CTXID AND PredicateID = @PREDID", Connection);
                    command.Parameters.Add(new FbParameter("CTXID", FbDbType.Integer));
                    command.Parameters.Add(new FbParameter("PREDID", FbDbType.Integer));
                    command.Parameters["CTXID"].Value = ctx.PatternMemberID;
                    command.Parameters["PREDID"].Value = pred.PatternMemberID;
                    break;
                case "CO":
                    //C->->->O
                    command = new FbCommand("SELECT TripleFlavor, Context, Subject, Predicate, Object FROM Quadruples WHERE ContextID = @CTXID AND ObjectID = @OBJID AND TripleFlavor = @TFV", Connection);
                    command.Parameters.Add(new FbParameter("CTXID", FbDbType.Integer));
                    command.Parameters.Add(new FbParameter("OBJID", FbDbType.Integer));
                    command.Parameters.Add(new FbParameter("TFV", FbDbType.Integer));
                    command.Parameters["CTXID"].Value = ctx.PatternMemberID;
                    command.Parameters["OBJID"].Value = obj.PatternMemberID;
                    command.Parameters["TFV"].Value = RDFModelEnums.RDFTripleFlavors.SPO;
                    break;
                case "CL":
                    //C->->->L
                    command = new FbCommand("SELECT TripleFlavor, Context, Subject, Predicate, Object FROM Quadruples WHERE ContextID = @CTXID AND ObjectID = @OBJID AND TripleFlavor = @TFV", Connection);
                    command.Parameters.Add(new FbParameter("CTXID", FbDbType.Integer));
                    command.Parameters.Add(new FbParameter("OBJID", FbDbType.Integer));
                    command.Parameters.Add(new FbParameter("TFV", FbDbType.Integer));
                    command.Parameters["CTXID"].Value = ctx.PatternMemberID;
                    command.Parameters["OBJID"].Value = lit.PatternMemberID;
                    command.Parameters["TFV"].Value = RDFModelEnums.RDFTripleFlavors.SPL;
                    break;
                case "CSP":
                    //C->S->P->
                    command = new FbCommand("SELECT TripleFlavor, Context, Subject, Predicate, Object FROM Quadruples WHERE ContextID = @CTXID AND SubjectID = @SUBJID AND PredicateID = @PREDID", Connection);
                    command.Parameters.Add(new FbParameter("CTXID", FbDbType.Integer));
                    command.Parameters.Add(new FbParameter("SUBJID", FbDbType.Integer));
                    command.Parameters.Add(new FbParameter("PREDID", FbDbType.Integer));
                    command.Parameters["CTXID"].Value = ctx.PatternMemberID;
                    command.Parameters["SUBJID"].Value = subj.PatternMemberID;
                    command.Parameters["PREDID"].Value = pred.PatternMemberID;
                    break;
                case "CSO":
                    //C->S->->O
                    command = new FbCommand("SELECT TripleFlavor, Context, Subject, Predicate, Object FROM Quadruples WHERE ContextID = @CTXID AND SubjectID = @SUBJID AND ObjectID = @OBJID AND TripleFlavor = @TFV", Connection);
                    command.Parameters.Add(new FbParameter("CTXID", FbDbType.Integer));
                    command.Parameters.Add(new FbParameter("SUBJID", FbDbType.Integer));
                    command.Parameters.Add(new FbParameter("OBJID", FbDbType.Integer));
                    command.Parameters.Add(new FbParameter("TFV", FbDbType.Integer));
                    command.Parameters["CTXID"].Value = ctx.PatternMemberID;
                    command.Parameters["SUBJID"].Value = subj.PatternMemberID;
                    command.Parameters["OBJID"].Value = obj.PatternMemberID;
                    command.Parameters["TFV"].Value = RDFModelEnums.RDFTripleFlavors.SPO;
                    break;
                case "CSL":
                    //C->S->->L
                    command = new FbCommand("SELECT TripleFlavor, Context, Subject, Predicate, Object FROM Quadruples WHERE ContextID = @CTXID AND SubjectID = @SUBJID AND ObjectID = @OBJID AND TripleFlavor = @TFV", Connection);
                    command.Parameters.Add(new FbParameter("CTXID", FbDbType.Integer));
                    command.Parameters.Add(new FbParameter("SUBJID", FbDbType.Integer));
                    command.Parameters.Add(new FbParameter("OBJID", FbDbType.Integer));
                    command.Parameters.Add(new FbParameter("TFV", FbDbType.Integer));
                    command.Parameters["CTXID"].Value = ctx.PatternMemberID;
                    command.Parameters["SUBJID"].Value = subj.PatternMemberID;
                    command.Parameters["OBJID"].Value = lit.PatternMemberID;
                    command.Parameters["TFV"].Value = RDFModelEnums.RDFTripleFlavors.SPL;
                    break;
                case "CPO":
                    //C->->P->O
                    command = new FbCommand("SELECT TripleFlavor, Context, Subject, Predicate, Object FROM Quadruples WHERE ContextID = @CTXID AND PredicateID = @PREDID AND ObjectID = @OBJID AND TripleFlavor = @TFV", Connection);
                    command.Parameters.Add(new FbParameter("CTXID", FbDbType.Integer));
                    command.Parameters.Add(new FbParameter("PREDID", FbDbType.Integer));
                    command.Parameters.Add(new FbParameter("OBJID", FbDbType.Integer));
                    command.Parameters.Add(new FbParameter("TFV", FbDbType.Integer));
                    command.Parameters["CTXID"].Value = ctx.PatternMemberID;
                    command.Parameters["PREDID"].Value = pred.PatternMemberID;
                    command.Parameters["OBJID"].Value = obj.PatternMemberID;
                    command.Parameters["TFV"].Value = RDFModelEnums.RDFTripleFlavors.SPO;
                    break;
                case "CPL":
                    //C->->P->L
                    command = new FbCommand("SELECT TripleFlavor, Context, Subject, Predicate, Object FROM Quadruples WHERE ContextID = @CTXID AND PredicateID = @PREDID AND ObjectID = @OBJID AND TripleFlavor = @TFV", Connection);
                    command.Parameters.Add(new FbParameter("CTXID", FbDbType.Integer));
                    command.Parameters.Add(new FbParameter("PREDID", FbDbType.Integer));
                    command.Parameters.Add(new FbParameter("OBJID", FbDbType.Integer));
                    command.Parameters.Add(new FbParameter("TFV", FbDbType.Integer));
                    command.Parameters["CTXID"].Value = ctx.PatternMemberID;
                    command.Parameters["PREDID"].Value = pred.PatternMemberID;
                    command.Parameters["OBJID"].Value = lit.PatternMemberID;
                    command.Parameters["TFV"].Value = RDFModelEnums.RDFTripleFlavors.SPL;
                    break;
                case "CSPO":
                    //C->S->P->O
                    command = new FbCommand("SELECT TripleFlavor, Context, Subject, Predicate, Object FROM Quadruples WHERE ContextID = @CTXID AND SubjectID = @SUBJID AND PredicateID = @PREDID AND ObjectID = @OBJID AND TripleFlavor = @TFV", Connection);
                    command.Parameters.Add(new FbParameter("CTXID", FbDbType.Integer));
                    command.Parameters.Add(new FbParameter("SUBJID", FbDbType.Integer));
                    command.Parameters.Add(new FbParameter("PREDID", FbDbType.Integer));
                    command.Parameters.Add(new FbParameter("OBJID", FbDbType.Integer));
                    command.Parameters.Add(new FbParameter("TFV", FbDbType.Integer));
                    command.Parameters["CTXID"].Value = ctx.PatternMemberID;
                    command.Parameters["SUBJID"].Value = subj.PatternMemberID;
                    command.Parameters["PREDID"].Value = pred.PatternMemberID;
                    command.Parameters["OBJID"].Value = obj.PatternMemberID;
                    command.Parameters["TFV"].Value = RDFModelEnums.RDFTripleFlavors.SPO;
                    break;
                case "CSPL":
                    //C->S->P->L
                    command = new FbCommand("SELECT TripleFlavor, Context, Subject, Predicate, Object FROM Quadruples WHERE ContextID = @CTXID AND SubjectID = @SUBJID AND PredicateID = @PREDID AND ObjectID = @OBJID AND TripleFlavor = @TFV", Connection);
                    command.Parameters.Add(new FbParameter("CTXID", FbDbType.Integer));
                    command.Parameters.Add(new FbParameter("SUBJID", FbDbType.Integer));
                    command.Parameters.Add(new FbParameter("PREDID", FbDbType.Integer));
                    command.Parameters.Add(new FbParameter("OBJID", FbDbType.Integer));
                    command.Parameters.Add(new FbParameter("TFV", FbDbType.Integer));
                    command.Parameters["CTXID"].Value = ctx.PatternMemberID;
                    command.Parameters["SUBJID"].Value = subj.PatternMemberID;
                    command.Parameters["PREDID"].Value = pred.PatternMemberID;
                    command.Parameters["OBJID"].Value = lit.PatternMemberID;
                    command.Parameters["TFV"].Value = RDFModelEnums.RDFTripleFlavors.SPL;
                    break;
                case "SP":
                    //->S->P->
                    command = new FbCommand("SELECT TripleFlavor, Context, Subject, Predicate, Object FROM Quadruples WHERE SubjectID = @SUBJID AND PredicateID = @PREDID", Connection);
                    command.Parameters.Add(new FbParameter("SUBJID", FbDbType.Integer));
                    command.Parameters.Add(new FbParameter("PREDID", FbDbType.Integer));
                    command.Parameters["SUBJID"].Value = subj.PatternMemberID;
                    command.Parameters["PREDID"].Value = pred.PatternMemberID;
                    break;
                case "SO":
                    //->S->->O
                    command = new FbCommand("SELECT TripleFlavor, Context, Subject, Predicate, Object FROM Quadruples WHERE SubjectID = @SUBJID AND ObjectID = @OBJID AND TripleFlavor = @TFV", Connection);
                    command.Parameters.Add(new FbParameter("SUBJID", FbDbType.Integer));
                    command.Parameters.Add(new FbParameter("OBJID", FbDbType.Integer));
                    command.Parameters.Add(new FbParameter("TFV", FbDbType.Integer));
                    command.Parameters["SUBJID"].Value = subj.PatternMemberID;
                    command.Parameters["OBJID"].Value = obj.PatternMemberID;
                    command.Parameters["TFV"].Value = RDFModelEnums.RDFTripleFlavors.SPO;
                    break;
                case "SL":
                    //->S->->L
                    command = new FbCommand("SELECT TripleFlavor, Context, Subject, Predicate, Object FROM Quadruples WHERE SubjectID = @SUBJID AND ObjectID = @OBJID AND TripleFlavor = @TFV", Connection);
                    command.Parameters.Add(new FbParameter("SUBJID", FbDbType.Integer));
                    command.Parameters.Add(new FbParameter("OBJID", FbDbType.Integer));
                    command.Parameters.Add(new FbParameter("TFV", FbDbType.Integer));
                    command.Parameters["SUBJID"].Value = subj.PatternMemberID;
                    command.Parameters["OBJID"].Value = lit.PatternMemberID;
                    command.Parameters["TFV"].Value = RDFModelEnums.RDFTripleFlavors.SPL;
                    break;
                case "PO":
                    //->->P->O
                    command = new FbCommand("SELECT TripleFlavor, Context, Subject, Predicate, Object FROM Quadruples WHERE PredicateID = @PREDID AND ObjectID = @OBJID AND TripleFlavor = @TFV", Connection);
                    command.Parameters.Add(new FbParameter("PREDID", FbDbType.Integer));
                    command.Parameters.Add(new FbParameter("OBJID", FbDbType.Integer));
                    command.Parameters.Add(new FbParameter("TFV", FbDbType.Integer));
                    command.Parameters["PREDID"].Value = pred.PatternMemberID;
                    command.Parameters["OBJID"].Value = obj.PatternMemberID;
                    command.Parameters["TFV"].Value = RDFModelEnums.RDFTripleFlavors.SPO;
                    break;
                case "PL":
                    //->->P->L
                    command = new FbCommand("SELECT TripleFlavor, Context, Subject, Predicate, Object FROM Quadruples WHERE PredicateID = @PREDID AND ObjectID = @OBJID AND TripleFlavor = @TFV", Connection);
                    command.Parameters.Add(new FbParameter("PREDID", FbDbType.Integer));
                    command.Parameters.Add(new FbParameter("OBJID", FbDbType.Integer));
                    command.Parameters.Add(new FbParameter("TFV", FbDbType.Integer));
                    command.Parameters["PREDID"].Value = pred.PatternMemberID;
                    command.Parameters["OBJID"].Value = lit.PatternMemberID;
                    command.Parameters["TFV"].Value = RDFModelEnums.RDFTripleFlavors.SPL;
                    break;
                case "SPO":
                    //->S->P->O
                    command = new FbCommand("SELECT TripleFlavor, Context, Subject, Predicate, Object FROM Quadruples WHERE SubjectID = @SUBJID AND PredicateID = @PREDID AND ObjectID = @OBJID AND TripleFlavor = @TFV", Connection);
                    command.Parameters.Add(new FbParameter("SUBJID", FbDbType.Integer));
                    command.Parameters.Add(new FbParameter("PREDID", FbDbType.Integer));
                    command.Parameters.Add(new FbParameter("OBJID", FbDbType.Integer));
                    command.Parameters.Add(new FbParameter("TFV", FbDbType.Integer));
                    command.Parameters["SUBJID"].Value = subj.PatternMemberID;
                    command.Parameters["PREDID"].Value = pred.PatternMemberID;
                    command.Parameters["OBJID"].Value = obj.PatternMemberID;
                    command.Parameters["TFV"].Value = RDFModelEnums.RDFTripleFlavors.SPO;
                    break;
                case "SPL":
                    //->S->P->L
                    command = new FbCommand("SELECT TripleFlavor, Context, Subject, Predicate, Object FROM Quadruples WHERE SubjectID = @SUBJID AND PredicateID = @PREDID AND ObjectID = @OBJID AND TripleFlavor = @TFV", Connection);
                    command.Parameters.Add(new FbParameter("SUBJID", FbDbType.Integer));
                    command.Parameters.Add(new FbParameter("PREDID", FbDbType.Integer));
                    command.Parameters.Add(new FbParameter("OBJID", FbDbType.Integer));
                    command.Parameters.Add(new FbParameter("TFV", FbDbType.Integer));
                    command.Parameters["SUBJID"].Value = subj.PatternMemberID;
                    command.Parameters["PREDID"].Value = pred.PatternMemberID;
                    command.Parameters["OBJID"].Value = lit.PatternMemberID;
                    command.Parameters["TFV"].Value = RDFModelEnums.RDFTripleFlavors.SPL;
                    break;
                default:
                    //->->->
                    command = new FbCommand("SELECT TripleFlavor, Context, Subject, Predicate, Object FROM Quadruples", Connection);
                    break;
            }

            //Prepare and execute command
            try
            {
                //Open connection
                Connection.Open();

                //Prepare command
                command.Prepare();
                command.CommandTimeout = 180; //Setup 3mins timeout

                //Execute command
                using (FbDataReader quadruples = command.ExecuteReader())
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
                throw new RDFStoreException("Cannot read data from Firebird store because: " + ex.Message, ex);
            }

            return result;
        }
        #endregion

        #region Diagnostics
        /// <summary>
        /// Performs the preliminary diagnostics controls on the underlying Firebird database
        /// </summary>
        private RDFStoreEnums.RDFStoreSQLErrors Diagnostics()
        {
            try
            {
                //Open connection
                Connection.Open();

                //Create command
                FbCommand command = new FbCommand("SELECT COUNT(*) FROM RDB$RELATIONS WHERE RDB$RELATION_NAME = 'QUADRUPLES'", Connection);

                //Execute command
                int result = int.Parse(command.ExecuteScalar().ToString());

                //Close connection
                Connection.Close();

                //Return the diagnostics state
                return result == 0 ? RDFStoreEnums.RDFStoreSQLErrors.QuadruplesTableNotFound : RDFStoreEnums.RDFStoreSQLErrors.NoErrors;
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
        /// Prepares the underlying Firebird database
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
                    FbCommand command = new FbCommand("CREATE TABLE Quadruples (QuadrupleID BIGINT NOT NULL PRIMARY KEY, TripleFlavor INTEGER NOT NULL, Context VARCHAR(1000) NOT NULL, ContextID BIGINT NOT NULL, Subject VARCHAR(1000) NOT NULL, SubjectID BIGINT NOT NULL, Predicate VARCHAR(1000) NOT NULL, PredicateID BIGINT NOT NULL, Object VARCHAR(1000) NOT NULL, ObjectID BIGINT NOT NULL)", Connection);
                    command.ExecuteNonQuery();
                    command.CommandText = "CREATE INDEX IDX_ContextID ON Quadruples(ContextID)";
                    command.ExecuteNonQuery();
                    command.CommandText = "CREATE INDEX IDX_SubjectID ON Quadruples(SubjectID)";
                    command.ExecuteNonQuery();
                    command.CommandText = "CREATE INDEX IDX_PredicateID ON Quadruples(PredicateID)";
                    command.ExecuteNonQuery();
                    command.CommandText = "CREATE INDEX IDX_ObjectID ON Quadruples(ObjectID,TripleFlavor)";
                    command.ExecuteNonQuery();
                    command.CommandText = "CREATE INDEX IDX_SubjectID_PredicateID ON Quadruples(SubjectID,PredicateID)";
                    command.ExecuteNonQuery();
                    command.CommandText = "CREATE INDEX IDX_SubjectID_ObjectID ON Quadruples(SubjectID,ObjectID,TripleFlavor)";
                    command.ExecuteNonQuery();
                    command.CommandText = "CREATE INDEX IDX_PredicateID_ObjectID ON Quadruples(PredicateID,ObjectID,TripleFlavor)";
                    command.ExecuteNonQuery();

                    //Close connection
                    Connection.Close();
                }
                catch (Exception ex)
                {
                    //Close connection
                    Connection.Close();

                    //Propagate exception
                    throw new RDFStoreException("Cannot prepare Firebird store because: " + ex.Message, ex);
                }
            }

            //Otherwise, an exception must be thrown because it has not been possible to connect to the database
            else if (check == RDFStoreEnums.RDFStoreSQLErrors.InvalidDataSource)
                throw new RDFStoreException("Cannot prepare Firebird store because: unable to open the given datasource.");
        }
        #endregion		

        #endregion
    }
}