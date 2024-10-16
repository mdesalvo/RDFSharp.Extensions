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

using Neo4j.Driver;
using System;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using RDFSharp.Model;
using RDFSharp.Store;

namespace RDFSharp.Extensions.Neo4j
{
    /// <summary>
    /// RDFNeo4jStore represents a RDFStore backed on Neo4j engine
    /// </summary>
    public class RDFNeo4jStore : RDFStore, IDisposable
    {
        #region Properties
        /// <summary>
        /// Count of the Neo4j database quadruples (-1 in case of errors)
        /// </summary>
        public override long QuadruplesCount 
			=> GetQuadruplesCount();

		/// <summary>
        /// Asynchronous count of the Neo4jdatabase quadruples (-1 in case of errors)
        /// </summary>
        public Task<long> QuadruplesCountAsync 
			=> GetQuadruplesCountAsync();

        /// <summary>
        /// Driver to handle underlying Neo4j database
        /// </summary>
        internal IDriver Driver { get; set; } //https://neo4j.com/docs/dotnet-manual/current/client-applications/

        /// <summary>
        /// Flag indicating that the Neo4j store instance has already been disposed
        /// </summary>
        internal bool Disposed { get; set; }
        #endregion

        #region Ctors
        /// <summary>
        /// Default-ctor to build a Neo4j store instance (with eventual options)
        /// </summary>
        public RDFNeo4jStore(string neo4jUri, string neo4jUsername, string neo4jPassword, RDFNeo4jOptions neo4jStoreOptions=null)
        {
            #region Guards
            if (string.IsNullOrEmpty(neo4jUri))
            	throw new RDFStoreException("Cannot connect to Neo4j store because: given \"neo4jConnectionString\" parameter is null or empty.");
            #endregion

            //Initialize options
            if (neo4jStoreOptions == null)
                neo4jStoreOptions = new RDFNeo4jOptions();

            //Initialize store structures
            StoreType = "NEO4J";
            StoreID = RDFModelUtilities.CreateHash(ToString());
            Driver = GraphDatabase.Driver(neo4jUri, AuthTokens.Basic(neo4jUsername, neo4jPassword));
            Disposed = false;

            //Perform initial diagnostics
            InitializeStore();
        }
        
        /// <summary>
        /// Destroys the Neo4j store instance
        /// </summary>
        ~RDFNeo4jStore() => Dispose(false);
        #endregion

        #region Interfaces
        /// <summary>
        /// Gives the string representation of the Neo4j store
        /// </summary>
        public override string ToString()
            => string.Concat(base.ToString());

        /// <summary>
        /// Disposes the Neo4j storeinstance 
        /// </summary>
        public void Dispose()
        {
            Dispose(true);
            GC.SuppressFinalize(this);
        }

        /// <summary>
        /// Disposes the Neo4j store instance (business logic of resources disposal)
        /// </summary>
        protected virtual void Dispose(bool disposing)
        {
            if (Disposed)
                return;

            if (disposing)
            {
                //Dispose
                Driver.Dispose();
                //remove
                Driver = null;
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
                
                try
                {
                    //Open connection
                    
                    //Prepare command
                    
                    //Open transaction
                    
                    //Iterate triples
                    foreach (RDFTriple triple in graph)
                    {
                        //Valorize parameters
                        
                        //Execute command
                        
                    }

                    //Close transaction
                    
                    //Close connection
                    
                }
                catch (Exception ex)
                {
                    //Rollback transaction
                    
                    //Close connection
                    
                    //Propagate exception
                    throw new RDFStoreException("Cannot insert data into Neo4j store because: " + ex.Message, ex);
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
                
                //Valorize parameters
                
                try
                {
                    //Open connection
                    
                    //Prepare command
                    
                    //Open transaction
                    
                    //Execute command
                    
                    //Close transaction
                    
                    //Close connection
                    
                }
                catch (Exception ex)
                {
                    //Rollback transaction
                    
                    //Close connection
                    
                    //Propagate exception
                    throw new RDFStoreException("Cannot insert data into Neo4j store because: " + ex.Message, ex);
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
                
                //Valorize parameters
                
                try
                {
                    //Open connection
                    
                    //Prepare command
                    
                    //Open transaction
                    
                    //Execute command
                    
                    //Close transaction
                    
                    //Close connection
                    
                }
                catch (Exception ex)
                {
                    //Rollback transaction
                    
                    //Close connection
                    
                    //Propagate exception
                    throw new RDFStoreException("Cannot remove data from Neo4j store because: " + ex.Message, ex);
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
                
                //Valorize parameters
                
                try
                {
                    //Open connection
                    
                    //Prepare command
                    
                    //Open transaction
                    
                    //Execute command
                    
                    //Close transaction
                    
                    //Close connection
                    
                }
                catch (Exception ex)
                {
                    //Rollback transaction
                    
                    //Close connection
                    
                    //Propagate exception
                    throw new RDFStoreException("Cannot remove data from Neo4j store because: " + ex.Message, ex);
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
                
                //Valorize parameters
                
                try
                {
                    //Open connection
                    
                    //Prepare command
                    
                    //Open transaction
                    
                    //Execute command
                    
                    //Close transaction
                    
                    //Close connection
                    
                }
                catch (Exception ex)
                {
                    //Rollback transaction
                    
                    //Close connection
                    
                    //Propagate exception
                    throw new RDFStoreException("Cannot remove data from Neo4j store because: " + ex.Message, ex);
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
                
                //Valorize parameters
                
                try
                {
                    //Open connection
                    
                    //Prepare command
                    
                    //Open transaction
                    
                    //Execute command
                    
                    //Close transaction
                    
                    //Close connection
                    
                }
                catch (Exception ex)
                {
                    //Rollback transaction
                    
                    //Close connection
                    
                    //Propagate exception
                    throw new RDFStoreException("Cannot remove data from Neo4j store because: " + ex.Message, ex);
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
                
                //Valorize parameters
                
                try
                {
                    //Open connection
                    
                    //Prepare command
                    
                    //Open transaction
                    
                    //Execute command
                    
                    //Close transaction
                    
                    //Close connection
                    
                }
                catch (Exception ex)
                {
                    //Rollback transaction
                    
                    //Close connection
                    
                    //Propagate exception
                    throw new RDFStoreException("Cannot remove data from Neo4j store because: " + ex.Message, ex);
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
                
                //Valorize parameters
                
                try
                {
                    //Open connection
                    
                    //Prepare command
                    
                    //Open transaction
                    
                    //Execute command
                    
                    //Close transaction
                    
                    //Close connection
                    
                }
                catch (Exception ex)
                {
                    //Rollback transaction
                    
                    //Close connection
                    
                    //Propagate exception
                    throw new RDFStoreException("Cannot remove data from Neo4j store because: " + ex.Message, ex);
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
                
                //Valorize parameters
                
                try
                {
                    //Open connection
                    
                    //Prepare command
                    
                    //Open transaction
                    
                    //Execute command
                    
                    //Close transaction
                    
                    //Close connection
                    
                }
                catch (Exception ex)
                {
                    //Rollback transaction
                    
                    //Close connection
                    
                    //Propagate exception
                    throw new RDFStoreException("Cannot remove data from Neo4j store because: " + ex.Message, ex);
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
                
                //Valorize parameters
                
                try
                {
                    //Open connection
                    
                    //Prepare command
                    
                    //Open transaction
                    
                    //Execute command
                    
                    //Close transaction
                    
                    //Close connection
                    
                }
                catch (Exception ex)
                {
                    //Rollback transaction
                    
                    //Close connection
                    

                    //Propagate exception
                    throw new RDFStoreException("Cannot remove data from Neo4j store because: " + ex.Message, ex);
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
                
                //Valorize parameters
                
                try
                {
                    //Open connection
                    
                    //Prepare command
                    
                    //Open transaction
                    
                    //Execute command
                    
                    //Close transaction
                    
                    //Close connection
                    
                }
                catch (Exception ex)
                {
                    //Rollback transaction
                    
                    //Close connection
                    
                    //Propagate exception
                    throw new RDFStoreException("Cannot remove data from Neo4j store because: " + ex.Message, ex);
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
                
                //Valorize parameters
                
                try
                {
                    //Open connection
                    
                    //Prepare command
                    
                    //Open transaction
                   

                    //Execute command
                    
                    //Close transaction
                    
                    //Close connection
                    
                }
                catch (Exception ex)
                {
                    //Rollback transaction
                    
                    //Close connection
                    
                    //Propagate exception
                    throw new RDFStoreException("Cannot remove data from Neo4j store because: " + ex.Message, ex);
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
                
                //Valorize parameters
                
                try
                {
                    //Open connection
                    
                    //Prepare command
                    
                    //Open transaction
                    
                    //Execute command
                    
                    //Close transaction
                    
                    //Close connection
                    
                }
                catch (Exception ex)
                {
                    //Rollback transaction
                    
                    //Close connection
                    
                    //Propagate exception
                    throw new RDFStoreException("Cannot remove data from Neo4j store because: " + ex.Message, ex);
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
                
                //Valorize parameters
                
                try
                {
                    //Open connection
                    
                    //Prepare command
                    
                    //Open transaction
                    
                    //Execute command
                    
                    //Close transaction
                    
                    //Close connection
                    
                }
                catch (Exception ex)
                {
                    //Rollback transaction
                    
                    //Close connection
                    
                    //Propagate exception
                    throw new RDFStoreException("Cannot remove data from Neo4j store because: " + ex.Message, ex);
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
                
                //Valorize parameters
                
                try
                {
                    //Open connection
                    
                    //Prepare command
                    
                    //Open transaction
                    
                    //Execute command
                    
                    //Close transaction
                    
                    //Close connection
                    
                }
                catch (Exception ex)
                {
                    //Rollback transaction
                    
                    //Close connection
                    
                    //Propagate exception
                    throw new RDFStoreException("Cannot remove data from Neo4j store because: " + ex.Message, ex);
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
                
                //Valorize parameters
                
                try
                {
                    //Open connection
                    
                    //Prepare command
                    
                    //Open transaction
                    
                    //Execute command
                    
                    //Close transaction
                    
                    //Close connection
                    
                }
                catch (Exception ex)
                {
                    //Rollback transaction
                    
                    //Close connection
                    
                    //Propagate exception
                    throw new RDFStoreException("Cannot remove data from Neo4j store because: " + ex.Message, ex);
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
                
                //Valorize parameters
                
                try
                {
                    //Open connection
                    
                    //Prepare command
                    
                    //Open transaction
                    
                    //Execute command
                    
                    //Close transaction
                    
                    //Close connection
                    
                }
                catch (Exception ex)
                {
                    //Rollback transaction
                    
                    //Close connection
                    
                    //Propagate exception
                    throw new RDFStoreException("Cannot remove data from Neo4j store because: " + ex.Message, ex);
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
                
                //Valorize parameters
                
                try
                {
                    //Open connection
                    
                    //Prepare command
                    
                    //Open transaction
                    
                    //Execute command
                    
                    //Close transaction
                    
                    //Close connection
                    
                }
                catch (Exception ex)
                {
                    //Rollback transaction
                    
                    //Close connection
                    
                    //Propagate exception
                    throw new RDFStoreException("Cannot remove data from Neo4j store because: " + ex.Message, ex);
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
                
                //Valorize parameters
                
                try
                {
                    //Open connection
                    
                    //Prepare command
                    
                    //Open transaction
                    
                    //Execute command
                    
                    //Close transaction
                    
                    //Close connection
                    
                }
                catch (Exception ex)
                {
                    //Rollback transaction
                    
                    //Close connection
                    
                    //Propagate exception
                    throw new RDFStoreException("Cannot remove data from Neo4j store because: " + ex.Message, ex);
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
               

                //Valorize parameters
                
                try
                {
                    //Open connection
                    
                    //Prepare command
                    
                    //Open transaction
                    
                    //Execute command
                    
                    //Close transaction
                    
                    //Close connection
                    
                }
                catch (Exception ex)
                {
                    //Rollback transaction
                    
                    //Close connection
                    
                    //Propagate exception
                    throw new RDFStoreException("Cannot remove data from Neo4j store because: " + ex.Message, ex);
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
                
                //Valorize parameters
                
                try
                {
                    //Open connection
                    
                    //Prepare command
                    
                    //Open transaction
                    
                    //Execute command
                    
                    //Close transaction
                    
                    //Close connection
                    
                }
                catch (Exception ex)
                {
                    //Rollback transaction
                    
                    //Close connection
                    
                    //Propagate exception
                    throw new RDFStoreException("Cannot remove data from Neo4j store because: " + ex.Message, ex);
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
                
                //Valorize parameters
                
                try
                {
                    //Open connection
                    
                    //Prepare command
                    
                    //Open transaction
                    
                    //Execute command
                    
                    //Close transaction
                    
                    //Close connection
                    
                }
                catch (Exception ex)
                {
                    //Rollback transaction
                    
                    //Close connection
                    
                    //Propagate exception
                    throw new RDFStoreException("Cannot remove data from Neo4j store because: " + ex.Message, ex);
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
            
            try
            {
                //Open connection
                
                //Prepare command
                
                //Open transaction
                
                //Execute command
                
                //Close transaction
                
                //Close connection
                
            }
            catch (Exception ex)
            {
                //Rollback transaction
                
                //Close connection
                
                //Propagate exception
                throw new RDFStoreException("Cannot remove data from Neo4j store because: " + ex.Message, ex);
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
            
            //Valorize parameters
            
            //Prepare and execute command
            try
            {
                //Open connection
                
                //Prepare command
                
                //Execute command
                
                //Close connection               

                //Give result
                return false; //TODO
            }
            catch (Exception ex)
            {
                //Close connection
                
                //Propagate exception
                throw new RDFStoreException("Cannot read data from Neo4j store because: " + ex.Message, ex);
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
                    
                    break;
                case "S":
                    //->S->->
                    
                    break;
                case "P":
                    //->->P->
                    
                    break;
                case "O":
                    //->->->O
                    
                    break;
                case "L":
                    //->->->L
                    
                    break;
                case "CS":
                    //C->S->->
                    
                    break;
                case "CP":
                    //C->->P->
                    
                    break;
                case "CO":
                    //C->->->O
                    
                    break;
                case "CL":
                    //C->->->L
                    
                    break;
                case "CSP":
                    //C->S->P->
                    
                    break;
                case "CSO":
                    //C->S->->O
                    
                    break;
                case "CSL":
                    //C->S->->L
                    
                    break;
                case "CPO":
                    //C->->P->O
                    
                    break;
                case "CPL":
                    //C->->P->L
                   
                    break;
                case "CSPO":
                    //C->S->P->O
                   
                    break;
                case "CSPL":
                    //C->S->P->L
                    
                    break;
                case "SP":
                    //->S->P->
                    
                    break;
                case "SO":
                    //->S->->O
                    
                    break;
                case "SL":
                    //->S->->L
                   
                    break;
                case "PO":
                    //->->P->O
                    
                    break;
                case "PL":
                    //->->P->L
                    
                    break;
                case "SPO":
                    //->S->P->O
                    
                    break;
                case "SPL":
                    //->S->P->L
                    
                    break;
                default:
                    //->->->
                    
                    break;
            }

            //Prepare and execute command
            try
            {
                //Open connection               

                //Prepare command                

                //Execute command
               
                //Close connection
                
            }
            catch (Exception ex)
            {
                //Close connection
                
                //Propagate exception
                throw new RDFStoreException("Cannot read data from Neo4j store because: " + ex.Message, ex);
            }

            return result;
        }
        
        /// <summary>
        /// Counts the Neo4j database quadruples
        /// </summary>
        private long GetQuadruplesCount()
        {
            try
            {
                //Open connection
                
                //Create command
               
                //Execute command
                
                //Close connection
                
                //Return the quadruples count
                return  0; //TODO
            }
            catch
            {
                //Close connection
                
                //Return the quadruples count (-1 to indicate error)
                return -1;
            }
        }

		/// <summary>
        /// Asynchronously counts the Neo4j database quadruples
        /// </summary>
        private async Task<long> GetQuadruplesCountAsync()
        {
            try
            {
                //Open connection
                
                //Create command
                
                //Execute command
                
                //Close connection
                
                //Return the quadruples count
                return  0;
            }
            catch
            {
                //Close connection
                
                //Return the quadruples count (-1 to indicate error)
                return -1;
            }
        }
        #endregion

        #region Diagnostics
        /// <summary>
        /// Performs the preliminary diagnostics controls on the underlying Neo4j database
        /// </summary>
        private RDFStoreEnums.RDFStoreSQLErrors Diagnostics()
        {
            try
            {
                //Open connection
                
                //Create command
                
                //Execute command
                
                //Close connection
                
                //Return the diagnostics state
                int result = 0;
                return  result == 0 ? RDFStoreEnums.RDFStoreSQLErrors.QuadruplesTableNotFound
                                    : RDFStoreEnums.RDFStoreSQLErrors.NoErrors;
            }
            catch
            {
                //Close connection
                
                //Return the diagnostics state
                return RDFStoreEnums.RDFStoreSQLErrors.InvalidDataSource;
            }
        }

        /// <summary>
        /// Initializes the underlying Neo4j database
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
                    
                    //Create & Execute command
                    
                    //Close connection
                    
                }
                catch (Exception ex)
                {
                    //Close connection
                   
                    //Propagate exception
                    throw new RDFStoreException("Cannot prepare Neo4j store because: " + ex.Message, ex);
                }
            }

            //Otherwise, an exception must be thrown because it has not been possible to connect to the instance/database
            else if (check == RDFStoreEnums.RDFStoreSQLErrors.InvalidDataSource)
                throw new RDFStoreException("Cannot prepare Neo4j store because: unable to connect to the server instance or to open the selected database.");
        }
        #endregion

        #endregion
    }

    /// <summary>
    /// RDFNeo4jOptions is a collector of options for customizing the default behaviour of an Neo4j store
    /// </summary>
    public class RDFNeo4jOptions
    {
        #region Properties
        /// <summary>
        /// Timeout in seconds for MATCH queries executed on the Neo4j store(default: 120)
        /// </summary>
        public int MatchTimeout { get; set; } = 120;

        /// <summary>
        /// Timeout in seconds for REMOVE queries executed on the Neo4j store(default: 120)
        /// </summary>
        public int RemoveTimeout { get; set; } = 120;

        /// <summary>
        /// Timeout in seconds for MERGE queries executed on the Neo4j store(default: 120)
        /// </summary>
        public int MergeTimeout { get; set; } = 120;
        #endregion
    }
}