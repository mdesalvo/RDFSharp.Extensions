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
        public override long QuadruplesCount => GetQuadruplesCount();

		/// <summary>
        /// Asynchronous count of the Neo4jdatabase quadruples (-1 in case of errors)
        /// </summary>
        public Task<long> QuadruplesCountAsync => GetQuadruplesCountAsync();

        /// <summary>
        /// Driver to handle underlying Neo4j database
        /// </summary>
        public IDriver Driver { get; set; }

        /// <summary>
        /// Flag indicating that the Neo4j store instance has already been disposed
        /// </summary>
        internal bool Disposed { get; set; }
        #endregion

        #region Ctors
        /// <summary>
        /// Default-ctor to build a Neo4j store instance with given credentials
        /// </summary>
        public RDFNeo4jStore(string neo4jUri, string neo4jUsername, string neo4jPassword)
        {
            #region Guards
            if (string.IsNullOrEmpty(neo4jUri))
            	throw new RDFStoreException("Cannot connect to Neo4j store because: given \"neo4jConnectionString\" parameter is null or empty.");
            #endregion

            //Initialize store structures
            StoreType = "NEO4J";
            StoreID = RDFModelUtilities.CreateHash(ToString());
            Driver = GraphDatabase.Driver(neo4jUri, AuthTokens.Basic(neo4jUsername, neo4jPassword));
            Disposed = false;

            //Perform initial diagnostics
            InitializeStoreAsync().GetAwaiter().GetResult();
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
        /// Merges the given graph into the store
        /// </summary>
        public override RDFStore MergeGraph(RDFGraph graph)
        {
            if (graph != null)
            {
                string graphContext = graph.Context.ToString();
                using (IAsyncSession neo4jSession = Driver.AsyncSession())
                {
                    foreach (RDFTriple triple in graph)
                        try
                        {
                            neo4jSession.ExecuteWriteAsync(
                                async tx =>
                                {
                                    switch (triple.TripleFlavor)
                                    {
                                        case RDFModelEnums.RDFTripleFlavors.SPO:
                                            IResultCursor insertSPOResult = await tx.RunAsync(
                                                "MERGE (s:Resource { uri:$subj })-[p:Property { uri:$pred, ctx:$ctx }]->(o:Resource { uri:$obj }) RETURN s,p,o",
                                                new 
                                                { 
                                                    subj=triple.Subject.ToString(), 
                                                    pred=triple.Predicate.ToString(), 
                                                    ctx=graphContext,
                                                    obj=triple.Object.ToString()
                                                });
                                            await insertSPOResult.ConsumeAsync();
                                            break;

                                        case RDFModelEnums.RDFTripleFlavors.SPL:
                                            IResultCursor insertSPLResult = await tx.RunAsync(
                                                "MERGE (s:Resource { uri:$subj })-[p:Property { uri:$pred, ctx:$ctx }]->(l:Literal { value:$val }) RETURN s,p,l",
                                                new 
                                                { 
                                                    subj=triple.Subject.ToString(), 
                                                    pred=triple.Predicate.ToString(), 
                                                    ctx=graphContext,
                                                    val=triple.Object.ToString()
                                                });
                                            await insertSPLResult.ConsumeAsync();
                                            break;
                                    }                                    
                                }).GetAwaiter().GetResult();
                            neo4jSession.CloseAsync().GetAwaiter().GetResult();
                        }
                        catch (Exception ex)
                        {
                            neo4jSession.CloseAsync().GetAwaiter().GetResult();

                            throw new RDFStoreException("Cannot insert data into Neo4j store because: " + ex.Message, ex);
                        }
                }
            }
            return this;
        }

        /// <summary>
        /// Adds the given quadruple to the store
        /// </summary>
        public override RDFStore AddQuadruple(RDFQuadruple quadruple)
        {
            if (quadruple != null)
            {
                using (IAsyncSession neo4jSession = Driver.AsyncSession())
                {
                    try
                    {
                        neo4jSession.ExecuteWriteAsync(
                            async tx =>
                            {
                                switch (quadruple.TripleFlavor)
                                {
                                    case RDFModelEnums.RDFTripleFlavors.SPO:
                                        IResultCursor insertSPOResult = await tx.RunAsync(
                                            "MERGE (s:Resource { uri:$subj })-[p:Property { uri:$pred, ctx:$ctx }]->(o:Resource { uri:$obj }) RETURN s,p,o",
                                            new 
                                            { 
                                                subj=quadruple.Subject.ToString(), 
                                                pred=quadruple.Predicate.ToString(), 
                                                ctx=quadruple.Context.ToString(),
                                                obj=quadruple.Object.ToString()
                                            });
                                        await insertSPOResult.ConsumeAsync();
                                        break;

                                    case RDFModelEnums.RDFTripleFlavors.SPL:
                                        IResultCursor insertSPLResult = await tx.RunAsync(
                                            "MERGE (s:Resource { uri:$subj })-[p:Property { uri:$pred, ctx:$ctx }]->(l:Literal { value:$val }) RETURN s,p,l",
                                            new 
                                            { 
                                                subj=quadruple.Subject.ToString(), 
                                                pred=quadruple.Predicate.ToString(), 
                                                ctx=quadruple.Context.ToString(),
                                                val=quadruple.Object.ToString()
                                            });
                                        await insertSPLResult.ConsumeAsync();
                                        break;
                                }                                
                            }).GetAwaiter().GetResult();
                        neo4jSession.CloseAsync().GetAwaiter().GetResult();
                    }
                    catch (Exception ex)
                    {
                        neo4jSession.CloseAsync().GetAwaiter().GetResult();

                        throw new RDFStoreException("Cannot insert data into Neo4j store because: " + ex.Message, ex);
                    }
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
                using (IAsyncSession neo4jSession = Driver.AsyncSession())
                {
                    try
                    {
                        neo4jSession.ExecuteWriteAsync(
                            async tx =>
                            {
                                switch (quadruple.TripleFlavor)
                                {
                                    case RDFModelEnums.RDFTripleFlavors.SPO:
                                        IResultCursor deleteSPOResult = await tx.RunAsync(
                                            "MATCH (:Resource { uri:$subj })-[p:Property { uri:$pred, ctx:$ctx }]->(:Resource { uri:$obj }) "+
                                            "DELETE p",
                                            new 
                                            { 
                                                subj=quadruple.Subject.ToString(), 
                                                pred=quadruple.Predicate.ToString(), 
                                                ctx=quadruple.Context.ToString(),
                                                obj=quadruple.Object.ToString()
                                            });
                                        await deleteSPOResult.ConsumeAsync();
                                        break;

                                    case RDFModelEnums.RDFTripleFlavors.SPL:
                                        IResultCursor deleteSPLResult = await tx.RunAsync(
                                            "MATCH (:Resource { uri:$subj })-[p:Property { uri:$pred, ctx:$ctx }]->(:Literal { value:$val }) "+
                                            "DELETE p",
                                            new 
                                            { 
                                                subj=quadruple.Subject.ToString(), 
                                                pred=quadruple.Predicate.ToString(), 
                                                ctx=quadruple.Context.ToString(),
                                                val=quadruple.Object.ToString()
                                            });
                                        await deleteSPLResult.ConsumeAsync();
                                        break;
                                }                                
                            }).GetAwaiter().GetResult();
                        neo4jSession.CloseAsync().GetAwaiter().GetResult();
                    }
                    catch (Exception ex)
                    {
                        neo4jSession.CloseAsync().GetAwaiter().GetResult();

                        throw new RDFStoreException("Cannot remove data from Neo4j store because: " + ex.Message, ex);
                    }
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
                using (IAsyncSession neo4jSession = Driver.AsyncSession())
                {
                    try
                    {
                        neo4jSession.ExecuteWriteAsync(
                            async tx =>
                            {
                                IResultCursor deleteCResult = await tx.RunAsync(
                                    "MATCH (:Resource)-[p:Property { ctx:$ctx }]->() "+
                                    "DELETE p",
                                    new 
                                    { 
                                        ctx=contextResource.ToString()
                                    });
                                await deleteCResult.ConsumeAsync();                                
                            }).GetAwaiter().GetResult();
                        neo4jSession.CloseAsync().GetAwaiter().GetResult();
                    }
                    catch (Exception ex)
                    {
                        neo4jSession.CloseAsync().GetAwaiter().GetResult();

                        throw new RDFStoreException("Cannot remove data from Neo4j store because: " + ex.Message, ex);
                    }
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
                using (IAsyncSession neo4jSession = Driver.AsyncSession())
                {
                    try
                    {
                        neo4jSession.ExecuteWriteAsync(
                            async tx =>
                            {
                                IResultCursor deleteSResult = await tx.RunAsync(
                                    "MATCH (:Resource { uri:$subj })-[p:Property]->() "+
                                    "DELETE p",
                                    new 
                                    { 
                                        subj=subjectResource.ToString()
                                    });
                                await deleteSResult.ConsumeAsync();                                
                            }).GetAwaiter().GetResult();
                        neo4jSession.CloseAsync().GetAwaiter().GetResult();
                    }
                    catch (Exception ex)
                    {
                        neo4jSession.CloseAsync().GetAwaiter().GetResult();

                        throw new RDFStoreException("Cannot remove data from Neo4j store because: " + ex.Message, ex);
                    }
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
                using (IAsyncSession neo4jSession = Driver.AsyncSession())
                {
                    try
                    {
                        neo4jSession.ExecuteWriteAsync(
                            async tx =>
                            {
                                IResultCursor deletePResult = await tx.RunAsync(
                                    "MATCH (:Resource)-[p:Property { uri:$pred }]->() "+
                                    "DELETE p",
                                    new 
                                    { 
                                        pred=predicateResource.ToString()
                                    });
                                await deletePResult.ConsumeAsync();                                
                            }).GetAwaiter().GetResult();
                        neo4jSession.CloseAsync().GetAwaiter().GetResult();
                    }
                    catch (Exception ex)
                    {
                        neo4jSession.CloseAsync().GetAwaiter().GetResult();

                        throw new RDFStoreException("Cannot remove data from Neo4j store because: " + ex.Message, ex);
                    }
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
                using (IAsyncSession neo4jSession = Driver.AsyncSession())
                {
                    try
                    {
                        neo4jSession.ExecuteWriteAsync(
                            async tx =>
                            {
                                IResultCursor deleteOResult = await tx.RunAsync(
                                    "MATCH (:Resource)-[p:Property]->(:Resource { uri:$obj }) "+
                                    "DELETE p",
                                    new 
                                    { 
                                        obj=objectResource.ToString()
                                    });
                                await deleteOResult.ConsumeAsync();                                
                            }).GetAwaiter().GetResult();
                        neo4jSession.CloseAsync().GetAwaiter().GetResult();
                    }
                    catch (Exception ex)
                    {
                        neo4jSession.CloseAsync().GetAwaiter().GetResult();

                        throw new RDFStoreException("Cannot remove data from Neo4j store because: " + ex.Message, ex);
                    }
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
                using (IAsyncSession neo4jSession = Driver.AsyncSession())
                {
                    try
                    {
                        neo4jSession.ExecuteWriteAsync(
                            async tx =>
                            {
                                IResultCursor deleteLResult = await tx.RunAsync(
                                    "MATCH (:Resource)-[p:Property]->(:Literal { value:$val }) "+
                                    "DELETE p",
                                    new 
                                    { 
                                        val=literalObject.ToString()
                                    });
                                await deleteLResult.ConsumeAsync();                                
                            }).GetAwaiter().GetResult();
                        neo4jSession.CloseAsync().GetAwaiter().GetResult();
                    }
                    catch (Exception ex)
                    {
                        neo4jSession.CloseAsync().GetAwaiter().GetResult();

                        throw new RDFStoreException("Cannot remove data from Neo4j store because: " + ex.Message, ex);
                    }
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
                using (IAsyncSession neo4jSession = Driver.AsyncSession())
                {
                    try
                    {
                        neo4jSession.ExecuteWriteAsync(
                            async tx =>
                            {
                                IResultCursor deleteCSResult = await tx.RunAsync(
                                    "MATCH (:Resource { uri:$subj })-[p:Property { ctx:$ctx }]->() "+
                                    "DELETE p",
                                    new 
                                    {
                                        subj=subjectResource.ToString(),
                                        ctx=contextResource.ToString()
                                    });
                                await deleteCSResult.ConsumeAsync();                                
                            }).GetAwaiter().GetResult();
                        neo4jSession.CloseAsync().GetAwaiter().GetResult();
                    }
                    catch (Exception ex)
                    {
                        neo4jSession.CloseAsync().GetAwaiter().GetResult();

                        throw new RDFStoreException("Cannot remove data from Neo4j store because: " + ex.Message, ex);
                    }
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
                using (IAsyncSession neo4jSession = Driver.AsyncSession())
                {
                    try
                    {
                        neo4jSession.ExecuteWriteAsync(
                            async tx =>
                            {
                                IResultCursor deleteCPResult = await tx.RunAsync(
                                    "MATCH (:Resource)-[p:Property { uri:$pred, ctx:$ctx }]->() "+
                                    "DELETE p",
                                    new 
                                    {
                                        pred=predicateResource.ToString(),
                                        ctx=contextResource.ToString()
                                    });
                                await deleteCPResult.ConsumeAsync();                                
                            }).GetAwaiter().GetResult();
                        neo4jSession.CloseAsync().GetAwaiter().GetResult();
                    }
                    catch (Exception ex)
                    {
                        neo4jSession.CloseAsync().GetAwaiter().GetResult();

                        throw new RDFStoreException("Cannot remove data from Neo4j store because: " + ex.Message, ex);
                    }
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
                using (IAsyncSession neo4jSession = Driver.AsyncSession())
                {
                    try
                    {
                        neo4jSession.ExecuteWriteAsync(
                            async tx =>
                            {
                                IResultCursor deleteCOResult = await tx.RunAsync(
                                    "MATCH (:Resource)-[p:Property { ctx:$ctx }]->(:Resource { uri:$obj }) "+
                                    "DELETE p",
                                    new 
                                    {
                                        ctx=contextResource.ToString(),
                                        obj=objectResource.ToString()                                        
                                    });
                                await deleteCOResult.ConsumeAsync();                                
                            }).GetAwaiter().GetResult();
                        neo4jSession.CloseAsync().GetAwaiter().GetResult();
                    }
                    catch (Exception ex)
                    {
                        neo4jSession.CloseAsync().GetAwaiter().GetResult();

                        throw new RDFStoreException("Cannot remove data from Neo4j store because: " + ex.Message, ex);
                    }
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
                using (IAsyncSession neo4jSession = Driver.AsyncSession())
                {
                    try
                    {
                        neo4jSession.ExecuteWriteAsync(
                            async tx =>
                            {
                                IResultCursor deleteCLResult = await tx.RunAsync(
                                    "MATCH (:Resource)-[p:Property { ctx:$ctx }]->(:Literal { value:$val }) "+
                                    "DELETE p",
                                    new 
                                    {
                                        ctx=contextResource.ToString(),
                                        val=objectLiteral.ToString()                                        
                                    });
                                await deleteCLResult.ConsumeAsync();                                
                            }).GetAwaiter().GetResult();
                        neo4jSession.CloseAsync().GetAwaiter().GetResult();
                    }
                    catch (Exception ex)
                    {
                        neo4jSession.CloseAsync().GetAwaiter().GetResult();

                        throw new RDFStoreException("Cannot remove data from Neo4j store because: " + ex.Message, ex);
                    }
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
                using (IAsyncSession neo4jSession = Driver.AsyncSession())
                {
                    try
                    {
                        neo4jSession.ExecuteWriteAsync(
                            async tx =>
                            {
                                IResultCursor deleteCSPResult = await tx.RunAsync(
                                    "MATCH (:Resource { uri:$subj })-[p:Property { uri:$pred, ctx:$ctx }]->() "+
                                    "DELETE p",
                                    new 
                                    {
                                        subj=subjectResource.ToString(),
                                        pred=predicateResource.ToString(),
                                        ctx=contextResource.ToString()          
                                    });
                                await deleteCSPResult.ConsumeAsync();                                
                            }).GetAwaiter().GetResult();
                        neo4jSession.CloseAsync().GetAwaiter().GetResult();
                    }
                    catch (Exception ex)
                    {
                        neo4jSession.CloseAsync().GetAwaiter().GetResult();

                        throw new RDFStoreException("Cannot remove data from Neo4j store because: " + ex.Message, ex);
                    }
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
                using (IAsyncSession neo4jSession = Driver.AsyncSession())
                {
                    try
                    {
                        neo4jSession.ExecuteWriteAsync(
                            async tx =>
                            {
                                IResultCursor deleteCSOResult = await tx.RunAsync(
                                    "MATCH (:Resource { uri:$subj })-[p:Property { ctx:$ctx }]->(:Resource { uri:$obj }) "+
                                    "DELETE p",
                                    new 
                                    {
                                        subj=subjectResource.ToString(),
                                        ctx=contextResource.ToString(),
                                        obj=objectResource.ToString(),
                                    });
                                await deleteCSOResult.ConsumeAsync();                                
                            }).GetAwaiter().GetResult();
                        neo4jSession.CloseAsync().GetAwaiter().GetResult();
                    }
                    catch (Exception ex)
                    {
                        neo4jSession.CloseAsync().GetAwaiter().GetResult();

                        throw new RDFStoreException("Cannot remove data from Neo4j store because: " + ex.Message, ex);
                    }
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
                using (IAsyncSession neo4jSession = Driver.AsyncSession())
                {
                    try
                    {
                        neo4jSession.ExecuteWriteAsync(
                            async tx =>
                            {
                                IResultCursor deleteCSLResult = await tx.RunAsync(
                                    "MATCH (:Resource { uri:$subj })-[p:Property { ctx:$ctx }]->(:Literal { value:$val }) "+
                                    "DELETE p",
                                    new 
                                    {
                                        subj=subjectResource.ToString(),
                                        ctx=contextResource.ToString(),
                                        val=objectLiteral.ToString(),
                                    });
                                await deleteCSLResult.ConsumeAsync();                                
                            }).GetAwaiter().GetResult();
                        neo4jSession.CloseAsync().GetAwaiter().GetResult();
                    }
                    catch (Exception ex)
                    {
                        neo4jSession.CloseAsync().GetAwaiter().GetResult();

                        throw new RDFStoreException("Cannot remove data from Neo4j store because: " + ex.Message, ex);
                    }
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
                using (IAsyncSession neo4jSession = Driver.AsyncSession())
                {
                    try
                    {
                        neo4jSession.ExecuteWriteAsync(
                            async tx =>
                            {
                                IResultCursor deleteCPOResult = await tx.RunAsync(
                                    "MATCH (:Resource)-[p:Property { uri:$pred, ctx:$ctx }]->(:Resource { uri:$obj }) "+
                                    "DELETE p",
                                    new 
                                    {
                                        pred=predicateResource.ToString(),
                                        ctx=contextResource.ToString(),
                                        obj=objectResource.ToString(),
                                    });
                                await deleteCPOResult.ConsumeAsync();                                
                            }).GetAwaiter().GetResult();
                        neo4jSession.CloseAsync().GetAwaiter().GetResult();
                    }
                    catch (Exception ex)
                    {
                        neo4jSession.CloseAsync().GetAwaiter().GetResult();

                        throw new RDFStoreException("Cannot remove data from Neo4j store because: " + ex.Message, ex);
                    }
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
                using (IAsyncSession neo4jSession = Driver.AsyncSession())
                {
                    try
                    {
                        neo4jSession.ExecuteWriteAsync(
                            async tx =>
                            {
                                IResultCursor deleteCPLResult = await tx.RunAsync(
                                    "MATCH (:Resource)-[p:Property { uri:$pred, ctx:$ctx }]->(:Literal { value:$val }) "+
                                    "DELETE p",
                                    new 
                                    {
                                        pred=predicateResource.ToString(),
                                        ctx=contextResource.ToString(),
                                        val=objectLiteral.ToString(),
                                    });
                                await deleteCPLResult.ConsumeAsync();                                
                            }).GetAwaiter().GetResult();
                        neo4jSession.CloseAsync().GetAwaiter().GetResult();
                    }
                    catch (Exception ex)
                    {
                        neo4jSession.CloseAsync().GetAwaiter().GetResult();

                        throw new RDFStoreException("Cannot remove data from Neo4j store because: " + ex.Message, ex);
                    }
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
                using (IAsyncSession neo4jSession = Driver.AsyncSession())
                {
                    try
                    {
                        neo4jSession.ExecuteWriteAsync(
                            async tx =>
                            {
                                IResultCursor deleteSPResult = await tx.RunAsync(
                                    "MATCH (:Resource { uri:$subj })-[p:Property { uri:$pred }]->() "+
                                    "DELETE p",
                                    new 
                                    {
                                        subj=subjectResource.ToString(),
                                        pred=predicateResource.ToString()     
                                    });
                                await deleteSPResult.ConsumeAsync();                                
                            }).GetAwaiter().GetResult();
                        neo4jSession.CloseAsync().GetAwaiter().GetResult();
                    }
                    catch (Exception ex)
                    {
                        neo4jSession.CloseAsync().GetAwaiter().GetResult();

                        throw new RDFStoreException("Cannot remove data from Neo4j store because: " + ex.Message, ex);
                    }
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
                using (IAsyncSession neo4jSession = Driver.AsyncSession())
                {
                    try
                    {
                        neo4jSession.ExecuteWriteAsync(
                            async tx =>
                            {
                                IResultCursor deleteSOResult = await tx.RunAsync(
                                    "MATCH (:Resource { uri:$subj })-[p:Property]->(:Resource { uri:$obj }) "+
                                    "DELETE p",
                                    new 
                                    {
                                        subj=subjectResource.ToString(),
                                        obj=objectResource.ToString()     
                                    });
                                await deleteSOResult.ConsumeAsync();                                
                            }).GetAwaiter().GetResult();
                        neo4jSession.CloseAsync().GetAwaiter().GetResult();
                    }
                    catch (Exception ex)
                    {
                        neo4jSession.CloseAsync().GetAwaiter().GetResult();

                        throw new RDFStoreException("Cannot remove data from Neo4j store because: " + ex.Message, ex);
                    }
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
                using (IAsyncSession neo4jSession = Driver.AsyncSession())
                {
                    try
                    {
                        neo4jSession.ExecuteWriteAsync(
                            async tx =>
                            {
                                IResultCursor deleteSLResult = await tx.RunAsync(
                                    "MATCH (:Resource { uri:$subj })-[p:Property]->(:Literal { value:$val }) "+
                                    "DELETE p",
                                    new 
                                    {
                                        subj=subjectResource.ToString(),
                                        val=objectLiteral.ToString()     
                                    });
                                await deleteSLResult.ConsumeAsync();                                
                            }).GetAwaiter().GetResult();
                        neo4jSession.CloseAsync().GetAwaiter().GetResult();
                    }
                    catch (Exception ex)
                    {
                        neo4jSession.CloseAsync().GetAwaiter().GetResult();

                        throw new RDFStoreException("Cannot remove data from Neo4j store because: " + ex.Message, ex);
                    }
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
                using (IAsyncSession neo4jSession = Driver.AsyncSession())
                {
                    try
                    {
                        neo4jSession.ExecuteWriteAsync(
                            async tx =>
                            {
                                IResultCursor deletePOResult = await tx.RunAsync(
                                    "MATCH (:Resource)-[p:Property { uri:$pred }]->(:Resource { uri:$obj }) "+
                                    "DELETE p",
                                    new 
                                    {
                                        pred=predicateResource.ToString(),
                                        obj=objectResource.ToString()     
                                    });
                                await deletePOResult.ConsumeAsync();                                
                            }).GetAwaiter().GetResult();
                        neo4jSession.CloseAsync().GetAwaiter().GetResult();
                    }
                    catch (Exception ex)
                    {
                        neo4jSession.CloseAsync().GetAwaiter().GetResult();

                        throw new RDFStoreException("Cannot remove data from Neo4j store because: " + ex.Message, ex);
                    }
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
                using (IAsyncSession neo4jSession = Driver.AsyncSession())
                {
                    try
                    {
                        neo4jSession.ExecuteWriteAsync(
                            async tx =>
                            {
                                IResultCursor deletePLResult = await tx.RunAsync(
                                    "MATCH (:Resource)-[p:Property { uri:$pred }]->(:Literal { value:$val }) "+
                                    "DELETE p",
                                    new 
                                    {
                                        pred=predicateResource.ToString(),
                                        val=objectLiteral.ToString()     
                                    });
                                await deletePLResult.ConsumeAsync();                                
                            }).GetAwaiter().GetResult();
                        neo4jSession.CloseAsync().GetAwaiter().GetResult();
                    }
                    catch (Exception ex)
                    {
                        neo4jSession.CloseAsync().GetAwaiter().GetResult();

                        throw new RDFStoreException("Cannot remove data from Neo4j store because: " + ex.Message, ex);
                    }
                }
            }
            return this;
        }

        /// <summary>
        /// Clears the quadruples of the store
        /// </summary>
        public override void ClearQuadruples()
        {
            using (IAsyncSession neo4jSession = Driver.AsyncSession())
            {
                try
                {
                    neo4jSession.ExecuteWriteAsync(
                        async tx =>
                        {
                            IResultCursor deleteAllResult = await tx.RunAsync(
                                "MATCH (n) DETACH DELETE (n)", null);
                            await deleteAllResult.ConsumeAsync();                           
                        }).GetAwaiter().GetResult();
                    neo4jSession.CloseAsync().GetAwaiter().GetResult();
                }
                catch (Exception ex)
                {
                    neo4jSession.CloseAsync().GetAwaiter().GetResult();

                    throw new RDFStoreException("Cannot remove data from Neo4j store because: " + ex.Message, ex);
                }
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

            using (IAsyncSession neo4jSession = Driver.AsyncSession())
            {
                try
                {
                    bool result = false;

                    neo4jSession.ExecuteReadAsync(
                        async tx =>
                        {
                            switch (quadruple.TripleFlavor)
                            {
                                case RDFModelEnums.RDFTripleFlavors.SPO:
                                    IResultCursor matchSPOResult = await tx.RunAsync(
                                        "MATCH (:Resource { uri:$subj })-[p:Property { uri:$pred, ctx:$ctx }]->(:Resource { uri:$obj }) "+
                                        "RETURN (COUNT(*) > 0) AS checkExists",
                                        new 
                                        { 
                                            subj=quadruple.Subject.ToString(), 
                                            pred=quadruple.Predicate.ToString(), 
                                            ctx=quadruple.Context.ToString(),
                                            obj=quadruple.Object.ToString()
                                        });
                                    IRecord matchSPORecord = await matchSPOResult.SingleAsync();
                                    result = matchSPORecord.Get<bool>("checkExists");
                                    break;

                                case RDFModelEnums.RDFTripleFlavors.SPL:
                                    IResultCursor matchSPLResult = await tx.RunAsync(
                                        "MATCH (:Resource { uri:$subj })-[p:Property { uri:$pred, ctx:$ctx }]->(:Literal { value:$val }) "+
                                        "RETURN (COUNT(*) > 0) AS checkExists",
                                        new 
                                        { 
                                            subj=quadruple.Subject.ToString(), 
                                            pred=quadruple.Predicate.ToString(), 
                                            ctx=quadruple.Context.ToString(),
                                            val=quadruple.Object.ToString()
                                        });
                                    IRecord matchSPLRecord = await matchSPLResult.SingleAsync();
                                    result = matchSPLRecord.Get<bool>("checkExists");
                                    break;
                            }                                
                        }).GetAwaiter().GetResult();
                    neo4jSession.CloseAsync().GetAwaiter().GetResult();

                    return result;
                }
                catch (Exception ex)
                {
                    neo4jSession.CloseAsync().GetAwaiter().GetResult();

                    throw new RDFStoreException("Cannot read data from Neo4j store because: " + ex.Message, ex);
                }
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
        /// Initializes the underlying Neo4j database
        /// </summary>
        private async Task InitializeStoreAsync()
        {
            using (IAsyncSession neo4jSession = Driver.AsyncSession())
            {
                try
                {
                    //Indicize r:Resource nodes
                    await neo4jSession.ExecuteWriteAsync(
                        async tx =>
                        {
                            IResultCursor resourceIdxResult = await tx.RunAsync(
                                "CREATE INDEX resIdx IF NOT EXISTS FOR (r:Resource) ON (r.uri) OPTIONS {}", null);
                            return await resourceIdxResult.ConsumeAsync();
                        });

                    //Indicize p:Property arcs
                    await neo4jSession.ExecuteWriteAsync(
                        async tx =>
                        {
                            IResultCursor propertyIdxResult = await tx.RunAsync(
                                "CREATE INDEX propIdx IF NOT EXISTS FOR ()-[p:Property]->() ON (p.uri) OPTIONS {}", null);
                            return await propertyIdxResult.ConsumeAsync();
                        });
                    await neo4jSession.ExecuteWriteAsync(
                        async tx =>
                        {
                            IResultCursor contextIdxResult = await tx.RunAsync(
                                "CREATE INDEX ctxIdx IF NOT EXISTS FOR ()-[p:Property]->() ON (p.ctx) OPTIONS {}", null);
                            return await contextIdxResult.ConsumeAsync();
                        });

                    //Indicize l:Literal nodes
                    await neo4jSession.ExecuteWriteAsync(
                        async tx =>
                        {
                            IResultCursor literalIdxResult = await tx.RunAsync(
                                "CREATE INDEX litIdx IF NOT EXISTS FOR (l:Literal) ON (l.value) OPTIONS {}", null);
                            return await literalIdxResult.ConsumeAsync();
                        });

                    await neo4jSession.CloseAsync();
                }
                catch (Exception ex)
                {
                    await neo4jSession.CloseAsync();

                    //Propagate exception
                    throw new RDFStoreException("Cannot prepare Neo4j store because: " + ex.Message, ex);
                }
            }
        }
        #endregion

        #endregion
    }
}