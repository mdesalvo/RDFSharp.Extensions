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

using Neo4j.Driver;
using System;
using System.Text;
using System.Threading.Tasks;
using RDFSharp.Model;
using RDFSharp.Store;
using RDFSharp.Query;

namespace RDFSharp.Extensions.Neo4j
{
    /// <summary>
    /// RDFNeo4jStore represents a RDFStore backed on Neo4j engine
    /// </summary>
    public sealed class RDFNeo4jStore : RDFStore, IDisposable
    {
        #region Properties
        /// <summary>
        /// Count of the Neo4j database quadruples (-1 in case of errors)
        /// </summary>
        public override long QuadruplesCount => GetQuadruplesCount();

        /// <summary>
        /// Asynchronous count of the Neo4j database quadruples (-1 in case of errors)
        /// </summary>
        public Task<long> QuadruplesCountAsync => GetQuadruplesCountAsync();

        /// <summary>
        /// Driver to handle underlying Neo4j database
        /// </summary>
        private IDriver Driver { get; set; }
        private IServerInfo ServerInfo { get; set; }

        /// <summary>
        /// Name of underlying Neo4j database
        /// </summary>
        private string DatabaseName { get; }

        /// <summary>
        /// Flag indicating that the Neo4j store instance has already been disposed
        /// </summary>
        private bool Disposed { get; set; }
        #endregion

        #region Ctors
        /// <summary>
        /// Default-ctor to build a Neo4j store instance with given credentials
        /// </summary>
        public RDFNeo4jStore(string neo4jUri, string neo4jUsername, string neo4jPassword, string databaseName="neo4j")
        {
            #region Guards
            if (string.IsNullOrEmpty(neo4jUri))
                throw new RDFStoreException("Cannot connect to Neo4j store because: given \"neo4jUri\" parameter is null or empty.");
            if (string.IsNullOrEmpty(neo4jUsername))
                throw new RDFStoreException("Cannot connect to Neo4j store because: given \"neo4jUsername\" parameter is null or empty.");
            if (string.IsNullOrEmpty(neo4jPassword))
                throw new RDFStoreException("Cannot connect to Neo4j store because: given \"neo4jPassword\" parameter is null or empty.");
            #endregion

            //Initialize driver
            IAuthToken token = AuthTokens.Basic(neo4jUsername, neo4jPassword);
            Driver = GraphDatabase.Driver(neo4jUri, token);
            try
            {
                Driver.VerifyConnectivityAsync().GetAwaiter().GetResult();
                Driver.VerifyAuthenticationAsync(token).GetAwaiter().GetResult();
                //Fetch server info
                ServerInfo = Driver.GetServerInfoAsync().GetAwaiter().GetResult();
            }
            catch (Exception ex)
            {
                throw new RDFStoreException("Cannot connect to Neo4j store because: " + ex.Message, ex);
            }

            //Initialize store
            DatabaseName = databaseName;
            StoreType = "NEO4J";
            StoreID = RDFModelUtilities.CreateHash(ToString());
            Disposed = false;

            //Prepare store
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
            => $"{base.ToString()}|ADDRESS={ServerInfo.Address}|AGENT={ServerInfo.Agent}|PROTOCOL={ServerInfo.ProtocolVersion}|DATABASE={DatabaseName}";

        /// <summary>
        /// Disposes the Neo4j store instance
        /// </summary>
        public void Dispose()
        {
            Dispose(true);
            GC.SuppressFinalize(this);
        }

        /// <summary>
        /// Disposes the Neo4j store instance (business logic of resources disposal)
        /// </summary>
        private void Dispose(bool disposing)
        {
            if (Disposed)
                return;

            if (disposing)
            {
                //Dispose
                Driver.Dispose();
                //Remove
                Driver = null;
                ServerInfo = null;
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
                RDFContext graphContext = new RDFContext(graph.Context);
                foreach (RDFTriple triple in graph)
                    AddQuadruple(new RDFQuadruple(graphContext, triple));
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
                using (IAsyncSession neo4jSession = Driver.AsyncSession(s => s.WithDatabase(DatabaseName)))
                {
                    try
                    {
                        neo4jSession.ExecuteWriteAsync(
                            async tx =>
                            {
                                switch (quadruple.TripleFlavor)
                                {
                                    case RDFModelEnums.RDFTripleFlavors.SPO:
                                        await tx.RunAsync(
                                            "MERGE (s:Resource { uri:$subj })-[p:Property { uri:$pred, ctx:$ctx }]->(o:Resource { uri:$obj })",
                                            new
                                            {
                                                subj = quadruple.Subject.ToString(),
                                                pred = quadruple.Predicate.ToString(),
                                                ctx = quadruple.Context.ToString(),
                                                obj = quadruple.Object.ToString()
                                            });
                                        break;

                                    case RDFModelEnums.RDFTripleFlavors.SPL:
                                        await tx.RunAsync(
                                            "MERGE (s:Resource { uri:$subj })-[p:Property { uri:$pred, ctx:$ctx }]->(l:Literal { value:$val })",
                                            new
                                            {
                                                subj = quadruple.Subject.ToString(),
                                                pred = quadruple.Predicate.ToString(),
                                                ctx = quadruple.Context.ToString(),
                                                val = quadruple.Object.ToString()
                                            });
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
                using (IAsyncSession neo4jSession = Driver.AsyncSession(s => s.WithDatabase(DatabaseName)))
                {
                    try
                    {
                        neo4jSession.ExecuteWriteAsync(
                            async tx =>
                            {
                                switch (quadruple.TripleFlavor)
                                {
                                    case RDFModelEnums.RDFTripleFlavors.SPO:
                                        await tx.RunAsync(
                                            "MATCH (s:Resource { uri:$subj })-[p:Property { uri:$pred, ctx:$ctx }]->(o:Resource { uri:$obj }) " +
                                            "DELETE p",
                                            new
                                            {
                                                subj = quadruple.Subject.ToString(),
                                                pred = quadruple.Predicate.ToString(),
                                                ctx = quadruple.Context.ToString(),
                                                obj = quadruple.Object.ToString()
                                            });
                                        break;

                                    case RDFModelEnums.RDFTripleFlavors.SPL:
                                        await tx.RunAsync(
                                            "MATCH (s:Resource { uri:$subj })-[p:Property { uri:$pred, ctx:$ctx }]->(l:Literal { value:$val }) " +
                                            "DELETE p",
                                            new
                                            {
                                                subj = quadruple.Subject.ToString(),
                                                pred = quadruple.Predicate.ToString(),
                                                ctx = quadruple.Context.ToString(),
                                                val = quadruple.Object.ToString()
                                            });
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
                using (IAsyncSession neo4jSession = Driver.AsyncSession(s => s.WithDatabase(DatabaseName)))
                {
                    try
                    {
                        neo4jSession.ExecuteWriteAsync(
                            async tx =>
                            {
                                await tx.RunAsync(
                                    "MATCH (s:Resource)-[p:Property { ctx:$ctx }]->() " +
                                    "DELETE p",
                                    new
                                    {
                                        ctx = contextResource.ToString()
                                    });
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
                using (IAsyncSession neo4jSession = Driver.AsyncSession(s => s.WithDatabase(DatabaseName)))
                {
                    try
                    {
                        neo4jSession.ExecuteWriteAsync(
                            async tx =>
                            {
                                await tx.RunAsync(
                                    "MATCH (s:Resource { uri:$subj })-[p:Property]->() " +
                                    "DELETE p",
                                    new
                                    {
                                        subj = subjectResource.ToString()
                                    });
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
                using (IAsyncSession neo4jSession = Driver.AsyncSession(s => s.WithDatabase(DatabaseName)))
                {
                    try
                    {
                        neo4jSession.ExecuteWriteAsync(
                            async tx =>
                            {
                                await tx.RunAsync(
                                    "MATCH (s:Resource)-[p:Property { uri:$pred }]->() " +
                                    "DELETE p",
                                    new
                                    {
                                        pred = predicateResource.ToString()
                                    });
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
                using (IAsyncSession neo4jSession = Driver.AsyncSession(s => s.WithDatabase(DatabaseName)))
                {
                    try
                    {
                        neo4jSession.ExecuteWriteAsync(
                            async tx =>
                            {
                                await tx.RunAsync(
                                    "MATCH (s:Resource)-[p:Property]->(o:Resource { uri:$obj }) " +
                                    "DELETE p",
                                    new
                                    {
                                        obj = objectResource.ToString()
                                    });
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
                using (IAsyncSession neo4jSession = Driver.AsyncSession(s => s.WithDatabase(DatabaseName)))
                {
                    try
                    {
                        neo4jSession.ExecuteWriteAsync(
                            async tx =>
                            {
                                await tx.RunAsync(
                                    "MATCH (s:Resource)-[p:Property]->(l:Literal { value:$val }) " +
                                    "DELETE p",
                                    new
                                    {
                                        val = literalObject.ToString()
                                    });
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
                using (IAsyncSession neo4jSession = Driver.AsyncSession(s => s.WithDatabase(DatabaseName)))
                {
                    try
                    {
                        neo4jSession.ExecuteWriteAsync(
                            async tx =>
                            {
                                await tx.RunAsync(
                                    "MATCH (s:Resource { uri:$subj })-[p:Property { ctx:$ctx }]->() " +
                                    "DELETE p",
                                    new
                                    {
                                        subj = subjectResource.ToString(),
                                        ctx = contextResource.ToString()
                                    });
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
                using (IAsyncSession neo4jSession = Driver.AsyncSession(s => s.WithDatabase(DatabaseName)))
                {
                    try
                    {
                        neo4jSession.ExecuteWriteAsync(
                            async tx =>
                            {
                                await tx.RunAsync(
                                    "MATCH (s:Resource)-[p:Property { uri:$pred, ctx:$ctx }]->() " +
                                    "DELETE p",
                                    new
                                    {
                                        pred = predicateResource.ToString(),
                                        ctx = contextResource.ToString()
                                    });
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
                using (IAsyncSession neo4jSession = Driver.AsyncSession(s => s.WithDatabase(DatabaseName)))
                {
                    try
                    {
                        neo4jSession.ExecuteWriteAsync(
                            async tx =>
                            {
                                await tx.RunAsync(
                                    "MATCH (s:Resource)-[p:Property { ctx:$ctx }]->(o:Resource { uri:$obj }) " +
                                    "DELETE p",
                                    new
                                    {
                                        ctx = contextResource.ToString(),
                                        obj = objectResource.ToString()
                                    });
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
                using (IAsyncSession neo4jSession = Driver.AsyncSession(s => s.WithDatabase(DatabaseName)))
                {
                    try
                    {
                        neo4jSession.ExecuteWriteAsync(
                            async tx =>
                            {
                                await tx.RunAsync(
                                    "MATCH (s:Resource)-[p:Property { ctx:$ctx }]->(l:Literal { value:$val }) " +
                                    "DELETE p",
                                    new
                                    {
                                        ctx = contextResource.ToString(),
                                        val = objectLiteral.ToString()
                                    });
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
                using (IAsyncSession neo4jSession = Driver.AsyncSession(s => s.WithDatabase(DatabaseName)))
                {
                    try
                    {
                        neo4jSession.ExecuteWriteAsync(
                            async tx =>
                            {
                                await tx.RunAsync(
                                    "MATCH (s:Resource { uri:$subj })-[p:Property { uri:$pred, ctx:$ctx }]->() " +
                                    "DELETE p",
                                    new
                                    {
                                        subj = subjectResource.ToString(),
                                        pred = predicateResource.ToString(),
                                        ctx = contextResource.ToString()
                                    });
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
                using (IAsyncSession neo4jSession = Driver.AsyncSession(s => s.WithDatabase(DatabaseName)))
                {
                    try
                    {
                        neo4jSession.ExecuteWriteAsync(
                            async tx =>
                            {
                                await tx.RunAsync(
                                    "MATCH (s:Resource { uri:$subj })-[p:Property { ctx:$ctx }]->(o:Resource { uri:$obj }) " +
                                    "DELETE p",
                                    new
                                    {
                                        subj = subjectResource.ToString(),
                                        ctx = contextResource.ToString(),
                                        obj = objectResource.ToString()
                                    });
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
                using (IAsyncSession neo4jSession = Driver.AsyncSession(s => s.WithDatabase(DatabaseName)))
                {
                    try
                    {
                        neo4jSession.ExecuteWriteAsync(
                            async tx =>
                            {
                                await tx.RunAsync(
                                    "MATCH (s:Resource { uri:$subj })-[p:Property { ctx:$ctx }]->(l:Literal { value:$val }) " +
                                    "DELETE p",
                                    new
                                    {
                                        subj = subjectResource.ToString(),
                                        ctx = contextResource.ToString(),
                                        val = objectLiteral.ToString()
                                    });
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
                using (IAsyncSession neo4jSession = Driver.AsyncSession(s => s.WithDatabase(DatabaseName)))
                {
                    try
                    {
                        neo4jSession.ExecuteWriteAsync(
                            async tx =>
                            {
                                await tx.RunAsync(
                                    "MATCH (s:Resource)-[p:Property { uri:$pred, ctx:$ctx }]->(o:Resource { uri:$obj }) " +
                                    "DELETE p",
                                    new
                                    {
                                        pred = predicateResource.ToString(),
                                        ctx = contextResource.ToString(),
                                        obj = objectResource.ToString()
                                    });
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
                using (IAsyncSession neo4jSession = Driver.AsyncSession(s => s.WithDatabase(DatabaseName)))
                {
                    try
                    {
                        neo4jSession.ExecuteWriteAsync(
                            async tx =>
                            {
                                await tx.RunAsync(
                                    "MATCH (s:Resource)-[p:Property { uri:$pred, ctx:$ctx }]->(l:Literal { value:$val }) " +
                                    "DELETE p",
                                    new
                                    {
                                        pred = predicateResource.ToString(),
                                        ctx = contextResource.ToString(),
                                        val = objectLiteral.ToString()
                                    });
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
                using (IAsyncSession neo4jSession = Driver.AsyncSession(s => s.WithDatabase(DatabaseName)))
                {
                    try
                    {
                        neo4jSession.ExecuteWriteAsync(
                            async tx =>
                            {
                                await tx.RunAsync(
                                    "MATCH (s:Resource { uri:$subj })-[p:Property { uri:$pred }]->() " +
                                    "DELETE p",
                                    new
                                    {
                                        subj = subjectResource.ToString(),
                                        pred = predicateResource.ToString()
                                    });
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
                using (IAsyncSession neo4jSession = Driver.AsyncSession(s => s.WithDatabase(DatabaseName)))
                {
                    try
                    {
                        neo4jSession.ExecuteWriteAsync(
                            async tx =>
                            {
                                await tx.RunAsync(
                                    "MATCH (s:Resource { uri:$subj })-[p:Property]->(o:Resource { uri:$obj }) " +
                                    "DELETE p",
                                    new
                                    {
                                        subj = subjectResource.ToString(),
                                        obj = objectResource.ToString()
                                    });
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
                using (IAsyncSession neo4jSession = Driver.AsyncSession(s => s.WithDatabase(DatabaseName)))
                {
                    try
                    {
                        neo4jSession.ExecuteWriteAsync(
                            async tx =>
                            {
                                await tx.RunAsync(
                                    "MATCH (s:Resource { uri:$subj })-[p:Property]->(l:Literal { value:$val }) " +
                                    "DELETE p",
                                    new
                                    {
                                        subj = subjectResource.ToString(),
                                        val = objectLiteral.ToString()
                                    });
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
                using (IAsyncSession neo4jSession = Driver.AsyncSession(s => s.WithDatabase(DatabaseName)))
                {
                    try
                    {
                        neo4jSession.ExecuteWriteAsync(
                            async tx =>
                            {
                                await tx.RunAsync(
                                    "MATCH (s:Resource)-[p:Property { uri:$pred }]->(o:Resource { uri:$obj }) " +
                                    "DELETE p",
                                    new
                                    {
                                        pred = predicateResource.ToString(),
                                        obj = objectResource.ToString()
                                    });
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
                using (IAsyncSession neo4jSession = Driver.AsyncSession(s => s.WithDatabase(DatabaseName)))
                {
                    try
                    {
                        neo4jSession.ExecuteWriteAsync(
                            async tx =>
                            {
                                await tx.RunAsync(
                                    "MATCH (s:Resource)-[p:Property { uri:$pred }]->(l:Literal { value:$val }) " +
                                    "DELETE p",
                                    new
                                    {
                                        pred = predicateResource.ToString(),
                                        val = objectLiteral.ToString()
                                    });
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
            using (IAsyncSession neo4jSession = Driver.AsyncSession(s => s.WithDatabase(DatabaseName)))
            {
                try
                {
                    neo4jSession.ExecuteWriteAsync(tx => tx.RunAsync("MATCH (n) DETACH DELETE (n)", null)).GetAwaiter().GetResult();
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

            using (IAsyncSession neo4jSession = Driver.AsyncSession(s => s.WithDatabase(DatabaseName)))
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
                                        "MATCH (s:Resource { uri:$subj })-[p:Property { uri:$pred, ctx:$ctx }]->(o:Resource { uri:$obj }) " +
                                        "RETURN (COUNT(*) > 0) AS checkExists",
                                        new
                                        {
                                            subj = quadruple.Subject.ToString(),
                                            pred = quadruple.Predicate.ToString(),
                                            ctx = quadruple.Context.ToString(),
                                            obj = quadruple.Object.ToString()
                                        });
                                    IRecord matchSPORecord = await matchSPOResult.SingleAsync();
                                    result = matchSPORecord.Get<bool>("checkExists");
                                    break;

                                case RDFModelEnums.RDFTripleFlavors.SPL:
                                    IResultCursor matchSPLResult = await tx.RunAsync(
                                        "MATCH (s:Resource { uri:$subj })-[p:Property { uri:$pred, ctx:$ctx }]->(l:Literal { value:$val }) " +
                                        "RETURN (COUNT(*) > 0) AS checkExists",
                                        new
                                        {
                                            subj = quadruple.Subject.ToString(),
                                            pred = quadruple.Predicate.ToString(),
                                            ctx = quadruple.Context.ToString(),
                                            val = quadruple.Object.ToString()
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
            RDFMemoryStore store = new RDFMemoryStore();
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
            using (IAsyncSession neo4jSession = Driver.AsyncSession(s => s.WithDatabase(DatabaseName)))
            {
                try
                {
                    switch (queryFilters.ToString())
                    {
                        case "C":
                            //C->->->
                            neo4jSession.ExecuteReadAsync(
                                async tx =>
                                {
                                    IResultCursor matchCResult = await tx.RunAsync(
                                        "MATCH (s:Resource)-[p:Property { ctx:$ctx }]->(o:Resource) " +
                                        "RETURN s.uri as subject, p.uri as predicate, p.ctx as context, o.uri as object",
                                        new
                                        {
                                            ctx = ctx.ToString()
                                        });
                                    await FetchSPOQuadruplesAsync(matchCResult, store);
                                }).GetAwaiter().GetResult();
                            neo4jSession.ExecuteReadAsync(
                                async tx =>
                                {
                                    IResultCursor matchCResult = await tx.RunAsync(
                                        "MATCH (s:Resource)-[p:Property { ctx:$ctx }]->(l:Literal) " +
                                        "RETURN s.uri as subject, p.uri as predicate, p.ctx as context, l.value as literal",
                                        new
                                        {
                                            ctx = ctx.ToString()
                                        });
                                    await FetchSPLQuadruplesAsync(matchCResult, store);
                                }).GetAwaiter().GetResult();
                            break;
                        case "S":
                            //->S->->
                            neo4jSession.ExecuteReadAsync(
                                async tx =>
                                {
                                    IResultCursor matchSResult = await tx.RunAsync(
                                        "MATCH (s:Resource { uri:$subj })-[p:Property]->(o:Resource) " +
                                        "RETURN s.uri as subject, p.uri as predicate, p.ctx as context, o.uri as object",
                                        new
                                        {
                                            subj = subj.ToString()
                                        });
                                    await FetchSPOQuadruplesAsync(matchSResult, store);
                                }).GetAwaiter().GetResult();
                            neo4jSession.ExecuteReadAsync(
                                async tx =>
                                {
                                    IResultCursor matchSResult = await tx.RunAsync(
                                        "MATCH (s:Resource { uri:$subj })-[p:Property]->(l:Literal) " +
                                        "RETURN s.uri as subject, p.uri as predicate, p.ctx as context, l.value as literal",
                                        new
                                        {
                                            subj = subj.ToString()
                                        });
                                    await FetchSPLQuadruplesAsync(matchSResult, store);
                                }).GetAwaiter().GetResult();
                            break;
                        case "P":
                            //->->P->
                            neo4jSession.ExecuteReadAsync(
                                async tx =>
                                {
                                    IResultCursor matchPResult = await tx.RunAsync(
                                        "MATCH (s:Resource)-[p:Property { uri:$pred }]->(o:Resource) " +
                                        "RETURN s.uri as subject, p.uri as predicate, p.ctx as context, o.uri as object",
                                        new
                                        {
                                            pred = pred.ToString()
                                        });
                                    await FetchSPOQuadruplesAsync(matchPResult, store);
                                }).GetAwaiter().GetResult();
                            neo4jSession.ExecuteReadAsync(
                                async tx =>
                                {
                                    IResultCursor matchPResult = await tx.RunAsync(
                                        "MATCH (s:Resource)-[p:Property { uri:$pred }]->(l:Literal) " +
                                        "RETURN s.uri as subject, p.uri as predicate, p.ctx as context, l.value as literal",
                                        new
                                        {
                                            pred = pred.ToString()
                                        });
                                    await FetchSPLQuadruplesAsync(matchPResult, store);
                                }).GetAwaiter().GetResult();
                            break;
                        case "O":
                            //->->->O
                            neo4jSession.ExecuteReadAsync(
                                async tx =>
                                {
                                    IResultCursor matchOResult = await tx.RunAsync(
                                        "MATCH (s:Resource)-[p:Property]->(o:Resource{ uri:$obj }) " +
                                        "RETURN s.uri as subject, p.uri as predicate, p.ctx as context, o.uri as object",
                                        new
                                        {
                                            obj = obj.ToString()
                                        });
                                    await FetchSPOQuadruplesAsync(matchOResult, store);
                                }).GetAwaiter().GetResult();
                            break;
                        case "L":
                            //->->->L
                            neo4jSession.ExecuteReadAsync(
                                async tx =>
                                {
                                    IResultCursor matchLResult = await tx.RunAsync(
                                        "MATCH (s:Resource)-[p:Property]->(l:Literal { value:$val }) " +
                                        "RETURN s.uri as subject, p.uri as predicate, p.ctx as context, l.value as literal",
                                        new
                                        {
                                            val = lit.ToString()
                                        });
                                    await FetchSPLQuadruplesAsync(matchLResult, store);
                                }).GetAwaiter().GetResult();
                            break;
                        case "CS":
                            //C->S->->
                            neo4jSession.ExecuteReadAsync(
                                async tx =>
                                {
                                    IResultCursor matchCSResult = await tx.RunAsync(
                                        "MATCH (s:Resource { uri:$subj })-[p:Property { ctx:$ctx }]->(o:Resource) " +
                                        "RETURN s.uri as subject, p.uri as predicate, p.ctx as context, o.uri as object",
                                        new
                                        {
                                            subj = subj.ToString(),
                                            ctx = ctx.ToString()
                                        });
                                    await FetchSPOQuadruplesAsync(matchCSResult, store);
                                }).GetAwaiter().GetResult();
                            neo4jSession.ExecuteReadAsync(
                                async tx =>
                                {
                                    IResultCursor matchCSResult = await tx.RunAsync(
                                        "MATCH (s:Resource { uri:$subj })-[p:Property { ctx:$ctx }]->(l:Literal) " +
                                        "RETURN s.uri as subject, p.uri as predicate, p.ctx as context, l.value as literal",
                                        new
                                        {
                                            subj = subj.ToString(),
                                            ctx = ctx.ToString()
                                        });
                                    await FetchSPLQuadruplesAsync(matchCSResult, store);
                                }).GetAwaiter().GetResult();
                            break;
                        case "CP":
                            //C->->P->
                            neo4jSession.ExecuteReadAsync(
                                async tx =>
                                {
                                    IResultCursor matchCPResult = await tx.RunAsync(
                                        "MATCH (s:Resource)-[p:Property { uri:$pred, ctx:$ctx }]->(o:Resource) " +
                                        "RETURN s.uri as subject, p.uri as predicate, p.ctx as context, o.uri as object",
                                        new
                                        {
                                            pred = pred.ToString(),
                                            ctx = ctx.ToString()
                                        });
                                    await FetchSPOQuadruplesAsync(matchCPResult, store);
                                }).GetAwaiter().GetResult();
                            neo4jSession.ExecuteReadAsync(
                                async tx =>
                                {
                                    IResultCursor matchCPResult = await tx.RunAsync(
                                        "MATCH (s:Resource)-[p:Property { uri:$pred, ctx:$ctx }]->(l:Literal) " +
                                        "RETURN s.uri as subject, p.uri as predicate, p.ctx as context, l.value as literal",
                                        new
                                        {
                                            pred = pred.ToString(),
                                            ctx = ctx.ToString()
                                        });
                                    await FetchSPLQuadruplesAsync(matchCPResult, store);
                                }).GetAwaiter().GetResult();
                            break;
                        case "CO":
                            //C->->->O
                            neo4jSession.ExecuteReadAsync(
                                async tx =>
                                {
                                    IResultCursor matchCOResult = await tx.RunAsync(
                                        "MATCH (s:Resource)-[p:Property { ctx:$ctx }]->(o:Resource { uri:$obj }) " +
                                        "RETURN s.uri as subject, p.uri as predicate, p.ctx as context, o.uri as object",
                                        new
                                        {
                                            ctx = ctx.ToString(),
                                            obj = obj.ToString()
                                        });
                                    await FetchSPOQuadruplesAsync(matchCOResult, store);
                                }).GetAwaiter().GetResult();
                            break;
                        case "CL":
                            //C->->->L
                            neo4jSession.ExecuteReadAsync(
                                async tx =>
                                {
                                    IResultCursor matchCLResult = await tx.RunAsync(
                                        "MATCH (s:Resource)-[p:Property { ctx:$ctx }]->(l:Literal { value:$val }) " +
                                        "RETURN s.uri as subject, p.uri as predicate, p.ctx as context, l.value as literal",
                                        new
                                        {
                                            ctx = ctx.ToString(),
                                            val = lit.ToString()
                                        });
                                    await FetchSPLQuadruplesAsync(matchCLResult, store);
                                }).GetAwaiter().GetResult();
                            break;
                        case "CSP":
                            //C->S->P->
                            neo4jSession.ExecuteReadAsync(
                                async tx =>
                                {
                                    IResultCursor matchCSPResult = await tx.RunAsync(
                                        "MATCH (s:Resource { uri:$subj })-[p:Property { uri:$pred, ctx:$ctx }]->(o:Resource) " +
                                        "RETURN s.uri as subject, p.uri as predicate, p.ctx as context, o.uri as object",
                                        new
                                        {
                                            subj = subj.ToString(),
                                            pred = pred.ToString(),
                                            ctx = ctx.ToString()
                                        });
                                    await FetchSPOQuadruplesAsync(matchCSPResult, store);
                                }).GetAwaiter().GetResult();
                            neo4jSession.ExecuteReadAsync(
                                async tx =>
                                {
                                    IResultCursor matchCSPResult = await tx.RunAsync(
                                        "MATCH (s:Resource { uri:$subj })-[p:Property { uri:$pred, ctx:$ctx }]->(l:Literal) " +
                                        "RETURN s.uri as subject, p.uri as predicate, p.ctx as context, l.value as literal",
                                        new
                                        {
                                            subj = subj.ToString(),
                                            pred = pred.ToString(),
                                            ctx = ctx.ToString()
                                        });
                                    await FetchSPLQuadruplesAsync(matchCSPResult, store);
                                }).GetAwaiter().GetResult();
                            break;
                        case "CSO":
                            //C->S->->O
                            neo4jSession.ExecuteReadAsync(
                                async tx =>
                                {
                                    IResultCursor matchCSOResult = await tx.RunAsync(
                                        "MATCH (s:Resource { uri:$subj })-[p:Property { ctx:$ctx }]->(o:Resource { uri:$obj }) " +
                                        "RETURN s.uri as subject, p.uri as predicate, p.ctx as context, o.uri as object",
                                        new
                                        {
                                            subj = subj.ToString(),
                                            ctx = ctx.ToString(),
                                            obj = obj.ToString()
                                        });
                                    await FetchSPOQuadruplesAsync(matchCSOResult, store);
                                }).GetAwaiter().GetResult();
                            break;
                        case "CSL":
                            //C->S->->L
                            neo4jSession.ExecuteReadAsync(
                                async tx =>
                                {
                                    IResultCursor matchCSLResult = await tx.RunAsync(
                                        "MATCH (s:Resource { uri:$subj })-[p:Property { ctx:$ctx }]->(l:Literal { value:$val }) " +
                                        "RETURN s.uri as subject, p.uri as predicate, p.ctx as context, l.value as literal",
                                        new
                                        {
                                            subj = subj.ToString(),
                                            ctx = ctx.ToString(),
                                            val = lit.ToString()
                                        });
                                    await FetchSPLQuadruplesAsync(matchCSLResult, store);
                                }).GetAwaiter().GetResult();
                            break;
                        case "CPO":
                            //C->->P->O
                            neo4jSession.ExecuteReadAsync(
                                async tx =>
                                {
                                    IResultCursor matchCPOResult = await tx.RunAsync(
                                        "MATCH (s:Resource)-[p:Property { uri:$pred, ctx:$ctx }]->(o:Resource { uri:$obj }) " +
                                        "RETURN s.uri as subject, p.uri as predicate, p.ctx as context, o.uri as object",
                                        new
                                        {
                                            pred = pred.ToString(),
                                            ctx = ctx.ToString(),
                                            obj = obj.ToString()
                                        });
                                    await FetchSPOQuadruplesAsync(matchCPOResult, store);
                                }).GetAwaiter().GetResult();
                            break;
                        case "CPL":
                            //C->->P->L
                            neo4jSession.ExecuteReadAsync(
                                async tx =>
                                {
                                    IResultCursor matchCPLResult = await tx.RunAsync(
                                        "MATCH (s:Resource)-[p:Property { uri:$pred, ctx:$ctx }]->(l:Literal { value:$val }) " +
                                        "RETURN s.uri as subject, p.uri as predicate, p.ctx as context, l.value as literal",
                                        new
                                        {
                                            pred = pred.ToString(),
                                            ctx = ctx.ToString(),
                                            val = lit.ToString()
                                        });
                                    await FetchSPLQuadruplesAsync(matchCPLResult, store);
                                }).GetAwaiter().GetResult();
                            break;
                        case "CSPO":
                            //C->S->P->O
                            neo4jSession.ExecuteReadAsync(
                                async tx =>
                                {
                                    IResultCursor matchCSPOResult = await tx.RunAsync(
                                        "MATCH (s:Resource { uri:$subj })-[p:Property { uri:$pred, ctx:$ctx }]->(o:Resource { uri:$obj }) " +
                                        "RETURN s.uri as subject, p.uri as predicate, p.ctx as context, o.uri as object",
                                        new
                                        {
                                            subj = subj.ToString(),
                                            pred = pred.ToString(),
                                            ctx = ctx.ToString(),
                                            obj = obj.ToString()
                                        });
                                    await FetchSPOQuadruplesAsync(matchCSPOResult, store);
                                }).GetAwaiter().GetResult();
                            break;
                        case "CSPL":
                            //C->S->P->L
                            neo4jSession.ExecuteReadAsync(
                                async tx =>
                                {
                                    IResultCursor matchCSPLResult = await tx.RunAsync(
                                        "MATCH (s:Resource { uri:$subj })-[p:Property { uri:$pred, ctx:$ctx }]->(l:Literal { value:$val }) " +
                                        "RETURN s.uri as subject, p.uri as predicate, p.ctx as context, l.value as literal",
                                        new
                                        {
                                            subj = subj.ToString(),
                                            pred = pred.ToString(),
                                            ctx = ctx.ToString(),
                                            val = lit.ToString()
                                        });
                                    await FetchSPLQuadruplesAsync(matchCSPLResult, store);
                                }).GetAwaiter().GetResult();
                            break;
                        case "SP":
                            //->S->P->
                            neo4jSession.ExecuteReadAsync(
                                async tx =>
                                {
                                    IResultCursor matchSPResult = await tx.RunAsync(
                                        "MATCH (s:Resource { uri:$subj })-[p:Property { uri:$pred }]->(o:Resource) " +
                                        "RETURN s.uri as subject, p.uri as predicate, p.ctx as context, o.uri as object",
                                        new
                                        {
                                            subj = subj.ToString(),
                                            pred = pred.ToString()
                                        });
                                    await FetchSPOQuadruplesAsync(matchSPResult, store);
                                }).GetAwaiter().GetResult();
                            neo4jSession.ExecuteReadAsync(
                                async tx =>
                                {
                                    IResultCursor matchSPResult = await tx.RunAsync(
                                        "MATCH (s:Resource { uri:$subj })-[p:Property { uri:$pred }]->(l:Literal) " +
                                        "RETURN s.uri as subject, p.uri as predicate, p.ctx as context, l.value as literal",
                                        new
                                        {
                                            subj = subj.ToString(),
                                            pred = pred.ToString()
                                        });
                                    await FetchSPLQuadruplesAsync(matchSPResult, store);
                                }).GetAwaiter().GetResult();
                            break;
                        case "SO":
                            //->S->->O
                            neo4jSession.ExecuteReadAsync(
                                async tx =>
                                {
                                    IResultCursor matchSOResult = await tx.RunAsync(
                                        "MATCH (s:Resource { uri:$subj })-[p:Property]->(o:Resource { uri:$obj }) " +
                                        "RETURN s.uri as subject, p.uri as predicate, p.ctx as context, o.uri as object",
                                        new
                                        {
                                            subj = subj.ToString(),
                                            obj = obj.ToString()
                                        });
                                    await FetchSPOQuadruplesAsync(matchSOResult, store);
                                }).GetAwaiter().GetResult();
                            break;
                        case "SL":
                            //->S->->L
                            neo4jSession.ExecuteReadAsync(
                                async tx =>
                                {
                                    IResultCursor matchSLResult = await tx.RunAsync(
                                        "MATCH (s:Resource { uri:$subj })-[p:Property]->(l:Literal { value:$val }) " +
                                        "RETURN s.uri as subject, p.uri as predicate, p.ctx as context, l.value as literal",
                                        new
                                        {
                                            subj = subj.ToString(),
                                            val = lit.ToString()
                                        });
                                    await FetchSPLQuadruplesAsync(matchSLResult, store);
                                }).GetAwaiter().GetResult();
                            break;
                        case "PO":
                            //->->P->O
                            neo4jSession.ExecuteReadAsync(
                                async tx =>
                                {
                                    IResultCursor matchPOResult = await tx.RunAsync(
                                        "MATCH (s:Resource)-[p:Property { uri:$pred }]->(o:Resource { uri:$obj }) " +
                                        "RETURN s.uri as subject, p.uri as predicate, p.ctx as context, o.uri as object",
                                        new
                                        {
                                            pred = pred.ToString(),
                                            obj = obj.ToString()
                                        });
                                    await FetchSPOQuadruplesAsync(matchPOResult, store);
                                }).GetAwaiter().GetResult();
                            break;
                        case "PL":
                            //->->P->L
                            neo4jSession.ExecuteReadAsync(
                                async tx =>
                                {
                                    IResultCursor matchPLResult = await tx.RunAsync(
                                        "MATCH (s:Resource)-[p:Property { uri:$pred }]->(l:Literal { value:$val }) " +
                                        "RETURN s.uri as subject, p.uri as predicate, p.ctx as context, l.value as literal",
                                        new
                                        {
                                            pred = pred.ToString(),
                                            val = lit.ToString()
                                        });
                                    await FetchSPLQuadruplesAsync(matchPLResult, store);
                                }).GetAwaiter().GetResult();
                            break;
                        case "SPO":
                            //->S->P->O
                            neo4jSession.ExecuteReadAsync(
                                async tx =>
                                {
                                    IResultCursor matchSPOResult = await tx.RunAsync(
                                        "MATCH (s:Resource { uri:$subj })-[p:Property { uri:$pred }]->(o:Resource { uri:$obj }) " +
                                        "RETURN s.uri as subject, p.uri as predicate, p.ctx as context, o.uri as object",
                                        new
                                        {
                                            subj = subj.ToString(),
                                            pred = pred.ToString(),
                                            obj = obj.ToString()
                                        });
                                    await FetchSPOQuadruplesAsync(matchSPOResult, store);
                                }).GetAwaiter().GetResult();
                            break;
                        case "SPL":
                            //->S->P->L
                            neo4jSession.ExecuteReadAsync(
                                async tx =>
                                {
                                    IResultCursor matchSPLResult = await tx.RunAsync(
                                        "MATCH (s:Resource { uri:$subj })-[p:Property { uri:$pred }]->(l:Literal { value:$val }) " +
                                        "RETURN s.uri as subject, p.uri as predicate, p.ctx as context, l.value as literal",
                                        new
                                        {
                                            subj = subj.ToString(),
                                            pred = pred.ToString(),
                                            val = lit.ToString()
                                        });
                                    await FetchSPLQuadruplesAsync(matchSPLResult, store);
                                }).GetAwaiter().GetResult();
                            break;
                        default:
                            //->->->
                            neo4jSession.ExecuteReadAsync(
                                async tx =>
                                {
                                    IResultCursor matchALLResult = await tx.RunAsync(
                                        "MATCH (s:Resource)-[p:Property]->(o:Resource) " +
                                        "RETURN s.uri as subject, p.uri as predicate, p.ctx as context, o.uri as object", null);
                                    await FetchSPOQuadruplesAsync(matchALLResult, store);
                                }).GetAwaiter().GetResult();
                            neo4jSession.ExecuteReadAsync(
                                async tx =>
                                {
                                    IResultCursor matchALLResult = await tx.RunAsync(
                                        "MATCH (s:Resource)-[p:Property]->(l:Literal) " +
                                        "RETURN s.uri as subject, p.uri as predicate, p.ctx as context, l.value as literal", null);
                                    await FetchSPLQuadruplesAsync(matchALLResult, store);
                                }).GetAwaiter().GetResult();
                            break;
                    }
                    neo4jSession.CloseAsync().GetAwaiter().GetResult();
                }
                catch (Exception ex)
                {
                    neo4jSession.CloseAsync().GetAwaiter().GetResult();

                    throw new RDFStoreException("Cannot read data from Neo4j store because: " + ex.Message, ex);
                }
            }

            return store;
        }

        /// <summary>
        /// Asynchronously fetches the SPO quadruples from the given result cursor and adds them to the given store
        /// </summary>
        private async Task FetchSPOQuadruplesAsync(IResultCursor resultCursor, RDFMemoryStore store)
        {
            while (await resultCursor.FetchAsync())
            {
                store.AddQuadruple(new RDFQuadruple(
                    new RDFContext(resultCursor.Current.Get<string>("context")),
                    new RDFResource(resultCursor.Current.Get<string>("subject")),
                    new RDFResource(resultCursor.Current.Get<string>("predicate")),
                    new RDFResource(resultCursor.Current.Get<string>("object"))));
            }
        }

        /// <summary>
        /// Asynchronously fetches the SPL quadruples from the given result cursor and adds them to the given store
        /// </summary>
        private async Task FetchSPLQuadruplesAsync(IResultCursor resultCursor, RDFMemoryStore store)
        {
            while (await resultCursor.FetchAsync())
            {
                if (RDFQueryUtilities.ParseRDFPatternMember(resultCursor.Current.Get<string>("literal")) is RDFLiteral literal)
                    store.AddQuadruple(new RDFQuadruple(
                        new RDFContext(resultCursor.Current.Get<string>("context")),
                        new RDFResource(resultCursor.Current.Get<string>("subject")),
                        new RDFResource(resultCursor.Current.Get<string>("predicate")),
                        literal));
            }
        }

        /// <summary>
        /// Counts the Neo4j database quadruples
        /// </summary>
        private long GetQuadruplesCount()
            => GetQuadruplesCountAsync().GetAwaiter().GetResult();

        /// <summary>
        /// Asynchronously counts the Neo4j database quadruples
        /// </summary>
        private async Task<long> GetQuadruplesCountAsync()
        {
            using (IAsyncSession neo4jSession = Driver.AsyncSession(s => s.WithDatabase(DatabaseName)))
            {
                try
                {
                    long quadruplesCount = 0;

                    await neo4jSession.ExecuteReadAsync(
                        async tx =>
                        {
                            IResultCursor countResult = await tx.RunAsync(
                                "MATCH (s:Resource)-[p:Property]->() " +
                                "RETURN (COUNT(*)) AS quadruplesCount", null);
                            IRecord countRecord = await countResult.SingleAsync();
                            quadruplesCount = countRecord.Get<long>("quadruplesCount");
                        });
                    await neo4jSession.CloseAsync();

                    return quadruplesCount;
                }
                catch
                {
                    await neo4jSession.CloseAsync();

                    return -1;
                }
            }
        }
        #endregion

        #region Diagnostics
        /// <summary>
        /// Initializes the underlying Neo4j database
        /// </summary>
        private async Task InitializeStoreAsync()
        {
            using (IAsyncSession neo4jSession = Driver.AsyncSession(s => s.WithDatabase(DatabaseName)))
            {
                try
                {
                    await neo4jSession.ExecuteWriteAsync(
                        async tx =>
                        {
                            await tx.RunAsync("CREATE INDEX resIdx  IF NOT EXISTS FOR (r:Resource)        ON (r.uri) OPTIONS {}", null);
                            await tx.RunAsync("CREATE INDEX propIdx IF NOT EXISTS FOR ()-[p:Property]->() ON (p.uri) OPTIONS {}", null);
                            await tx.RunAsync("CREATE INDEX ctxIdx  IF NOT EXISTS FOR ()-[p:Property]->() ON (p.ctx) OPTIONS {}", null);
                            await tx.RunAsync("CREATE INDEX litIdx  IF NOT EXISTS FOR (l:Literal)         ON (l.value) OPTIONS {}", null);
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