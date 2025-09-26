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
using RDFSharp.Model;
using RDFSharp.Query;
using RDFSharp.Store;
using System;
using System.Collections.Generic;
using System.Text;
using System.Threading.Tasks;

namespace RDFSharp.Extensions.Neo4j
{
    /// <summary>
    /// RDFNeo4jStore represents a RDFStore backed on Neo4j engine
    /// </summary>
#if NET8_0_OR_GREATER
    public sealed class RDFNeo4jStore : RDFStore, IDisposable, IAsyncDisposable
#else
    public sealed class RDFNeo4jStore : RDFStore, IDisposable
#endif
    {
        #region Properties
        /// <summary>
        /// Count of the Neo4j database quadruples (-1 in case of errors)
        /// </summary>
        public override long QuadruplesCount
            => GetQuadruplesCountAsync().GetAwaiter().GetResult();

        /// <summary>
        /// Asynchronous count of the Neo4j database quadruples (-1 in case of errors)
        /// </summary>
        public override Task<long> QuadruplesCountAsync
            => GetQuadruplesCountAsync();

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
        ~RDFNeo4jStore()
            => Dispose(false);
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

#if NET8_0_OR_GREATER
        /// <summary>
        /// Asynchronously disposes the MySQL store (IAsyncDisposable)
        /// </summary>
        ValueTask IAsyncDisposable.DisposeAsync()
        {
            Dispose(true);
            GC.SuppressFinalize(this);
            return ValueTask.CompletedTask;
        }
#endif

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
                Driver?.Dispose();
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
            => MergeGraphAsync(graph).GetAwaiter().GetResult();

        /// <summary>
        /// Asynchronously merges the given graph into the store
        /// </summary>
        public override async Task<RDFStore> MergeGraphAsync(RDFGraph graph)
        {
            if (graph != null)
            {
                RDFContext graphContext = new RDFContext(graph.Context);
                foreach (RDFTriple triple in graph)
                    await AddQuadrupleAsync(new RDFQuadruple(graphContext, triple));
            }
            return this;
        }

        /// <summary>
        /// Adds the given quadruple to the store
        /// </summary>
        public override RDFStore AddQuadruple(RDFQuadruple quadruple)
            => AddQuadrupleAsync(quadruple).GetAwaiter().GetResult();

        /// <summary>
        /// Asynchronously adds the given quadruple to the store
        /// </summary>
        public override async Task<RDFStore> AddQuadrupleAsync(RDFQuadruple quadruple)
        {
            if (quadruple != null)
            {
                using (IAsyncSession neo4jSession = Driver.AsyncSession(ssn => ssn.WithDatabase(DatabaseName)))
                {
                    try
                    {
                        await neo4jSession.ExecuteWriteAsync(
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
                            });
                        await neo4jSession.CloseAsync();
                    }
                    catch (Exception ex)
                    {
                        await neo4jSession.CloseAsync();

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
            => RemoveQuadrupleAsync(quadruple).GetAwaiter().GetResult();

        /// <summary>
        /// Asynchronously removes the given quadruple from the store
        /// </summary>
        public override async Task<RDFStore> RemoveQuadrupleAsync(RDFQuadruple quadruple)
        {
            if (quadruple != null)
            {
                using (IAsyncSession neo4jSession = Driver.AsyncSession(ssn => ssn.WithDatabase(DatabaseName)))
                {
                    try
                    {
                        await neo4jSession.ExecuteWriteAsync(
                            async tx =>
                            {
                                switch (quadruple.TripleFlavor)
                                {
                                    case RDFModelEnums.RDFTripleFlavors.SPO:
                                        await tx.RunAsync(
                                            "MATCH (s:Resource { uri:$subj })-[p:Property { uri:$pred, ctx:$ctx }]->(o:Resource { uri:$obj }) " +
                                            "DELETE p;",
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
                                            "DELETE p;",
                                            new
                                            {
                                                subj = quadruple.Subject.ToString(),
                                                pred = quadruple.Predicate.ToString(),
                                                ctx = quadruple.Context.ToString(),
                                                val = quadruple.Object.ToString()
                                            });
                                        break;
                                }
                            });
                        await neo4jSession.CloseAsync();
                    }
                    catch (Exception ex)
                    {
                        await neo4jSession.CloseAsync();

                        throw new RDFStoreException("Cannot remove data from Neo4j store because: " + ex.Message, ex);
                    }
                }
            }
            return this;
        }

        /// <summary>
        /// Removes the quadruples which satisfy the given combination of CSPOL accessors<br/>
        /// (null values are handled as * selectors. Object and Literal params, if given, must be mutually exclusive!)
        /// </summary>
        public override RDFStore RemoveQuadruples(RDFContext c=null, RDFResource s=null, RDFResource p=null, RDFResource o=null, RDFLiteral l=null)
            => RemoveQuadruplesAsync(c, s, p, o, l).GetAwaiter().GetResult();

        /// <summary>
        /// Asynchronously removes the quadruples which satisfy the given combination of CSPOL accessors<br/>
        /// (null values are handled as * selectors. Object and Literal params, if given, must be mutually exclusive!)
        /// </summary>
        public override async Task<RDFStore> RemoveQuadruplesAsync(RDFContext c=null, RDFResource s=null, RDFResource p=null, RDFResource o=null, RDFLiteral l=null)
        {
            #region Guards
            if (o != null && l != null)
                throw new RDFStoreException("Cannot access a store when both object and literals are given: they must be mutually exclusive!");
            #endregion

            //Build filters
            StringBuilder queryFilters = new StringBuilder();
            if (c != null) queryFilters.Append('C');
            if (s != null) queryFilters.Append('S');
            if (p != null) queryFilters.Append('P');
            if (o != null) queryFilters.Append('O');
            if (l != null) queryFilters.Append('L');

            (string query, object parameters) neo4jQuery = (string.Empty, null);
            switch (queryFilters.ToString())
            {
                case "C":
                    neo4jQuery.query = "MATCH (s:Resource)-[p:Property { ctx:$ctx }]->() DELETE p";
                    neo4jQuery.parameters = new { ctx = c.ToString() };
                    break;
                case "S":
                    neo4jQuery.query = "MATCH (s:Resource { uri:$subj })-[p:Property]->() DELETE p";
                    neo4jQuery.parameters = new { subj = s.ToString() };
                    break;
                case "P":
                    neo4jQuery.query = "MATCH (s:Resource)-[p:Property { uri:$pred }]->() DELETE p";
                    neo4jQuery.parameters = new { pred = p.ToString() };
                    break;
                case "O":
                    neo4jQuery.query = "MATCH (s:Resource)-[p:Property]->(o:Resource { uri:$obj }) DELETE p";
                    neo4jQuery.parameters = new { obj = o.ToString() };
                    break;
                case "L":
                    neo4jQuery.query = "MATCH (s:Resource)-[p:Property]->(l:Literal { value:$val }) DELETE p";
                    neo4jQuery.parameters = new { val = l.ToString() };
                    break;
                case "CS":
                    neo4jQuery.query = "MATCH (s:Resource { uri:$subj })-[p:Property { ctx:$ctx }]->() DELETE p";
                    neo4jQuery.parameters = new { subj = s.ToString(), ctx = c.ToString() };
                    break;
                case "CP":
                    neo4jQuery.query = "MATCH (s:Resource)-[p:Property { uri:$pred, ctx:$ctx }]->() DELETE p";
                    neo4jQuery.parameters = new { pred = p.ToString(), ctx = c.ToString() };
                    break;
                case "CO":
                    neo4jQuery.query = "MATCH (s:Resource)-[p:Property { ctx:$ctx }]->(o:Resource { uri:$obj }) DELETE p";
                    neo4jQuery.parameters = new { ctx = c.ToString(), obj = o.ToString() };
                    break;
                case "CL":
                    neo4jQuery.query = "MATCH (s:Resource)-[p:Property { ctx:$ctx }]->(l:Literal { value:$val }) DELETE p";
                    neo4jQuery.parameters = new { ctx = c.ToString(), val = l.ToString() };
                    break;
                case "CSP":
                    neo4jQuery.query = "MATCH (s:Resource { uri:$subj })-[p:Property { uri:$pred, ctx:$ctx }]->() DELETE p";
                    neo4jQuery.parameters = new { subj = s.ToString(), pred = p.ToString(), ctx = c.ToString() };
                    break;
                case "CSO":
                    neo4jQuery.query = "MATCH (s:Resource { uri:$subj })-[p:Property { ctx:$ctx }]->(o:Resource { uri:$obj }) DELETE p";
                    neo4jQuery.parameters = new { subj = s.ToString(), ctx = c.ToString(), obj = o.ToString() };
                    break;
                case "CSL":
                    neo4jQuery.query = "MATCH (s:Resource { uri:$subj })-[p:Property { ctx:$ctx }]->(l:Literal { value:$val }) DELETE p";
                    neo4jQuery.parameters = new { subj = s.ToString(), ctx = c.ToString(), val = l.ToString() };
                    break;
                case "CPO":
                    neo4jQuery.query = "MATCH (s:Resource)-[p:Property { uri:$pred, ctx:$ctx }]->(o:Resource { uri:$obj }) DELETE p";
                    neo4jQuery.parameters = new { pred = p.ToString(), ctx = c.ToString(), obj = o.ToString() };
                    break;
                case "CPL":
                    neo4jQuery.query = "MATCH (s:Resource)-[p:Property { uri:$pred, ctx:$ctx }]->(l:Literal { value:$val }) DELETE p";
                    neo4jQuery.parameters = new { pred = p.ToString(), ctx = c.ToString(), val = l.ToString() };
                    break;
                case "CSPO":
                    neo4jQuery.query = "MATCH (s:Resource { uri:$subj })-[p:Property { uri:$pred, ctx:$ctx }]->(o:Resource { uri:$obj }) DELETE p";
                    neo4jQuery.parameters = new { subj = s.ToString(), pred = p.ToString(), ctx = c.ToString(), obj = o.ToString() };
                    break;
                case "CSPL":
                    neo4jQuery.query = "MATCH (s:Resource { uri:$subj })-[p:Property { uri:$pred, ctx:$ctx }]->(l:Literal { value:$val }) DELETE p";
                    neo4jQuery.parameters = new { subj = s.ToString(), pred = p.ToString(), ctx = c.ToString(), val = l.ToString() };
                    break;
                case "SP":
                    neo4jQuery.query = "MATCH (s:Resource { uri:$subj })-[p:Property { uri:$pred }]->() DELETE p";
                    neo4jQuery.parameters = new { subj = s.ToString(), pred = p.ToString() };
                    break;
                case "SO":
                    neo4jQuery.query = "MATCH (s:Resource { uri:$subj })-[p:Property]->(o:Resource { uri:$obj }) DELETE p";
                    neo4jQuery.parameters = new { subj = s.ToString(), obj = o.ToString() };
                    break;
                case "SL":
                    neo4jQuery.query = "MATCH (s:Resource { uri:$subj })-[p:Property]->(l:Literal { value:$val }) DELETE p";
                    neo4jQuery.parameters = new { subj = s.ToString(), val = l.ToString() };
                    break;
                case "SPO":
                    neo4jQuery.query = "MATCH (s:Resource { uri:$subj })-[p:Property { uri:$pred }]->(o:Resource { uri:$obj }) DELETE p";
                    neo4jQuery.parameters = new { subj = s.ToString(), pred = p.ToString(), obj = o.ToString() };
                    break;
                case "SPL":
                    neo4jQuery.query = "MATCH (s:Resource { uri:$subj })-[p:Property { uri:$pred }]->(l:Literal { value:$val }) DELETE p";
                    neo4jQuery.parameters = new { subj = s.ToString(), pred = p.ToString(), val = l.ToString() };
                    break;
                case "PO":
                    neo4jQuery.query = "MATCH (s:Resource)-[p:Property { uri:$pred }]->(o:Resource { uri:$obj }) DELETE p";
                    neo4jQuery.parameters = new { pred = p.ToString(), obj = o.ToString() };
                    break;
                case "PL":
                    neo4jQuery.query = "MATCH (s:Resource)-[p:Property { uri:$pred }]->(l:Literal { value:$val }) DELETE p";
                    neo4jQuery.parameters = new { pred = p.ToString(), val = l.ToString() };
                    break;
                //SELECT *
                default:
                    neo4jQuery.query = "MATCH (n) DETACH DELETE (n)";
                    break;
            }

            using (IAsyncSession neo4jSession = Driver.AsyncSession(ssn => ssn.WithDatabase(DatabaseName)))
            {
                try
                {
                    await neo4jSession.ExecuteWriteAsync(
                        async tx => await tx.RunAsync(neo4jQuery.query, neo4jQuery.parameters));
                    await neo4jSession.CloseAsync();
                }
                catch (Exception ex)
                {
                    await neo4jSession.CloseAsync();

                    throw new RDFStoreException("Cannot remove data from Neo4j store because: " + ex.Message, ex);
                }
            }

            return this;
        }

        /// <summary>
        /// Clears the quadruples of the store
        /// </summary>
        public override void ClearQuadruples()
            => ClearQuadruplesAsync().GetAwaiter().GetResult();

        /// <summary>
        /// Asynchronously clears the quadruples of the store
        /// </summary>
        public override async Task ClearQuadruplesAsync()
        {
            using (IAsyncSession neo4jSession = Driver.AsyncSession(ssn => ssn.WithDatabase(DatabaseName)))
            {
                try
                {
                    await neo4jSession.ExecuteWriteAsync(
                        async tx => await tx.RunAsync("MATCH (n) DETACH DELETE (n)", null));
                    await neo4jSession.CloseAsync();
                }
                catch (Exception ex)
                {
                    await neo4jSession.CloseAsync();

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
            => ContainsQuadrupleAsync(quadruple).GetAwaiter().GetResult();

        /// <summary>
        /// Asynchronously checks if the given quadruple is found in the store
        /// </summary>
        public override async Task<bool> ContainsQuadrupleAsync(RDFQuadruple quadruple)
        {
            //Guard against tricky input
            if (quadruple == null)
                return false;

            using (IAsyncSession neo4jSession = Driver.AsyncSession(ssn => ssn.WithDatabase(DatabaseName)))
            {
                try
                {
                    bool result = false;

                    await neo4jSession.ExecuteReadAsync(
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
                        });
                    await neo4jSession.CloseAsync();

                    return result;
                }
                catch (Exception ex)
                {
                    await neo4jSession.CloseAsync();

                    throw new RDFStoreException("Cannot read data from Neo4j store because: " + ex.Message, ex);
                }
            }
        }

        /// <summary>
        /// Selects the quadruples which satisfy the given combination of CSPOL accessors<br/>
        /// (null values are handled as * selectors. Object and Literal params, if given, must be mutually exclusive!)
        /// </summary>
        /// <exception cref="RDFStoreException"></exception>
        public override List<RDFQuadruple> SelectQuadruples(RDFContext c=null, RDFResource s=null, RDFResource p=null, RDFResource o=null, RDFLiteral l=null)
            => SelectQuadruplesAsync(c, s, p, o, l).GetAwaiter().GetResult();

        /// <summary>
        /// Asynchronously selects the quadruples which satisfy the given combination of CSPOL accessors<br/>
        /// (null values are handled as * selectors. Object and Literal params, if given, must be mutually exclusive!)
        /// </summary>
        /// <exception cref="RDFStoreException"></exception>
        public override async Task<List<RDFQuadruple>> SelectQuadruplesAsync(RDFContext c=null, RDFResource s=null, RDFResource p=null, RDFResource o=null, RDFLiteral l=null)
        {
            List<RDFQuadruple> result = new List<RDFQuadruple>();

            //Build filters
            StringBuilder queryFilters = new StringBuilder();
            if (c != null) queryFilters.Append('C');
            if (s != null) queryFilters.Append('S');
            if (p != null) queryFilters.Append('P');
            if (o != null) queryFilters.Append('O');
            if (l != null) queryFilters.Append('L');

            string oQuery = null, lQuery = null;
            object parameters = null;
            switch (queryFilters.ToString())
            {
                case "C":
                    oQuery = "MATCH (s:Resource)-[p:Property { ctx:$ctx }]->(o:Resource) RETURN s.uri as subject, p.uri as predicate, p.ctx as context, o.uri as object";
                    lQuery = "MATCH (s:Resource)-[p:Property { ctx:$ctx }]->(l:Literal) RETURN s.uri as subject, p.uri as predicate, p.ctx as context, l.value as literal";
                    parameters = new { ctx = c.ToString() };
                    break;
                case "S":
                    oQuery = "MATCH (s:Resource { uri:$subj })-[p:Property]->(o:Resource) RETURN s.uri as subject, p.uri as predicate, p.ctx as context, o.uri as object";
                    lQuery = "MATCH (s:Resource { uri:$subj })-[p:Property]->(l:Literal) RETURN s.uri as subject, p.uri as predicate, p.ctx as context, l.value as literal";
                    parameters = new { subj = s.ToString() };
                    break;
                case "P":
                    oQuery = "MATCH (s:Resource)-[p:Property { uri:$pred }]->(o:Resource) RETURN s.uri as subject, p.uri as predicate, p.ctx as context, o.uri as object";
                    lQuery = "MATCH (s:Resource)-[p:Property { uri:$pred }]->(l:Literal) RETURN s.uri as subject, p.uri as predicate, p.ctx as context, l.value as literal";
                    parameters = new { pred = p.ToString() };
                    break;
                case "O":
                    oQuery = "MATCH (s:Resource)-[p:Property]->(o:Resource{ uri:$obj }) RETURN s.uri as subject, p.uri as predicate, p.ctx as context, o.uri as object";
                    parameters = new { obj = o.ToString() };
                    break;
                case "L":
                    lQuery = "MATCH (s:Resource)-[p:Property]->(l:Literal { value:$val }) RETURN s.uri as subject, p.uri as predicate, p.ctx as context, l.value as literal";
                    parameters = new { val = l.ToString() };
                    break;
                case "CS":
                    oQuery = "MATCH (s:Resource { uri:$subj })-[p:Property { ctx:$ctx }]->(o:Resource) RETURN s.uri as subject, p.uri as predicate, p.ctx as context, o.uri as object";
                    lQuery = "MATCH (s:Resource { uri:$subj })-[p:Property { ctx:$ctx }]->(l:Literal) RETURN s.uri as subject, p.uri as predicate, p.ctx as context, l.value as literal";
                    parameters = new { subj = s.ToString(), ctx = c.ToString() };
                    break;
                case "CP":
                    oQuery = "MATCH (s:Resource)-[p:Property { uri:$pred, ctx:$ctx }]->(o:Resource) RETURN s.uri as subject, p.uri as predicate, p.ctx as context, o.uri as object";
                    lQuery = "MATCH (s:Resource)-[p:Property { uri:$pred, ctx:$ctx }]->(l:Literal) RETURN s.uri as subject, p.uri as predicate, p.ctx as context, l.value as literal";
                    parameters = new { pred = p.ToString(), ctx = c.ToString() };
                    break;
                case "CO":
                    oQuery = "MATCH (s:Resource)-[p:Property { ctx:$ctx }]->(o:Resource { uri:$obj }) RETURN s.uri as subject, p.uri as predicate, p.ctx as context, o.uri as object";
                    parameters = new { ctx = c.ToString(), obj = o.ToString() };
                    break;
                case "CL":
                    lQuery = "MATCH (s:Resource)-[p:Property { ctx:$ctx }]->(l:Literal { value:$val }) RETURN s.uri as subject, p.uri as predicate, p.ctx as context, l.value as literal";
                    parameters = new { ctx = c.ToString(), val = l.ToString() };
                    break;
                case "CSP":
                    oQuery = "MATCH (s:Resource { uri:$subj })-[p:Property { uri:$pred, ctx:$ctx }]->(o:Resource) RETURN s.uri as subject, p.uri as predicate, p.ctx as context, o.uri as object";
                    lQuery = "MATCH (s:Resource { uri:$subj })-[p:Property { uri:$pred, ctx:$ctx }]->(l:Literal) RETURN s.uri as subject, p.uri as predicate, p.ctx as context, l.value as literal";
                    parameters = new { subj = s.ToString(), pred = p.ToString(), ctx = c.ToString() };
                    break;
                case "CSO":
                    oQuery = "MATCH (s:Resource { uri:$subj })-[p:Property { ctx:$ctx }]->(o:Resource { uri:$obj }) RETURN s.uri as subject, p.uri as predicate, p.ctx as context, o.uri as object";
                    parameters = new { subj = s.ToString(), ctx = c.ToString(), obj = o.ToString() };
                    break;
                case "CSL":
                    lQuery = "MATCH (s:Resource { uri:$subj })-[p:Property { ctx:$ctx }]->(l:Literal { value:$val }) RETURN s.uri as subject, p.uri as predicate, p.ctx as context, l.value as literal";
                    parameters = new { subj = s.ToString(), ctx = c.ToString(), val = l.ToString() };
                    break;
                case "CPO":
                    oQuery = "MATCH (s:Resource)-[p:Property { uri:$pred, ctx:$ctx }]->(o:Resource { uri:$obj }) RETURN s.uri as subject, p.uri as predicate, p.ctx as context, o.uri as object";
                    parameters = new { pred = p.ToString(), ctx = c.ToString(), obj = o.ToString() };
                    break;
                case "CPL":
                    lQuery = "MATCH (s:Resource)-[p:Property { uri:$pred, ctx:$ctx }]->(l:Literal { value:$val }) RETURN s.uri as subject, p.uri as predicate, p.ctx as context, l.value as literal";
                    parameters = new { pred = p.ToString(), ctx = c.ToString(), val = l.ToString() };
                    break;
                case "CSPO":
                    oQuery = "MATCH (s:Resource { uri:$subj })-[p:Property { uri:$pred, ctx:$ctx }]->(o:Resource { uri:$obj }) RETURN s.uri as subject, p.uri as predicate, p.ctx as context, o.uri as object";
                    parameters = new { subj = s.ToString(), pred = p.ToString(), ctx = c.ToString(), obj = o.ToString() };
                    break;
                case "CSPL":
                    lQuery = "MATCH (s:Resource { uri:$subj })-[p:Property { uri:$pred, ctx:$ctx }]->(l:Literal { value:$val }) RETURN s.uri as subject, p.uri as predicate, p.ctx as context, l.value as literal";
                    parameters = new { subj = s.ToString(), pred = p.ToString(), ctx = c.ToString(), val = l.ToString() };
                    break;
                case "SP":
                    oQuery = "MATCH (s:Resource { uri:$subj })-[p:Property { uri:$pred }]->(o:Resource) RETURN s.uri as subject, p.uri as predicate, p.ctx as context, o.uri as object";
                    lQuery = "MATCH (s:Resource { uri:$subj })-[p:Property { uri:$pred }]->(l:Literal) RETURN s.uri as subject, p.uri as predicate, p.ctx as context, l.value as literal";
                    parameters = new { subj = s.ToString(), pred = p.ToString() };
                    break;
                case "SO":
                    oQuery = "MATCH (s:Resource { uri:$subj })-[p:Property]->(o:Resource { uri:$obj }) RETURN s.uri as subject, p.uri as predicate, p.ctx as context, o.uri as object";
                    parameters = new { subj = s.ToString(), obj = o.ToString() };
                    break;
                case "SL":
                    lQuery = "MATCH (s:Resource { uri:$subj })-[p:Property]->(l:Literal { value:$val }) RETURN s.uri as subject, p.uri as predicate, p.ctx as context, l.value as literal";
                    parameters = new { subj = s.ToString(), val = l.ToString() };
                    break;
                case "SPO":
                    oQuery = "MATCH (s:Resource { uri:$subj })-[p:Property { uri:$pred }]->(o:Resource { uri:$obj }) RETURN s.uri as subject, p.uri as predicate, p.ctx as context, o.uri as object";
                    parameters = new { subj = s.ToString(), pred = p.ToString(), obj = o.ToString() };
                    break;
                case "SPL":
                    lQuery = "MATCH (s:Resource { uri:$subj })-[p:Property { uri:$pred }]->(l:Literal { value:$val }) RETURN s.uri as subject, p.uri as predicate, p.ctx as context, l.value as literal";
                    parameters = new { subj = s.ToString(), pred = p.ToString(), val = l.ToString() };
                    break;
                case "PO":
                    oQuery = "MATCH (s:Resource)-[p:Property { uri:$pred }]->(o:Resource { uri:$obj }) RETURN s.uri as subject, p.uri as predicate, p.ctx as context, o.uri as object";
                    parameters = new { pred = p.ToString(), obj = o.ToString() };
                    break;
                case "PL":
                    lQuery = "MATCH (s:Resource)-[p:Property { uri:$pred }]->(l:Literal { value:$val }) RETURN s.uri as subject, p.uri as predicate, p.ctx as context, l.value as literal";
                    parameters = new { pred = p.ToString(), val = l.ToString() };
                    break;
                //SELECT *
                default:
                    oQuery = "MATCH (s:Resource)-[p:Property]->(o:Resource) RETURN s.uri as subject, p.uri as predicate, p.ctx as context, o.uri as object";
                    lQuery = "MATCH (s:Resource)-[p:Property]->(l:Literal) RETURN s.uri as subject, p.uri as predicate, p.ctx as context, l.value as literal";
                    break;
            }

            using (IAsyncSession neo4jSession = Driver.AsyncSession(ssn => ssn.WithDatabase(DatabaseName)))
            {
                try
                {
                    if (!string.IsNullOrEmpty(oQuery))
                    {
                        await neo4jSession.ExecuteReadAsync(
                            async tx =>
                            {
                                IResultCursor oQueryResult = await tx.RunAsync(oQuery, parameters);
                                while (await oQueryResult.FetchAsync())
                                {
                                    result.Add(new RDFQuadruple(
                                        new RDFContext(oQueryResult.Current.Get<string>("context")),
                                        new RDFResource(oQueryResult.Current.Get<string>("subject")),
                                        new RDFResource(oQueryResult.Current.Get<string>("predicate")),
                                        new RDFResource(oQueryResult.Current.Get<string>("object"))));
                                }
                            });
                    }

                    if (!string.IsNullOrEmpty(lQuery))
                    {
                        await neo4jSession.ExecuteReadAsync(
                            async tx =>
                            {
                                IResultCursor lQueryResult = await tx.RunAsync(lQuery, parameters);
                                while (await lQueryResult.FetchAsync())
                                {
                                    if (RDFQueryUtilities.ParseRDFPatternMember(lQueryResult.Current.Get<string>("literal")) is RDFLiteral literal)
                                    {
                                        result.Add(new RDFQuadruple(
                                            new RDFContext(lQueryResult.Current.Get<string>("context")),
                                            new RDFResource(lQueryResult.Current.Get<string>("subject")),
                                            new RDFResource(lQueryResult.Current.Get<string>("predicate")),
                                            literal));
                                    }
                                }
                            });
                    }

                    await neo4jSession.CloseAsync();
                }
                catch (Exception ex)
                {
                    await neo4jSession.CloseAsync();

                    throw new RDFStoreException("Cannot read data from Neo4j store because: " + ex.Message, ex);
                }
            }

            return result;
        }

        /// <summary>
        /// Asynchronously counts the Neo4j database quadruples
        /// </summary>
        private async Task<long> GetQuadruplesCountAsync()
        {
            using (IAsyncSession neo4jSession = Driver.AsyncSession(ssn => ssn.WithDatabase(DatabaseName)))
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
            using (IAsyncSession neo4jSession = Driver.AsyncSession(ssn => ssn.WithDatabase(DatabaseName)))
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