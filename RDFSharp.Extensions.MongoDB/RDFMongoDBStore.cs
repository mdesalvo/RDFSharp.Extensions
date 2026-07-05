/*
   Copyright 2012-2026 Marco De Salvo

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

using MongoDB.Bson;
using MongoDB.Driver;
using RDFSharp.Model;
using RDFSharp.Query;
using RDFSharp.Store;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace RDFSharp.Extensions.MongoDB
{
    /// <summary>
    /// RDFMongoDBStore represents a RDFStore backed on MongoDB engine
    /// </summary>
#if NET8_0_OR_GREATER
    public sealed class RDFMongoDBStore : RDFStore, IDisposable, IAsyncDisposable
#else
    public sealed class RDFMongoDBStore : RDFStore, IDisposable
#endif
    {
        #region Properties
        /// <summary>
        /// Count of the MongoDB database quadruples (-1 in case of errors)
        /// </summary>
        public override long QuadruplesCount
            => GetQuadruplesCountAsync().GetAwaiter().GetResult();

        /// <summary>
        /// Asynchronous count of the MongoDB database quadruples (-1 in case of errors)
        /// </summary>
        public override Task<long> QuadruplesCountAsync
            => GetQuadruplesCountAsync();

        /// <summary>
        /// Client to handle underlying MongoDB database
        /// </summary>
        private IMongoClient Client { get; set; }

        /// <summary>
        /// Collection storing the quadruples of the store
        /// </summary>
        private IMongoCollection<RDFMongoDBQuadruple> Collection { get; set; }

        /// <summary>
        /// Name of underlying MongoDB database
        /// </summary>
        private string DatabaseName { get; }

        /// <summary>
        /// Addresses of the servers backing the underlying MongoDB client
        /// </summary>
        private string ServerAddresses { get; set; }

        /// <summary>
        /// Flag indicating that the MongoDB store instance has already been disposed
        /// </summary>
        private bool Disposed { get; set; }
        #endregion

        #region Ctors
        /// <summary>
        /// Default-ctor to build a MongoDB store instance with given connection string
        /// </summary>
        public RDFMongoDBStore(string mongoConnectionString, string databaseName="rdfsharp")
        {
            #region Guards
            if (string.IsNullOrEmpty(mongoConnectionString))
                throw new RDFStoreException("Cannot connect to MongoDB store because: given \"mongoConnectionString\" parameter is null or empty.");
            if (string.IsNullOrEmpty(databaseName))
                throw new RDFStoreException("Cannot connect to MongoDB store because: given \"databaseName\" parameter is null or empty.");
            #endregion

            //Initialize client
            Client = new MongoClient(mongoConnectionString);
            try
            {
                //Verify connectivity
                Client.GetDatabase(databaseName).RunCommand<BsonDocument>(new BsonDocument("ping", 1));
                //Fetch server info
                ServerAddresses = string.Join(",", Client.Settings.Servers.Select(srv => $"{srv.Host}:{srv.Port}"));
            }
            catch (Exception ex)
            {
                throw new RDFStoreException("Cannot connect to MongoDB store because: " + ex.Message, ex);
            }

            //Initialize store
            DatabaseName = databaseName;
            Collection = Client.GetDatabase(databaseName).GetCollection<RDFMongoDBQuadruple>("quadruples");
            StoreType = "MONGODB";
            StoreID = RDFModelUtilities.CreateHash(ToString());
            Disposed = false;

            //Prepare store
            InitializeStoreAsync().GetAwaiter().GetResult();
        }

        /// <summary>
        /// Destroys the MongoDB store instance
        /// </summary>
        ~RDFMongoDBStore()
            => Dispose(false);
        #endregion

        #region Interfaces
        /// <summary>
        /// Gives the string representation of the MongoDB store
        /// </summary>
        public override string ToString()
            => $"{base.ToString()}|SERVERS={ServerAddresses}|DATABASE={DatabaseName}";

        /// <summary>
        /// Disposes the MongoDB store instance
        /// </summary>
        public void Dispose()
        {
            Dispose(true);
            GC.SuppressFinalize(this);
        }

#if NET8_0_OR_GREATER
        /// <summary>
        /// Asynchronously disposes the MongoDB store (IAsyncDisposable)
        /// </summary>
        ValueTask IAsyncDisposable.DisposeAsync()
        {
            Dispose(true);
            GC.SuppressFinalize(this);
            return ValueTask.CompletedTask;
        }
#endif

        /// <summary>
        /// Disposes the MongoDB store instance (business logic of resources disposal)
        /// </summary>
        private void Dispose(bool disposing)
        {
            if (Disposed)
                return;

            if (disposing)
            {
                //The MongoDB driver's client does not own unmanaged resources requiring explicit
                //disposal (its connection pool is reclaimed by the runtime): only remove references
                Client = null;
                Collection = null;
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

                //Merge is executed as a single upsert bulk-write, since the driver natively
                //supports it and each replacement is idempotent (keyed on QuadrupleID)
                List<WriteModel<RDFMongoDBQuadruple>> upsertBatch = new List<WriteModel<RDFMongoDBQuadruple>>();
                foreach (RDFTriple triple in graph)
                {
                    RDFMongoDBQuadruple mongoQuadruple = new RDFMongoDBQuadruple(new RDFQuadruple(graphContext, triple));
                    upsertBatch.Add(new ReplaceOneModel<RDFMongoDBQuadruple>(
                        Builders<RDFMongoDBQuadruple>.Filter.Eq(q => q.QuadrupleID, mongoQuadruple.QuadrupleID), mongoQuadruple) { IsUpsert = true });
                }

                if (upsertBatch.Count > 0)
                {
                    try
                    {
                        await Collection.BulkWriteAsync(upsertBatch, new BulkWriteOptions { IsOrdered = false });
                    }
                    catch (Exception ex)
                    {
                        throw new RDFStoreException("Cannot insert batch data into MongoDB store because: " + ex.Message, ex);
                    }
                }
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
                try
                {
                    RDFMongoDBQuadruple mongoQuadruple = new RDFMongoDBQuadruple(quadruple);

                    //Upsert-by-QuadrupleID gives the same idempotency of a MERGE: adding an
                    //already existing quadruple is a no-op replacement, never a duplicate
                    await Collection.ReplaceOneAsync(
                        Builders<RDFMongoDBQuadruple>.Filter.Eq(q => q.QuadrupleID, mongoQuadruple.QuadrupleID), mongoQuadruple, new ReplaceOptions { IsUpsert = true });
                }
                catch (Exception ex)
                {
                    throw new RDFStoreException("Cannot insert data into MongoDB store because: " + ex.Message, ex);
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
                try
                {
                    await Collection.DeleteOneAsync(Builders<RDFMongoDBQuadruple>.Filter.Eq(q => q.QuadrupleID, quadruple.QuadrupleID));
                }
                catch (Exception ex)
                {
                    throw new RDFStoreException("Cannot delete data from MongoDB store because: " + ex.Message, ex);
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

            try
            {
                FilterDefinition<RDFMongoDBQuadruple> filter = BuildQuadrupleFilter(c, s, p, o, l);
                await Collection.DeleteManyAsync(filter);
            }
            catch (Exception ex)
            {
                throw new RDFStoreException("Cannot delete data from MongoDB store because: " + ex.Message, ex);
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
            try
            {
                await Collection.DeleteManyAsync(Builders<RDFMongoDBQuadruple>.Filter.Empty);
            }
            catch (Exception ex)
            {
                throw new RDFStoreException("Cannot delete data from MongoDB store because: " + ex.Message, ex);
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

            try
            {
                long count = await Collection.CountDocumentsAsync(
                    Builders<RDFMongoDBQuadruple>.Filter.Eq(q => q.QuadrupleID, quadruple.QuadrupleID), new CountOptions { Limit = 1 });
                return count > 0;
            }
            catch (Exception ex)
            {
                throw new RDFStoreException("Cannot read data from MongoDB store because: " + ex.Message, ex);
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
            #region Guards
            if (o != null && l != null)
                throw new RDFStoreException("Cannot access a store when both object and literals are given: they must be mutually exclusive!");
            #endregion

            List<RDFQuadruple> result = new List<RDFQuadruple>();

            try
            {
                FilterDefinition<RDFMongoDBQuadruple> filter = BuildQuadrupleFilter(c, s, p, o, l);
                List<RDFMongoDBQuadruple> mongoQuadruples = await Collection.Find(filter).ToListAsync();

                foreach (RDFMongoDBQuadruple mongoQuadruple in mongoQuadruples)
                {
                    RDFContext qContext = new RDFContext(mongoQuadruple.Context);
                    RDFResource qSubject = new RDFResource(mongoQuadruple.Subject);
                    RDFResource qPredicate = new RDFResource(mongoQuadruple.Predicate);

                    result.Add(mongoQuadruple.TripleFlavor == (int)RDFModelEnums.RDFTripleFlavors.SPO
                        ? new RDFQuadruple(qContext, qSubject, qPredicate, new RDFResource(mongoQuadruple.Object))
                        : new RDFQuadruple(qContext, qSubject, qPredicate, (RDFLiteral)RDFQueryUtilities.ParseRDFPatternMember(mongoQuadruple.Object)));
                }
            }
            catch (Exception ex)
            {
                throw new RDFStoreException("Cannot read data from MongoDB store because: " + ex.Message, ex);
            }

            return result;
        }

        /// <summary>
        /// Asynchronously counts the MongoDB database quadruples
        /// </summary>
        private async Task<long> GetQuadruplesCountAsync()
        {
            try
            {
                return await Collection.CountDocumentsAsync(Builders<RDFMongoDBQuadruple>.Filter.Empty);
            }
            catch
            {
                return -1;
            }
        }
        #endregion

        #region Helpers
        /// <summary>
        /// Builds a MongoDB filter definition from the given CSPOL combination
        /// </summary>
        private static FilterDefinition<RDFMongoDBQuadruple> BuildQuadrupleFilter(RDFContext c, RDFResource s, RDFResource p, RDFResource o, RDFLiteral l)
        {
            FilterDefinitionBuilder<RDFMongoDBQuadruple> filterBuilder = Builders<RDFMongoDBQuadruple>.Filter;
            FilterDefinition<RDFMongoDBQuadruple> filter = filterBuilder.Empty;

            if (c != null)
                filter &= filterBuilder.Eq(q => q.Context, c.ToString());
            if (s != null)
                filter &= filterBuilder.Eq(q => q.Subject, s.ToString());
            if (p != null)
                filter &= filterBuilder.Eq(q => q.Predicate, p.ToString());
            if (o != null)
            {
                filter &= filterBuilder.Eq(q => q.Object, o.ToString());
                filter &= filterBuilder.Eq(q => q.TripleFlavor, (int)RDFModelEnums.RDFTripleFlavors.SPO);
            }
            if (l != null)
            {
                filter &= filterBuilder.Eq(q => q.Object, l.ToString());
                filter &= filterBuilder.Eq(q => q.TripleFlavor, (int)RDFModelEnums.RDFTripleFlavors.SPL);
            }

            return filter;
        }
        #endregion

        #region Diagnostics
        /// <summary>
        /// Initializes the underlying MongoDB collection
        /// </summary>
        private async Task InitializeStoreAsync()
        {
            try
            {
                await Collection.Indexes.CreateManyAsync(new[]
                {
                    new CreateIndexModel<RDFMongoDBQuadruple>(
                        Builders<RDFMongoDBQuadruple>.IndexKeys.Ascending(q => q.Context),
                        new CreateIndexOptions { Name = "ctxIdx" }),
                    new CreateIndexModel<RDFMongoDBQuadruple>(
                        Builders<RDFMongoDBQuadruple>.IndexKeys.Ascending(q => q.Subject),
                        new CreateIndexOptions { Name = "subjIdx" }),
                    new CreateIndexModel<RDFMongoDBQuadruple>(
                        Builders<RDFMongoDBQuadruple>.IndexKeys.Ascending(q => q.Predicate),
                        new CreateIndexOptions { Name = "predIdx" }),
                    new CreateIndexModel<RDFMongoDBQuadruple>(
                        Builders<RDFMongoDBQuadruple>.IndexKeys.Ascending(q => q.Object).Ascending(q => q.TripleFlavor),
                        new CreateIndexOptions { Name = "objFlavorIdx" })
                });
            }
            catch (Exception ex)
            {
                //Propagate exception
                throw new RDFStoreException("Cannot prepare MongoDB store because: " + ex.Message, ex);
            }
        }
        #endregion

        #endregion
    }
}