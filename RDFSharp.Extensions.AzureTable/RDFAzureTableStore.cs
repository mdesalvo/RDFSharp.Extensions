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

using Azure.Data.Tables;
using System;
using System.Text;
using RDFSharp.Model;
using RDFSharp.Store;
using Azure;
using System.Collections.Generic;
using System.Globalization;
using System.Net;
using System.Linq;
using RDFSharp.Query;
using System.Threading.Tasks;

namespace RDFSharp.Extensions.AzureTable
{
    /// <summary>
    /// RDFAzureTableStore represents a RDFStore backed on Azure Table service
    /// </summary>
    #if NET8_0_OR_GREATER
    public sealed class RDFAzureTableStore : RDFStore, IDisposable, IAsyncDisposable
    #else
    public sealed class RDFAzureTableStore : RDFStore, IDisposable
    #endif
    {
        #region Properties
        /// <summary>
        /// Count of the Azure Table service quadruples (-1 in case of errors)
        /// </summary>
        public override long QuadruplesCount
            => GetQuadruplesCountAsync().GetAwaiter().GetResult();

        /// <summary>
        /// Asynchronous count of the Azure Table service quadruples (-1 in case of errors)
        /// </summary>
        public override Task<long> QuadruplesCountAsync
            => GetQuadruplesCountAsync();

        private TableServiceClient ServiceClient { get; set; }
        private TableClient Client { get; set; }
        private bool Disposed { get; set; }

        private static readonly string[] SelectColumns = { "RowKey" };
        #endregion

        #region Ctors
        /// <summary>
        /// Default-ctor to build a local (emulator/azurite) Azure Table store instance
        /// </summary>
        public RDFAzureTableStore() : this("DefaultEndpointsProtocol=http;AccountName=devstoreaccount1;AccountKey=Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==;BlobEndpoint=http://127.0.0.1:10000/devstoreaccount1;QueueEndpoint=http://127.0.0.1:10001/devstoreaccount1;TableEndpoint=http://127.0.0.1:10002/devstoreaccount1;") { }

        /// <summary>
        /// Default-ctor to build an Azure Table store instance
        /// </summary>
        public RDFAzureTableStore(string azureStorageConnectionString)
        {
            #region Guards
            if (string.IsNullOrEmpty(azureStorageConnectionString))
                throw new RDFStoreException("Cannot connect to Azure Table store because: given \"azureStorageConnectionString\" parameter is null or empty.");
            #endregion

            //Initialize store structures
            StoreType = "AZURE-TABLE";
            ServiceClient = new TableServiceClient(azureStorageConnectionString);
            Client = ServiceClient.GetTableClient("Quadruples");
            StoreID = RDFModelUtilities.CreateHash(ToString());
            Disposed = false;

            //Initialize working table
            try
            {
                Client.CreateIfNotExists();
            }
            catch (Exception ex)
            {
                throw new RDFStoreException("Cannot create Azure Table store because: " + ex.Message, ex);
            }
        }

        /// <summary>
        /// Destroys the Azure Table store instance
        /// </summary>
        ~RDFAzureTableStore()
            => Dispose(false);
        #endregion

        #region Interfaces
        /// <summary>
        /// Gives the string representation of the Azure Table store
        /// </summary>
        public override string ToString()
            => $"{base.ToString()}|ACCOUNT-NAME={Client.AccountName};TABLE-NAME={Client.Name}";

        /// <summary>
        /// Disposes the Azure Table store instance (IDisposable)
        /// </summary>
        public void Dispose()
        {
            Dispose(true);
            GC.SuppressFinalize(this);
        }

#if NET8_0_OR_GREATER
        /// <summary>
        /// Asynchronously disposes the Azure Table store (IAsyncDisposable)
        /// </summary>
        ValueTask IAsyncDisposable.DisposeAsync()
        {
            Dispose(true);
            GC.SuppressFinalize(this);
            return ValueTask.CompletedTask;
        }
#endif
        
        /// <summary>
        /// Disposes the Azure Table store instance  (business logic of resources disposal)
        /// </summary>
        private void Dispose(bool disposing)
        {
            if (Disposed)
                return;

            if (disposing)
            {
                ServiceClient = null;
                Client = null;
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
                try
                {
                    //Execute the merge operation as a set of upsert batches
                    foreach (IEnumerable<TableTransactionAction> batch in PrepareUpsertBatch(graph))
                    {
                        Response<IReadOnlyList<Response>> transactionResponse = await Client.SubmitTransactionAsync(batch);

                        if (transactionResponse.GetRawResponse().IsError)
                            throw new Exception(transactionResponse.ToString());
                    }
                }
                catch (Exception ex)
                {
                    throw new RDFStoreException("Cannot insert batch data into Azure Table store because: " + ex.Message, ex);
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
                    Response response = await Client.UpsertEntityAsync(new RDFAzureTableQuadruple(quadruple));

                    if (response.IsError)
                        throw new Exception(response.ToString());
                }
                catch (Exception ex)
                {
                    throw new RDFStoreException("Cannot insert data into Azure Table store because: " + ex.Message, ex);
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
                    Response response = await Client.DeleteEntityAsync("RDFSHARP", quadruple.QuadrupleID.ToString(CultureInfo.InvariantCulture));

                    if (response.IsError && response.Status != (int)HttpStatusCode.NotFound)
                        throw new Exception(response.ToString());
                }
                catch (Exception ex)
                {
                    throw new RDFStoreException("Cannot delete data from Azure Table store because: " + ex.Message, ex);
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

            try
            {
                //Fetch entities for deletion
                Pageable<RDFAzureTableQuadruple> quadruples=null;
                switch (queryFilters.ToString())
                {
                    case "C":
                        quadruples = Client.Query<RDFAzureTableQuadruple>(q => string.Equals(q.PartitionKey, "RDFSHARP") && string.Equals(q.C, c.ToString()));
                        break;
                    case "S":
                        quadruples = Client.Query<RDFAzureTableQuadruple>(q => string.Equals(q.PartitionKey, "RDFSHARP") && string.Equals(q.S, s.ToString()));
                        break;
                    case "P":
                        quadruples = Client.Query<RDFAzureTableQuadruple>(q => string.Equals(q.PartitionKey, "RDFSHARP") && string.Equals(q.P, p.ToString()));
                        break;
                    case "O":
                        quadruples = Client.Query<RDFAzureTableQuadruple>(q => string.Equals(q.PartitionKey, "RDFSHARP") && string.Equals(q.O, o.ToString()) && q.F == (int)RDFModelEnums.RDFTripleFlavors.SPO);
                        break;
                    case "L":
                        quadruples = Client.Query<RDFAzureTableQuadruple>(q => string.Equals(q.PartitionKey, "RDFSHARP") && string.Equals(q.O, o.ToString()) && q.F == (int)RDFModelEnums.RDFTripleFlavors.SPL);
                        break;
                    case "CS":
                        quadruples = Client.Query<RDFAzureTableQuadruple>(q => string.Equals(q.PartitionKey, "RDFSHARP") && string.Equals(q.C, c.ToString()) && string.Equals(q.S, s.ToString()));
                        break;
                    case "CP":
                        quadruples = Client.Query<RDFAzureTableQuadruple>(q => string.Equals(q.PartitionKey, "RDFSHARP") && string.Equals(q.C, c.ToString()) && string.Equals(q.P, p.ToString()));
                        break;
                    case "CO":
                        quadruples = Client.Query<RDFAzureTableQuadruple>(q => string.Equals(q.PartitionKey, "RDFSHARP") && string.Equals(q.C, c.ToString()) && string.Equals(q.O, o.ToString()) && q.F == (int)RDFModelEnums.RDFTripleFlavors.SPO);
                        break;
                    case "CL":
                        quadruples = Client.Query<RDFAzureTableQuadruple>(q => string.Equals(q.PartitionKey, "RDFSHARP") && string.Equals(q.C, c.ToString()) && string.Equals(q.O, o.ToString()) && q.F == (int)RDFModelEnums.RDFTripleFlavors.SPL);
                        break;
                    case "CSP":
                        quadruples = Client.Query<RDFAzureTableQuadruple>(q => string.Equals(q.PartitionKey, "RDFSHARP") && string.Equals(q.C, c.ToString()) && string.Equals(q.S, s.ToString()) && string.Equals(q.P, p.ToString()));
                        break;
                    case "CSO":
                        quadruples = Client.Query<RDFAzureTableQuadruple>(q => string.Equals(q.PartitionKey, "RDFSHARP") && string.Equals(q.C, c.ToString()) && string.Equals(q.S, s.ToString()) && string.Equals(q.O, o.ToString()) && q.F == (int)RDFModelEnums.RDFTripleFlavors.SPO);
                        break;
                    case "CSL":
                        quadruples = Client.Query<RDFAzureTableQuadruple>(q => string.Equals(q.PartitionKey, "RDFSHARP") && string.Equals(q.C, c.ToString()) && string.Equals(q.S, s.ToString()) && string.Equals(q.O, o.ToString()) && q.F == (int)RDFModelEnums.RDFTripleFlavors.SPL);
                        break;
                    case "CPO":
                        quadruples = Client.Query<RDFAzureTableQuadruple>(q => string.Equals(q.PartitionKey, "RDFSHARP") && string.Equals(q.C, c.ToString()) && string.Equals(q.P, p.ToString()) && string.Equals(q.O, o.ToString()) && q.F == (int)RDFModelEnums.RDFTripleFlavors.SPO);
                        break;
                    case "CPL":
                        quadruples = Client.Query<RDFAzureTableQuadruple>(q => string.Equals(q.PartitionKey, "RDFSHARP") && string.Equals(q.C, c.ToString()) && string.Equals(q.P, p.ToString()) && string.Equals(q.O, o.ToString()) && q.F == (int)RDFModelEnums.RDFTripleFlavors.SPL);
                        break;
                    case "CSPO":
                        quadruples = Client.Query<RDFAzureTableQuadruple>(q => string.Equals(q.PartitionKey, "RDFSHARP") && string.Equals(q.C, c.ToString()) && string.Equals(q.S, s.ToString()) && string.Equals(q.P, p.ToString()) && string.Equals(q.O, o.ToString()) && q.F == (int)RDFModelEnums.RDFTripleFlavors.SPO);
                        break;
                    case "CSPL":
                        quadruples = Client.Query<RDFAzureTableQuadruple>(q => string.Equals(q.PartitionKey, "RDFSHARP") && string.Equals(q.C, c.ToString()) && string.Equals(q.S, s.ToString()) && string.Equals(q.P, p.ToString()) && string.Equals(q.O, o.ToString()) && q.F == (int)RDFModelEnums.RDFTripleFlavors.SPL);
                        break;
                    case "SP":
                        quadruples = Client.Query<RDFAzureTableQuadruple>(q => string.Equals(q.PartitionKey, "RDFSHARP") && string.Equals(q.S, s.ToString()) && string.Equals(q.P, p.ToString()));
                        break;
                    case "SO":
                        quadruples = Client.Query<RDFAzureTableQuadruple>(q => string.Equals(q.PartitionKey, "RDFSHARP") && string.Equals(q.S, s.ToString()) && string.Equals(q.O, o.ToString()) && q.F == (int)RDFModelEnums.RDFTripleFlavors.SPO);
                        break;
                    case "SL":
                        quadruples = Client.Query<RDFAzureTableQuadruple>(q => string.Equals(q.PartitionKey, "RDFSHARP") && string.Equals(q.S, s.ToString()) && string.Equals(q.O, o.ToString()) && q.F == (int)RDFModelEnums.RDFTripleFlavors.SPL);
                        break;
                    case "SPO":
                        quadruples = Client.Query<RDFAzureTableQuadruple>(q => string.Equals(q.PartitionKey, "RDFSHARP") && string.Equals(q.S, s.ToString()) && string.Equals(q.P, p.ToString()) && string.Equals(q.O, o.ToString()) && q.F == (int)RDFModelEnums.RDFTripleFlavors.SPO);
                        break;
                    case "SPL":
                        quadruples = Client.Query<RDFAzureTableQuadruple>(q => string.Equals(q.PartitionKey, "RDFSHARP") && string.Equals(q.S, s.ToString()) && string.Equals(q.P, p.ToString()) && string.Equals(q.O, o.ToString()) && q.F == (int)RDFModelEnums.RDFTripleFlavors.SPL);
                        break;
                    case "PO":
                        quadruples = Client.Query<RDFAzureTableQuadruple>(q => string.Equals(q.PartitionKey, "RDFSHARP") && string.Equals(q.P, p.ToString()) && string.Equals(q.O, o.ToString()) && q.F == (int)RDFModelEnums.RDFTripleFlavors.SPO);
                        break;
                    case "PL":
                        quadruples = Client.Query<RDFAzureTableQuadruple>(q => string.Equals(q.PartitionKey, "RDFSHARP") && string.Equals(q.P, p.ToString()) && string.Equals(q.O, o.ToString()) && q.F == (int)RDFModelEnums.RDFTripleFlavors.SPL);
                        break;
                }

                //Execute the operation as a set of delete batches
                foreach (IEnumerable<TableTransactionAction> batch in PrepareDeleteBatch(quadruples.AsEnumerable()))
                {
                    Response<IReadOnlyList<Response>> transactionResponse = await Client.SubmitTransactionAsync(batch);

                    if (transactionResponse.GetRawResponse().IsError)
                        throw new Exception(transactionResponse.ToString());
                }
            }
            catch (Exception ex)
            {
                throw new RDFStoreException("Cannot delete batch data from Azure Table store because: " + ex.Message, ex);
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
                //Fetch entities candidates for deletion
                Pageable<RDFAzureTableQuadruple> quadruples = Client.Query<RDFAzureTableQuadruple>(q => string.Equals(q.PartitionKey, "RDFSHARP"));

                //Execute the remove operation as a set of delete batches
                foreach (IEnumerable<TableTransactionAction> batch in PrepareDeleteBatch(quadruples.AsEnumerable()))
                {
                    Response<IReadOnlyList<Response>> transactionResponse = await Client.SubmitTransactionAsync(batch);

                    if (transactionResponse.GetRawResponse().IsError)
                        throw new Exception(transactionResponse.ToString());
                }
            }
            catch (Exception ex)
            {
                throw new RDFStoreException("Cannot delete batch data from Azure Table store because: " + ex.Message, ex);
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
                NullableResponse<RDFAzureTableQuadruple> response = await Client.GetEntityIfExistsAsync<RDFAzureTableQuadruple>("RDFSHARP", quadruple.QuadrupleID.ToString(CultureInfo.InvariantCulture));
                return response.HasValue;
            }
            catch (Exception ex)
            {
                throw new RDFStoreException("Cannot read data from Azure Table store because: " + ex.Message, ex);
            }
        }

        /// <summary>
        /// Selects the quadruples which satisfy the given combination of CSPOL accessors<br/>
        /// (null values are handled as * selectors. Object and Literal params, if given, must be mutually exclusive!)
        /// </summary>
        /// <exception cref="RDFStoreException"></exception>
        public override List<RDFQuadruple> SelectQuadruples(RDFContext c=null, RDFResource s=null, RDFResource p=null, RDFResource o=null, RDFLiteral l=null)
            => SelectQuadruplesAsync(c,s,p,o,l).GetAwaiter().GetResult();
        
        /// <summary>
        /// Asynchronously selects the quadruples which satisfy the given combination of CSPOL accessors<br/>
        /// (null values are handled as * selectors. Object and Literal params, if given, must be mutually exclusive!)
        /// </summary>
        /// <exception cref="RDFStoreException"></exception>
        public override async Task<List<RDFQuadruple>> SelectQuadruplesAsync(RDFContext c=null, RDFResource s=null, RDFResource p=null, RDFResource o=null, RDFLiteral l=null)
        {
            List<RDFQuadruple>  result = new List<RDFQuadruple>();

            //Build filters
            StringBuilder queryFilters = new StringBuilder();
            if (c != null) queryFilters.Append('C');
            if (s != null) queryFilters.Append('S');
            if (p != null) queryFilters.Append('P');
            if (o != null) queryFilters.Append('O');
            if (l != null) queryFilters.Append('L');

            try
            {
                //Fetch entities for retrieval
                AsyncPageable<RDFAzureTableQuadruple> quadruples=null;
                switch (queryFilters.ToString())
                {
                    case "C":
                        quadruples = Client.QueryAsync<RDFAzureTableQuadruple>(q => string.Equals(q.PartitionKey, "RDFSHARP") && string.Equals(q.C, c.ToString()));
                        break;
                    case "S":
                        quadruples = Client.QueryAsync<RDFAzureTableQuadruple>(q => string.Equals(q.PartitionKey, "RDFSHARP") && string.Equals(q.S, s.ToString()));
                        break;
                    case "P":
                        quadruples = Client.QueryAsync<RDFAzureTableQuadruple>(q => string.Equals(q.PartitionKey, "RDFSHARP") && string.Equals(q.P, p.ToString()));
                        break;
                    case "O":
                        quadruples = Client.QueryAsync<RDFAzureTableQuadruple>(q => string.Equals(q.PartitionKey, "RDFSHARP") && string.Equals(q.O, o.ToString()) && q.F == (int)RDFModelEnums.RDFTripleFlavors.SPO);
                        break;
                    case "L":
                        quadruples = Client.QueryAsync<RDFAzureTableQuadruple>(q => string.Equals(q.PartitionKey, "RDFSHARP") && string.Equals(q.O, o.ToString()) && q.F == (int)RDFModelEnums.RDFTripleFlavors.SPL);
                        break;
                    case "CS":
                        quadruples = Client.QueryAsync<RDFAzureTableQuadruple>(q => string.Equals(q.PartitionKey, "RDFSHARP") && string.Equals(q.C, c.ToString()) && string.Equals(q.S, s.ToString()));
                        break;
                    case "CP":
                        quadruples = Client.QueryAsync<RDFAzureTableQuadruple>(q => string.Equals(q.PartitionKey, "RDFSHARP") && string.Equals(q.C, c.ToString()) && string.Equals(q.P, p.ToString()));
                        break;
                    case "CO":
                        quadruples = Client.QueryAsync<RDFAzureTableQuadruple>(q => string.Equals(q.PartitionKey, "RDFSHARP") && string.Equals(q.C, c.ToString()) && string.Equals(q.O, o.ToString()) && q.F == (int)RDFModelEnums.RDFTripleFlavors.SPO);
                        break;
                    case "CL":
                        quadruples = Client.QueryAsync<RDFAzureTableQuadruple>(q => string.Equals(q.PartitionKey, "RDFSHARP") && string.Equals(q.C, c.ToString()) && string.Equals(q.O, o.ToString()) && q.F == (int)RDFModelEnums.RDFTripleFlavors.SPL);
                        break;
                    case "CSP":
                        quadruples = Client.QueryAsync<RDFAzureTableQuadruple>(q => string.Equals(q.PartitionKey, "RDFSHARP") && string.Equals(q.C, c.ToString()) && string.Equals(q.S, s.ToString()) && string.Equals(q.P, p.ToString()));
                        break;
                    case "CSO":
                        quadruples = Client.QueryAsync<RDFAzureTableQuadruple>(q => string.Equals(q.PartitionKey, "RDFSHARP") && string.Equals(q.C, c.ToString()) && string.Equals(q.S, s.ToString()) && string.Equals(q.O, o.ToString()) && q.F == (int)RDFModelEnums.RDFTripleFlavors.SPO);
                        break;
                    case "CSL":
                        quadruples = Client.QueryAsync<RDFAzureTableQuadruple>(q => string.Equals(q.PartitionKey, "RDFSHARP") && string.Equals(q.C, c.ToString()) && string.Equals(q.S, s.ToString()) && string.Equals(q.O, o.ToString()) && q.F == (int)RDFModelEnums.RDFTripleFlavors.SPL);
                        break;
                    case "CPO":
                        quadruples = Client.QueryAsync<RDFAzureTableQuadruple>(q => string.Equals(q.PartitionKey, "RDFSHARP") && string.Equals(q.C, c.ToString()) && string.Equals(q.P, p.ToString()) && string.Equals(q.O, o.ToString()) && q.F == (int)RDFModelEnums.RDFTripleFlavors.SPO);
                        break;
                    case "CPL":
                        quadruples = Client.QueryAsync<RDFAzureTableQuadruple>(q => string.Equals(q.PartitionKey, "RDFSHARP") && string.Equals(q.C, c.ToString()) && string.Equals(q.P, p.ToString()) && string.Equals(q.O, o.ToString()) && q.F == (int)RDFModelEnums.RDFTripleFlavors.SPL);
                        break;
                    case "CSPO":
                        quadruples = Client.QueryAsync<RDFAzureTableQuadruple>(q => string.Equals(q.PartitionKey, "RDFSHARP") && string.Equals(q.C, c.ToString()) && string.Equals(q.S, s.ToString()) && string.Equals(q.P, p.ToString()) && string.Equals(q.O, o.ToString()) && q.F == (int)RDFModelEnums.RDFTripleFlavors.SPO);
                        break;
                    case "CSPL":
                        quadruples = Client.QueryAsync<RDFAzureTableQuadruple>(q => string.Equals(q.PartitionKey, "RDFSHARP") && string.Equals(q.C, c.ToString()) && string.Equals(q.S, s.ToString()) && string.Equals(q.P, p.ToString()) && string.Equals(q.O, o.ToString()) && q.F == (int)RDFModelEnums.RDFTripleFlavors.SPL);
                        break;
                    case "SP":
                        quadruples = Client.QueryAsync<RDFAzureTableQuadruple>(q => string.Equals(q.PartitionKey, "RDFSHARP") && string.Equals(q.S, s.ToString()) && string.Equals(q.P, p.ToString()));
                        break;
                    case "SO":
                        quadruples = Client.QueryAsync<RDFAzureTableQuadruple>(q => string.Equals(q.PartitionKey, "RDFSHARP") && string.Equals(q.S, s.ToString()) && string.Equals(q.O, o.ToString()) && q.F == (int)RDFModelEnums.RDFTripleFlavors.SPO);
                        break;
                    case "SL":
                        quadruples = Client.QueryAsync<RDFAzureTableQuadruple>(q => string.Equals(q.PartitionKey, "RDFSHARP") && string.Equals(q.S, s.ToString()) && string.Equals(q.O, o.ToString()) && q.F == (int)RDFModelEnums.RDFTripleFlavors.SPL);
                        break;
                    case "SPO":
                        quadruples = Client.QueryAsync<RDFAzureTableQuadruple>(q => string.Equals(q.PartitionKey, "RDFSHARP") && string.Equals(q.S, s.ToString()) && string.Equals(q.P, p.ToString()) && string.Equals(q.O, o.ToString()) && q.F == (int)RDFModelEnums.RDFTripleFlavors.SPO);
                        break;
                    case "SPL":
                        quadruples = Client.QueryAsync<RDFAzureTableQuadruple>(q => string.Equals(q.PartitionKey, "RDFSHARP") && string.Equals(q.S, s.ToString()) && string.Equals(q.P, p.ToString()) && string.Equals(q.O, o.ToString()) && q.F == (int)RDFModelEnums.RDFTripleFlavors.SPL);
                        break;
                    case "PO":
                        quadruples = Client.QueryAsync<RDFAzureTableQuadruple>(q => string.Equals(q.PartitionKey, "RDFSHARP") && string.Equals(q.P, p.ToString()) && string.Equals(q.O, o.ToString()) && q.F == (int)RDFModelEnums.RDFTripleFlavors.SPO);
                        break;
                    case "PL":
                        quadruples = Client.QueryAsync<RDFAzureTableQuadruple>(q => string.Equals(q.PartitionKey, "RDFSHARP") && string.Equals(q.P, p.ToString()) && string.Equals(q.O, o.ToString()) && q.F == (int)RDFModelEnums.RDFTripleFlavors.SPL);
                        break;
                }

                //Transform entities into quadruples
                IAsyncEnumerator<RDFAzureTableQuadruple> quadruplesEnum = quadruples.GetAsyncEnumerator();
                while (await quadruplesEnum.MoveNextAsync())
                {
                    RDFContext qContext = new RDFContext(quadruplesEnum.Current.C);
                    RDFPatternMember qSubject = RDFQueryUtilities.ParseRDFPatternMember(quadruplesEnum.Current.S);
                    RDFPatternMember qPredicate = RDFQueryUtilities.ParseRDFPatternMember(quadruplesEnum.Current.P);
                    RDFPatternMember qObject = RDFQueryUtilities.ParseRDFPatternMember(quadruplesEnum.Current.O);
                    result.Add(quadruplesEnum.Current.F == (int)RDFModelEnums.RDFTripleFlavors.SPO
                        ? new RDFQuadruple(qContext, (RDFResource)qSubject, (RDFResource)qPredicate, (RDFResource)qObject)
                        : new RDFQuadruple(qContext, (RDFResource)qSubject, (RDFResource)qPredicate, (RDFLiteral)qObject));
                }
            }
            catch (Exception ex)
            {
                throw new RDFStoreException("Cannot read data from Azure Table store because: " + ex.Message, ex);
            }

            return result;
        }

        /// <summary>
        /// Asynchronously counts the Azure Table service quadruples
        /// </summary>
        private async Task<long> GetQuadruplesCountAsync()
        {
            try
            {
                AsyncPageable<RDFAzureTableQuadruple> quadruples = Client.QueryAsync<RDFAzureTableQuadruple>(
                    qent => string.Equals(qent.PartitionKey, "RDFSHARP"), select: SelectColumns);

                long quadruplesCount = 0;
                IAsyncEnumerator<RDFAzureTableQuadruple> quadruplesEnum = quadruples.GetAsyncEnumerator();
                while (await quadruplesEnum.MoveNextAsync())
                    quadruplesCount++;

                //Return the quadruples count
                return quadruplesCount;
            }
            catch
            {
                //Return the quadruples count (-1 to indicate error)
                return -1L;
            }
        }
        #endregion

        #endregion

        #region Utilities

        /// <summary>
        /// Chunks the triples of the given graph into upsert batches of 100 entities
        /// </summary>
        private static IEnumerable<IEnumerable<TableTransactionAction>> PrepareUpsertBatch(RDFGraph graph)
        {
            RDFContext graphContext = new RDFContext(graph.Context);

            List<TableTransactionAction> batch = new List<TableTransactionAction>(100);
            foreach (RDFTriple triple in graph)
            {
                batch.Add(new TableTransactionAction(TableTransactionActionType.UpdateReplace,
                    new RDFAzureTableQuadruple(new RDFQuadruple(graphContext, triple))));
                if (batch.Count == 100)
                {
                    yield return batch;

                    batch = new List<TableTransactionAction>(100);
                }
            }

            if (batch.Count > 0)
                yield return batch;
        }

        /// <summary>
        /// Chunks the given entities into delete batches of 100 items
        /// </summary>
        private static IEnumerable<IEnumerable<TableTransactionAction>> PrepareDeleteBatch(IEnumerable<RDFAzureTableQuadruple> quadruples)
        {
            List<TableTransactionAction> batch = new List<TableTransactionAction>(100);
            foreach (RDFAzureTableQuadruple quadruple in quadruples)
            {
                batch.Add(new TableTransactionAction(TableTransactionActionType.Delete, quadruple));
                if (batch.Count == 100)
                {
                    yield return batch;

                    batch = new List<TableTransactionAction>(100);
                }
            }

            if (batch.Count > 0)
                yield return batch;
        }

        #endregion
    }
}