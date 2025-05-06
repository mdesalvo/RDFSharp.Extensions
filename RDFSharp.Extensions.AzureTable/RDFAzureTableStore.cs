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
using System.Net;
using System.Linq;
using RDFSharp.Query;
using System.Threading.Tasks;

namespace RDFSharp.Extensions.AzureTable
{
    /// <summary>
    /// RDFAzureTableStore represents a RDFStore backed on Azure Table service
    /// </summary>
    public sealed class RDFAzureTableStore : RDFStore, IDisposable
    {
        #region Properties
        /// <summary>
        /// Count of the Azure Table service quadruples (-1 in case of errors)
        /// </summary>
        public override long QuadruplesCount 
            => GetQuadruplesCount();

        /// <summary>
        /// Asynchronous count of the Azure Table service quadruples (-1 in case of errors)
        /// </summary>
        public Task<long> QuadruplesCountAsync 
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
        ~RDFAzureTableStore() => Dispose(false);
        #endregion

        #region Interfaces
        /// <summary>
        /// Gives the string representation of the Azure Table store 
        /// </summary>
        public override string ToString()
            => string.Concat(base.ToString(), "|ACCOUNT-NAME=", Client.AccountName, ";TABLE-NAME=", Client.Name);

        /// <summary>
        /// Disposes the Azure Table store instance 
        /// </summary>
        public void Dispose()
        {
            Dispose(true);
            GC.SuppressFinalize(this);
        }

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
        {
            if (graph != null)
            {
                try
                {
                    //Execute the merge operation as a set of upsert batches
                    foreach (IEnumerable<TableTransactionAction> batch in PrepareUpsertBatch(graph))
                    {
                        Response<IReadOnlyList<Response>> transactionResponse = Client.SubmitTransaction(batch);

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
        {
            if (quadruple != null)
            {
                try
                {
                    Response response = Client.UpsertEntity(new RDFAzureTableQuadruple(quadruple));

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
        {
            if (quadruple != null)
            {
                try
                {
                    Response response = Client.DeleteEntity("RDFSHARP", quadruple.QuadrupleID.ToString());

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
        /// Removes the quadruples with the given context
        /// </summary>
        public override RDFStore RemoveQuadruplesByContext(RDFContext contextResource)
        {
            if (contextResource != null)
            {
                try
                {
                    //Fetch entities candidates for deletion
                    Pageable<RDFAzureTableQuadruple> quadruples = Client.Query<RDFAzureTableQuadruple>(qent => 
                        string.Equals(qent.PartitionKey, "RDFSHARP") && string.Equals(qent.C, contextResource.ToString()));

                    //Execute the remove operation as a set of delete batches
                    foreach (IEnumerable<TableTransactionAction> batch in PrepareDeleteBatch(quadruples.AsEnumerable()))
                    {
                        Response<IReadOnlyList<Response>> transactionResponse = Client.SubmitTransaction(batch);

                        if (transactionResponse.GetRawResponse().IsError)
                            throw new Exception(transactionResponse.ToString());
                    }
                }
                catch (Exception ex)
                {
                    throw new RDFStoreException("Cannot delete batch data from Azure Table store because: " + ex.Message, ex);
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
                try
                {
                    //Fetch entities candidates for deletion
                    Pageable<RDFAzureTableQuadruple> quadruples = Client.Query<RDFAzureTableQuadruple>(qent =>
                        string.Equals(qent.PartitionKey, "RDFSHARP") && string.Equals(qent.S, subjectResource.ToString()));

                    //Execute the remove operation as a set of delete batches
                    foreach (IEnumerable<TableTransactionAction> batch in PrepareDeleteBatch(quadruples.AsEnumerable()))
                    {
                        Response<IReadOnlyList<Response>> transactionResponse = Client.SubmitTransaction(batch);

                        if (transactionResponse.GetRawResponse().IsError)
                            throw new Exception(transactionResponse.ToString());
                    }
                }
                catch (Exception ex)
                {
                    throw new RDFStoreException("Cannot delete batch data from Azure Table store because: " + ex.Message, ex);
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
                try
                {
                    //Fetch entities candidates for deletion
                    Pageable < RDFAzureTableQuadruple > quadruples = Client.Query<RDFAzureTableQuadruple>(qent =>
                        string.Equals(qent.PartitionKey, "RDFSHARP") && string.Equals(qent.P, predicateResource.ToString()));

                    //Execute the remove operation as a set of delete batches
                    foreach (IEnumerable<TableTransactionAction> batch in PrepareDeleteBatch(quadruples.AsEnumerable()))
                    {
                        Response<IReadOnlyList<Response>> transactionResponse = Client.SubmitTransaction(batch);

                        if (transactionResponse.GetRawResponse().IsError)
                            throw new Exception(transactionResponse.ToString());
                    }
                }
                catch (Exception ex)
                {
                    throw new RDFStoreException("Cannot delete batch data from Azure Table store because: " + ex.Message, ex);
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
                try
                {
                    //Fetch entities candidates for deletion
                    Pageable<RDFAzureTableQuadruple> quadruples = Client.Query<RDFAzureTableQuadruple>(qent =>
                        string.Equals(qent.PartitionKey, "RDFSHARP") && string.Equals(qent.O, objectResource.ToString()) && qent.F == (int)RDFModelEnums.RDFTripleFlavors.SPO);

                    //Execute the remove operation as a set of delete batches
                    foreach (IEnumerable<TableTransactionAction> batch in PrepareDeleteBatch(quadruples.AsEnumerable()))
                    {
                        Response<IReadOnlyList<Response>> transactionResponse = Client.SubmitTransaction(batch);

                        if (transactionResponse.GetRawResponse().IsError)
                            throw new Exception(transactionResponse.ToString());
                    }
                }
                catch (Exception ex)
                {
                    throw new RDFStoreException("Cannot delete batch data from Azure Table store because: " + ex.Message, ex);
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
                try
                {
                    //Fetch entities candidates for deletion
                    Pageable<RDFAzureTableQuadruple> quadruples = Client.Query<RDFAzureTableQuadruple>(qent =>
                        string.Equals(qent.PartitionKey, "RDFSHARP") && string.Equals(qent.O, literalObject.ToString()) && qent.F == (int)RDFModelEnums.RDFTripleFlavors.SPL);

                    //Execute the remove operation as a set of delete batches
                    foreach (IEnumerable<TableTransactionAction> batch in PrepareDeleteBatch(quadruples.AsEnumerable()))
                    {
                        Response<IReadOnlyList<Response>> transactionResponse = Client.SubmitTransaction(batch);

                        if (transactionResponse.GetRawResponse().IsError)
                            throw new Exception(transactionResponse.ToString());
                    }
                }
                catch (Exception ex)
                {
                    throw new RDFStoreException("Cannot delete batch data from Azure Table store because: " + ex.Message, ex);
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
                try
                {
                    //Fetch entities candidates for deletion
                    Pageable<RDFAzureTableQuadruple> quadruples = Client.Query<RDFAzureTableQuadruple>(qent =>
                        string.Equals(qent.PartitionKey, "RDFSHARP") && string.Equals(qent.C, contextResource.ToString()) && string.Equals(qent.S, subjectResource.ToString()));

                    //Execute the remove operation as a set of delete batches
                    foreach (IEnumerable<TableTransactionAction> batch in PrepareDeleteBatch(quadruples.AsEnumerable()))
                    {
                        Response<IReadOnlyList<Response>> transactionResponse = Client.SubmitTransaction(batch);

                        if (transactionResponse.GetRawResponse().IsError)
                            throw new Exception(transactionResponse.ToString());
                    }
                }
                catch (Exception ex)
                {
                    throw new RDFStoreException("Cannot delete batch data from Azure Table store because: " + ex.Message, ex);
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
                try
                {
                    //Fetch entities candidates for deletion
                    Pageable<RDFAzureTableQuadruple> quadruples = Client.Query<RDFAzureTableQuadruple>(qent =>
                        string.Equals(qent.PartitionKey, "RDFSHARP") && string.Equals(qent.C, contextResource.ToString()) && string.Equals(qent.P, predicateResource.ToString()));

                    //Execute the remove operation as a set of delete batches
                    foreach (IEnumerable<TableTransactionAction> batch in PrepareDeleteBatch(quadruples.AsEnumerable()))
                    {
                        Response<IReadOnlyList<Response>> transactionResponse = Client.SubmitTransaction(batch);

                        if (transactionResponse.GetRawResponse().IsError)
                            throw new Exception(transactionResponse.ToString());
                    }
                }
                catch (Exception ex)
                {
                    throw new RDFStoreException("Cannot delete batch data from Azure Table store because: " + ex.Message, ex);
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
                try
                {
                    //Fetch entities candidates for deletion
                    Pageable<RDFAzureTableQuadruple> quadruples = Client.Query<RDFAzureTableQuadruple>(qent =>
                        string.Equals(qent.PartitionKey, "RDFSHARP") && string.Equals(qent.C, contextResource.ToString()) && string.Equals(qent.O, objectResource.ToString()) && qent.F == (int)RDFModelEnums.RDFTripleFlavors.SPO);

                    //Execute the remove operation as a set of delete batches
                    foreach (IEnumerable<TableTransactionAction> batch in PrepareDeleteBatch(quadruples.AsEnumerable()))
                    {
                        Response<IReadOnlyList<Response>> transactionResponse = Client.SubmitTransaction(batch);

                        if (transactionResponse.GetRawResponse().IsError)
                            throw new Exception(transactionResponse.ToString());
                    }
                }
                catch (Exception ex)
                {
                    throw new RDFStoreException("Cannot delete batch data from Azure Table store because: " + ex.Message, ex);
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
                try
                {
                    //Fetch entities candidates for deletion
                    Pageable<RDFAzureTableQuadruple> quadruples = Client.Query<RDFAzureTableQuadruple>(qent =>
                        string.Equals(qent.PartitionKey, "RDFSHARP") && string.Equals(qent.C, contextResource.ToString()) && string.Equals(qent.O, objectLiteral.ToString()) && qent.F == (int)RDFModelEnums.RDFTripleFlavors.SPL);

                    //Execute the remove operation as a set of delete batches
                    foreach (IEnumerable<TableTransactionAction> batch in PrepareDeleteBatch(quadruples.AsEnumerable()))
                    {
                        Response<IReadOnlyList<Response>> transactionResponse = Client.SubmitTransaction(batch);

                        if (transactionResponse.GetRawResponse().IsError)
                            throw new Exception(transactionResponse.ToString());
                    }
                }
                catch (Exception ex)
                {
                    throw new RDFStoreException("Cannot delete batch data from Azure Table store because: " + ex.Message, ex);
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
                try
                {
                    //Fetch entities candidates for deletion
                    Pageable<RDFAzureTableQuadruple> quadruples = Client.Query<RDFAzureTableQuadruple>(qent =>
                        string.Equals(qent.PartitionKey, "RDFSHARP") && string.Equals(qent.C, contextResource.ToString()) && string.Equals(qent.S, subjectResource.ToString()) && string.Equals(qent.P, predicateResource.ToString()));

                    //Execute the remove operation as a set of delete batches
                    foreach (IEnumerable<TableTransactionAction> batch in PrepareDeleteBatch(quadruples.AsEnumerable()))
                    {
                        Response<IReadOnlyList<Response>> transactionResponse = Client.SubmitTransaction(batch);

                        if (transactionResponse.GetRawResponse().IsError)
                            throw new Exception(transactionResponse.ToString());
                    }
                }
                catch (Exception ex)
                {
                    throw new RDFStoreException("Cannot delete batch data from Azure Table store because: " + ex.Message, ex);
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
                try
                {
                    //Fetch entities candidates for deletion
                    Pageable<RDFAzureTableQuadruple> quadruples = Client.Query<RDFAzureTableQuadruple>(qent =>
                        string.Equals(qent.PartitionKey, "RDFSHARP") && string.Equals(qent.C, contextResource.ToString()) && string.Equals(qent.S, subjectResource.ToString()) && string.Equals(qent.O, objectResource.ToString()) && qent.F == (int)RDFModelEnums.RDFTripleFlavors.SPO);

                    //Execute the remove operation as a set of delete batches
                    foreach (IEnumerable<TableTransactionAction> batch in PrepareDeleteBatch(quadruples.AsEnumerable()))
                    {
                        Response<IReadOnlyList<Response>> transactionResponse = Client.SubmitTransaction(batch);

                        if (transactionResponse.GetRawResponse().IsError)
                            throw new Exception(transactionResponse.ToString());
                    }
                }
                catch (Exception ex)
                {
                    throw new RDFStoreException("Cannot delete batch data from Azure Table store because: " + ex.Message, ex);
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
                try
                {
                    //Fetch entities candidates for deletion
                    Pageable<RDFAzureTableQuadruple> quadruples = Client.Query<RDFAzureTableQuadruple>(qent =>
                        string.Equals(qent.PartitionKey, "RDFSHARP") && string.Equals(qent.C, contextResource.ToString()) && string.Equals(qent.S, subjectResource.ToString()) && string.Equals(qent.O, objectLiteral.ToString()) && qent.F == (int)RDFModelEnums.RDFTripleFlavors.SPL);

                    //Execute the remove operation as a set of delete batches
                    foreach (IEnumerable<TableTransactionAction> batch in PrepareDeleteBatch(quadruples.AsEnumerable()))
                    {
                        Response<IReadOnlyList<Response>> transactionResponse = Client.SubmitTransaction(batch);

                        if (transactionResponse.GetRawResponse().IsError)
                            throw new Exception(transactionResponse.ToString());
                    }
                }
                catch (Exception ex)
                {
                    throw new RDFStoreException("Cannot delete batch data from Azure Table store because: " + ex.Message, ex);
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
                try
                {
                    //Fetch entities candidates for deletion
                    Pageable<RDFAzureTableQuadruple> quadruples = Client.Query<RDFAzureTableQuadruple>(qent =>
                        string.Equals(qent.PartitionKey, "RDFSHARP") && string.Equals(qent.C, contextResource.ToString()) && string.Equals(qent.P, predicateResource.ToString()) && string.Equals(qent.O, objectResource.ToString()) && qent.F == (int)RDFModelEnums.RDFTripleFlavors.SPO);

                    //Execute the remove operation as a set of delete batches
                    foreach (IEnumerable<TableTransactionAction> batch in PrepareDeleteBatch(quadruples.AsEnumerable()))
                    {
                        Response<IReadOnlyList<Response>> transactionResponse = Client.SubmitTransaction(batch);

                        if (transactionResponse.GetRawResponse().IsError)
                            throw new Exception(transactionResponse.ToString());
                    }
                }
                catch (Exception ex)
                {
                    throw new RDFStoreException("Cannot delete batch data from Azure Table store because: " + ex.Message, ex);
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
                try
                {
                    //Fetch entities candidates for deletion
                    Pageable<RDFAzureTableQuadruple> quadruples = Client.Query<RDFAzureTableQuadruple>(qent =>
                        string.Equals(qent.PartitionKey, "RDFSHARP") && string.Equals(qent.C, contextResource.ToString()) && string.Equals(qent.P, predicateResource.ToString()) && string.Equals(qent.O, objectLiteral.ToString()) && qent.F == (int)RDFModelEnums.RDFTripleFlavors.SPL);

                    //Execute the remove operation as a set of delete batches
                    foreach (IEnumerable<TableTransactionAction> batch in PrepareDeleteBatch(quadruples.AsEnumerable()))
                    {
                        Response<IReadOnlyList<Response>> transactionResponse = Client.SubmitTransaction(batch);

                        if (transactionResponse.GetRawResponse().IsError)
                            throw new Exception(transactionResponse.ToString());
                    }
                }
                catch (Exception ex)
                {
                    throw new RDFStoreException("Cannot delete batch data from Azure Table store because: " + ex.Message, ex);
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
                try
                {
                    //Fetch entities candidates for deletion
                    Pageable<RDFAzureTableQuadruple> quadruples = Client.Query<RDFAzureTableQuadruple>(qent =>
                        string.Equals(qent.PartitionKey, "RDFSHARP") && string.Equals(qent.S, subjectResource.ToString()) && string.Equals(qent.P, predicateResource.ToString()));

                    //Execute the remove operation as a set of delete batches
                    foreach (IEnumerable<TableTransactionAction> batch in PrepareDeleteBatch(quadruples.AsEnumerable()))
                    {
                        Response<IReadOnlyList<Response>> transactionResponse = Client.SubmitTransaction(batch);

                        if (transactionResponse.GetRawResponse().IsError)
                            throw new Exception(transactionResponse.ToString());
                    }
                }
                catch (Exception ex)
                {
                    throw new RDFStoreException("Cannot delete batch data from Azure Table store because: " + ex.Message, ex);
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
                try
                {
                    //Fetch entities candidates for deletion
                    Pageable<RDFAzureTableQuadruple> quadruples = Client.Query<RDFAzureTableQuadruple>(qent =>
                        string.Equals(qent.PartitionKey, "RDFSHARP") && string.Equals(qent.S, subjectResource.ToString()) && string.Equals(qent.O, objectResource.ToString()) && qent.F == (int)RDFModelEnums.RDFTripleFlavors.SPO);

                    //Execute the remove operation as a set of delete batches
                    foreach (IEnumerable<TableTransactionAction> batch in PrepareDeleteBatch(quadruples.AsEnumerable()))
                    {
                        Response<IReadOnlyList<Response>> transactionResponse = Client.SubmitTransaction(batch);

                        if (transactionResponse.GetRawResponse().IsError)
                            throw new Exception(transactionResponse.ToString());
                    }
                }
                catch (Exception ex)
                {
                    throw new RDFStoreException("Cannot delete batch data from Azure Table store because: " + ex.Message, ex);
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
                try
                {
                    //Fetch entities candidates for deletion
                    Pageable<RDFAzureTableQuadruple> quadruples = Client.Query<RDFAzureTableQuadruple>(qent =>
                        string.Equals(qent.PartitionKey, "RDFSHARP") && string.Equals(qent.S, subjectResource.ToString()) && string.Equals(qent.O, objectLiteral.ToString()) && qent.F == (int)RDFModelEnums.RDFTripleFlavors.SPL);

                    //Execute the remove operation as a set of delete batches
                    foreach (IEnumerable<TableTransactionAction> batch in PrepareDeleteBatch(quadruples.AsEnumerable()))
                    {
                        Response<IReadOnlyList<Response>> transactionResponse = Client.SubmitTransaction(batch);

                        if (transactionResponse.GetRawResponse().IsError)
                            throw new Exception(transactionResponse.ToString());
                    }
                }
                catch (Exception ex)
                {
                    throw new RDFStoreException("Cannot delete batch data from Azure Table store because: " + ex.Message, ex);
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
                try
                {
                    //Fetch entities candidates for deletion
                    Pageable<RDFAzureTableQuadruple> quadruples = Client.Query<RDFAzureTableQuadruple>(qent =>
                        string.Equals(qent.PartitionKey, "RDFSHARP") && string.Equals(qent.P, predicateResource.ToString()) && string.Equals(qent.O, objectResource.ToString()) && qent.F == (int)RDFModelEnums.RDFTripleFlavors.SPO);

                    //Execute the remove operation as a set of delete batches
                    foreach (IEnumerable<TableTransactionAction> batch in PrepareDeleteBatch(quadruples.AsEnumerable()))
                    {
                        Response<IReadOnlyList<Response>> transactionResponse = Client.SubmitTransaction(batch);

                        if (transactionResponse.GetRawResponse().IsError)
                            throw new Exception(transactionResponse.ToString());
                    }
                }
                catch (Exception ex)
                {
                    throw new RDFStoreException("Cannot delete batch data from Azure Table store because: " + ex.Message, ex);
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
                try
                {
                    //Fetch entities candidates for deletion
                    Pageable<RDFAzureTableQuadruple> quadruples = Client.Query<RDFAzureTableQuadruple>(qent =>
                        string.Equals(qent.PartitionKey, "RDFSHARP") && string.Equals(qent.P, predicateResource.ToString()) && string.Equals(qent.O, objectLiteral.ToString()) && qent.F == (int)RDFModelEnums.RDFTripleFlavors.SPL);

                    //Execute the remove operation as a set of delete batches
                    foreach (IEnumerable<TableTransactionAction> batch in PrepareDeleteBatch(quadruples.AsEnumerable()))
                    {
                        Response<IReadOnlyList<Response>> transactionResponse = Client.SubmitTransaction(batch);

                        if (transactionResponse.GetRawResponse().IsError)
                            throw new Exception(transactionResponse.ToString());
                    }
                }
                catch (Exception ex)
                {
                    throw new RDFStoreException("Cannot delete batch data from Azure Table store because: " + ex.Message, ex);
                }
            }
            return this;
        }

        /// <summary>
        /// Clears the quadruples of the store
        /// </summary>
        public override void ClearQuadruples()
        {
            try
            {
                //Fetch entities candidates for deletion
                Pageable<RDFAzureTableQuadruple> quadruples = Client.Query<RDFAzureTableQuadruple>(qent =>
                    string.Equals(qent.PartitionKey, "RDFSHARP"));

                //Execute the remove operation as a set of delete batches
                foreach (IEnumerable<TableTransactionAction> batch in PrepareDeleteBatch(quadruples.AsEnumerable()))
                {
                    Response<IReadOnlyList<Response>> transactionResponse = Client.SubmitTransaction(batch);

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
        {
            //Guard against tricky input
            if (quadruple == null)
                return false;

            try
            {
                NullableResponse<RDFAzureTableQuadruple> response = Client.GetEntityIfExists<RDFAzureTableQuadruple>("RDFSHARP", quadruple.QuadrupleID.ToString());
                return response.HasValue;
            }
            catch (Exception ex)
            {
                throw new RDFStoreException("Cannot read data from Azure Table store because: " + ex.Message, ex);
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

            try
            {
                //Intersect the filters
                Pageable<RDFAzureTableQuadruple> quadruples;
                switch (queryFilters.ToString())
                {
                    case "C":
                        //C->->->
                        quadruples = Client.Query<RDFAzureTableQuadruple>(qent =>
                            string.Equals(qent.PartitionKey, "RDFSHARP") && string.Equals(qent.C, ctx.ToString()));
                        break;
                    case "S":
                        //->S->->
                        quadruples = Client.Query<RDFAzureTableQuadruple>(qent =>
                            string.Equals(qent.PartitionKey, "RDFSHARP") && string.Equals(qent.S, subj.ToString()));
                        break;
                    case "P":
                        //->->P->
                        quadruples = Client.Query<RDFAzureTableQuadruple>(qent =>
                            string.Equals(qent.PartitionKey, "RDFSHARP") && string.Equals(qent.P, pred.ToString()));
                        break;
                    case "O":
                        //->->->O
                        quadruples = Client.Query<RDFAzureTableQuadruple>(qent =>
                            string.Equals(qent.PartitionKey, "RDFSHARP") && string.Equals(qent.O, obj.ToString()) && qent.F == (int)RDFModelEnums.RDFTripleFlavors.SPO);
                        break;
                    case "L":
                        //->->->L
                        quadruples = Client.Query<RDFAzureTableQuadruple>(qent =>
                            string.Equals(qent.PartitionKey, "RDFSHARP") && string.Equals(qent.O, lit.ToString()) && qent.F == (int)RDFModelEnums.RDFTripleFlavors.SPL);
                        break;
                    case "CS":
                        //C->S->->
                        quadruples = Client.Query<RDFAzureTableQuadruple>(qent =>
                            string.Equals(qent.PartitionKey, "RDFSHARP") && string.Equals(qent.C, ctx.ToString()) && string.Equals(qent.S, subj.ToString()));
                        break;
                    case "CP":
                        //C->->P->
                        quadruples = Client.Query<RDFAzureTableQuadruple>(qent =>
                            string.Equals(qent.PartitionKey, "RDFSHARP") && string.Equals(qent.C, ctx.ToString()) && string.Equals(qent.P, pred.ToString()));
                        break;
                    case "CO":
                        //C->->->O
                        quadruples = Client.Query<RDFAzureTableQuadruple>(qent =>
                            string.Equals(qent.PartitionKey, "RDFSHARP") && string.Equals(qent.C, ctx.ToString()) && string.Equals(qent.O, obj.ToString()) && qent.F == (int)RDFModelEnums.RDFTripleFlavors.SPO);
                        break;
                    case "CL":
                        //C->->->L
                        quadruples = Client.Query<RDFAzureTableQuadruple>(qent =>
                            string.Equals(qent.PartitionKey, "RDFSHARP") && string.Equals(qent.C, ctx.ToString()) && string.Equals(qent.O, lit.ToString()) && qent.F == (int)RDFModelEnums.RDFTripleFlavors.SPL);
                        break;
                    case "CSP":
                        //C->S->P->
                        quadruples = Client.Query<RDFAzureTableQuadruple>(qent =>
                            string.Equals(qent.PartitionKey, "RDFSHARP") && string.Equals(qent.C, ctx.ToString()) && string.Equals(qent.S, subj.ToString()) && string.Equals(qent.P, pred.ToString()));
                        break;
                    case "CSO":
                        //C->S->->O
                        quadruples = Client.Query<RDFAzureTableQuadruple>(qent =>
                            string.Equals(qent.PartitionKey, "RDFSHARP") && string.Equals(qent.C, ctx.ToString()) && string.Equals(qent.S, subj.ToString()) && string.Equals(qent.O, obj.ToString()) && qent.F == (int)RDFModelEnums.RDFTripleFlavors.SPO);
                        break;
                    case "CSL":
                        //C->S->->L
                        quadruples = Client.Query<RDFAzureTableQuadruple>(qent =>
                            string.Equals(qent.PartitionKey, "RDFSHARP") && string.Equals(qent.C, ctx.ToString()) && string.Equals(qent.S, subj.ToString()) && string.Equals(qent.O, lit.ToString()) && qent.F == (int)RDFModelEnums.RDFTripleFlavors.SPL);
                        break;
                    case "CPO":
                        //C->->P->O
                        quadruples = Client.Query<RDFAzureTableQuadruple>(qent =>
                            string.Equals(qent.PartitionKey, "RDFSHARP") && string.Equals(qent.C, ctx.ToString()) && string.Equals(qent.P, pred.ToString()) && string.Equals(qent.O, obj.ToString()) && qent.F == (int)RDFModelEnums.RDFTripleFlavors.SPO);
                        break;
                    case "CPL":
                        //C->->P->L
                        quadruples = Client.Query<RDFAzureTableQuadruple>(qent =>
                            string.Equals(qent.PartitionKey, "RDFSHARP") && string.Equals(qent.C, ctx.ToString()) && string.Equals(qent.P, pred.ToString()) && string.Equals(qent.O, lit.ToString()) && qent.F == (int)RDFModelEnums.RDFTripleFlavors.SPL);
                        break;
                    case "CSPO":
                        //C->S->P->O
                        quadruples = Client.Query<RDFAzureTableQuadruple>(qent =>
                            string.Equals(qent.PartitionKey, "RDFSHARP") && string.Equals(qent.C, ctx.ToString()) && string.Equals(qent.S, subj.ToString()) && string.Equals(qent.P, pred.ToString()) && string.Equals(qent.O, obj.ToString()) && qent.F == (int)RDFModelEnums.RDFTripleFlavors.SPO);
                        break;
                    case "CSPL":
                        //C->S->P->L
                        quadruples = Client.Query<RDFAzureTableQuadruple>(qent =>
                            string.Equals(qent.PartitionKey, "RDFSHARP") && string.Equals(qent.C, ctx.ToString()) && string.Equals(qent.S, subj.ToString()) && string.Equals(qent.P, pred.ToString()) && string.Equals(qent.O, lit.ToString()) && qent.F == (int)RDFModelEnums.RDFTripleFlavors.SPL);
                        break;
                    case "SP":
                        //->S->P->
                        quadruples = Client.Query<RDFAzureTableQuadruple>(qent =>
                            string.Equals(qent.PartitionKey, "RDFSHARP") && string.Equals(qent.S, subj.ToString()) && string.Equals(qent.P, pred.ToString()));
                        break;
                    case "SO":
                        //->S->->O
                        quadruples = Client.Query<RDFAzureTableQuadruple>(qent =>
                            string.Equals(qent.PartitionKey, "RDFSHARP") && string.Equals(qent.S, subj.ToString()) && string.Equals(qent.O, obj.ToString()) && qent.F == (int)RDFModelEnums.RDFTripleFlavors.SPO);
                        break;
                    case "SL":
                        //->S->->L
                        quadruples = Client.Query<RDFAzureTableQuadruple>(qent =>
                            string.Equals(qent.PartitionKey, "RDFSHARP") && string.Equals(qent.S, subj.ToString()) && string.Equals(qent.O, lit.ToString()) && qent.F == (int)RDFModelEnums.RDFTripleFlavors.SPL);
                        break;
                    case "PO":
                        //->->P->O
                        quadruples = Client.Query<RDFAzureTableQuadruple>(qent =>
                            string.Equals(qent.PartitionKey, "RDFSHARP") && string.Equals(qent.P, pred.ToString()) && string.Equals(qent.O, obj.ToString()) && qent.F == (int)RDFModelEnums.RDFTripleFlavors.SPO);
                        break;
                    case "PL":
                        //->->P->L
                        quadruples = Client.Query<RDFAzureTableQuadruple>(qent =>
                            string.Equals(qent.PartitionKey, "RDFSHARP") && string.Equals(qent.P, pred.ToString()) && string.Equals(qent.O, lit.ToString()) && qent.F == (int)RDFModelEnums.RDFTripleFlavors.SPL);
                        break;
                    case "SPO":
                        //->S->P->O
                        quadruples = Client.Query<RDFAzureTableQuadruple>(qent =>
                            string.Equals(qent.PartitionKey, "RDFSHARP") && string.Equals(qent.S, subj.ToString()) && string.Equals(qent.P, pred.ToString()) && string.Equals(qent.O, obj.ToString()) && qent.F == (int)RDFModelEnums.RDFTripleFlavors.SPO);
                        break;
                    case "SPL":
                        //->S->P->L
                        quadruples = Client.Query<RDFAzureTableQuadruple>(qent =>
                            string.Equals(qent.PartitionKey, "RDFSHARP") && string.Equals(qent.S, subj.ToString()) && string.Equals(qent.P, pred.ToString()) && string.Equals(qent.O, lit.ToString()) && qent.F == (int)RDFModelEnums.RDFTripleFlavors.SPL);
                        break;
                    default:
                        //->->->
                        quadruples = Client.Query<RDFAzureTableQuadruple>(qent =>
                            string.Equals(qent.PartitionKey, "RDFSHARP"));
                        break;
                }

                //Transform fetched query entities into quadruples
                foreach (RDFAzureTableQuadruple quadruple in quadruples.AsEnumerable())
                {
                    RDFContext qContext = new RDFContext(quadruple.C);
                    RDFPatternMember qSubject = RDFQueryUtilities.ParseRDFPatternMember(quadruple.S);
                    RDFPatternMember qPredicate = RDFQueryUtilities.ParseRDFPatternMember(quadruple.P);
                    RDFPatternMember qObject = RDFQueryUtilities.ParseRDFPatternMember(quadruple.O);
                    result.AddQuadruple(quadruple.F == (int)RDFModelEnums.RDFTripleFlavors.SPO
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
        /// Counts the Azure Table service quadruples
        /// </summary>
        private long GetQuadruplesCount()
        {
            try
            {
                Pageable<RDFAzureTableQuadruple> quadruples = Client.Query<RDFAzureTableQuadruple>(qent =>
                    string.Equals(qent.PartitionKey, "RDFSHARP"), select: SelectColumns);

                //Return the quadruples count
                return quadruples.LongCount();
            }
            catch
            {
                //Return the quadruples count (-1 to indicate error)
                return -1;
            }
        }

        /// <summary>
        /// Asynchronously counts the Azure Table service quadruples
        /// </summary>
        private async Task<long> GetQuadruplesCountAsync()
        {
            try
            {
                AsyncPageable<RDFAzureTableQuadruple> quadruples = Client.QueryAsync<RDFAzureTableQuadruple>(qent =>
                    string.Equals(qent.PartitionKey, "RDFSHARP"), select: SelectColumns);

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