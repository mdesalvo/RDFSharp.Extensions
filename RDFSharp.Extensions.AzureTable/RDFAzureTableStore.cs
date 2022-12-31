/*
   Copyright 2012-2022 Marco De Salvo

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
using System.Linq;
using System.Transactions;

namespace RDFSharp.Extensions.AzureTable
{
    /// <summary>
    /// RDFAzureTableStore represents a RDFStore backed on Azure Table service
    /// </summary>
    public class RDFAzureTableStore : RDFStore, IDisposable
    {
        #region Properties
        internal TableServiceClient ServiceClient { get; set; }
        internal TableClient Client { get; set; }
        internal bool Disposed { get; set; }
        #endregion

        #region Ctors
        /// <summary>
        /// Default-ctor to build a local emulator Azure Table store instance
        /// </summary>
        public RDFAzureTableStore() : this("DefaultEndpointsProtocol=http;AccountName=devstoreaccount1;AccountKey=Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==;BlobEndpoint=http://127.0.0.1:10000/devstoreaccount1;QueueEndpoint=http://127.0.0.1:10001/devstoreaccount1;TableEndpoint=http://127.0.0.1:10002/devstoreaccount1;") { }

        /// <summary>
        /// Default-ctor to build a Azure Table store instance
        /// </summary>
        public RDFAzureTableStore(string azureStorageConnectionString)
        {
            //Guard against tricky paths
            if (string.IsNullOrEmpty(azureStorageConnectionString))
            	throw new RDFStoreException("Cannot connect to Azure Table store because: given \"azureStorageConnectionString\" parameter is null or empty.");

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
        protected virtual void Dispose(bool disposing)
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
                    //Execute the merge operation as a set of upsert batches of 100 items
                    foreach (IEnumerable<TableTransactionAction> batch in PrepareBatch(graph, 100))
                    {
                        Response<IReadOnlyList<Response>> transactionResponse = Client.SubmitTransaction(batch);

                        //In case of error we have to signal
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

                    //In case of error we have to signal
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
        /// Removes the given quadruples from the store
        /// </summary>
        public override RDFStore RemoveQuadruple(RDFQuadruple quadruple)
        {
            if (quadruple != null)
            {
                //TODO
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
                //TODO
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
                //TODO
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
                //TODO
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
                //TODO
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
                //TODO
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
                //TODO
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
                //TODO
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
                //TODO
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
                //TODO
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
                //TODO
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
                //TODO
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
                //TODO
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
                //TODO
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
                //TODO
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
                //TODO
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
                //TODO
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
                //TODO
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
                //TODO
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
                //TODO
            }
            return this;
        }

        /// <summary>
        /// Clears the quadruples of the store
        /// </summary>
        public override void ClearQuadruples()
        {
            //TODO
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

            //TODO
            return false;
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

            //TODO

            return result;
        }
        #endregion

        #endregion

        #region Utilities

        /// <summary>
        /// Chunks the triples of the given graph into upsert batches of 100 Azure Table entities
        /// </summary>
        private static IEnumerable<IEnumerable<TableTransactionAction>> PrepareBatch(RDFGraph graph, int batchSize=100)
        {
            RDFContext graphContext = new RDFContext(graph.Context);

            List<TableTransactionAction> batch = new List<TableTransactionAction>(batchSize);
            foreach (RDFTriple triple in graph)
            {
                RDFQuadruple quadruple = triple.TripleFlavor == RDFModelEnums.RDFTripleFlavors.SPO
                    ? new RDFQuadruple(graphContext, (RDFResource)triple.Subject, (RDFResource)triple.Predicate, (RDFResource)triple.Object)
                    : new RDFQuadruple(graphContext, (RDFResource)triple.Subject, (RDFResource)triple.Predicate, (RDFLiteral)triple.Object);
                
                batch.Add(new TableTransactionAction(TableTransactionActionType.UpdateReplace, new RDFAzureTableQuadruple(quadruple)));
                if (batch.Count == batchSize)
                {
                    yield return batch;
                    batch = new List<TableTransactionAction>(batchSize);
                }
            }

            if (batch.Count > 0)
                yield return batch;
        }

        #endregion
    }
}