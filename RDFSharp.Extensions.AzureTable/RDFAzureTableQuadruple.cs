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

using Azure;
using Azure.Data.Tables;
using RDFSharp.Store;
using System;

namespace RDFSharp.Extensions.AzureTable
{
    /// <summary>
    /// Represents an RDFQuadruple stored in Azure Table service
    /// </summary>
    public class RDFAzureTableQuadruple : ITableEntity
    {
        #region Properties

        /// <summary>
        /// PartitionKey
        /// </summary>
        public string PartitionKey { get; set; } = "RDFSHARP";

        /// <summary>
        /// RowKey
        /// </summary>
        public string RowKey { get; set; }

        /// <summary>
        /// Timestamp
        /// </summary>
        public DateTimeOffset? Timestamp { get; set; }

        /// <summary>
        /// ETag
        /// </summary>
        public ETag ETag { get; set; }

        /// <summary>
        /// Context of the quadruple
        /// </summary>
        public string Context { get; set; }

        /// <summary>
        /// Subject of the quadruple
        /// </summary>
        public string Subject { get; set; }

        /// <summary>
        /// Predicate of the quadruple
        /// </summary>
        public string Predicate { get; set; }

        /// <summary>
        /// Object of the quadruple
        /// </summary>
        public string Object { get; set; }

        /// <summary>
        /// Flavor of the quadruple (SPO=1, SPL=2)
        /// </summary>
        public int Flavor { get; set; }

        #endregion

        #region Ctors

        /// <summary>
        /// Builds an empty entity
        /// </summary>
        public RDFAzureTableQuadruple() { }

        /// <summary>
        /// Builds an entity from the given quadruple
        /// </summary>
        public RDFAzureTableQuadruple(RDFQuadruple quadruple)
        {
            RowKey = quadruple.QuadrupleID.ToString();
            Context = quadruple.Context.ToString();
            Subject = quadruple.Subject.ToString();
            Predicate = quadruple.Predicate.ToString();
            Object = quadruple.Object.ToString();
            Flavor = (int)quadruple.TripleFlavor;
        }

        #endregion
    }
}