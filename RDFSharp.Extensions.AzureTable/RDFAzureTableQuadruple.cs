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
    internal class RDFAzureTableQuadruple : ITableEntity
    {
        #region Properties

        //ITableEntity
        public string PartitionKey { get; set; }
        public string RowKey { get; set; }
        public DateTimeOffset? Timestamp { get; set; }
        public ETag ETag { get; set; }

        public string Context { get; set; }
        public string Subject { get; set; }
        public string Predicate { get; set; }
        public string Object { get; set; }
        public int Flavor { get; set; }

        #endregion

        #region Ctors

        internal RDFAzureTableQuadruple(RDFQuadruple quadruple)
        {
            //ITableEntity
            this.PartitionKey = "RDFSHARP";
            this.RowKey = quadruple.QuadrupleID.ToString();

            this.Context = quadruple.Context.ToString();
            this.Subject = quadruple.Subject.ToString();
            this.Predicate = quadruple.Predicate.ToString();
            this.Object = quadruple.Object.ToString();
            this.Flavor = (int)quadruple.TripleFlavor;
        }

        #endregion
    }
}