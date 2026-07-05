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

using MongoDB.Bson.Serialization.Attributes;
using RDFSharp.Store;

namespace RDFSharp.Extensions.MongoDB
{
    /// <summary>
    /// Represents an RDFQuadruple stored in a MongoDB collection
    /// </summary>
    public sealed class RDFMongoDBQuadruple
    {
        #region Properties
        /// <summary>
        /// Unique identifier of the quadruple (also used as MongoDB's _id)
        /// </summary>
        [BsonId]
        public long QuadrupleID { get; set; }

        /// <summary>
        /// Flavor of the quadruple (SPO=1, SPL=2)
        /// </summary>
        [BsonElement("flavor")]
        public int TripleFlavor { get; set; }

        /// <summary>
        /// Context of the quadruple
        /// </summary>
        [BsonElement("context")]
        public string Context { get; set; }

        /// <summary>
        /// Subject of the quadruple
        /// </summary>
        [BsonElement("subject")]
        public string Subject { get; set; }

        /// <summary>
        /// Predicate of the quadruple
        /// </summary>
        [BsonElement("predicate")]
        public string Predicate { get; set; }

        /// <summary>
        /// Object/Literal of the quadruple
        /// </summary>
        [BsonElement("object")]
        public string Object { get; set; }
        #endregion

        #region Ctors
        /// <summary>
        /// Builds an empty document
        /// </summary>
        public RDFMongoDBQuadruple() { }

        /// <summary>
        /// Builds a document from the given quadruple
        /// </summary>
        public RDFMongoDBQuadruple(RDFQuadruple quadruple)
        {
            #region Guards
            if (quadruple == null)
                throw new RDFStoreException("Cannot create MongoDB quadruple because given \"quadruple\" parameter is null");
            #endregion

            QuadrupleID = quadruple.QuadrupleID;
            TripleFlavor = (int)quadruple.TripleFlavor;
            Context = quadruple.Context.ToString();
            Subject = quadruple.Subject.ToString();
            Predicate = quadruple.Predicate.ToString();
            Object = quadruple.Object.ToString();
        }
        #endregion
    }
}