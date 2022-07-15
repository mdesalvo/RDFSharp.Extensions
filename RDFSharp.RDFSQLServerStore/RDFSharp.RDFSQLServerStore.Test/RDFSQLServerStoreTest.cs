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

using Microsoft.VisualStudio.TestTools.UnitTesting;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using RDFSharp.Model;

namespace RDFSharp.Store.Test
{
    [TestClass]
    public class RDFSQLServerStoreTest
    {
        //This test suite is based on a local installation of SQLServer Express using Windows authentication
        private string GetConnectionString(string database)
            => $"Server=.\\SQLEXPRESS;Database={database};Trusted_Connection=True;";

        #region Tests        
        [TestMethod]
        public void ShouldCreateStore()
        {
            RDFSQLServerStore store = new RDFSQLServerStore(
                GetConnectionString("RDFSQLServerStoreTest_ShouldCreateStore"));

            Assert.IsNotNull(store);
            Assert.IsTrue(string.Equals(store.StoreType, "SQLServer"));
            Assert.IsTrue(store.StoreID.Equals(RDFModelUtilities.CreateHash(store.ToString())));
            Assert.IsTrue(string.Equals(store.ToString(), string.Concat("SQLServer|SERVER=", store.Connection.DataSource, ";DATABASE=", store.Connection.Database)));
        }

        [TestMethod]
        public void ShouldThrowExceptionOnCreatingStoreBecauseNullOrEmptyPath()
            => Assert.ThrowsException<RDFStoreException>(() => new RDFSQLServerStore(null));

        [TestMethod]
        public void ShouldThrowExceptionOnCreatingStoreBecauseUnaccessiblePath()
            => Assert.ThrowsException<RDFStoreException>(() => new RDFSQLServerStore(GetConnectionString("http://example.org/file")));

        [TestMethod]
        public void ShouldCreateStoreUsingDispose()
        {
            RDFSQLServerStore store = default;
            using(store = new RDFSQLServerStore(GetConnectionString("RDFSQLServerStoreTest_ShouldCreateStoreUsingDispose")))
            {
                Assert.IsNotNull(store);
                Assert.IsTrue(string.Equals(store.StoreType, "SQLServer"));
                Assert.IsTrue(store.StoreID.Equals(RDFModelUtilities.CreateHash(store.ToString())));
                Assert.IsTrue(string.Equals(store.ToString(), string.Concat("SQLServer|SERVER=", store.Connection.DataSource, ";DATABASE=", store.Connection.Database)));
                Assert.IsFalse(store.Disposed);
            }

            Assert.IsTrue(store.Disposed);
            Assert.IsNull(store.Connection);
        }

        [TestMethod]
        public void ShouldCreateStoreInvokingDispose()
        {
            RDFSQLServerStore store = new RDFSQLServerStore(GetConnectionString("RDFSQLServerStoreTest_ShouldCreateStoreInvokingDispose"));

            Assert.IsNotNull(store);
            Assert.IsTrue(string.Equals(store.StoreType, "SQLServer"));
            Assert.IsTrue(store.StoreID.Equals(RDFModelUtilities.CreateHash(store.ToString())));
            Assert.IsTrue(string.Equals(store.ToString(), string.Concat("SQLServer|SERVER=", store.Connection.DataSource, ";DATABASE=", store.Connection.Database)));
            Assert.IsFalse(store.Disposed);

            store.Dispose();
            Assert.IsTrue(store.Disposed);
            Assert.IsNull(store.Connection);
        }

        [TestMethod]
        public void ShouldAddQuadruple()
        {
            RDFQuadruple quadruple = new RDFQuadruple(new RDFContext(new Uri("ex:ctx")), new RDFResource("ex:subj"), new RDFResource("ex:pred"), new RDFResource("ex:obj"));

            RDFSQLServerStore store = new RDFSQLServerStore(GetConnectionString("RDFSQLServerStoreTest_ShouldAddQuadruple"));
            store.AddQuadruple(quadruple);
            store.AddQuadruple(quadruple); //Will not be inserted, since duplicate quadruples are not allowed
            store.AddQuadruple(null); //Will not be inserted, since null quadruples are not allowed

            RDFMemoryStore memStore = store.SelectAllQuadruples();

            Assert.IsNotNull(memStore);
            Assert.IsTrue(memStore.QuadruplesCount == 1);
            Assert.IsTrue(memStore.Single().Equals(quadruple));
        }

        [TestMethod]
        public void ShouldMergeGraph()
        {
            RDFGraph graph = new RDFGraph(new List<RDFTriple>() {
                new RDFTriple(new RDFResource("ex:subj"), new RDFResource("ex:pred"), new RDFResource("ex:obj"))
            }).SetContext(new Uri("ex:ctx"));

            RDFSQLServerStore store = new RDFSQLServerStore(GetConnectionString("RDFSQLServerStoreTest_ShouldMergeGraph"));
            store.MergeGraph(graph);
            store.MergeGraph(null); //Will not be merged, since null graphs are not allowed

            RDFMemoryStore memStore = store.SelectAllQuadruples();

            Assert.IsNotNull(memStore);
            Assert.IsTrue(memStore.QuadruplesCount == 1);
            Assert.IsTrue(memStore.Single().Equals(new RDFQuadruple(new RDFContext("ex:ctx"), new RDFResource("ex:subj"), new RDFResource("ex:pred"), new RDFResource("ex:obj"))));
        }

        [TestMethod]
        public void ShouldRemoveQuadruple()
        {
            RDFQuadruple quadruple = new RDFQuadruple(new RDFContext(new Uri("ex:ctx")), new RDFResource("ex:subj"), new RDFResource("ex:pred"), new RDFResource("ex:obj"));

            RDFSQLServerStore store = new RDFSQLServerStore(GetConnectionString("RDFSQLServerStoreTest_ShouldRemoveQuadruple"));
            store.AddQuadruple(quadruple);
            store.RemoveQuadruple(quadruple);
            store.RemoveQuadruple(null); //Will not be removed, since null quadruples are not allowed

            RDFMemoryStore memStore = store.SelectAllQuadruples();

            Assert.IsNotNull(memStore);
            Assert.IsTrue(memStore.QuadruplesCount == 0);
        }

        [TestMethod]
        public void ShouldNotRemoveQuadruple()
        {
            RDFQuadruple quadruple = new RDFQuadruple(new RDFContext(new Uri("ex:ctx")), new RDFResource("ex:subj"), new RDFResource("ex:pred"), new RDFResource("ex:obj"));
            RDFQuadruple quadruple2 = new RDFQuadruple(new RDFContext(new Uri("ex:ctx2")), new RDFResource("ex:subj"), new RDFResource("ex:pred"), new RDFResource("ex:obj"));

            RDFSQLServerStore store = new RDFSQLServerStore(GetConnectionString("RDFSQLServerStoreTest_ShouldNotRemoveQuadruple"));
            store.AddQuadruple(quadruple);
            store.RemoveQuadruple(quadruple2);

            RDFMemoryStore memStore = store.SelectAllQuadruples();

            Assert.IsNotNull(memStore);
            Assert.IsTrue(memStore.QuadruplesCount == 1);
            Assert.IsTrue(memStore.Single().Equals(quadruple));
        }

        [TestMethod]
        public void ShouldRemoveQuadruplesByContext()
        {
            RDFQuadruple quadruple = new RDFQuadruple(new RDFContext(new Uri("ex:ctx")), new RDFResource("ex:subj"), new RDFResource("ex:pred"), new RDFResource("ex:obj"));

            RDFSQLServerStore store = new RDFSQLServerStore(GetConnectionString("RDFSQLServerStoreTest_ShouldRemoveQuadruplesByContext"));
            store.AddQuadruple(quadruple);
            store.RemoveQuadruplesByContext(new RDFContext(new Uri("ex:ctx")));
            store.RemoveQuadruplesByContext(null); //Will not be removed, since null contexts are not allowed

            RDFMemoryStore memStore = store.SelectAllQuadruples();

            Assert.IsNotNull(memStore);
            Assert.IsTrue(memStore.QuadruplesCount == 0);
        }

        [TestMethod]
        public void ShouldNotRemoveQuadruplesByContext()
        {
            RDFQuadruple quadruple = new RDFQuadruple(new RDFContext(new Uri("ex:ctx")), new RDFResource("ex:subj"), new RDFResource("ex:pred"), new RDFResource("ex:obj"));

            RDFSQLServerStore store = new RDFSQLServerStore(GetConnectionString("RDFSQLServerStoreTest_ShouldNotRemoveQuadruplesByContext"));
            store.AddQuadruple(quadruple);
            store.RemoveQuadruplesByContext(new RDFContext(new Uri("ex:ctx2")));

            RDFMemoryStore memStore = store.SelectAllQuadruples();

            Assert.IsNotNull(memStore);
            Assert.IsTrue(memStore.QuadruplesCount == 1);
            Assert.IsTrue(memStore.Single().Equals(quadruple));
        }

        [TestMethod]
        public void ShouldRemoveQuadruplesBySubject()
        {
            RDFQuadruple quadruple = new RDFQuadruple(new RDFContext(new Uri("ex:ctx")), new RDFResource("ex:subj"), new RDFResource("ex:pred"), new RDFResource("ex:obj"));

            RDFSQLServerStore store = new RDFSQLServerStore(GetConnectionString("RDFSQLServerStoreTest_ShouldRemoveQuadruplesBySubject"));
            store.AddQuadruple(quadruple);
            store.RemoveQuadruplesBySubject(new RDFResource("ex:subj"));
            store.RemoveQuadruplesBySubject(null); //Will not be removed, since null subjects are not allowed

            RDFMemoryStore memStore = store.SelectAllQuadruples();

            Assert.IsNotNull(memStore);
            Assert.IsTrue(memStore.QuadruplesCount == 0);
        }

        [TestMethod]
        public void ShouldNotRemoveQuadruplesBySubject()
        {
            RDFQuadruple quadruple = new RDFQuadruple(new RDFContext(new Uri("ex:ctx")), new RDFResource("ex:subj"), new RDFResource("ex:pred"), new RDFResource("ex:obj"));

            RDFSQLServerStore store = new RDFSQLServerStore(GetConnectionString("RDFSQLServerStoreTest_ShouldNotRemoveQuadruplesBySubject"));
            store.AddQuadruple(quadruple);
            store.RemoveQuadruplesBySubject(new RDFResource("ex:subj2"));

            RDFMemoryStore memStore = store.SelectAllQuadruples();

            Assert.IsNotNull(memStore);
            Assert.IsTrue(memStore.QuadruplesCount == 1);
            Assert.IsTrue(memStore.Single().Equals(quadruple));
        }

        [TestMethod]
        public void ShouldRemoveQuadruplesByPredicate()
        {
            RDFQuadruple quadruple = new RDFQuadruple(new RDFContext(new Uri("ex:ctx")), new RDFResource("ex:subj"), new RDFResource("ex:pred"), new RDFResource("ex:obj"));

            RDFSQLServerStore store = new RDFSQLServerStore(GetConnectionString("RDFSQLServerStoreTest_ShouldRemoveQuadruplesByPredicate"));
            store.AddQuadruple(quadruple);
            store.RemoveQuadruplesByPredicate(new RDFResource("ex:pred"));
            store.RemoveQuadruplesByPredicate(null); //Will not be removed, since null predicates are not allowed

            RDFMemoryStore memStore = store.SelectAllQuadruples();

            Assert.IsNotNull(memStore);
            Assert.IsTrue(memStore.QuadruplesCount == 0);
        }

        [TestMethod]
        public void ShouldNotRemoveQuadruplesByPredicate()
        {
            RDFQuadruple quadruple = new RDFQuadruple(new RDFContext(new Uri("ex:ctx")), new RDFResource("ex:subj"), new RDFResource("ex:pred"), new RDFResource("ex:obj"));

            RDFSQLServerStore store = new RDFSQLServerStore(GetConnectionString("RDFSQLServerStoreTest_ShouldNotRemoveQuadruplesByPredicate"));
            store.AddQuadruple(quadruple);
            store.RemoveQuadruplesByPredicate(new RDFResource("ex:pred2"));

            RDFMemoryStore memStore = store.SelectAllQuadruples();

            Assert.IsNotNull(memStore);
            Assert.IsTrue(memStore.QuadruplesCount == 1);
            Assert.IsTrue(memStore.Single().Equals(quadruple));
        }

        [TestMethod]
        public void ShouldRemoveQuadruplesByObject()
        {
            RDFQuadruple quadruple = new RDFQuadruple(new RDFContext(new Uri("ex:ctx")), new RDFResource("ex:subj"), new RDFResource("ex:pred"), new RDFResource("ex:obj"));

            RDFSQLServerStore store = new RDFSQLServerStore(GetConnectionString("RDFSQLServerStoreTest_ShouldRemoveQuadruplesByObject"));
            store.AddQuadruple(quadruple);
            store.RemoveQuadruplesByObject(new RDFResource("ex:obj"));
            store.RemoveQuadruplesByObject(null); //Will not be removed, since null objects are not allowed

            RDFMemoryStore memStore = store.SelectAllQuadruples();

            Assert.IsNotNull(memStore);
            Assert.IsTrue(memStore.QuadruplesCount == 0);
        }

        [TestMethod]
        public void ShouldNotRemoveQuadruplesByObject()
        {
            RDFQuadruple quadruple = new RDFQuadruple(new RDFContext(new Uri("ex:ctx")), new RDFResource("ex:subj"), new RDFResource("ex:pred"), new RDFResource("ex:obj"));

            RDFSQLServerStore store = new RDFSQLServerStore(GetConnectionString("RDFSQLServerStoreTest_ShouldNotRemoveQuadruplesByObject"));
            store.AddQuadruple(quadruple);
            store.RemoveQuadruplesByObject(new RDFResource("ex:obj2"));

            RDFMemoryStore memStore = store.SelectAllQuadruples();

            Assert.IsNotNull(memStore);
            Assert.IsTrue(memStore.QuadruplesCount == 1);
            Assert.IsTrue(memStore.Single().Equals(quadruple));
        }

        [TestMethod]
        public void ShouldRemoveQuadruplesByLiteral()
        {
            RDFQuadruple quadruple = new RDFQuadruple(new RDFContext(new Uri("ex:ctx")), new RDFResource("ex:subj"), new RDFResource("ex:pred"), new RDFPlainLiteral("hello"));

            RDFSQLServerStore store = new RDFSQLServerStore(GetConnectionString("RDFSQLServerStoreTest_ShouldRemoveQuadruplesByLiteral"));
            store.AddQuadruple(quadruple);
            store.RemoveQuadruplesByLiteral(new RDFPlainLiteral("hello"));
            store.RemoveQuadruplesByLiteral(null); //Will not be removed, since null literals are not allowed

            RDFMemoryStore memStore = store.SelectAllQuadruples();

            Assert.IsNotNull(memStore);
            Assert.IsTrue(memStore.QuadruplesCount == 0);
        }

        [TestMethod]
        public void ShouldNotRemoveQuadruplesByLiteral()
        {
            RDFQuadruple quadruple = new RDFQuadruple(new RDFContext(new Uri("ex:ctx")), new RDFResource("ex:subj"), new RDFResource("ex:pred"), new RDFPlainLiteral("hello"));

            RDFSQLServerStore store = new RDFSQLServerStore(GetConnectionString("RDFSQLServerStoreTest_ShouldNotRemoveQuadruplesByLiteral"));
            store.AddQuadruple(quadruple);
            store.RemoveQuadruplesByLiteral(new RDFPlainLiteral("hello", "en-US"));

            RDFMemoryStore memStore = store.SelectAllQuadruples();

            Assert.IsNotNull(memStore);
            Assert.IsTrue(memStore.QuadruplesCount == 1);
            Assert.IsTrue(memStore.Single().Equals(quadruple));
        }

        [TestMethod]
        public void ShouldRemoveQuadruplesByContextSubject()
        {
            RDFQuadruple quadruple = new RDFQuadruple(new RDFContext(new Uri("ex:ctx")), new RDFResource("ex:subj"), new RDFResource("ex:pred"), new RDFResource("ex:obj"));

            RDFSQLServerStore store = new RDFSQLServerStore(GetConnectionString("RDFSQLServerStoreTest_ShouldRemoveQuadruplesByContextSubject"));
            store.AddQuadruple(quadruple);
            store.RemoveQuadruplesByContextSubject(new RDFContext(new Uri("ex:ctx")), new RDFResource("ex:subj"));
            store.RemoveQuadruplesByContextSubject(null, null); //Will not be removed, since null params are not allowed

            RDFMemoryStore memStore = store.SelectAllQuadruples();

            Assert.IsNotNull(memStore);
            Assert.IsTrue(memStore.QuadruplesCount == 0);
        }

        [TestMethod]
        public void ShouldNotRemoveQuadruplesByContextSubject()
        {
            RDFQuadruple quadruple = new RDFQuadruple(new RDFContext(new Uri("ex:ctx")), new RDFResource("ex:subj"), new RDFResource("ex:pred"), new RDFResource("ex:obj"));

            RDFSQLServerStore store = new RDFSQLServerStore(GetConnectionString("RDFSQLServerStoreTest_ShouldNotRemoveQuadruplesByContextSubject"));
            store.AddQuadruple(quadruple);
            store.RemoveQuadruplesByContextSubject(new RDFContext(new Uri("ex:ctx2")), new RDFResource("ex:subj"));

            RDFMemoryStore memStore = store.SelectAllQuadruples();

            Assert.IsNotNull(memStore);
            Assert.IsTrue(memStore.QuadruplesCount == 1);
            Assert.IsTrue(memStore.Single().Equals(quadruple));
        }

        [TestMethod]
        public void ShouldRemoveQuadruplesByContextPredicate()
        {
            RDFQuadruple quadruple = new RDFQuadruple(new RDFContext(new Uri("ex:ctx")), new RDFResource("ex:subj"), new RDFResource("ex:pred"), new RDFResource("ex:obj"));

            RDFSQLServerStore store = new RDFSQLServerStore(GetConnectionString("RDFSQLServerStoreTest_ShouldRemoveQuadruplesByContextPredicate"));
            store.AddQuadruple(quadruple);
            store.RemoveQuadruplesByContextPredicate(new RDFContext(new Uri("ex:ctx")), new RDFResource("ex:pred"));
            store.RemoveQuadruplesByContextPredicate(null, null); //Will not be removed, since null params are not allowed

            RDFMemoryStore memStore = store.SelectAllQuadruples();

            Assert.IsNotNull(memStore);
            Assert.IsTrue(memStore.QuadruplesCount == 0);
        }

        [TestMethod]
        public void ShouldNotRemoveQuadruplesByContextPredicate()
        {
            RDFQuadruple quadruple = new RDFQuadruple(new RDFContext(new Uri("ex:ctx")), new RDFResource("ex:subj"), new RDFResource("ex:pred"), new RDFResource("ex:obj"));

            RDFSQLServerStore store = new RDFSQLServerStore(GetConnectionString("RDFSQLServerStoreTest_ShouldNotRemoveQuadruplesByContextPredicate"));
            store.AddQuadruple(quadruple);
            store.RemoveQuadruplesByContextPredicate(new RDFContext(new Uri("ex:ctx2")), new RDFResource("ex:pred"));

            RDFMemoryStore memStore = store.SelectAllQuadruples();

            Assert.IsNotNull(memStore);
            Assert.IsTrue(memStore.QuadruplesCount == 1);
            Assert.IsTrue(memStore.Single().Equals(quadruple));
        }

        [TestMethod]
        public void ShouldRemoveQuadruplesByContextObject()
        {
            RDFQuadruple quadruple = new RDFQuadruple(new RDFContext(new Uri("ex:ctx")), new RDFResource("ex:subj"), new RDFResource("ex:pred"), new RDFResource("ex:obj"));

            RDFSQLServerStore store = new RDFSQLServerStore(GetConnectionString("RDFSQLServerStoreTest_ShouldRemoveQuadruplesByContextObject"));
            store.AddQuadruple(quadruple);
            store.RemoveQuadruplesByContextObject(new RDFContext(new Uri("ex:ctx")), new RDFResource("ex:obj"));
            store.RemoveQuadruplesByContextObject(null, null); //Will not be removed, since null params are not allowed

            RDFMemoryStore memStore = store.SelectAllQuadruples();

            Assert.IsNotNull(memStore);
            Assert.IsTrue(memStore.QuadruplesCount == 0);
        }

        [TestMethod]
        public void ShouldNotRemoveQuadruplesByContextObject()
        {
            RDFQuadruple quadruple = new RDFQuadruple(new RDFContext(new Uri("ex:ctx")), new RDFResource("ex:subj"), new RDFResource("ex:pred"), new RDFResource("ex:obj"));

            RDFSQLServerStore store = new RDFSQLServerStore(GetConnectionString("RDFSQLServerStoreTest_ShouldNotRemoveQuadruplesByContextObject"));
            store.AddQuadruple(quadruple);
            store.RemoveQuadruplesByContextObject(new RDFContext(new Uri("ex:ctx2")), new RDFResource("ex:obj"));

            RDFMemoryStore memStore = store.SelectAllQuadruples();

            Assert.IsNotNull(memStore);
            Assert.IsTrue(memStore.QuadruplesCount == 1);
            Assert.IsTrue(memStore.Single().Equals(quadruple));
        }

        [TestMethod]
        public void ShouldRemoveQuadruplesByContextLiteral()
        {
            RDFQuadruple quadruple = new RDFQuadruple(new RDFContext(new Uri("ex:ctx")), new RDFResource("ex:subj"), new RDFResource("ex:pred"), new RDFPlainLiteral("hello"));

            RDFSQLServerStore store = new RDFSQLServerStore(GetConnectionString("RDFSQLServerStoreTest_ShouldRemoveQuadruplesByContextLiteral"));
            store.AddQuadruple(quadruple);
            store.RemoveQuadruplesByContextLiteral(new RDFContext(new Uri("ex:ctx")), new RDFPlainLiteral("hello"));
            store.RemoveQuadruplesByContextLiteral(null, null); //Will not be removed, since null params are not allowed

            RDFMemoryStore memStore = store.SelectAllQuadruples();

            Assert.IsNotNull(memStore);
            Assert.IsTrue(memStore.QuadruplesCount == 0);
        }

        [TestMethod]
        public void ShouldNotRemoveQuadruplesByContextLiteral()
        {
            RDFQuadruple quadruple = new RDFQuadruple(new RDFContext(new Uri("ex:ctx")), new RDFResource("ex:subj"), new RDFResource("ex:pred"), new RDFPlainLiteral("hello"));

            RDFSQLServerStore store = new RDFSQLServerStore(GetConnectionString("RDFSQLServerStoreTest_ShouldNotRemoveQuadruplesByContextLiteral"));
            store.AddQuadruple(quadruple);
            store.RemoveQuadruplesByContextLiteral(new RDFContext(new Uri("ex:ctx2")), new RDFPlainLiteral("hello"));

            RDFMemoryStore memStore = store.SelectAllQuadruples();

            Assert.IsNotNull(memStore);
            Assert.IsTrue(memStore.QuadruplesCount == 1);
            Assert.IsTrue(memStore.Single().Equals(quadruple));
        }

        [TestMethod]
        public void ShouldRemoveQuadruplesByContextSubjectPredicate()
        {
            RDFQuadruple quadruple = new RDFQuadruple(new RDFContext(new Uri("ex:ctx")), new RDFResource("ex:subj"), new RDFResource("ex:pred"), new RDFResource("ex:obj"));

            RDFSQLServerStore store = new RDFSQLServerStore(GetConnectionString("RDFSQLServerStoreTest_ShouldRemoveQuadruplesByContextSubjectPredicate"));
            store.AddQuadruple(quadruple);
            store.RemoveQuadruplesByContextSubjectPredicate(new RDFContext(new Uri("ex:ctx")), new RDFResource("ex:subj"), new RDFResource("ex:pred"));
            store.RemoveQuadruplesByContextSubjectPredicate(null, null, null); //Will not be removed, since null params are not allowed

            RDFMemoryStore memStore = store.SelectAllQuadruples();

            Assert.IsNotNull(memStore);
            Assert.IsTrue(memStore.QuadruplesCount == 0);
        }

        [TestMethod]
        public void ShouldNotRemoveQuadruplesByContextSubjectPredicate()
        {
            RDFQuadruple quadruple = new RDFQuadruple(new RDFContext(new Uri("ex:ctx")), new RDFResource("ex:subj"), new RDFResource("ex:pred"), new RDFResource("ex:obj"));

            RDFSQLServerStore store = new RDFSQLServerStore(GetConnectionString("RDFSQLServerStoreTest_ShouldNotRemoveQuadruplesByContextSubjectPredicate"));
            store.AddQuadruple(quadruple);
            store.RemoveQuadruplesByContextSubjectPredicate(new RDFContext(new Uri("ex:ctx2")), new RDFResource("ex:subj"), new RDFResource("ex:pred"));

            RDFMemoryStore memStore = store.SelectAllQuadruples();

            Assert.IsNotNull(memStore);
            Assert.IsTrue(memStore.QuadruplesCount == 1);
            Assert.IsTrue(memStore.Single().Equals(quadruple));
        }

        [TestMethod]
        public void ShouldRemoveQuadruplesByContextSubjectObject()
        {
            RDFQuadruple quadruple = new RDFQuadruple(new RDFContext(new Uri("ex:ctx")), new RDFResource("ex:subj"), new RDFResource("ex:pred"), new RDFResource("ex:obj"));

            RDFSQLServerStore store = new RDFSQLServerStore(GetConnectionString("RDFSQLServerStoreTest_ShouldRemoveQuadruplesByContextSubjectObject"));
            store.AddQuadruple(quadruple);
            store.RemoveQuadruplesByContextSubjectObject(new RDFContext(new Uri("ex:ctx")), new RDFResource("ex:subj"), new RDFResource("ex:obj"));
            store.RemoveQuadruplesByContextSubjectObject(null, null, null); //Will not be removed, since null params are not allowed

            RDFMemoryStore memStore = store.SelectAllQuadruples();

            Assert.IsNotNull(memStore);
            Assert.IsTrue(memStore.QuadruplesCount == 0);
        }

        [TestMethod]
        public void ShouldNotRemoveQuadruplesByContextSubjectObject()
        {
            RDFQuadruple quadruple = new RDFQuadruple(new RDFContext(new Uri("ex:ctx")), new RDFResource("ex:subj"), new RDFResource("ex:pred"), new RDFResource("ex:obj"));

            RDFSQLServerStore store = new RDFSQLServerStore(GetConnectionString("RDFSQLServerStoreTest_ShouldNotRemoveQuadruplesByContextSubjectObject"));
            store.AddQuadruple(quadruple);
            store.RemoveQuadruplesByContextSubjectObject(new RDFContext(new Uri("ex:ctx2")), new RDFResource("ex:subj"), new RDFResource("ex:obj"));

            RDFMemoryStore memStore = store.SelectAllQuadruples();

            Assert.IsNotNull(memStore);
            Assert.IsTrue(memStore.QuadruplesCount == 1);
            Assert.IsTrue(memStore.Single().Equals(quadruple));
        }

        [TestMethod]
        public void ShouldRemoveQuadruplesByContextSubjectLiteral()
        {
            RDFQuadruple quadruple = new RDFQuadruple(new RDFContext(new Uri("ex:ctx")), new RDFResource("ex:subj"), new RDFResource("ex:pred"), new RDFPlainLiteral("hello"));

            RDFSQLServerStore store = new RDFSQLServerStore(GetConnectionString("RDFSQLServerStoreTest_ShouldRemoveQuadruplesByContextSubjectLiteral"));
            store.AddQuadruple(quadruple);
            store.RemoveQuadruplesByContextSubjectLiteral(new RDFContext(new Uri("ex:ctx")), new RDFResource("ex:subj"), new RDFPlainLiteral("hello"));
            store.RemoveQuadruplesByContextSubjectLiteral(null, null, null); //Will not be removed, since null params are not allowed

            RDFMemoryStore memStore = store.SelectAllQuadruples();

            Assert.IsNotNull(memStore);
            Assert.IsTrue(memStore.QuadruplesCount == 0);
        }

        [TestMethod]
        public void ShouldNotRemoveQuadruplesByContextSubjectLiteral()
        {
            RDFQuadruple quadruple = new RDFQuadruple(new RDFContext(new Uri("ex:ctx")), new RDFResource("ex:subj"), new RDFResource("ex:pred"), new RDFPlainLiteral("hello"));

            RDFSQLServerStore store = new RDFSQLServerStore(GetConnectionString("RDFSQLServerStoreTest_ShouldNotRemoveQuadruplesByContextSubjectLiteral"));
            store.AddQuadruple(quadruple);
            store.RemoveQuadruplesByContextSubjectLiteral(new RDFContext(new Uri("ex:ctx2")), new RDFResource("ex:subj"), new RDFPlainLiteral("hello"));

            RDFMemoryStore memStore = store.SelectAllQuadruples();

            Assert.IsNotNull(memStore);
            Assert.IsTrue(memStore.QuadruplesCount == 1);
            Assert.IsTrue(memStore.Single().Equals(quadruple));
        }

        [TestMethod]
        public void ShouldRemoveQuadruplesByContextPredicateObject()
        {
            RDFQuadruple quadruple = new RDFQuadruple(new RDFContext(new Uri("ex:ctx")), new RDFResource("ex:subj"), new RDFResource("ex:pred"), new RDFResource("ex:obj"));

            RDFSQLServerStore store = new RDFSQLServerStore(GetConnectionString("RDFSQLServerStoreTest_ShouldRemoveQuadruplesByContextPredicateObject"));
            store.AddQuadruple(quadruple);
            store.RemoveQuadruplesByContextPredicateObject(new RDFContext(new Uri("ex:ctx")), new RDFResource("ex:pred"), new RDFResource("ex:obj"));
            store.RemoveQuadruplesByContextPredicateObject(null, null, null); //Will not be removed, since null params are not allowed

            RDFMemoryStore memStore = store.SelectAllQuadruples();

            Assert.IsNotNull(memStore);
            Assert.IsTrue(memStore.QuadruplesCount == 0);
        }

        [TestMethod]
        public void ShouldNotRemoveQuadruplesByContextPredicateObject()
        {
            RDFQuadruple quadruple = new RDFQuadruple(new RDFContext(new Uri("ex:ctx")), new RDFResource("ex:subj"), new RDFResource("ex:pred"), new RDFResource("ex:obj"));

            RDFSQLServerStore store = new RDFSQLServerStore(GetConnectionString("RDFSQLServerStoreTest_ShouldNotRemoveQuadruplesByContextPredicateObject"));
            store.AddQuadruple(quadruple);
            store.RemoveQuadruplesByContextPredicateObject(new RDFContext(new Uri("ex:ctx2")), new RDFResource("ex:pred"), new RDFResource("ex:obj"));

            RDFMemoryStore memStore = store.SelectAllQuadruples();

            Assert.IsNotNull(memStore);
            Assert.IsTrue(memStore.QuadruplesCount == 1);
            Assert.IsTrue(memStore.Single().Equals(quadruple));
        }

        [TestMethod]
        public void ShouldRemoveQuadruplesByContextPredicateLiteral()
        {
            RDFQuadruple quadruple = new RDFQuadruple(new RDFContext(new Uri("ex:ctx")), new RDFResource("ex:subj"), new RDFResource("ex:pred"), new RDFPlainLiteral("hello"));

            RDFSQLServerStore store = new RDFSQLServerStore(GetConnectionString("RDFSQLServerStoreTest_ShouldRemoveQuadruplesByContextPredicateLiteral"));
            store.AddQuadruple(quadruple);
            store.RemoveQuadruplesByContextPredicateLiteral(new RDFContext(new Uri("ex:ctx")), new RDFResource("ex:pred"), new RDFPlainLiteral("hello"));
            store.RemoveQuadruplesByContextPredicateLiteral(null, null, null); //Will not be removed, since null params are not allowed

            RDFMemoryStore memStore = store.SelectAllQuadruples();

            Assert.IsNotNull(memStore);
            Assert.IsTrue(memStore.QuadruplesCount == 0);
        }

        [TestMethod]
        public void ShouldNotRemoveQuadruplesByContextPredicateLiteral()
        {
            RDFQuadruple quadruple = new RDFQuadruple(new RDFContext(new Uri("ex:ctx")), new RDFResource("ex:subj"), new RDFResource("ex:pred"), new RDFPlainLiteral("hello"));

            RDFSQLServerStore store = new RDFSQLServerStore(GetConnectionString("RDFSQLServerStoreTest_ShouldNotRemoveQuadruplesByContextPredicateLiteral"));
            store.AddQuadruple(quadruple);
            store.RemoveQuadruplesByContextPredicateLiteral(new RDFContext(new Uri("ex:ctx2")), new RDFResource("ex:pred"), new RDFPlainLiteral("hello"));

            RDFMemoryStore memStore = store.SelectAllQuadruples();

            Assert.IsNotNull(memStore);
            Assert.IsTrue(memStore.QuadruplesCount == 1);
            Assert.IsTrue(memStore.Single().Equals(quadruple));
        }

        [TestMethod]
        public void ShouldRemoveQuadruplesBySubjectPredicate()
        {
            RDFQuadruple quadruple = new RDFQuadruple(new RDFContext(new Uri("ex:ctx")), new RDFResource("ex:subj"), new RDFResource("ex:pred"), new RDFResource("ex:obj"));

            RDFSQLServerStore store = new RDFSQLServerStore(GetConnectionString("RDFSQLServerStoreTest_ShouldRemoveQuadruplesBySubjectPredicate"));
            store.AddQuadruple(quadruple);
            store.RemoveQuadruplesBySubjectPredicate(new RDFResource("ex:subj"), new RDFResource("ex:pred"));
            store.RemoveQuadruplesBySubjectPredicate(null, null); //Will not be removed, since null params are not allowed

            RDFMemoryStore memStore = store.SelectAllQuadruples();

            Assert.IsNotNull(memStore);
            Assert.IsTrue(memStore.QuadruplesCount == 0);
        }

        [TestMethod]
        public void ShouldNotRemoveQuadruplesBySubjectPredicate()
        {
            RDFQuadruple quadruple = new RDFQuadruple(new RDFContext(new Uri("ex:ctx")), new RDFResource("ex:subj"), new RDFResource("ex:pred"), new RDFResource("ex:obj"));

            RDFSQLServerStore store = new RDFSQLServerStore(GetConnectionString("RDFSQLServerStoreTest_ShouldNotRemoveQuadruplesBySubjectPredicate"));
            store.AddQuadruple(quadruple);
            store.RemoveQuadruplesBySubjectPredicate(new RDFResource("ex:subj2"), new RDFResource("ex:pred"));

            RDFMemoryStore memStore = store.SelectAllQuadruples();

            Assert.IsNotNull(memStore);
            Assert.IsTrue(memStore.QuadruplesCount == 1);
            Assert.IsTrue(memStore.Single().Equals(quadruple));
        }

        [TestMethod]
        public void ShouldRemoveQuadruplesBySubjectObject()
        {
            RDFQuadruple quadruple = new RDFQuadruple(new RDFContext(new Uri("ex:ctx")), new RDFResource("ex:subj"), new RDFResource("ex:pred"), new RDFResource("ex:obj"));

            RDFSQLServerStore store = new RDFSQLServerStore(GetConnectionString("RDFSQLServerStoreTest_ShouldRemoveQuadruplesBySubjectObject"));
            store.AddQuadruple(quadruple);
            store.RemoveQuadruplesBySubjectObject(new RDFResource("ex:subj"), new RDFResource("ex:obj"));
            store.RemoveQuadruplesBySubjectObject(null, null); //Will not be removed, since null params are not allowed

            RDFMemoryStore memStore = store.SelectAllQuadruples();

            Assert.IsNotNull(memStore);
            Assert.IsTrue(memStore.QuadruplesCount == 0);
        }

        [TestMethod]
        public void ShouldNotRemoveQuadruplesBySubjectObject()
        {
            RDFQuadruple quadruple = new RDFQuadruple(new RDFContext(new Uri("ex:ctx")), new RDFResource("ex:subj"), new RDFResource("ex:pred"), new RDFResource("ex:obj"));

            RDFSQLServerStore store = new RDFSQLServerStore(GetConnectionString("RDFSQLServerStoreTest_ShouldNotRemoveQuadruplesBySubjectObject"));
            store.AddQuadruple(quadruple);
            store.RemoveQuadruplesBySubjectObject(new RDFResource("ex:subj2"), new RDFResource("ex:obj"));

            RDFMemoryStore memStore = store.SelectAllQuadruples();

            Assert.IsNotNull(memStore);
            Assert.IsTrue(memStore.QuadruplesCount == 1);
            Assert.IsTrue(memStore.Single().Equals(quadruple));
        }

        [TestMethod]
        public void ShouldRemoveQuadruplesBySubjectLiteral()
        {
            RDFQuadruple quadruple = new RDFQuadruple(new RDFContext(new Uri("ex:ctx")), new RDFResource("ex:subj"), new RDFResource("ex:pred"), new RDFPlainLiteral("hello"));

            RDFSQLServerStore store = new RDFSQLServerStore(GetConnectionString("RDFSQLServerStoreTest_ShouldRemoveQuadruplesBySubjectLiteral"));
            store.AddQuadruple(quadruple);
            store.RemoveQuadruplesBySubjectLiteral(new RDFResource("ex:subj"), new RDFPlainLiteral("hello"));
            store.RemoveQuadruplesBySubjectLiteral(null, null); //Will not be removed, since null params are not allowed

            RDFMemoryStore memStore = store.SelectAllQuadruples();

            Assert.IsNotNull(memStore);
            Assert.IsTrue(memStore.QuadruplesCount == 0);
        }

        [TestMethod]
        public void ShouldNotRemoveQuadruplesBySubjectLiteral()
        {
            RDFQuadruple quadruple = new RDFQuadruple(new RDFContext(new Uri("ex:ctx")), new RDFResource("ex:subj"), new RDFResource("ex:pred"), new RDFPlainLiteral("hello"));

            RDFSQLServerStore store = new RDFSQLServerStore(GetConnectionString("RDFSQLServerStoreTest_ShouldNotRemoveQuadruplesBySubjectLiteral"));
            store.AddQuadruple(quadruple);
            store.RemoveQuadruplesBySubjectLiteral(new RDFResource("ex:subj2"), new RDFPlainLiteral("hello"));

            RDFMemoryStore memStore = store.SelectAllQuadruples();

            Assert.IsNotNull(memStore);
            Assert.IsTrue(memStore.QuadruplesCount == 1);
            Assert.IsTrue(memStore.Single().Equals(quadruple));
        }

        [TestMethod]
        public void ShouldRemoveQuadruplesByPredicateObject()
        {
            RDFQuadruple quadruple = new RDFQuadruple(new RDFContext(new Uri("ex:ctx")), new RDFResource("ex:subj"), new RDFResource("ex:pred"), new RDFResource("ex:obj"));

            RDFSQLServerStore store = new RDFSQLServerStore(GetConnectionString("RDFSQLServerStoreTest_ShouldRemoveQuadruplesByPredicateObject"));
            store.AddQuadruple(quadruple);
            store.RemoveQuadruplesByPredicateObject(new RDFResource("ex:pred"), new RDFResource("ex:obj"));
            store.RemoveQuadruplesByPredicateObject(null, null); //Will not be removed, since null params are not allowed

            RDFMemoryStore memStore = store.SelectAllQuadruples();

            Assert.IsNotNull(memStore);
            Assert.IsTrue(memStore.QuadruplesCount == 0);
        }

        [TestMethod]
        public void ShouldNotRemoveQuadruplesByPredicateObject()
        {
            RDFQuadruple quadruple = new RDFQuadruple(new RDFContext(new Uri("ex:ctx")), new RDFResource("ex:subj"), new RDFResource("ex:pred"), new RDFResource("ex:obj"));

            RDFSQLServerStore store = new RDFSQLServerStore(GetConnectionString("RDFSQLServerStoreTest_ShouldNotRemoveQuadruplesByPredicateObject"));
            store.AddQuadruple(quadruple);
            store.RemoveQuadruplesByPredicateObject(new RDFResource("ex:pred2"), new RDFResource("ex:obj"));

            RDFMemoryStore memStore = store.SelectAllQuadruples();

            Assert.IsNotNull(memStore);
            Assert.IsTrue(memStore.QuadruplesCount == 1);
            Assert.IsTrue(memStore.Single().Equals(quadruple));
        }

        [TestMethod]
        public void ShouldRemoveQuadruplesByPredicateLiteral()
        {
            RDFQuadruple quadruple = new RDFQuadruple(new RDFContext(new Uri("ex:ctx")), new RDFResource("ex:subj"), new RDFResource("ex:pred"), new RDFPlainLiteral("hello"));

            RDFSQLServerStore store = new RDFSQLServerStore(GetConnectionString("RDFSQLServerStoreTest_ShouldRemoveQuadruplesByPredicateLiteral"));
            store.AddQuadruple(quadruple);
            store.RemoveQuadruplesByPredicateLiteral(new RDFResource("ex:pred"), new RDFPlainLiteral("hello"));
            store.RemoveQuadruplesByPredicateLiteral(null, null); //Will not be removed, since null params are not allowed

            RDFMemoryStore memStore = store.SelectAllQuadruples();

            Assert.IsNotNull(memStore);
            Assert.IsTrue(memStore.QuadruplesCount == 0);
        }

        [TestMethod]
        public void ShouldNotRemoveQuadruplesByPredicateLiteral()
        {
            RDFQuadruple quadruple = new RDFQuadruple(new RDFContext(new Uri("ex:ctx")), new RDFResource("ex:subj"), new RDFResource("ex:pred"), new RDFPlainLiteral("hello"));

            RDFSQLServerStore store = new RDFSQLServerStore(GetConnectionString("RDFSQLServerStoreTest_ShouldNotRemoveQuadruplesByPredicateLiteral"));
            store.AddQuadruple(quadruple);
            store.RemoveQuadruplesByPredicateLiteral(new RDFResource("ex:pred2"), new RDFPlainLiteral("hello"));

            RDFMemoryStore memStore = store.SelectAllQuadruples();

            Assert.IsNotNull(memStore);
            Assert.IsTrue(memStore.QuadruplesCount == 1);
            Assert.IsTrue(memStore.Single().Equals(quadruple));
        }

        [TestMethod]
        public void ShouldClearQuadruples()
        {
            RDFQuadruple quadruple = new RDFQuadruple(new RDFContext(new Uri("ex:ctx")), new RDFResource("ex:subj"), new RDFResource("ex:pred"), new RDFPlainLiteral("hello"));

            RDFSQLServerStore store = new RDFSQLServerStore(GetConnectionString("RDFSQLServerStoreTest_ShouldClearQuadruples"));
            store.ClearQuadruples();

            RDFMemoryStore memStore = store.SelectAllQuadruples();

            Assert.IsNotNull(memStore);
            Assert.IsTrue(memStore.QuadruplesCount == 0);
        }

        [TestMethod]
        public void ShouldContainQuadruple()
        {
            RDFQuadruple quadruple1 = new RDFQuadruple(new RDFContext(new Uri("ex:ctx")), new RDFResource("ex:subj"), new RDFResource("ex:pred"), new RDFPlainLiteral("hello"));
            RDFQuadruple quadruple2 = new RDFQuadruple(new RDFContext(new Uri("ex:ctx2")), new RDFResource("ex:subj"), new RDFResource("ex:pred"), new RDFPlainLiteral("hello"));

            RDFSQLServerStore store = new RDFSQLServerStore(GetConnectionString("RDFSQLServerStoreTest_ShouldContainQuadruple"));
            store.AddQuadruple(quadruple1);
            store.AddQuadruple(quadruple2);

            Assert.IsTrue(store.ContainsQuadruple(quadruple1));
            Assert.IsTrue(store.ContainsQuadruple(quadruple2));
        }

        [TestMethod]
        public void ShouldNotContainQuadruple()
        {
            RDFQuadruple quadruple = new RDFQuadruple(new RDFContext(new Uri("ex:ctx")), new RDFResource("ex:subj"), new RDFResource("ex:pred"), new RDFPlainLiteral("hello"));
            RDFQuadruple quadruple2 = new RDFQuadruple(new RDFContext(new Uri("ex:ctx2")), new RDFResource("ex:subj"), new RDFResource("ex:pred"), new RDFPlainLiteral("hello"));

            RDFSQLServerStore store = new RDFSQLServerStore(GetConnectionString("RDFSQLServerStoreTest_ShouldNotContainQuadruple"));
            store.AddQuadruple(quadruple);

            Assert.IsFalse(store.ContainsQuadruple(quadruple2));
            Assert.IsFalse(store.ContainsQuadruple(null));
        }

        [TestMethod]
        public void ShouldSelectQuadruplesByContext()
        {
            RDFQuadruple quadruple = new RDFQuadruple(new RDFContext(new Uri("ex:ctx")), new RDFResource("ex:subj"), new RDFResource("ex:pred"), new RDFResource("ex:obj"));

            RDFSQLServerStore store = new RDFSQLServerStore(GetConnectionString("RDFSQLServerStoreTest_ShouldSelectQuadruplesByContext"));
            store.AddQuadruple(quadruple);
            RDFMemoryStore result = store.SelectQuadruplesByContext(new RDFContext(new Uri("ex:ctx")));

            Assert.IsNotNull(result);
            Assert.IsTrue(result.ContainsQuadruple(quadruple));
        }

        [TestMethod]
        public void ShouldNotSelectQuadruplesByContext()
        {
            RDFQuadruple quadruple = new RDFQuadruple(new RDFContext(new Uri("ex:ctx")), new RDFResource("ex:subj"), new RDFResource("ex:pred"), new RDFResource("ex:obj"));

            RDFSQLServerStore store = new RDFSQLServerStore(GetConnectionString("RDFSQLServerStoreTest_ShouldNotSelectQuadruplesByContext"));
            store.AddQuadruple(quadruple);
            RDFMemoryStore result = store.SelectQuadruplesByContext(new RDFContext(new Uri("ex:ctx2")));

            Assert.IsNotNull(result);
            Assert.IsFalse(result.ContainsQuadruple(quadruple));
        }

        [TestMethod]
        public void ShouldSelectQuadruplesBySubject()
        {
            RDFQuadruple quadruple = new RDFQuadruple(new RDFContext(new Uri("ex:ctx")), new RDFResource("ex:subj"), new RDFResource("ex:pred"), new RDFResource("ex:obj"));

            RDFSQLServerStore store = new RDFSQLServerStore(GetConnectionString("RDFSQLServerStoreTest_ShouldSelectQuadruplesBySubject"));
            store.AddQuadruple(quadruple);
            RDFMemoryStore result = store.SelectQuadruplesBySubject(new RDFResource("ex:subj"));

            Assert.IsNotNull(result);
            Assert.IsTrue(result.ContainsQuadruple(quadruple));
        }

        [TestMethod]
        public void ShouldNotSelectQuadruplesBySubject()
        {
            RDFQuadruple quadruple = new RDFQuadruple(new RDFContext(new Uri("ex:ctx")), new RDFResource("ex:subj"), new RDFResource("ex:pred"), new RDFResource("ex:obj"));

            RDFSQLServerStore store = new RDFSQLServerStore(GetConnectionString("RDFSQLServerStoreTest_ShouldNotSelectQuadruplesBySubject"));
            store.AddQuadruple(quadruple);
            RDFMemoryStore result = store.SelectQuadruplesBySubject(new RDFResource("ex:subj2"));

            Assert.IsNotNull(result);
            Assert.IsFalse(result.ContainsQuadruple(quadruple));
        }

        [TestMethod]
        public void ShouldSelectQuadruplesByPredicate()
        {
            RDFQuadruple quadruple = new RDFQuadruple(new RDFContext(new Uri("ex:ctx")), new RDFResource("ex:subj"), new RDFResource("ex:pred"), new RDFResource("ex:obj"));

            RDFSQLServerStore store = new RDFSQLServerStore(GetConnectionString("RDFSQLServerStoreTest_ShouldSelectQuadruplesByPredicate"));
            store.AddQuadruple(quadruple);
            RDFMemoryStore result = store.SelectQuadruplesByPredicate(new RDFResource("ex:pred"));

            Assert.IsNotNull(result);
            Assert.IsTrue(result.ContainsQuadruple(quadruple));
        }

        [TestMethod]
        public void ShouldNotSelectQuadruplesByPredicate()
        {
            RDFQuadruple quadruple = new RDFQuadruple(new RDFContext(new Uri("ex:ctx")), new RDFResource("ex:subj"), new RDFResource("ex:pred"), new RDFResource("ex:obj"));

            RDFSQLServerStore store = new RDFSQLServerStore(GetConnectionString("RDFSQLServerStoreTest_ShouldNotSelectQuadruplesByPredicate"));
            store.AddQuadruple(quadruple);
            RDFMemoryStore result = store.SelectQuadruplesByPredicate(new RDFResource("ex:pred2"));

            Assert.IsNotNull(result);
            Assert.IsFalse(result.ContainsQuadruple(quadruple));
        }

        [TestMethod]
        public void ShouldSelectQuadruplesByObject()
        {
            RDFQuadruple quadruple = new RDFQuadruple(new RDFContext(new Uri("ex:ctx")), new RDFResource("ex:subj"), new RDFResource("ex:pred"), new RDFResource("ex:obj"));

            RDFSQLServerStore store = new RDFSQLServerStore(GetConnectionString("RDFSQLServerStoreTest_ShouldSelectQuadruplesByObject"));
            store.AddQuadruple(quadruple);
            RDFMemoryStore result = store.SelectQuadruplesByObject(new RDFResource("ex:obj"));

            Assert.IsNotNull(result);
            Assert.IsTrue(result.ContainsQuadruple(quadruple));
        }

        [TestMethod]
        public void ShouldNotSelectQuadruplesByObject()
        {
            RDFQuadruple quadruple = new RDFQuadruple(new RDFContext(new Uri("ex:ctx")), new RDFResource("ex:subj"), new RDFResource("ex:pred"), new RDFResource("ex:obj"));

            RDFSQLServerStore store = new RDFSQLServerStore(GetConnectionString("RDFSQLServerStoreTest_ShouldNotSelectQuadruplesByObject"));
            store.AddQuadruple(quadruple);
            RDFMemoryStore result = store.SelectQuadruplesByObject(new RDFResource("ex:obj2"));

            Assert.IsNotNull(result);
            Assert.IsFalse(result.ContainsQuadruple(quadruple));
        }

        [TestMethod]
        public void ShouldSelectQuadruplesByLiteral()
        {
            RDFQuadruple quadruple = new RDFQuadruple(new RDFContext(new Uri("ex:ctx")), new RDFResource("ex:subj"), new RDFResource("ex:pred"), new RDFPlainLiteral("hello"));

            RDFSQLServerStore store = new RDFSQLServerStore(GetConnectionString("RDFSQLServerStoreTest_ShouldSelectQuadruplesByLiteral"));
            store.AddQuadruple(quadruple);
            RDFMemoryStore result = store.SelectQuadruplesByLiteral(new RDFPlainLiteral("hello"));

            Assert.IsNotNull(result);
            Assert.IsTrue(result.ContainsQuadruple(quadruple));
        }

        [TestMethod]
        public void ShouldNotSelectQuadruplesByLiteral()
        {
            RDFQuadruple quadruple = new RDFQuadruple(new RDFContext(new Uri("ex:ctx")), new RDFResource("ex:subj"), new RDFResource("ex:pred"), new RDFPlainLiteral("hello"));

            RDFSQLServerStore store = new RDFSQLServerStore(GetConnectionString("RDFSQLServerStoreTest_ShouldNotSelectQuadruplesByLiteral"));
            store.AddQuadruple(quadruple);
            RDFMemoryStore result = store.SelectQuadruplesByLiteral(new RDFPlainLiteral("hello","en"));

            Assert.IsNotNull(result);
            Assert.IsFalse(result.ContainsQuadruple(quadruple));
        }

        [TestMethod]
        public void ShouldSelectQuadruplesByContextSubject()
        {
            RDFQuadruple quadruple = new RDFQuadruple(new RDFContext(new Uri("ex:ctx")), new RDFResource("ex:subj"), new RDFResource("ex:pred"), new RDFResource("ex:obj"));

            RDFSQLServerStore store = new RDFSQLServerStore(GetConnectionString("RDFSQLServerStoreTest_ShouldSelectQuadruplesByContextSubject"));
            store.AddQuadruple(quadruple);
            RDFMemoryStore result = store.SelectQuadruples(new RDFContext(new Uri("ex:ctx")), new RDFResource("ex:subj"), null, null, null);

            Assert.IsNotNull(result);
            Assert.IsTrue(result.ContainsQuadruple(quadruple));
        }

        [TestMethod]
        public void ShouldNotSelectQuadruplesByContextSubject()
        {
            RDFQuadruple quadruple = new RDFQuadruple(new RDFContext(new Uri("ex:ctx")), new RDFResource("ex:subj"), new RDFResource("ex:pred"), new RDFResource("ex:obj"));

            RDFSQLServerStore store = new RDFSQLServerStore(GetConnectionString("RDFSQLServerStoreTest_ShouldNotSelectQuadruplesByContextSubject"));
            store.AddQuadruple(quadruple);
            RDFMemoryStore result = store.SelectQuadruples(new RDFContext(new Uri("ex:ctx2")), new RDFResource("ex:subj"), null, null, null);

            Assert.IsNotNull(result);
            Assert.IsFalse(result.ContainsQuadruple(quadruple));
        }

        [TestMethod]
        public void ShouldSelectQuadruplesByContextPredicate()
        {
            RDFQuadruple quadruple = new RDFQuadruple(new RDFContext(new Uri("ex:ctx")), new RDFResource("ex:subj"), new RDFResource("ex:pred"), new RDFResource("ex:obj"));

            RDFSQLServerStore store = new RDFSQLServerStore(GetConnectionString("RDFSQLServerStoreTest_ShouldSelectQuadruplesByContextPredicate"));
            store.AddQuadruple(quadruple);
            RDFMemoryStore result = store.SelectQuadruples(new RDFContext(new Uri("ex:ctx")), null, new RDFResource("ex:pred"), null, null);

            Assert.IsNotNull(result);
            Assert.IsTrue(result.ContainsQuadruple(quadruple));
        }

        [TestMethod]
        public void ShouldNotSelectQuadruplesByContextPredicate()
        {
            RDFQuadruple quadruple = new RDFQuadruple(new RDFContext(new Uri("ex:ctx")), new RDFResource("ex:subj"), new RDFResource("ex:pred"), new RDFResource("ex:obj"));

            RDFSQLServerStore store = new RDFSQLServerStore(GetConnectionString("RDFSQLServerStoreTest_ShouldNotSelectQuadruplesByContextPredicate"));
            store.AddQuadruple(quadruple);
            RDFMemoryStore result = store.SelectQuadruples(new RDFContext(new Uri("ex:ctx2")), null, new RDFResource("ex:pred"), null, null);

            Assert.IsNotNull(result);
            Assert.IsFalse(result.ContainsQuadruple(quadruple));
        }

        [TestMethod]
        public void ShouldSelectQuadruplesByContextObject()
        {
            RDFQuadruple quadruple = new RDFQuadruple(new RDFContext(new Uri("ex:ctx")), new RDFResource("ex:subj"), new RDFResource("ex:pred"), new RDFResource("ex:obj"));

            RDFSQLServerStore store = new RDFSQLServerStore(GetConnectionString("RDFSQLServerStoreTest_ShouldSelectQuadruplesByContextObject"));
            store.AddQuadruple(quadruple);
            RDFMemoryStore result = store.SelectQuadruples(new RDFContext(new Uri("ex:ctx")), null, null, new RDFResource("ex:obj"), null);

            Assert.IsNotNull(result);
            Assert.IsTrue(result.ContainsQuadruple(quadruple));
        }

        [TestMethod]
        public void ShouldNotSelectQuadruplesByContextObject()
        {
            RDFQuadruple quadruple = new RDFQuadruple(new RDFContext(new Uri("ex:ctx")), new RDFResource("ex:subj"), new RDFResource("ex:pred"), new RDFResource("ex:obj"));

            RDFSQLServerStore store = new RDFSQLServerStore(GetConnectionString("RDFSQLServerStoreTest_ShouldNotSelectQuadruplesByContextObject"));
            store.AddQuadruple(quadruple);
            RDFMemoryStore result = store.SelectQuadruples(new RDFContext(new Uri("ex:ctx2")), null, null, new RDFResource("ex:obj"), null);

            Assert.IsNotNull(result);
            Assert.IsFalse(result.ContainsQuadruple(quadruple));
        }

        [TestMethod]
        public void ShouldSelectQuadruplesByContextLiteral()
        {
            RDFQuadruple quadruple = new RDFQuadruple(new RDFContext(new Uri("ex:ctx")), new RDFResource("ex:subj"), new RDFResource("ex:pred"), new RDFPlainLiteral("hello"));

            RDFSQLServerStore store = new RDFSQLServerStore(GetConnectionString("RDFSQLServerStoreTest_ShouldSelectQuadruplesByContextLiteral"));
            store.AddQuadruple(quadruple);
            RDFMemoryStore result = store.SelectQuadruples(new RDFContext(new Uri("ex:ctx")), null, null, null, new RDFPlainLiteral("hello"));

            Assert.IsNotNull(result);
            Assert.IsTrue(result.ContainsQuadruple(quadruple));
        }

        [TestMethod]
        public void ShouldNotSelectQuadruplesByContextLiteral()
        {
            RDFQuadruple quadruple = new RDFQuadruple(new RDFContext(new Uri("ex:ctx")), new RDFResource("ex:subj"), new RDFResource("ex:pred"), new RDFPlainLiteral("hello"));

            RDFSQLServerStore store = new RDFSQLServerStore(GetConnectionString("RDFSQLServerStoreTest_ShouldNotSelectQuadruplesByContextLiteral"));
            store.AddQuadruple(quadruple);
            RDFMemoryStore result = store.SelectQuadruples(new RDFContext(new Uri("ex:ctx2")), null, null, null, new RDFPlainLiteral("hello"));

            Assert.IsNotNull(result);
            Assert.IsFalse(result.ContainsQuadruple(quadruple));
        }

        [TestMethod]
        public void ShouldSelectQuadruplesByContextSubjectPredicate()
        {
            RDFQuadruple quadruple = new RDFQuadruple(new RDFContext(new Uri("ex:ctx")), new RDFResource("ex:subj"), new RDFResource("ex:pred"), new RDFResource("ex:obj"));

            RDFSQLServerStore store = new RDFSQLServerStore(GetConnectionString("RDFSQLServerStoreTest_ShouldSelectQuadruplesByContextSubjectPredicate"));
            store.AddQuadruple(quadruple);
            RDFMemoryStore result = store.SelectQuadruples(new RDFContext(new Uri("ex:ctx")), new RDFResource("ex:subj"), new RDFResource("ex:pred"), null, null);

            Assert.IsNotNull(result);
            Assert.IsTrue(result.ContainsQuadruple(quadruple));
        }

        [TestMethod]
        public void ShouldNotSelectQuadruplesByContextSubjectPredicate()
        {
            RDFQuadruple quadruple = new RDFQuadruple(new RDFContext(new Uri("ex:ctx")), new RDFResource("ex:subj"), new RDFResource("ex:pred"), new RDFResource("ex:obj"));

            RDFSQLServerStore store = new RDFSQLServerStore(GetConnectionString("RDFSQLServerStoreTest_ShouldNotSelectQuadruplesByContextSubjectPredicate"));
            store.AddQuadruple(quadruple);
            RDFMemoryStore result = store.SelectQuadruples(new RDFContext(new Uri("ex:ctx2")), new RDFResource("ex:subj"), new RDFResource("ex:pred"), null, null);

            Assert.IsNotNull(result);
            Assert.IsFalse(result.ContainsQuadruple(quadruple));
        }

        [TestMethod]
        public void ShouldSelectQuadruplesByContextSubjectObject()
        {
            RDFQuadruple quadruple = new RDFQuadruple(new RDFContext(new Uri("ex:ctx")), new RDFResource("ex:subj"), new RDFResource("ex:pred"), new RDFResource("ex:obj"));

            RDFSQLServerStore store = new RDFSQLServerStore(GetConnectionString("RDFSQLServerStoreTest_ShouldSelectQuadruplesByContextSubjectObject"));
            store.AddQuadruple(quadruple);
            RDFMemoryStore result = store.SelectQuadruples(new RDFContext(new Uri("ex:ctx")), new RDFResource("ex:subj"), null, new RDFResource("ex:obj"), null);

            Assert.IsNotNull(result);
            Assert.IsTrue(result.ContainsQuadruple(quadruple));
        }

        [TestMethod]
        public void ShouldNotSelectQuadruplesByContextSubjectObject()
        {
            RDFQuadruple quadruple = new RDFQuadruple(new RDFContext(new Uri("ex:ctx")), new RDFResource("ex:subj"), new RDFResource("ex:pred"), new RDFResource("ex:obj"));

            RDFSQLServerStore store = new RDFSQLServerStore(GetConnectionString("RDFSQLServerStoreTest_ShouldNotSelectQuadruplesByContextSubjectObject"));
            store.AddQuadruple(quadruple);
            RDFMemoryStore result = store.SelectQuadruples(new RDFContext(new Uri("ex:ctx2")), new RDFResource("ex:subj"), null, new RDFResource("ex:obj"), null);

            Assert.IsNotNull(result);
            Assert.IsFalse(result.ContainsQuadruple(quadruple));
        }

        [TestMethod]
        public void ShouldSelectQuadruplesByContextSubjectLiteral()
        {
            RDFQuadruple quadruple = new RDFQuadruple(new RDFContext(new Uri("ex:ctx")), new RDFResource("ex:subj"), new RDFResource("ex:pred"), new RDFPlainLiteral("hello"));

            RDFSQLServerStore store = new RDFSQLServerStore(GetConnectionString("RDFSQLServerStoreTest_ShouldSelectQuadruplesByContextSubjectLiteral"));
            store.AddQuadruple(quadruple);
            RDFMemoryStore result = store.SelectQuadruples(new RDFContext(new Uri("ex:ctx")), new RDFResource("ex:subj"), null, null, new RDFPlainLiteral("hello"));

            Assert.IsNotNull(result);
            Assert.IsTrue(result.ContainsQuadruple(quadruple));
        }

        [TestMethod]
        public void ShouldNotSelectQuadruplesByContextSubjectLiteral()
        {
            RDFQuadruple quadruple = new RDFQuadruple(new RDFContext(new Uri("ex:ctx")), new RDFResource("ex:subj"), new RDFResource("ex:pred"), new RDFPlainLiteral("hello"));

            RDFSQLServerStore store = new RDFSQLServerStore(GetConnectionString("RDFSQLServerStoreTest_ShouldNotSelectQuadruplesByContextSubjectLiteral"));
            store.AddQuadruple(quadruple);
            RDFMemoryStore result = store.SelectQuadruples(new RDFContext(new Uri("ex:ctx2")), new RDFResource("ex:subj"), null, null, new RDFPlainLiteral("hello"));

            Assert.IsNotNull(result);
            Assert.IsFalse(result.ContainsQuadruple(quadruple));
        }

        [TestMethod]
        public void ShouldSelectQuadruplesByContextPredicateObject()
        {
            RDFQuadruple quadruple = new RDFQuadruple(new RDFContext(new Uri("ex:ctx")), new RDFResource("ex:subj"), new RDFResource("ex:pred"), new RDFResource("ex:obj"));

            RDFSQLServerStore store = new RDFSQLServerStore(GetConnectionString("RDFSQLServerStoreTest_ShouldSelectQuadruplesByContextPredicateObject"));
            store.AddQuadruple(quadruple);
            RDFMemoryStore result = store.SelectQuadruples(new RDFContext(new Uri("ex:ctx")), null, new RDFResource("ex:pred"), new RDFResource("ex:obj"), null);

            Assert.IsNotNull(result);
            Assert.IsTrue(result.ContainsQuadruple(quadruple));
        }

        [TestMethod]
        public void ShouldNotSelectQuadruplesByContextPredicateObject()
        {
            RDFQuadruple quadruple = new RDFQuadruple(new RDFContext(new Uri("ex:ctx")), new RDFResource("ex:subj"), new RDFResource("ex:pred"), new RDFResource("ex:obj"));

            RDFSQLServerStore store = new RDFSQLServerStore(GetConnectionString("RDFSQLServerStoreTest_ShouldNotSelectQuadruplesByContextPredicateObject"));
            store.AddQuadruple(quadruple);
            RDFMemoryStore result = store.SelectQuadruples(new RDFContext(new Uri("ex:ctx2")), null, new RDFResource("ex:pred"), new RDFResource("ex:obj"), null);

            Assert.IsNotNull(result);
            Assert.IsFalse(result.ContainsQuadruple(quadruple));
        }

        [TestMethod]
        public void ShouldSelectQuadruplesByContextPredicateLiteral()
        {
            RDFQuadruple quadruple = new RDFQuadruple(new RDFContext(new Uri("ex:ctx")), new RDFResource("ex:subj"), new RDFResource("ex:pred"), new RDFPlainLiteral("hello"));

            RDFSQLServerStore store = new RDFSQLServerStore(GetConnectionString("RDFSQLServerStoreTest_ShouldSelectQuadruplesByContextPredicateLiteral"));
            store.AddQuadruple(quadruple);
            RDFMemoryStore result = store.SelectQuadruples(new RDFContext(new Uri("ex:ctx")), null, new RDFResource("ex:pred"), null, new RDFPlainLiteral("hello"));

            Assert.IsNotNull(result);
            Assert.IsTrue(result.ContainsQuadruple(quadruple));
        }

        [TestMethod]
        public void ShouldNotSelectQuadruplesByContextPredicateLiteral()
        {
            RDFQuadruple quadruple = new RDFQuadruple(new RDFContext(new Uri("ex:ctx")), new RDFResource("ex:subj"), new RDFResource("ex:pred"), new RDFPlainLiteral("hello"));

            RDFSQLServerStore store = new RDFSQLServerStore(GetConnectionString("RDFSQLServerStoreTest_ShouldNotSelectQuadruplesByContextPredicateLiteral"));
            store.AddQuadruple(quadruple);
            RDFMemoryStore result = store.SelectQuadruples(new RDFContext(new Uri("ex:ctx2")), null, new RDFResource("ex:pred"), null, new RDFPlainLiteral("hello"));

            Assert.IsNotNull(result);
            Assert.IsFalse(result.ContainsQuadruple(quadruple));
        }

        [TestMethod]
        public void ShouldSelectQuadruplesByContextSubjectPredicateObject()
        {
            RDFQuadruple quadruple = new RDFQuadruple(new RDFContext(new Uri("ex:ctx")), new RDFResource("ex:subj"), new RDFResource("ex:pred"), new RDFResource("ex:obj"));

            RDFSQLServerStore store = new RDFSQLServerStore(GetConnectionString("RDFSQLServerStoreTest_ShouldSelectQuadruplesByContextSubjectPredicateObject"));
            store.AddQuadruple(quadruple);
            RDFMemoryStore result = store.SelectQuadruples(new RDFContext(new Uri("ex:ctx")), new RDFResource("ex:subj"), new RDFResource("ex:pred"), new RDFResource("ex:obj"), null);

            Assert.IsNotNull(result);
            Assert.IsTrue(result.ContainsQuadruple(quadruple));
        }

        [TestMethod]
        public void ShouldNotSelectQuadruplesByContextSubjectPredicateObject()
        {
            RDFQuadruple quadruple = new RDFQuadruple(new RDFContext(new Uri("ex:ctx")), new RDFResource("ex:subj"), new RDFResource("ex:pred"), new RDFResource("ex:obj"));

            RDFSQLServerStore store = new RDFSQLServerStore(GetConnectionString("RDFSQLServerStoreTest_ShouldNotSelectQuadruplesByContextSubjectPredicateObject"));
            store.AddQuadruple(quadruple);
            RDFMemoryStore result = store.SelectQuadruples(new RDFContext(new Uri("ex:ctx2")), new RDFResource("ex:subj"), new RDFResource("ex:pred"), new RDFResource("ex:obj"), null);

            Assert.IsNotNull(result);
            Assert.IsFalse(result.ContainsQuadruple(quadruple));
        }

        [TestMethod]
        public void ShouldSelectQuadruplesByContextSubjectPredicateLiteral()
        {
            RDFQuadruple quadruple = new RDFQuadruple(new RDFContext(new Uri("ex:ctx")), new RDFResource("ex:subj"), new RDFResource("ex:pred"), new RDFPlainLiteral("hello"));

            RDFSQLServerStore store = new RDFSQLServerStore(GetConnectionString("RDFSQLServerStoreTest_ShouldSelectQuadruplesByContextSubjectPredicateLiteral"));
            store.AddQuadruple(quadruple);
            RDFMemoryStore result = store.SelectQuadruples(new RDFContext(new Uri("ex:ctx")), new RDFResource("ex:subj"), new RDFResource("ex:pred"), null, new RDFPlainLiteral("hello"));

            Assert.IsNotNull(result);
            Assert.IsTrue(result.ContainsQuadruple(quadruple));
        }

        [TestMethod]
        public void ShouldNotSelectQuadruplesByContextSubjectPredicateLiteral()
        {
            RDFQuadruple quadruple = new RDFQuadruple(new RDFContext(new Uri("ex:ctx")), new RDFResource("ex:subj"), new RDFResource("ex:pred"), new RDFPlainLiteral("hello"));

            RDFSQLServerStore store = new RDFSQLServerStore(GetConnectionString("RDFSQLServerStoreTest_ShouldNotSelectQuadruplesByContextSubjectPredicateLiteral"));
            store.AddQuadruple(quadruple);
            RDFMemoryStore result = store.SelectQuadruples(new RDFContext(new Uri("ex:ctx2")), new RDFResource("ex:subj"), new RDFResource("ex:pred"), null, new RDFPlainLiteral("hello"));

            Assert.IsNotNull(result);
            Assert.IsFalse(result.ContainsQuadruple(quadruple));
        }

        [TestMethod]
        public void ShouldSelectQuadruplesBySubjectPredicate()
        {
            RDFQuadruple quadruple = new RDFQuadruple(new RDFContext(new Uri("ex:ctx")), new RDFResource("ex:subj"), new RDFResource("ex:pred"), new RDFResource("ex:obj"));

            RDFSQLServerStore store = new RDFSQLServerStore(GetConnectionString("RDFSQLServerStoreTest_ShouldSelectQuadruplesBySubjectPredicate"));
            store.AddQuadruple(quadruple);
            RDFMemoryStore result = store.SelectQuadruples(null, new RDFResource("ex:subj"), new RDFResource("ex:pred"), null, null);

            Assert.IsNotNull(result);
            Assert.IsTrue(result.ContainsQuadruple(quadruple));
        }

        [TestMethod]
        public void ShouldNotSelectQuadruplesBySubjectPredicate()
        {
            RDFQuadruple quadruple = new RDFQuadruple(new RDFContext(new Uri("ex:ctx")), new RDFResource("ex:subj"), new RDFResource("ex:pred"), new RDFResource("ex:obj"));

            RDFSQLServerStore store = new RDFSQLServerStore(GetConnectionString("RDFSQLServerStoreTest_ShouldNotSelectQuadruplesBySubjectPredicate"));
            store.AddQuadruple(quadruple);
            RDFMemoryStore result = store.SelectQuadruples(null, new RDFResource("ex:subj2"), new RDFResource("ex:pred"), null, null);

            Assert.IsNotNull(result);
            Assert.IsFalse(result.ContainsQuadruple(quadruple));
        }

        [TestMethod]
        public void ShouldSelectQuadruplesBySubjectObject()
        {
            RDFQuadruple quadruple = new RDFQuadruple(new RDFContext(new Uri("ex:ctx")), new RDFResource("ex:subj"), new RDFResource("ex:pred"), new RDFResource("ex:obj"));

            RDFSQLServerStore store = new RDFSQLServerStore(GetConnectionString("RDFSQLServerStoreTest_ShouldSelectQuadruplesBySubjectObject"));
            store.AddQuadruple(quadruple);
            RDFMemoryStore result = store.SelectQuadruples(null, new RDFResource("ex:subj"), null, new RDFResource("ex:obj"), null);

            Assert.IsNotNull(result);
            Assert.IsTrue(result.ContainsQuadruple(quadruple));
        }

        [TestMethod]
        public void ShouldNotSelectQuadruplesBySubjectObject()
        {
            RDFQuadruple quadruple = new RDFQuadruple(new RDFContext(new Uri("ex:ctx")), new RDFResource("ex:subj"), new RDFResource("ex:pred"), new RDFResource("ex:obj"));

            RDFSQLServerStore store = new RDFSQLServerStore(GetConnectionString("RDFSQLServerStoreTest_ShouldNotSelectQuadruplesBySubjectObject"));
            store.AddQuadruple(quadruple);
            RDFMemoryStore result = store.SelectQuadruples(null, new RDFResource("ex:subj2"), null, new RDFResource("ex:obj"), null);

            Assert.IsNotNull(result);
            Assert.IsFalse(result.ContainsQuadruple(quadruple));
        }

        [TestMethod]
        public void ShouldSelectQuadruplesBySubjectLiteral()
        {
            RDFQuadruple quadruple = new RDFQuadruple(new RDFContext(new Uri("ex:ctx")), new RDFResource("ex:subj"), new RDFResource("ex:pred"), new RDFPlainLiteral("hello"));

            RDFSQLServerStore store = new RDFSQLServerStore(GetConnectionString("RDFSQLServerStoreTest_ShouldSelectQuadruplesBySubjectLiteral"));
            store.AddQuadruple(quadruple);
            RDFMemoryStore result = store.SelectQuadruples(null, new RDFResource("ex:subj"), null, null, new RDFPlainLiteral("hello"));

            Assert.IsNotNull(result);
            Assert.IsTrue(result.ContainsQuadruple(quadruple));
        }

        [TestMethod]
        public void ShouldNotSelectQuadruplesBySubjectLiteral()
        {
            RDFQuadruple quadruple = new RDFQuadruple(new RDFContext(new Uri("ex:ctx")), new RDFResource("ex:subj"), new RDFResource("ex:pred"), new RDFPlainLiteral("hello"));

            RDFSQLServerStore store = new RDFSQLServerStore(GetConnectionString("RDFSQLServerStoreTest_ShouldNotSelectQuadruplesBySubjectLiteral"));
            store.AddQuadruple(quadruple);
            RDFMemoryStore result = store.SelectQuadruples(null, new RDFResource("ex:subj2"), null, null, new RDFPlainLiteral("hello"));

            Assert.IsNotNull(result);
            Assert.IsFalse(result.ContainsQuadruple(quadruple));
        }

        [TestMethod]
        public void ShouldSelectQuadruplesByPredicateObject()
        {
            RDFQuadruple quadruple = new RDFQuadruple(new RDFContext(new Uri("ex:ctx")), new RDFResource("ex:subj"), new RDFResource("ex:pred"), new RDFResource("ex:obj"));

            RDFSQLServerStore store = new RDFSQLServerStore(GetConnectionString("RDFSQLServerStoreTest_ShouldSelectQuadruplesByPredicateObject"));
            store.AddQuadruple(quadruple);
            RDFMemoryStore result = store.SelectQuadruples(null, null, new RDFResource("ex:pred"), new RDFResource("ex:obj"), null);

            Assert.IsNotNull(result);
            Assert.IsTrue(result.ContainsQuadruple(quadruple));
        }

        [TestMethod]
        public void ShouldNotSelectQuadruplesByPredicateObject()
        {
            RDFQuadruple quadruple = new RDFQuadruple(new RDFContext(new Uri("ex:ctx")), new RDFResource("ex:subj"), new RDFResource("ex:pred"), new RDFResource("ex:obj"));

            RDFSQLServerStore store = new RDFSQLServerStore(GetConnectionString("RDFSQLServerStoreTest_ShouldNotSelectQuadruplesByPredicateObject"));
            store.AddQuadruple(quadruple);
            RDFMemoryStore result = store.SelectQuadruples(null, null, new RDFResource("ex:pred2"), new RDFResource("ex:obj"), null);

            Assert.IsNotNull(result);
            Assert.IsFalse(result.ContainsQuadruple(quadruple));
        }

        [TestMethod]
        public void ShouldSelectQuadruplesByPredicateLiteral()
        {
            RDFQuadruple quadruple = new RDFQuadruple(new RDFContext(new Uri("ex:ctx")), new RDFResource("ex:subj"), new RDFResource("ex:pred"), new RDFPlainLiteral("hello"));

            RDFSQLServerStore store = new RDFSQLServerStore(GetConnectionString("RDFSQLServerStoreTest_ShouldSelectQuadruplesByPredicateLiteral"));
            store.AddQuadruple(quadruple);
            RDFMemoryStore result = store.SelectQuadruples(null, null, new RDFResource("ex:pred"), null, new RDFPlainLiteral("hello"));

            Assert.IsNotNull(result);
            Assert.IsTrue(result.ContainsQuadruple(quadruple));
        }

        [TestMethod]
        public void ShouldNotSelectQuadruplesByPredicateLiteral()
        {
            RDFQuadruple quadruple = new RDFQuadruple(new RDFContext(new Uri("ex:ctx")), new RDFResource("ex:subj"), new RDFResource("ex:pred"), new RDFPlainLiteral("hello"));

            RDFSQLServerStore store = new RDFSQLServerStore(GetConnectionString("RDFSQLServerStoreTest_ShouldNotSelectQuadruplesByPredicateLiteral"));
            store.AddQuadruple(quadruple);
            RDFMemoryStore result = store.SelectQuadruples(null, null, new RDFResource("ex:pred2"), null, new RDFPlainLiteral("hello"));

            Assert.IsNotNull(result);
            Assert.IsFalse(result.ContainsQuadruple(quadruple));
        }

        [TestMethod]
        public void ShouldSelectQuadruplesBySubjectPredicateObject()
        {
            RDFQuadruple quadruple = new RDFQuadruple(new RDFContext(new Uri("ex:ctx")), new RDFResource("ex:subj"), new RDFResource("ex:pred"), new RDFResource("ex:obj"));

            RDFSQLServerStore store = new RDFSQLServerStore(GetConnectionString("RDFSQLServerStoreTest_ShouldSelectQuadruplesBySubjectPredicateObject"));
            store.AddQuadruple(quadruple);
            RDFMemoryStore result = store.SelectQuadruples(null, new RDFResource("ex:subj"), new RDFResource("ex:pred"), new RDFResource("ex:obj"), null);

            Assert.IsNotNull(result);
            Assert.IsTrue(result.ContainsQuadruple(quadruple));
        }

        [TestMethod]
        public void ShouldNotSelectQuadruplesBySubjectPredicateObject()
        {
            RDFQuadruple quadruple = new RDFQuadruple(new RDFContext(new Uri("ex:ctx")), new RDFResource("ex:subj"), new RDFResource("ex:pred"), new RDFResource("ex:obj"));

            RDFSQLServerStore store = new RDFSQLServerStore(GetConnectionString("RDFSQLServerStoreTest_ShouldNotSelectQuadruplesBySubjectPredicateObject"));
            store.AddQuadruple(quadruple);
            RDFMemoryStore result = store.SelectQuadruples(null, new RDFResource("ex:subj2"), new RDFResource("ex:pred"), new RDFResource("ex:obj"), null);

            Assert.IsNotNull(result);
            Assert.IsFalse(result.ContainsQuadruple(quadruple));
        }

        [TestMethod]
        public void ShouldSelectQuadruplesBySubjectPredicateLiteral()
        {
            RDFQuadruple quadruple = new RDFQuadruple(new RDFContext(new Uri("ex:ctx")), new RDFResource("ex:subj"), new RDFResource("ex:pred"), new RDFPlainLiteral("hello"));

            RDFSQLServerStore store = new RDFSQLServerStore(GetConnectionString("RDFSQLServerStoreTest_ShouldSelectQuadruplesBySubjectPredicateLiteral"));
            store.AddQuadruple(quadruple);
            RDFMemoryStore result = store.SelectQuadruples(null, new RDFResource("ex:subj"), new RDFResource("ex:pred"), null, new RDFPlainLiteral("hello"));

            Assert.IsNotNull(result);
            Assert.IsTrue(result.ContainsQuadruple(quadruple));
        }

        [TestMethod]
        public void ShouldNotSelectQuadruplesBySubjectPredicateLiteral()
        {
            RDFQuadruple quadruple = new RDFQuadruple(new RDFContext(new Uri("ex:ctx")), new RDFResource("ex:subj"), new RDFResource("ex:pred"), new RDFPlainLiteral("hello"));

            RDFSQLServerStore store = new RDFSQLServerStore(GetConnectionString("RDFSQLServerStoreTest_ShouldNotSelectQuadruplesBySubjectPredicateLiteral"));
            store.AddQuadruple(quadruple);
            RDFMemoryStore result = store.SelectQuadruples(null, new RDFResource("ex:subj2"), new RDFResource("ex:pred"), null, new RDFPlainLiteral("hello"));

            Assert.IsNotNull(result);
            Assert.IsFalse(result.ContainsQuadruple(quadruple));
        }
        #endregion        
    }
}
