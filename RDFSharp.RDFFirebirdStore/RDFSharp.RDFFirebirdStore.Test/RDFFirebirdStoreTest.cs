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
    public class RDFFirebirdStoreTest
    {
        private RDFFirebirdStoreEnums.RDFFirebirdVersion FirebirdVersion { get; set; } = RDFFirebirdStoreEnums.RDFFirebirdVersion.Firebird4;
        private string User { get; set; } = "SYSDBA";
        private string Password { get; set; } = "masterkey";
        private string DataSource { get; set; } = "localhost";
        private string Charset { get; set; } = "NONE";
        private int Port { get; set; } = 3050;
        private int Dialect { get; set; } = 3;
        private int ServerType { get; set; } = 0;

        private string GetConnectionString(string database)
            => $"User={User};Password={Password};Database={database};DataSource={DataSource};Port={Port};Dialect={Dialect};Charset={Charset};ServerType={ServerType};";

        #region Tests        
        [TestMethod]
        public void ShouldCreateStore()
        {
            File.Delete(Path.Combine(Environment.CurrentDirectory,"ShouldCreateStore.fdb"));
            RDFFirebirdStore store = new RDFFirebirdStore(
                GetConnectionString(Path.Combine(Environment.CurrentDirectory,"ShouldCreateStore.fdb")), FirebirdVersion);

            Assert.IsNotNull(store);
            Assert.IsTrue(string.Equals(store.StoreType, "FIREBIRD"));
            Assert.IsTrue(store.StoreID.Equals(RDFModelUtilities.CreateHash(store.ToString())));
            Assert.IsTrue(string.Equals(store.ToString(), string.Concat("FIREBIRD|SERVER=", store.Connection.DataSource, ";DATABASE=", store.Connection.Database)));
            Assert.IsTrue(File.Exists(Path.Combine(Environment.CurrentDirectory, "ShouldCreateStore.fdb")));
        }

        [TestMethod]
        public void ShouldThrowExceptionOnCreatingStoreBecauseNullOrEmptyPath()
            => Assert.ThrowsException<RDFStoreException>(() => new RDFFirebirdStore(null, FirebirdVersion));

        [TestMethod]
        public void ShouldThrowExceptionOnCreatingStoreBecauseUnaccessiblePath()
            => Assert.ThrowsException<RDFStoreException>(() => new RDFFirebirdStore(GetConnectionString("http://example.org/file.fdb"), FirebirdVersion));

        [TestMethod]
        public void ShouldCreateStoreUsingDispose()
        {
            File.Delete(Path.Combine(Environment.CurrentDirectory,"ShouldCreateStoreUsingDispose.fdb"));
            RDFFirebirdStore store = default;
            using(store = new RDFFirebirdStore(GetConnectionString(Path.Combine(Environment.CurrentDirectory, "ShouldCreateStoreUsingDispose.fdb")), FirebirdVersion))
            {
                Assert.IsNotNull(store);
                Assert.IsTrue(string.Equals(store.StoreType, "FIREBIRD"));
                Assert.IsTrue(store.StoreID.Equals(RDFModelUtilities.CreateHash(store.ToString())));
                Assert.IsTrue(string.Equals(store.ToString(), string.Concat("FIREBIRD|SERVER=", store.Connection.DataSource, ";DATABASE=", store.Connection.Database)));
                Assert.IsTrue(File.Exists(Path.Combine(Environment.CurrentDirectory, "ShouldCreateStoreUsingDispose.fdb")));
                Assert.IsFalse(store.Disposed);
            }

            Assert.IsTrue(store.Disposed);
            Assert.IsNull(store.Connection);
        }

        [TestMethod]
        public void ShouldCreateStoreInvokingDispose()
        {
            File.Delete(Path.Combine(Environment.CurrentDirectory,"ShouldCreateStoreInvokingDispose.fdb"));
            RDFFirebirdStore store = new RDFFirebirdStore(GetConnectionString(Path.Combine(Environment.CurrentDirectory, "ShouldCreateStoreInvokingDispose.fdb")), FirebirdVersion);

            Assert.IsNotNull(store);
            Assert.IsTrue(string.Equals(store.StoreType, "FIREBIRD"));
            Assert.IsTrue(store.StoreID.Equals(RDFModelUtilities.CreateHash(store.ToString())));
            Assert.IsTrue(string.Equals(store.ToString(), string.Concat("FIREBIRD|SERVER=", store.Connection.DataSource, ";DATABASE=", store.Connection.Database)));
            Assert.IsTrue(File.Exists(Path.Combine(Environment.CurrentDirectory, "ShouldCreateStoreInvokingDispose.fdb")));
            Assert.IsFalse(store.Disposed);

            store.Dispose();
            Assert.IsTrue(store.Disposed);
            Assert.IsNull(store.Connection);
        }

        [TestMethod]
        public void ShouldAddQuadruple()
        {
            File.Delete(Path.Combine(Environment.CurrentDirectory,"ShouldAddQuadruple.fdb"));
            RDFQuadruple quadruple = new RDFQuadruple(new RDFContext(new Uri("ex:ctx")), new RDFResource("ex:subj"), new RDFResource("ex:pred"), new RDFResource("ex:obj"));

            RDFFirebirdStore store = new RDFFirebirdStore(GetConnectionString(Path.Combine(Environment.CurrentDirectory, "ShouldAddQuadruple.fdb")), FirebirdVersion);
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
            File.Delete(Path.Combine(Environment.CurrentDirectory,"ShouldMergeGraph.fdb"));
            RDFGraph graph = new RDFGraph(new List<RDFTriple>() {
                new RDFTriple(new RDFResource("ex:subj"), new RDFResource("ex:pred"), new RDFResource("ex:obj"))
            }).SetContext(new Uri("ex:ctx"));

            RDFFirebirdStore store = new RDFFirebirdStore(GetConnectionString(Path.Combine(Environment.CurrentDirectory, "ShouldMergeGraph.fdb")), FirebirdVersion);
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
            File.Delete(Path.Combine(Environment.CurrentDirectory,"ShouldRemoveQuadruple.fdb"));
            RDFQuadruple quadruple = new RDFQuadruple(new RDFContext(new Uri("ex:ctx")), new RDFResource("ex:subj"), new RDFResource("ex:pred"), new RDFResource("ex:obj"));

            RDFFirebirdStore store = new RDFFirebirdStore(GetConnectionString(Path.Combine(Environment.CurrentDirectory, "ShouldRemoveQuadruple.fdb")), FirebirdVersion);
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
            File.Delete(Path.Combine(Environment.CurrentDirectory,"ShouldNotRemoveQuadruple.fdb"));
            RDFQuadruple quadruple = new RDFQuadruple(new RDFContext(new Uri("ex:ctx")), new RDFResource("ex:subj"), new RDFResource("ex:pred"), new RDFResource("ex:obj"));
            RDFQuadruple quadruple2 = new RDFQuadruple(new RDFContext(new Uri("ex:ctx2")), new RDFResource("ex:subj"), new RDFResource("ex:pred"), new RDFResource("ex:obj"));

            RDFFirebirdStore store = new RDFFirebirdStore(GetConnectionString(Path.Combine(Environment.CurrentDirectory, "ShouldNotRemoveQuadruple.fdb")), FirebirdVersion);
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
            File.Delete(Path.Combine(Environment.CurrentDirectory,"ShouldRemoveQuadruplesByContext.fdb"));
            RDFQuadruple quadruple = new RDFQuadruple(new RDFContext(new Uri("ex:ctx")), new RDFResource("ex:subj"), new RDFResource("ex:pred"), new RDFResource("ex:obj"));

            RDFFirebirdStore store = new RDFFirebirdStore(GetConnectionString(Path.Combine(Environment.CurrentDirectory, "ShouldRemoveQuadruplesByContext.fdb")), FirebirdVersion);
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
            File.Delete(Path.Combine(Environment.CurrentDirectory,"ShouldNotRemoveQuadruplesByContext.fdb"));
            RDFQuadruple quadruple = new RDFQuadruple(new RDFContext(new Uri("ex:ctx")), new RDFResource("ex:subj"), new RDFResource("ex:pred"), new RDFResource("ex:obj"));

            RDFFirebirdStore store = new RDFFirebirdStore(GetConnectionString(Path.Combine(Environment.CurrentDirectory, "ShouldNotRemoveQuadruplesByContext.fdb")), FirebirdVersion);
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
            File.Delete(Path.Combine(Environment.CurrentDirectory,"ShouldRemoveQuadruplesBySubject.fdb"));
            RDFQuadruple quadruple = new RDFQuadruple(new RDFContext(new Uri("ex:ctx")), new RDFResource("ex:subj"), new RDFResource("ex:pred"), new RDFResource("ex:obj"));

            RDFFirebirdStore store = new RDFFirebirdStore(GetConnectionString(Path.Combine(Environment.CurrentDirectory, "ShouldRemoveQuadruplesBySubject.fdb")), FirebirdVersion);
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
            File.Delete(Path.Combine(Environment.CurrentDirectory,"ShouldNotRemoveQuadruplesBySubject.fdb"));
            RDFQuadruple quadruple = new RDFQuadruple(new RDFContext(new Uri("ex:ctx")), new RDFResource("ex:subj"), new RDFResource("ex:pred"), new RDFResource("ex:obj"));

            RDFFirebirdStore store = new RDFFirebirdStore(GetConnectionString(Path.Combine(Environment.CurrentDirectory, "ShouldNotRemoveQuadruplesBySubject.fdb")), FirebirdVersion);
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
            File.Delete(Path.Combine(Environment.CurrentDirectory,"ShouldRemoveQuadruplesByPredicate.fdb"));
            RDFQuadruple quadruple = new RDFQuadruple(new RDFContext(new Uri("ex:ctx")), new RDFResource("ex:subj"), new RDFResource("ex:pred"), new RDFResource("ex:obj"));

            RDFFirebirdStore store = new RDFFirebirdStore(GetConnectionString(Path.Combine(Environment.CurrentDirectory, "ShouldRemoveQuadruplesByPredicate.fdb")), FirebirdVersion);
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
            File.Delete(Path.Combine(Environment.CurrentDirectory,"ShouldNotRemoveQuadruplesByPredicate.fdb"));
            RDFQuadruple quadruple = new RDFQuadruple(new RDFContext(new Uri("ex:ctx")), new RDFResource("ex:subj"), new RDFResource("ex:pred"), new RDFResource("ex:obj"));

            RDFFirebirdStore store = new RDFFirebirdStore(GetConnectionString(Path.Combine(Environment.CurrentDirectory, "ShouldNotRemoveQuadruplesByPredicate.fdb")), FirebirdVersion);
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
            File.Delete(Path.Combine(Environment.CurrentDirectory,"ShouldRemoveQuadruplesByObject.fdb"));
            RDFQuadruple quadruple = new RDFQuadruple(new RDFContext(new Uri("ex:ctx")), new RDFResource("ex:subj"), new RDFResource("ex:pred"), new RDFResource("ex:obj"));

            RDFFirebirdStore store = new RDFFirebirdStore(GetConnectionString(Path.Combine(Environment.CurrentDirectory, "ShouldRemoveQuadruplesByObject.fdb")), FirebirdVersion);
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
            File.Delete(Path.Combine(Environment.CurrentDirectory,"ShouldNotRemoveQuadruplesByObject.fdb"));
            RDFQuadruple quadruple = new RDFQuadruple(new RDFContext(new Uri("ex:ctx")), new RDFResource("ex:subj"), new RDFResource("ex:pred"), new RDFResource("ex:obj"));

            RDFFirebirdStore store = new RDFFirebirdStore(GetConnectionString(Path.Combine(Environment.CurrentDirectory, "ShouldNotRemoveQuadruplesByObject.fdb")), FirebirdVersion);
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
            File.Delete(Path.Combine(Environment.CurrentDirectory,"ShouldRemoveQuadruplesByLiteral.fdb"));
            RDFQuadruple quadruple = new RDFQuadruple(new RDFContext(new Uri("ex:ctx")), new RDFResource("ex:subj"), new RDFResource("ex:pred"), new RDFPlainLiteral("hello"));

            RDFFirebirdStore store = new RDFFirebirdStore(GetConnectionString(Path.Combine(Environment.CurrentDirectory, "ShouldRemoveQuadruplesByLiteral.fdb")), FirebirdVersion);
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
            File.Delete(Path.Combine(Environment.CurrentDirectory,"ShouldNotRemoveQuadruplesByLiteral.fdb"));
            RDFQuadruple quadruple = new RDFQuadruple(new RDFContext(new Uri("ex:ctx")), new RDFResource("ex:subj"), new RDFResource("ex:pred"), new RDFPlainLiteral("hello"));

            RDFFirebirdStore store = new RDFFirebirdStore(GetConnectionString(Path.Combine(Environment.CurrentDirectory, "ShouldNotRemoveQuadruplesByLiteral.fdb")), FirebirdVersion);
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
            File.Delete(Path.Combine(Environment.CurrentDirectory,"ShouldRemoveQuadruplesByContextSubject.fdb"));
            RDFQuadruple quadruple = new RDFQuadruple(new RDFContext(new Uri("ex:ctx")), new RDFResource("ex:subj"), new RDFResource("ex:pred"), new RDFResource("ex:obj"));

            RDFFirebirdStore store = new RDFFirebirdStore(GetConnectionString(Path.Combine(Environment.CurrentDirectory, "ShouldRemoveQuadruplesByContextSubject.fdb")), FirebirdVersion);
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
            File.Delete(Path.Combine(Environment.CurrentDirectory,"ShouldNotRemoveQuadruplesByContextSubject.fdb"));
            RDFQuadruple quadruple = new RDFQuadruple(new RDFContext(new Uri("ex:ctx")), new RDFResource("ex:subj"), new RDFResource("ex:pred"), new RDFResource("ex:obj"));

            RDFFirebirdStore store = new RDFFirebirdStore(GetConnectionString(Path.Combine(Environment.CurrentDirectory, "ShouldNotRemoveQuadruplesByContextSubject.fdb")), FirebirdVersion);
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
            File.Delete(Path.Combine(Environment.CurrentDirectory,"ShouldRemoveQuadruplesByContextPredicate.fdb"));
            RDFQuadruple quadruple = new RDFQuadruple(new RDFContext(new Uri("ex:ctx")), new RDFResource("ex:subj"), new RDFResource("ex:pred"), new RDFResource("ex:obj"));

            RDFFirebirdStore store = new RDFFirebirdStore(GetConnectionString(Path.Combine(Environment.CurrentDirectory, "ShouldRemoveQuadruplesByContextPredicate.fdb")), FirebirdVersion);
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
            File.Delete(Path.Combine(Environment.CurrentDirectory,"ShouldNotRemoveQuadruplesByContextPredicate.fdb"));
            RDFQuadruple quadruple = new RDFQuadruple(new RDFContext(new Uri("ex:ctx")), new RDFResource("ex:subj"), new RDFResource("ex:pred"), new RDFResource("ex:obj"));

            RDFFirebirdStore store = new RDFFirebirdStore(GetConnectionString(Path.Combine(Environment.CurrentDirectory, "ShouldNotRemoveQuadruplesByContextPredicate.fdb")), FirebirdVersion);
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
            File.Delete(Path.Combine(Environment.CurrentDirectory,"ShouldRemoveQuadruplesByContextObject.fdb"));
            RDFQuadruple quadruple = new RDFQuadruple(new RDFContext(new Uri("ex:ctx")), new RDFResource("ex:subj"), new RDFResource("ex:pred"), new RDFResource("ex:obj"));

            RDFFirebirdStore store = new RDFFirebirdStore(GetConnectionString(Path.Combine(Environment.CurrentDirectory, "ShouldRemoveQuadruplesByContextObject.fdb")), FirebirdVersion);
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
            File.Delete(Path.Combine(Environment.CurrentDirectory,"ShouldNotRemoveQuadruplesByContextObject.fdb"));
            RDFQuadruple quadruple = new RDFQuadruple(new RDFContext(new Uri("ex:ctx")), new RDFResource("ex:subj"), new RDFResource("ex:pred"), new RDFResource("ex:obj"));

            RDFFirebirdStore store = new RDFFirebirdStore(GetConnectionString(Path.Combine(Environment.CurrentDirectory, "ShouldNotRemoveQuadruplesByContextObject.fdb")), FirebirdVersion);
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
            File.Delete(Path.Combine(Environment.CurrentDirectory,"ShouldRemoveQuadruplesByContextLiteral.fdb"));
            RDFQuadruple quadruple = new RDFQuadruple(new RDFContext(new Uri("ex:ctx")), new RDFResource("ex:subj"), new RDFResource("ex:pred"), new RDFPlainLiteral("hello"));

            RDFFirebirdStore store = new RDFFirebirdStore(GetConnectionString(Path.Combine(Environment.CurrentDirectory, "ShouldRemoveQuadruplesByContextLiteral.fdb")), FirebirdVersion);
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
            File.Delete(Path.Combine(Environment.CurrentDirectory,"ShouldNotRemoveQuadruplesByContextLiteral.fdb"));
            RDFQuadruple quadruple = new RDFQuadruple(new RDFContext(new Uri("ex:ctx")), new RDFResource("ex:subj"), new RDFResource("ex:pred"), new RDFPlainLiteral("hello"));

            RDFFirebirdStore store = new RDFFirebirdStore(GetConnectionString(Path.Combine(Environment.CurrentDirectory, "ShouldNotRemoveQuadruplesByContextLiteral.fdb")), FirebirdVersion);
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
            File.Delete(Path.Combine(Environment.CurrentDirectory,"ShouldRemoveQuadruplesByContextSubjectPredicate.fdb"));
            RDFQuadruple quadruple = new RDFQuadruple(new RDFContext(new Uri("ex:ctx")), new RDFResource("ex:subj"), new RDFResource("ex:pred"), new RDFResource("ex:obj"));

            RDFFirebirdStore store = new RDFFirebirdStore(GetConnectionString(Path.Combine(Environment.CurrentDirectory, "ShouldRemoveQuadruplesByContextSubjectPredicate.fdb")), FirebirdVersion);
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
            File.Delete(Path.Combine(Environment.CurrentDirectory,"ShouldNotRemoveQuadruplesByContextSubjectPredicate.fdb"));
            RDFQuadruple quadruple = new RDFQuadruple(new RDFContext(new Uri("ex:ctx")), new RDFResource("ex:subj"), new RDFResource("ex:pred"), new RDFResource("ex:obj"));

            RDFFirebirdStore store = new RDFFirebirdStore(GetConnectionString(Path.Combine(Environment.CurrentDirectory, "ShouldNotRemoveQuadruplesByContextSubjectPredicate.fdb")), FirebirdVersion);
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
            File.Delete(Path.Combine(Environment.CurrentDirectory,"ShouldRemoveQuadruplesByContextSubjectObject.fdb"));
            RDFQuadruple quadruple = new RDFQuadruple(new RDFContext(new Uri("ex:ctx")), new RDFResource("ex:subj"), new RDFResource("ex:pred"), new RDFResource("ex:obj"));

            RDFFirebirdStore store = new RDFFirebirdStore(GetConnectionString(Path.Combine(Environment.CurrentDirectory, "ShouldRemoveQuadruplesByContextSubjectObject.fdb")), FirebirdVersion);
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
            File.Delete(Path.Combine(Environment.CurrentDirectory,"ShouldNotRemoveQuadruplesByContextSubjectObject.fdb"));
            RDFQuadruple quadruple = new RDFQuadruple(new RDFContext(new Uri("ex:ctx")), new RDFResource("ex:subj"), new RDFResource("ex:pred"), new RDFResource("ex:obj"));

            RDFFirebirdStore store = new RDFFirebirdStore(GetConnectionString(Path.Combine(Environment.CurrentDirectory, "ShouldNotRemoveQuadruplesByContextSubjectObject.fdb")), FirebirdVersion);
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
            File.Delete(Path.Combine(Environment.CurrentDirectory,"ShouldRemoveQuadruplesByContextSubjectLiteral.fdb"));
            RDFQuadruple quadruple = new RDFQuadruple(new RDFContext(new Uri("ex:ctx")), new RDFResource("ex:subj"), new RDFResource("ex:pred"), new RDFPlainLiteral("hello"));

            RDFFirebirdStore store = new RDFFirebirdStore(GetConnectionString(Path.Combine(Environment.CurrentDirectory, "ShouldRemoveQuadruplesByContextSubjectLiteral.fdb")), FirebirdVersion);
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
            File.Delete(Path.Combine(Environment.CurrentDirectory,"ShouldNotRemoveQuadruplesByContextSubjectLiteral.fdb"));
            RDFQuadruple quadruple = new RDFQuadruple(new RDFContext(new Uri("ex:ctx")), new RDFResource("ex:subj"), new RDFResource("ex:pred"), new RDFPlainLiteral("hello"));

            RDFFirebirdStore store = new RDFFirebirdStore(GetConnectionString(Path.Combine(Environment.CurrentDirectory, "ShouldNotRemoveQuadruplesByContextSubjectLiteral.fdb")), FirebirdVersion);
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
            File.Delete(Path.Combine(Environment.CurrentDirectory,"ShouldRemoveQuadruplesByContextPredicateObject.fdb"));
            RDFQuadruple quadruple = new RDFQuadruple(new RDFContext(new Uri("ex:ctx")), new RDFResource("ex:subj"), new RDFResource("ex:pred"), new RDFResource("ex:obj"));

            RDFFirebirdStore store = new RDFFirebirdStore(GetConnectionString(Path.Combine(Environment.CurrentDirectory, "ShouldRemoveQuadruplesByContextPredicateObject.fdb")), FirebirdVersion);
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
            File.Delete(Path.Combine(Environment.CurrentDirectory,"ShouldNotRemoveQuadruplesByContextPredicateObject.fdb"));
            RDFQuadruple quadruple = new RDFQuadruple(new RDFContext(new Uri("ex:ctx")), new RDFResource("ex:subj"), new RDFResource("ex:pred"), new RDFResource("ex:obj"));

            RDFFirebirdStore store = new RDFFirebirdStore(GetConnectionString(Path.Combine(Environment.CurrentDirectory, "ShouldNotRemoveQuadruplesByContextPredicateObject.fdb")), FirebirdVersion);
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
            File.Delete(Path.Combine(Environment.CurrentDirectory,"ShouldRemoveQuadruplesByContextPredicateLiteral.fdb"));
            RDFQuadruple quadruple = new RDFQuadruple(new RDFContext(new Uri("ex:ctx")), new RDFResource("ex:subj"), new RDFResource("ex:pred"), new RDFPlainLiteral("hello"));

            RDFFirebirdStore store = new RDFFirebirdStore(GetConnectionString(Path.Combine(Environment.CurrentDirectory, "ShouldRemoveQuadruplesByContextPredicateLiteral.fdb")), FirebirdVersion);
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
            File.Delete(Path.Combine(Environment.CurrentDirectory,"ShouldNotRemoveQuadruplesByContextPredicateLiteral.fdb"));
            RDFQuadruple quadruple = new RDFQuadruple(new RDFContext(new Uri("ex:ctx")), new RDFResource("ex:subj"), new RDFResource("ex:pred"), new RDFPlainLiteral("hello"));

            RDFFirebirdStore store = new RDFFirebirdStore(GetConnectionString(Path.Combine(Environment.CurrentDirectory, "ShouldNotRemoveQuadruplesByContextPredicateLiteral.fdb")), FirebirdVersion);
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
            File.Delete(Path.Combine(Environment.CurrentDirectory,"ShouldRemoveQuadruplesBySubjectPredicate.fdb"));
            RDFQuadruple quadruple = new RDFQuadruple(new RDFContext(new Uri("ex:ctx")), new RDFResource("ex:subj"), new RDFResource("ex:pred"), new RDFResource("ex:obj"));

            RDFFirebirdStore store = new RDFFirebirdStore(GetConnectionString(Path.Combine(Environment.CurrentDirectory, "ShouldRemoveQuadruplesBySubjectPredicate.fdb")), FirebirdVersion);
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
            File.Delete(Path.Combine(Environment.CurrentDirectory,"ShouldNotRemoveQuadruplesBySubjectPredicate.fdb"));
            RDFQuadruple quadruple = new RDFQuadruple(new RDFContext(new Uri("ex:ctx")), new RDFResource("ex:subj"), new RDFResource("ex:pred"), new RDFResource("ex:obj"));

            RDFFirebirdStore store = new RDFFirebirdStore(GetConnectionString(Path.Combine(Environment.CurrentDirectory, "ShouldNotRemoveQuadruplesBySubjectPredicate.fdb")), FirebirdVersion);
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
            File.Delete(Path.Combine(Environment.CurrentDirectory,"ShouldRemoveQuadruplesBySubjectObject.fdb"));
            RDFQuadruple quadruple = new RDFQuadruple(new RDFContext(new Uri("ex:ctx")), new RDFResource("ex:subj"), new RDFResource("ex:pred"), new RDFResource("ex:obj"));

            RDFFirebirdStore store = new RDFFirebirdStore(GetConnectionString(Path.Combine(Environment.CurrentDirectory, "ShouldRemoveQuadruplesBySubjectObject.fdb")), FirebirdVersion);
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
            File.Delete(Path.Combine(Environment.CurrentDirectory,"ShouldNotRemoveQuadruplesBySubjectObject.fdb"));
            RDFQuadruple quadruple = new RDFQuadruple(new RDFContext(new Uri("ex:ctx")), new RDFResource("ex:subj"), new RDFResource("ex:pred"), new RDFResource("ex:obj"));

            RDFFirebirdStore store = new RDFFirebirdStore(GetConnectionString(Path.Combine(Environment.CurrentDirectory, "ShouldNotRemoveQuadruplesBySubjectObject.fdb")), FirebirdVersion);
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
            File.Delete(Path.Combine(Environment.CurrentDirectory,"ShouldRemoveQuadruplesBySubjectLiteral.fdb"));
            RDFQuadruple quadruple = new RDFQuadruple(new RDFContext(new Uri("ex:ctx")), new RDFResource("ex:subj"), new RDFResource("ex:pred"), new RDFPlainLiteral("hello"));

            RDFFirebirdStore store = new RDFFirebirdStore(GetConnectionString(Path.Combine(Environment.CurrentDirectory, "ShouldRemoveQuadruplesBySubjectLiteral.fdb")), FirebirdVersion);
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
            File.Delete(Path.Combine(Environment.CurrentDirectory,"ShouldNotRemoveQuadruplesBySubjectLiteral.fdb"));
            RDFQuadruple quadruple = new RDFQuadruple(new RDFContext(new Uri("ex:ctx")), new RDFResource("ex:subj"), new RDFResource("ex:pred"), new RDFPlainLiteral("hello"));

            RDFFirebirdStore store = new RDFFirebirdStore(GetConnectionString(Path.Combine(Environment.CurrentDirectory, "ShouldNotRemoveQuadruplesBySubjectLiteral.fdb")), FirebirdVersion);
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
            File.Delete(Path.Combine(Environment.CurrentDirectory,"ShouldRemoveQuadruplesByPredicateObject.fdb"));
            RDFQuadruple quadruple = new RDFQuadruple(new RDFContext(new Uri("ex:ctx")), new RDFResource("ex:subj"), new RDFResource("ex:pred"), new RDFResource("ex:obj"));

            RDFFirebirdStore store = new RDFFirebirdStore(GetConnectionString(Path.Combine(Environment.CurrentDirectory, "ShouldRemoveQuadruplesByPredicateObject.fdb")), FirebirdVersion);
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
            File.Delete(Path.Combine(Environment.CurrentDirectory,"ShouldNotRemoveQuadruplesByPredicateObject.fdb"));
            RDFQuadruple quadruple = new RDFQuadruple(new RDFContext(new Uri("ex:ctx")), new RDFResource("ex:subj"), new RDFResource("ex:pred"), new RDFResource("ex:obj"));

            RDFFirebirdStore store = new RDFFirebirdStore(GetConnectionString(Path.Combine(Environment.CurrentDirectory, "ShouldNotRemoveQuadruplesByPredicateObject.fdb")), FirebirdVersion);
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
            File.Delete(Path.Combine(Environment.CurrentDirectory,"ShouldRemoveQuadruplesByPredicateLiteral.fdb"));
            RDFQuadruple quadruple = new RDFQuadruple(new RDFContext(new Uri("ex:ctx")), new RDFResource("ex:subj"), new RDFResource("ex:pred"), new RDFPlainLiteral("hello"));

            RDFFirebirdStore store = new RDFFirebirdStore(GetConnectionString(Path.Combine(Environment.CurrentDirectory, "ShouldRemoveQuadruplesByPredicateLiteral.fdb")), FirebirdVersion);
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
            File.Delete(Path.Combine(Environment.CurrentDirectory,"ShouldNotRemoveQuadruplesByPredicateLiteral.fdb"));
            RDFQuadruple quadruple = new RDFQuadruple(new RDFContext(new Uri("ex:ctx")), new RDFResource("ex:subj"), new RDFResource("ex:pred"), new RDFPlainLiteral("hello"));

            RDFFirebirdStore store = new RDFFirebirdStore(GetConnectionString(Path.Combine(Environment.CurrentDirectory, "ShouldNotRemoveQuadruplesByPredicateLiteral.fdb")), FirebirdVersion);
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
            File.Delete(Path.Combine(Environment.CurrentDirectory,"ShouldClearQuadruples.fdb"));
            RDFQuadruple quadruple = new RDFQuadruple(new RDFContext(new Uri("ex:ctx")), new RDFResource("ex:subj"), new RDFResource("ex:pred"), new RDFPlainLiteral("hello"));

            RDFFirebirdStore store = new RDFFirebirdStore(GetConnectionString(Path.Combine(Environment.CurrentDirectory, "ShouldClearQuadruples.fdb")), FirebirdVersion);
            store.ClearQuadruples();

            RDFMemoryStore memStore = store.SelectAllQuadruples();

            Assert.IsNotNull(memStore);
            Assert.IsTrue(memStore.QuadruplesCount == 0);
        }

        [TestMethod]
        public void ShouldContainQuadruple()
        {
            File.Delete(Path.Combine(Environment.CurrentDirectory,"ShouldContainQuadruple.fdb"));
            RDFQuadruple quadruple1 = new RDFQuadruple(new RDFContext(new Uri("ex:ctx")), new RDFResource("ex:subj"), new RDFResource("ex:pred"), new RDFPlainLiteral("hello"));
            RDFQuadruple quadruple2 = new RDFQuadruple(new RDFContext(new Uri("ex:ctx2")), new RDFResource("ex:subj"), new RDFResource("ex:pred"), new RDFPlainLiteral("hello"));

            RDFFirebirdStore store = new RDFFirebirdStore(GetConnectionString(Path.Combine(Environment.CurrentDirectory, "ShouldContainQuadruple.fdb")), FirebirdVersion);
            store.AddQuadruple(quadruple1);
            store.AddQuadruple(quadruple2);

            Assert.IsTrue(store.ContainsQuadruple(quadruple1));
            Assert.IsTrue(store.ContainsQuadruple(quadruple2));
        }

        [TestMethod]
        public void ShouldNotContainQuadruple()
        {
            File.Delete(Path.Combine(Environment.CurrentDirectory,"ShouldNotContainQuadruple.fdb"));
            RDFQuadruple quadruple = new RDFQuadruple(new RDFContext(new Uri("ex:ctx")), new RDFResource("ex:subj"), new RDFResource("ex:pred"), new RDFPlainLiteral("hello"));
            RDFQuadruple quadruple2 = new RDFQuadruple(new RDFContext(new Uri("ex:ctx2")), new RDFResource("ex:subj"), new RDFResource("ex:pred"), new RDFPlainLiteral("hello"));

            RDFFirebirdStore store = new RDFFirebirdStore(GetConnectionString(Path.Combine(Environment.CurrentDirectory, "ShouldNotContainQuadruple.fdb")), FirebirdVersion);
            store.AddQuadruple(quadruple);

            Assert.IsFalse(store.ContainsQuadruple(quadruple2));
            Assert.IsFalse(store.ContainsQuadruple(null));
        }

        [TestMethod]
        public void ShouldSelectQuadruplesByContext()
        {
            File.Delete(Path.Combine(Environment.CurrentDirectory,"ShouldSelectQuadruplesByContext.fdb"));
            RDFQuadruple quadruple = new RDFQuadruple(new RDFContext(new Uri("ex:ctx")), new RDFResource("ex:subj"), new RDFResource("ex:pred"), new RDFResource("ex:obj"));

            RDFFirebirdStore store = new RDFFirebirdStore(GetConnectionString(Path.Combine(Environment.CurrentDirectory, "ShouldSelectQuadruplesByContext.fdb")), FirebirdVersion);
            store.AddQuadruple(quadruple);
            RDFMemoryStore result = store.SelectQuadruplesByContext(new RDFContext(new Uri("ex:ctx")));

            Assert.IsNotNull(result);
            Assert.IsTrue(result.ContainsQuadruple(quadruple));
        }

        [TestMethod]
        public void ShouldNotSelectQuadruplesByContext()
        {
            File.Delete(Path.Combine(Environment.CurrentDirectory,"ShouldNotSelectQuadruplesByContext.fdb"));
            RDFQuadruple quadruple = new RDFQuadruple(new RDFContext(new Uri("ex:ctx")), new RDFResource("ex:subj"), new RDFResource("ex:pred"), new RDFResource("ex:obj"));

            RDFFirebirdStore store = new RDFFirebirdStore(GetConnectionString(Path.Combine(Environment.CurrentDirectory, "ShouldNotSelectQuadruplesByContext.fdb")), FirebirdVersion);
            store.AddQuadruple(quadruple);
            RDFMemoryStore result = store.SelectQuadruplesByContext(new RDFContext(new Uri("ex:ctx2")));

            Assert.IsNotNull(result);
            Assert.IsFalse(result.ContainsQuadruple(quadruple));
        }

        [TestMethod]
        public void ShouldSelectQuadruplesBySubject()
        {
            File.Delete(Path.Combine(Environment.CurrentDirectory,"ShouldSelectQuadruplesBySubject.fdb"));
            RDFQuadruple quadruple = new RDFQuadruple(new RDFContext(new Uri("ex:ctx")), new RDFResource("ex:subj"), new RDFResource("ex:pred"), new RDFResource("ex:obj"));

            RDFFirebirdStore store = new RDFFirebirdStore(GetConnectionString(Path.Combine(Environment.CurrentDirectory, "ShouldSelectQuadruplesBySubject.fdb")), FirebirdVersion);
            store.AddQuadruple(quadruple);
            RDFMemoryStore result = store.SelectQuadruplesBySubject(new RDFResource("ex:subj"));

            Assert.IsNotNull(result);
            Assert.IsTrue(result.ContainsQuadruple(quadruple));
        }

        [TestMethod]
        public void ShouldNotSelectQuadruplesBySubject()
        {
            File.Delete(Path.Combine(Environment.CurrentDirectory,"ShouldNotSelectQuadruplesBySubject.fdb"));
            RDFQuadruple quadruple = new RDFQuadruple(new RDFContext(new Uri("ex:ctx")), new RDFResource("ex:subj"), new RDFResource("ex:pred"), new RDFResource("ex:obj"));

            RDFFirebirdStore store = new RDFFirebirdStore(GetConnectionString(Path.Combine(Environment.CurrentDirectory, "ShouldNotSelectQuadruplesBySubject.fdb")), FirebirdVersion);
            store.AddQuadruple(quadruple);
            RDFMemoryStore result = store.SelectQuadruplesBySubject(new RDFResource("ex:subj2"));

            Assert.IsNotNull(result);
            Assert.IsFalse(result.ContainsQuadruple(quadruple));
        }

        [TestMethod]
        public void ShouldSelectQuadruplesByPredicate()
        {
            File.Delete(Path.Combine(Environment.CurrentDirectory,"ShouldSelectQuadruplesByPredicate.fdb"));
            RDFQuadruple quadruple = new RDFQuadruple(new RDFContext(new Uri("ex:ctx")), new RDFResource("ex:subj"), new RDFResource("ex:pred"), new RDFResource("ex:obj"));

            RDFFirebirdStore store = new RDFFirebirdStore(GetConnectionString(Path.Combine(Environment.CurrentDirectory, "ShouldSelectQuadruplesByPredicate.fdb")), FirebirdVersion);
            store.AddQuadruple(quadruple);
            RDFMemoryStore result = store.SelectQuadruplesByPredicate(new RDFResource("ex:pred"));

            Assert.IsNotNull(result);
            Assert.IsTrue(result.ContainsQuadruple(quadruple));
        }

        [TestMethod]
        public void ShouldNotSelectQuadruplesByPredicate()
        {
            File.Delete(Path.Combine(Environment.CurrentDirectory,"ShouldNotSelectQuadruplesByPredicate.fdb"));
            RDFQuadruple quadruple = new RDFQuadruple(new RDFContext(new Uri("ex:ctx")), new RDFResource("ex:subj"), new RDFResource("ex:pred"), new RDFResource("ex:obj"));

            RDFFirebirdStore store = new RDFFirebirdStore(GetConnectionString(Path.Combine(Environment.CurrentDirectory, "ShouldNotSelectQuadruplesByPredicate.fdb")), FirebirdVersion);
            store.AddQuadruple(quadruple);
            RDFMemoryStore result = store.SelectQuadruplesByPredicate(new RDFResource("ex:pred2"));

            Assert.IsNotNull(result);
            Assert.IsFalse(result.ContainsQuadruple(quadruple));
        }

        [TestMethod]
        public void ShouldSelectQuadruplesByObject()
        {
            File.Delete(Path.Combine(Environment.CurrentDirectory,"ShouldSelectQuadruplesByObject.fdb"));
            RDFQuadruple quadruple = new RDFQuadruple(new RDFContext(new Uri("ex:ctx")), new RDFResource("ex:subj"), new RDFResource("ex:pred"), new RDFResource("ex:obj"));

            RDFFirebirdStore store = new RDFFirebirdStore(GetConnectionString(Path.Combine(Environment.CurrentDirectory, "ShouldSelectQuadruplesByObject.fdb")), FirebirdVersion);
            store.AddQuadruple(quadruple);
            RDFMemoryStore result = store.SelectQuadruplesByObject(new RDFResource("ex:obj"));

            Assert.IsNotNull(result);
            Assert.IsTrue(result.ContainsQuadruple(quadruple));
        }

        [TestMethod]
        public void ShouldNotSelectQuadruplesByObject()
        {
            File.Delete(Path.Combine(Environment.CurrentDirectory,"ShouldNotSelectQuadruplesByObject.fdb"));
            RDFQuadruple quadruple = new RDFQuadruple(new RDFContext(new Uri("ex:ctx")), new RDFResource("ex:subj"), new RDFResource("ex:pred"), new RDFResource("ex:obj"));

            RDFFirebirdStore store = new RDFFirebirdStore(GetConnectionString(Path.Combine(Environment.CurrentDirectory, "ShouldNotSelectQuadruplesByObject.fdb")), FirebirdVersion);
            store.AddQuadruple(quadruple);
            RDFMemoryStore result = store.SelectQuadruplesByObject(new RDFResource("ex:obj2"));

            Assert.IsNotNull(result);
            Assert.IsFalse(result.ContainsQuadruple(quadruple));
        }

        [TestMethod]
        public void ShouldSelectQuadruplesByLiteral()
        {
            File.Delete(Path.Combine(Environment.CurrentDirectory,"ShouldSelectQuadruplesByLiteral.fdb"));
            RDFQuadruple quadruple = new RDFQuadruple(new RDFContext(new Uri("ex:ctx")), new RDFResource("ex:subj"), new RDFResource("ex:pred"), new RDFPlainLiteral("hello"));

            RDFFirebirdStore store = new RDFFirebirdStore(GetConnectionString(Path.Combine(Environment.CurrentDirectory, "ShouldSelectQuadruplesByLiteral.fdb")), FirebirdVersion);
            store.AddQuadruple(quadruple);
            RDFMemoryStore result = store.SelectQuadruplesByLiteral(new RDFPlainLiteral("hello"));

            Assert.IsNotNull(result);
            Assert.IsTrue(result.ContainsQuadruple(quadruple));
        }

        [TestMethod]
        public void ShouldNotSelectQuadruplesByLiteral()
        {
            File.Delete(Path.Combine(Environment.CurrentDirectory,"ShouldNotSelectQuadruplesByLiteral.fdb"));
            RDFQuadruple quadruple = new RDFQuadruple(new RDFContext(new Uri("ex:ctx")), new RDFResource("ex:subj"), new RDFResource("ex:pred"), new RDFPlainLiteral("hello"));

            RDFFirebirdStore store = new RDFFirebirdStore(GetConnectionString(Path.Combine(Environment.CurrentDirectory, "ShouldNotSelectQuadruplesByLiteral.fdb")), FirebirdVersion);
            store.AddQuadruple(quadruple);
            RDFMemoryStore result = store.SelectQuadruplesByLiteral(new RDFPlainLiteral("hello","en"));

            Assert.IsNotNull(result);
            Assert.IsFalse(result.ContainsQuadruple(quadruple));
        }

        [TestMethod]
        public void ShouldSelectQuadruplesByContextSubject()
        {
            File.Delete(Path.Combine(Environment.CurrentDirectory,"ShouldSelectQuadruplesByContextSubject.fdb"));
            RDFQuadruple quadruple = new RDFQuadruple(new RDFContext(new Uri("ex:ctx")), new RDFResource("ex:subj"), new RDFResource("ex:pred"), new RDFResource("ex:obj"));

            RDFFirebirdStore store = new RDFFirebirdStore(GetConnectionString(Path.Combine(Environment.CurrentDirectory, "ShouldSelectQuadruplesByContextSubject.fdb")), FirebirdVersion);
            store.AddQuadruple(quadruple);
            RDFMemoryStore result = store.SelectQuadruples(new RDFContext(new Uri("ex:ctx")), new RDFResource("ex:subj"), null, null, null);

            Assert.IsNotNull(result);
            Assert.IsTrue(result.ContainsQuadruple(quadruple));
        }

        [TestMethod]
        public void ShouldNotSelectQuadruplesByContextSubject()
        {
            File.Delete(Path.Combine(Environment.CurrentDirectory,"ShouldNotSelectQuadruplesByContextSubject.fdb"));
            RDFQuadruple quadruple = new RDFQuadruple(new RDFContext(new Uri("ex:ctx")), new RDFResource("ex:subj"), new RDFResource("ex:pred"), new RDFResource("ex:obj"));

            RDFFirebirdStore store = new RDFFirebirdStore(GetConnectionString(Path.Combine(Environment.CurrentDirectory, "ShouldNotSelectQuadruplesByContextSubject.fdb")), FirebirdVersion);
            store.AddQuadruple(quadruple);
            RDFMemoryStore result = store.SelectQuadruples(new RDFContext(new Uri("ex:ctx2")), new RDFResource("ex:subj"), null, null, null);

            Assert.IsNotNull(result);
            Assert.IsFalse(result.ContainsQuadruple(quadruple));
        }

        [TestMethod]
        public void ShouldSelectQuadruplesByContextPredicate()
        {
            File.Delete(Path.Combine(Environment.CurrentDirectory,"ShouldSelectQuadruplesByContextPredicate.fdb"));
            RDFQuadruple quadruple = new RDFQuadruple(new RDFContext(new Uri("ex:ctx")), new RDFResource("ex:subj"), new RDFResource("ex:pred"), new RDFResource("ex:obj"));

            RDFFirebirdStore store = new RDFFirebirdStore(GetConnectionString(Path.Combine(Environment.CurrentDirectory, "ShouldSelectQuadruplesByContextPredicate.fdb")), FirebirdVersion);
            store.AddQuadruple(quadruple);
            RDFMemoryStore result = store.SelectQuadruples(new RDFContext(new Uri("ex:ctx")), null, new RDFResource("ex:pred"), null, null);

            Assert.IsNotNull(result);
            Assert.IsTrue(result.ContainsQuadruple(quadruple));
        }

        [TestMethod]
        public void ShouldNotSelectQuadruplesByContextPredicate()
        {
            File.Delete(Path.Combine(Environment.CurrentDirectory,"ShouldNotSelectQuadruplesByContextPredicate.fdb"));
            RDFQuadruple quadruple = new RDFQuadruple(new RDFContext(new Uri("ex:ctx")), new RDFResource("ex:subj"), new RDFResource("ex:pred"), new RDFResource("ex:obj"));

            RDFFirebirdStore store = new RDFFirebirdStore(GetConnectionString(Path.Combine(Environment.CurrentDirectory, "ShouldNotSelectQuadruplesByContextPredicate.fdb")), FirebirdVersion);
            store.AddQuadruple(quadruple);
            RDFMemoryStore result = store.SelectQuadruples(new RDFContext(new Uri("ex:ctx2")), null, new RDFResource("ex:pred"), null, null);

            Assert.IsNotNull(result);
            Assert.IsFalse(result.ContainsQuadruple(quadruple));
        }

        [TestMethod]
        public void ShouldSelectQuadruplesByContextObject()
        {
            File.Delete(Path.Combine(Environment.CurrentDirectory,"ShouldSelectQuadruplesByContextObject.fdb"));
            RDFQuadruple quadruple = new RDFQuadruple(new RDFContext(new Uri("ex:ctx")), new RDFResource("ex:subj"), new RDFResource("ex:pred"), new RDFResource("ex:obj"));

            RDFFirebirdStore store = new RDFFirebirdStore(GetConnectionString(Path.Combine(Environment.CurrentDirectory, "ShouldSelectQuadruplesByContextObject.fdb")), FirebirdVersion);
            store.AddQuadruple(quadruple);
            RDFMemoryStore result = store.SelectQuadruples(new RDFContext(new Uri("ex:ctx")), null, null, new RDFResource("ex:obj"), null);

            Assert.IsNotNull(result);
            Assert.IsTrue(result.ContainsQuadruple(quadruple));
        }

        [TestMethod]
        public void ShouldNotSelectQuadruplesByContextObject()
        {
            File.Delete(Path.Combine(Environment.CurrentDirectory,"ShouldNotSelectQuadruplesByContextObject.fdb"));
            RDFQuadruple quadruple = new RDFQuadruple(new RDFContext(new Uri("ex:ctx")), new RDFResource("ex:subj"), new RDFResource("ex:pred"), new RDFResource("ex:obj"));

            RDFFirebirdStore store = new RDFFirebirdStore(GetConnectionString(Path.Combine(Environment.CurrentDirectory, "ShouldNotSelectQuadruplesByContextObject.fdb")), FirebirdVersion);
            store.AddQuadruple(quadruple);
            RDFMemoryStore result = store.SelectQuadruples(new RDFContext(new Uri("ex:ctx2")), null, null, new RDFResource("ex:obj"), null);

            Assert.IsNotNull(result);
            Assert.IsFalse(result.ContainsQuadruple(quadruple));
        }

        [TestMethod]
        public void ShouldSelectQuadruplesByContextLiteral()
        {
            File.Delete(Path.Combine(Environment.CurrentDirectory,"ShouldSelectQuadruplesByContextLiteral.fdb"));
            RDFQuadruple quadruple = new RDFQuadruple(new RDFContext(new Uri("ex:ctx")), new RDFResource("ex:subj"), new RDFResource("ex:pred"), new RDFPlainLiteral("hello"));

            RDFFirebirdStore store = new RDFFirebirdStore(GetConnectionString(Path.Combine(Environment.CurrentDirectory, "ShouldSelectQuadruplesByContextLiteral.fdb")), FirebirdVersion);
            store.AddQuadruple(quadruple);
            RDFMemoryStore result = store.SelectQuadruples(new RDFContext(new Uri("ex:ctx")), null, null, null, new RDFPlainLiteral("hello"));

            Assert.IsNotNull(result);
            Assert.IsTrue(result.ContainsQuadruple(quadruple));
        }

        [TestMethod]
        public void ShouldNotSelectQuadruplesByContextLiteral()
        {
            File.Delete(Path.Combine(Environment.CurrentDirectory,"ShouldNotSelectQuadruplesByContextLiteral.fdb"));
            RDFQuadruple quadruple = new RDFQuadruple(new RDFContext(new Uri("ex:ctx")), new RDFResource("ex:subj"), new RDFResource("ex:pred"), new RDFPlainLiteral("hello"));

            RDFFirebirdStore store = new RDFFirebirdStore(GetConnectionString(Path.Combine(Environment.CurrentDirectory, "ShouldNotSelectQuadruplesByContextLiteral.fdb")), FirebirdVersion);
            store.AddQuadruple(quadruple);
            RDFMemoryStore result = store.SelectQuadruples(new RDFContext(new Uri("ex:ctx2")), null, null, null, new RDFPlainLiteral("hello"));

            Assert.IsNotNull(result);
            Assert.IsFalse(result.ContainsQuadruple(quadruple));
        }

        [TestMethod]
        public void ShouldSelectQuadruplesByContextSubjectPredicate()
        {
            File.Delete(Path.Combine(Environment.CurrentDirectory,"ShouldSelectQuadruplesByContextSubjectPredicate.fdb"));
            RDFQuadruple quadruple = new RDFQuadruple(new RDFContext(new Uri("ex:ctx")), new RDFResource("ex:subj"), new RDFResource("ex:pred"), new RDFResource("ex:obj"));

            RDFFirebirdStore store = new RDFFirebirdStore(GetConnectionString(Path.Combine(Environment.CurrentDirectory, "ShouldSelectQuadruplesByContextSubjectPredicate.fdb")), FirebirdVersion);
            store.AddQuadruple(quadruple);
            RDFMemoryStore result = store.SelectQuadruples(new RDFContext(new Uri("ex:ctx")), new RDFResource("ex:subj"), new RDFResource("ex:pred"), null, null);

            Assert.IsNotNull(result);
            Assert.IsTrue(result.ContainsQuadruple(quadruple));
        }

        [TestMethod]
        public void ShouldNotSelectQuadruplesByContextSubjectPredicate()
        {
            File.Delete(Path.Combine(Environment.CurrentDirectory,"ShouldNotSelectQuadruplesByContextSubjectPredicate.fdb"));
            RDFQuadruple quadruple = new RDFQuadruple(new RDFContext(new Uri("ex:ctx")), new RDFResource("ex:subj"), new RDFResource("ex:pred"), new RDFResource("ex:obj"));

            RDFFirebirdStore store = new RDFFirebirdStore(GetConnectionString(Path.Combine(Environment.CurrentDirectory, "ShouldNotSelectQuadruplesByContextSubjectPredicate.fdb")), FirebirdVersion);
            store.AddQuadruple(quadruple);
            RDFMemoryStore result = store.SelectQuadruples(new RDFContext(new Uri("ex:ctx2")), new RDFResource("ex:subj"), new RDFResource("ex:pred"), null, null);

            Assert.IsNotNull(result);
            Assert.IsFalse(result.ContainsQuadruple(quadruple));
        }

        [TestMethod]
        public void ShouldSelectQuadruplesByContextSubjectObject()
        {
            File.Delete(Path.Combine(Environment.CurrentDirectory,"ShouldSelectQuadruplesByContextSubjectObject.fdb"));
            RDFQuadruple quadruple = new RDFQuadruple(new RDFContext(new Uri("ex:ctx")), new RDFResource("ex:subj"), new RDFResource("ex:pred"), new RDFResource("ex:obj"));

            RDFFirebirdStore store = new RDFFirebirdStore(GetConnectionString(Path.Combine(Environment.CurrentDirectory, "ShouldSelectQuadruplesByContextSubjectObject.fdb")), FirebirdVersion);
            store.AddQuadruple(quadruple);
            RDFMemoryStore result = store.SelectQuadruples(new RDFContext(new Uri("ex:ctx")), new RDFResource("ex:subj"), null, new RDFResource("ex:obj"), null);

            Assert.IsNotNull(result);
            Assert.IsTrue(result.ContainsQuadruple(quadruple));
        }

        [TestMethod]
        public void ShouldNotSelectQuadruplesByContextSubjectObject()
        {
            File.Delete(Path.Combine(Environment.CurrentDirectory,"ShouldNotSelectQuadruplesByContextSubjectObject.fdb"));
            RDFQuadruple quadruple = new RDFQuadruple(new RDFContext(new Uri("ex:ctx")), new RDFResource("ex:subj"), new RDFResource("ex:pred"), new RDFResource("ex:obj"));

            RDFFirebirdStore store = new RDFFirebirdStore(GetConnectionString(Path.Combine(Environment.CurrentDirectory, "ShouldNotSelectQuadruplesByContextSubjectObject.fdb")), FirebirdVersion);
            store.AddQuadruple(quadruple);
            RDFMemoryStore result = store.SelectQuadruples(new RDFContext(new Uri("ex:ctx2")), new RDFResource("ex:subj"), null, new RDFResource("ex:obj"), null);

            Assert.IsNotNull(result);
            Assert.IsFalse(result.ContainsQuadruple(quadruple));
        }

        [TestMethod]
        public void ShouldSelectQuadruplesByContextSubjectLiteral()
        {
            File.Delete(Path.Combine(Environment.CurrentDirectory,"ShouldSelectQuadruplesByContextSubjectLiteral.fdb"));
            RDFQuadruple quadruple = new RDFQuadruple(new RDFContext(new Uri("ex:ctx")), new RDFResource("ex:subj"), new RDFResource("ex:pred"), new RDFPlainLiteral("hello"));

            RDFFirebirdStore store = new RDFFirebirdStore(GetConnectionString(Path.Combine(Environment.CurrentDirectory, "ShouldSelectQuadruplesByContextSubjectLiteral.fdb")), FirebirdVersion);
            store.AddQuadruple(quadruple);
            RDFMemoryStore result = store.SelectQuadruples(new RDFContext(new Uri("ex:ctx")), new RDFResource("ex:subj"), null, null, new RDFPlainLiteral("hello"));

            Assert.IsNotNull(result);
            Assert.IsTrue(result.ContainsQuadruple(quadruple));
        }

        [TestMethod]
        public void ShouldNotSelectQuadruplesByContextSubjectLiteral()
        {
            File.Delete(Path.Combine(Environment.CurrentDirectory,"ShouldNotSelectQuadruplesByContextSubjectLiteral.fdb"));
            RDFQuadruple quadruple = new RDFQuadruple(new RDFContext(new Uri("ex:ctx")), new RDFResource("ex:subj"), new RDFResource("ex:pred"), new RDFPlainLiteral("hello"));

            RDFFirebirdStore store = new RDFFirebirdStore(GetConnectionString(Path.Combine(Environment.CurrentDirectory, "ShouldNotSelectQuadruplesByContextSubjectLiteral.fdb")), FirebirdVersion);
            store.AddQuadruple(quadruple);
            RDFMemoryStore result = store.SelectQuadruples(new RDFContext(new Uri("ex:ctx2")), new RDFResource("ex:subj"), null, null, new RDFPlainLiteral("hello"));

            Assert.IsNotNull(result);
            Assert.IsFalse(result.ContainsQuadruple(quadruple));
        }

        [TestMethod]
        public void ShouldSelectQuadruplesByContextPredicateObject()
        {
            File.Delete(Path.Combine(Environment.CurrentDirectory,"ShouldSelectQuadruplesByContextPredicateObject.fdb"));
            RDFQuadruple quadruple = new RDFQuadruple(new RDFContext(new Uri("ex:ctx")), new RDFResource("ex:subj"), new RDFResource("ex:pred"), new RDFResource("ex:obj"));

            RDFFirebirdStore store = new RDFFirebirdStore(GetConnectionString(Path.Combine(Environment.CurrentDirectory, "ShouldSelectQuadruplesByContextPredicateObject.fdb")), FirebirdVersion);
            store.AddQuadruple(quadruple);
            RDFMemoryStore result = store.SelectQuadruples(new RDFContext(new Uri("ex:ctx")), null, new RDFResource("ex:pred"), new RDFResource("ex:obj"), null);

            Assert.IsNotNull(result);
            Assert.IsTrue(result.ContainsQuadruple(quadruple));
        }

        [TestMethod]
        public void ShouldNotSelectQuadruplesByContextPredicateObject()
        {
            File.Delete(Path.Combine(Environment.CurrentDirectory,"ShouldNotSelectQuadruplesByContextPredicateObject.fdb"));
            RDFQuadruple quadruple = new RDFQuadruple(new RDFContext(new Uri("ex:ctx")), new RDFResource("ex:subj"), new RDFResource("ex:pred"), new RDFResource("ex:obj"));

            RDFFirebirdStore store = new RDFFirebirdStore(GetConnectionString(Path.Combine(Environment.CurrentDirectory, "ShouldNotSelectQuadruplesByContextPredicateObject.fdb")), FirebirdVersion);
            store.AddQuadruple(quadruple);
            RDFMemoryStore result = store.SelectQuadruples(new RDFContext(new Uri("ex:ctx2")), null, new RDFResource("ex:pred"), new RDFResource("ex:obj"), null);

            Assert.IsNotNull(result);
            Assert.IsFalse(result.ContainsQuadruple(quadruple));
        }

        [TestMethod]
        public void ShouldSelectQuadruplesByContextPredicateLiteral()
        {
            File.Delete(Path.Combine(Environment.CurrentDirectory,"ShouldSelectQuadruplesByContextPredicateLiteral.fdb"));
            RDFQuadruple quadruple = new RDFQuadruple(new RDFContext(new Uri("ex:ctx")), new RDFResource("ex:subj"), new RDFResource("ex:pred"), new RDFPlainLiteral("hello"));

            RDFFirebirdStore store = new RDFFirebirdStore(GetConnectionString(Path.Combine(Environment.CurrentDirectory, "ShouldSelectQuadruplesByContextPredicateLiteral.fdb")), FirebirdVersion);
            store.AddQuadruple(quadruple);
            RDFMemoryStore result = store.SelectQuadruples(new RDFContext(new Uri("ex:ctx")), null, new RDFResource("ex:pred"), null, new RDFPlainLiteral("hello"));

            Assert.IsNotNull(result);
            Assert.IsTrue(result.ContainsQuadruple(quadruple));
        }

        [TestMethod]
        public void ShouldNotSelectQuadruplesByContextPredicateLiteral()
        {
            File.Delete(Path.Combine(Environment.CurrentDirectory,"ShouldNotSelectQuadruplesByContextPredicateLiteral.fdb"));
            RDFQuadruple quadruple = new RDFQuadruple(new RDFContext(new Uri("ex:ctx")), new RDFResource("ex:subj"), new RDFResource("ex:pred"), new RDFPlainLiteral("hello"));

            RDFFirebirdStore store = new RDFFirebirdStore(GetConnectionString(Path.Combine(Environment.CurrentDirectory, "ShouldNotSelectQuadruplesByContextPredicateLiteral.fdb")), FirebirdVersion);
            store.AddQuadruple(quadruple);
            RDFMemoryStore result = store.SelectQuadruples(new RDFContext(new Uri("ex:ctx2")), null, new RDFResource("ex:pred"), null, new RDFPlainLiteral("hello"));

            Assert.IsNotNull(result);
            Assert.IsFalse(result.ContainsQuadruple(quadruple));
        }

        [TestMethod]
        public void ShouldSelectQuadruplesByContextSubjectPredicateObject()
        {
            File.Delete(Path.Combine(Environment.CurrentDirectory,"ShouldSelectQuadruplesByContextSubjectPredicateObject.fdb"));
            RDFQuadruple quadruple = new RDFQuadruple(new RDFContext(new Uri("ex:ctx")), new RDFResource("ex:subj"), new RDFResource("ex:pred"), new RDFResource("ex:obj"));

            RDFFirebirdStore store = new RDFFirebirdStore(GetConnectionString(Path.Combine(Environment.CurrentDirectory, "ShouldSelectQuadruplesByContextSubjectPredicateObject.fdb")), FirebirdVersion);
            store.AddQuadruple(quadruple);
            RDFMemoryStore result = store.SelectQuadruples(new RDFContext(new Uri("ex:ctx")), new RDFResource("ex:subj"), new RDFResource("ex:pred"), new RDFResource("ex:obj"), null);

            Assert.IsNotNull(result);
            Assert.IsTrue(result.ContainsQuadruple(quadruple));
        }

        [TestMethod]
        public void ShouldNotSelectQuadruplesByContextSubjectPredicateObject()
        {
            File.Delete(Path.Combine(Environment.CurrentDirectory,"ShouldNotSelectQuadruplesByContextSubjectPredicateObject.fdb"));
            RDFQuadruple quadruple = new RDFQuadruple(new RDFContext(new Uri("ex:ctx")), new RDFResource("ex:subj"), new RDFResource("ex:pred"), new RDFResource("ex:obj"));

            RDFFirebirdStore store = new RDFFirebirdStore(GetConnectionString(Path.Combine(Environment.CurrentDirectory, "ShouldNotSelectQuadruplesByContextSubjectPredicateObject.fdb")), FirebirdVersion);
            store.AddQuadruple(quadruple);
            RDFMemoryStore result = store.SelectQuadruples(new RDFContext(new Uri("ex:ctx2")), new RDFResource("ex:subj"), new RDFResource("ex:pred"), new RDFResource("ex:obj"), null);

            Assert.IsNotNull(result);
            Assert.IsFalse(result.ContainsQuadruple(quadruple));
        }

        [TestMethod]
        public void ShouldSelectQuadruplesByContextSubjectPredicateLiteral()
        {
            File.Delete(Path.Combine(Environment.CurrentDirectory,"ShouldSelectQuadruplesByContextSubjectPredicateLiteral.fdb"));
            RDFQuadruple quadruple = new RDFQuadruple(new RDFContext(new Uri("ex:ctx")), new RDFResource("ex:subj"), new RDFResource("ex:pred"), new RDFPlainLiteral("hello"));

            RDFFirebirdStore store = new RDFFirebirdStore(GetConnectionString(Path.Combine(Environment.CurrentDirectory, "ShouldSelectQuadruplesByContextSubjectPredicateLiteral.fdb")), FirebirdVersion);
            store.AddQuadruple(quadruple);
            RDFMemoryStore result = store.SelectQuadruples(new RDFContext(new Uri("ex:ctx")), new RDFResource("ex:subj"), new RDFResource("ex:pred"), null, new RDFPlainLiteral("hello"));

            Assert.IsNotNull(result);
            Assert.IsTrue(result.ContainsQuadruple(quadruple));
        }

        [TestMethod]
        public void ShouldNotSelectQuadruplesByContextSubjectPredicateLiteral()
        {
            File.Delete(Path.Combine(Environment.CurrentDirectory,"ShouldNotSelectQuadruplesByContextSubjectPredicateLiteral.fdb"));
            RDFQuadruple quadruple = new RDFQuadruple(new RDFContext(new Uri("ex:ctx")), new RDFResource("ex:subj"), new RDFResource("ex:pred"), new RDFPlainLiteral("hello"));

            RDFFirebirdStore store = new RDFFirebirdStore(GetConnectionString(Path.Combine(Environment.CurrentDirectory, "ShouldNotSelectQuadruplesByContextSubjectPredicateLiteral.fdb")), FirebirdVersion);
            store.AddQuadruple(quadruple);
            RDFMemoryStore result = store.SelectQuadruples(new RDFContext(new Uri("ex:ctx2")), new RDFResource("ex:subj"), new RDFResource("ex:pred"), null, new RDFPlainLiteral("hello"));

            Assert.IsNotNull(result);
            Assert.IsFalse(result.ContainsQuadruple(quadruple));
        }

        [TestMethod]
        public void ShouldSelectQuadruplesBySubjectPredicate()
        {
            File.Delete(Path.Combine(Environment.CurrentDirectory,"ShouldSelectQuadruplesBySubjectPredicate.fdb"));
            RDFQuadruple quadruple = new RDFQuadruple(new RDFContext(new Uri("ex:ctx")), new RDFResource("ex:subj"), new RDFResource("ex:pred"), new RDFResource("ex:obj"));

            RDFFirebirdStore store = new RDFFirebirdStore(GetConnectionString(Path.Combine(Environment.CurrentDirectory, "ShouldSelectQuadruplesBySubjectPredicate.fdb")), FirebirdVersion);
            store.AddQuadruple(quadruple);
            RDFMemoryStore result = store.SelectQuadruples(null, new RDFResource("ex:subj"), new RDFResource("ex:pred"), null, null);

            Assert.IsNotNull(result);
            Assert.IsTrue(result.ContainsQuadruple(quadruple));
        }

        [TestMethod]
        public void ShouldNotSelectQuadruplesBySubjectPredicate()
        {
            File.Delete(Path.Combine(Environment.CurrentDirectory,"ShouldNotSelectQuadruplesBySubjectPredicate.fdb"));
            RDFQuadruple quadruple = new RDFQuadruple(new RDFContext(new Uri("ex:ctx")), new RDFResource("ex:subj"), new RDFResource("ex:pred"), new RDFResource("ex:obj"));

            RDFFirebirdStore store = new RDFFirebirdStore(GetConnectionString(Path.Combine(Environment.CurrentDirectory, "ShouldNotSelectQuadruplesBySubjectPredicate.fdb")), FirebirdVersion);
            store.AddQuadruple(quadruple);
            RDFMemoryStore result = store.SelectQuadruples(null, new RDFResource("ex:subj2"), new RDFResource("ex:pred"), null, null);

            Assert.IsNotNull(result);
            Assert.IsFalse(result.ContainsQuadruple(quadruple));
        }

        [TestMethod]
        public void ShouldSelectQuadruplesBySubjectObject()
        {
            File.Delete(Path.Combine(Environment.CurrentDirectory,"ShouldSelectQuadruplesBySubjectObject.fdb"));
            RDFQuadruple quadruple = new RDFQuadruple(new RDFContext(new Uri("ex:ctx")), new RDFResource("ex:subj"), new RDFResource("ex:pred"), new RDFResource("ex:obj"));

            RDFFirebirdStore store = new RDFFirebirdStore(GetConnectionString(Path.Combine(Environment.CurrentDirectory, "ShouldSelectQuadruplesBySubjectObject.fdb")), FirebirdVersion);
            store.AddQuadruple(quadruple);
            RDFMemoryStore result = store.SelectQuadruples(null, new RDFResource("ex:subj"), null, new RDFResource("ex:obj"), null);

            Assert.IsNotNull(result);
            Assert.IsTrue(result.ContainsQuadruple(quadruple));
        }

        [TestMethod]
        public void ShouldNotSelectQuadruplesBySubjectObject()
        {
            File.Delete(Path.Combine(Environment.CurrentDirectory,"ShouldNotSelectQuadruplesBySubjectObject.fdb"));
            RDFQuadruple quadruple = new RDFQuadruple(new RDFContext(new Uri("ex:ctx")), new RDFResource("ex:subj"), new RDFResource("ex:pred"), new RDFResource("ex:obj"));

            RDFFirebirdStore store = new RDFFirebirdStore(GetConnectionString(Path.Combine(Environment.CurrentDirectory, "ShouldNotSelectQuadruplesBySubjectObject.fdb")), FirebirdVersion);
            store.AddQuadruple(quadruple);
            RDFMemoryStore result = store.SelectQuadruples(null, new RDFResource("ex:subj2"), null, new RDFResource("ex:obj"), null);

            Assert.IsNotNull(result);
            Assert.IsFalse(result.ContainsQuadruple(quadruple));
        }

        [TestMethod]
        public void ShouldSelectQuadruplesBySubjectLiteral()
        {
            File.Delete(Path.Combine(Environment.CurrentDirectory,"ShouldSelectQuadruplesBySubjectLiteral.fdb"));
            RDFQuadruple quadruple = new RDFQuadruple(new RDFContext(new Uri("ex:ctx")), new RDFResource("ex:subj"), new RDFResource("ex:pred"), new RDFPlainLiteral("hello"));

            RDFFirebirdStore store = new RDFFirebirdStore(GetConnectionString(Path.Combine(Environment.CurrentDirectory, "ShouldSelectQuadruplesBySubjectLiteral.fdb")), FirebirdVersion);
            store.AddQuadruple(quadruple);
            RDFMemoryStore result = store.SelectQuadruples(null, new RDFResource("ex:subj"), null, null, new RDFPlainLiteral("hello"));

            Assert.IsNotNull(result);
            Assert.IsTrue(result.ContainsQuadruple(quadruple));
        }

        [TestMethod]
        public void ShouldNotSelectQuadruplesBySubjectLiteral()
        {
            File.Delete(Path.Combine(Environment.CurrentDirectory,"ShouldNotSelectQuadruplesBySubjectLiteral.fdb"));
            RDFQuadruple quadruple = new RDFQuadruple(new RDFContext(new Uri("ex:ctx")), new RDFResource("ex:subj"), new RDFResource("ex:pred"), new RDFPlainLiteral("hello"));

            RDFFirebirdStore store = new RDFFirebirdStore(GetConnectionString(Path.Combine(Environment.CurrentDirectory, "ShouldNotSelectQuadruplesBySubjectLiteral.fdb")), FirebirdVersion);
            store.AddQuadruple(quadruple);
            RDFMemoryStore result = store.SelectQuadruples(null, new RDFResource("ex:subj2"), null, null, new RDFPlainLiteral("hello"));

            Assert.IsNotNull(result);
            Assert.IsFalse(result.ContainsQuadruple(quadruple));
        }

        [TestMethod]
        public void ShouldSelectQuadruplesByPredicateObject()
        {
            File.Delete(Path.Combine(Environment.CurrentDirectory,"ShouldSelectQuadruplesByPredicateObject.fdb"));
            RDFQuadruple quadruple = new RDFQuadruple(new RDFContext(new Uri("ex:ctx")), new RDFResource("ex:subj"), new RDFResource("ex:pred"), new RDFResource("ex:obj"));

            RDFFirebirdStore store = new RDFFirebirdStore(GetConnectionString(Path.Combine(Environment.CurrentDirectory, "ShouldSelectQuadruplesByPredicateObject.fdb")), FirebirdVersion);
            store.AddQuadruple(quadruple);
            RDFMemoryStore result = store.SelectQuadruples(null, null, new RDFResource("ex:pred"), new RDFResource("ex:obj"), null);

            Assert.IsNotNull(result);
            Assert.IsTrue(result.ContainsQuadruple(quadruple));
        }

        [TestMethod]
        public void ShouldNotSelectQuadruplesByPredicateObject()
        {
            File.Delete(Path.Combine(Environment.CurrentDirectory,"ShouldNotSelectQuadruplesByPredicateObject.fdb"));
            RDFQuadruple quadruple = new RDFQuadruple(new RDFContext(new Uri("ex:ctx")), new RDFResource("ex:subj"), new RDFResource("ex:pred"), new RDFResource("ex:obj"));

            RDFFirebirdStore store = new RDFFirebirdStore(GetConnectionString(Path.Combine(Environment.CurrentDirectory, "ShouldNotSelectQuadruplesByPredicateObject.fdb")), FirebirdVersion);
            store.AddQuadruple(quadruple);
            RDFMemoryStore result = store.SelectQuadruples(null, null, new RDFResource("ex:pred2"), new RDFResource("ex:obj"), null);

            Assert.IsNotNull(result);
            Assert.IsFalse(result.ContainsQuadruple(quadruple));
        }

        [TestMethod]
        public void ShouldSelectQuadruplesByPredicateLiteral()
        {
            File.Delete(Path.Combine(Environment.CurrentDirectory,"ShouldSelectQuadruplesByPredicateLiteral.fdb"));
            RDFQuadruple quadruple = new RDFQuadruple(new RDFContext(new Uri("ex:ctx")), new RDFResource("ex:subj"), new RDFResource("ex:pred"), new RDFPlainLiteral("hello"));

            RDFFirebirdStore store = new RDFFirebirdStore(GetConnectionString(Path.Combine(Environment.CurrentDirectory, "ShouldSelectQuadruplesByPredicateLiteral.fdb")), FirebirdVersion);
            store.AddQuadruple(quadruple);
            RDFMemoryStore result = store.SelectQuadruples(null, null, new RDFResource("ex:pred"), null, new RDFPlainLiteral("hello"));

            Assert.IsNotNull(result);
            Assert.IsTrue(result.ContainsQuadruple(quadruple));
        }

        [TestMethod]
        public void ShouldNotSelectQuadruplesByPredicateLiteral()
        {
            File.Delete(Path.Combine(Environment.CurrentDirectory,"ShouldNotSelectQuadruplesByPredicateLiteral.fdb"));
            RDFQuadruple quadruple = new RDFQuadruple(new RDFContext(new Uri("ex:ctx")), new RDFResource("ex:subj"), new RDFResource("ex:pred"), new RDFPlainLiteral("hello"));

            RDFFirebirdStore store = new RDFFirebirdStore(GetConnectionString(Path.Combine(Environment.CurrentDirectory, "ShouldNotSelectQuadruplesByPredicateLiteral.fdb")), FirebirdVersion);
            store.AddQuadruple(quadruple);
            RDFMemoryStore result = store.SelectQuadruples(null, null, new RDFResource("ex:pred2"), null, new RDFPlainLiteral("hello"));

            Assert.IsNotNull(result);
            Assert.IsFalse(result.ContainsQuadruple(quadruple));
        }

        [TestMethod]
        public void ShouldSelectQuadruplesBySubjectPredicateObject()
        {
            File.Delete(Path.Combine(Environment.CurrentDirectory,"ShouldSelectQuadruplesBySubjectPredicateObject.fdb"));
            RDFQuadruple quadruple = new RDFQuadruple(new RDFContext(new Uri("ex:ctx")), new RDFResource("ex:subj"), new RDFResource("ex:pred"), new RDFResource("ex:obj"));

            RDFFirebirdStore store = new RDFFirebirdStore(GetConnectionString(Path.Combine(Environment.CurrentDirectory, "ShouldSelectQuadruplesBySubjectPredicateObject.fdb")), FirebirdVersion);
            store.AddQuadruple(quadruple);
            RDFMemoryStore result = store.SelectQuadruples(null, new RDFResource("ex:subj"), new RDFResource("ex:pred"), new RDFResource("ex:obj"), null);

            Assert.IsNotNull(result);
            Assert.IsTrue(result.ContainsQuadruple(quadruple));
        }

        [TestMethod]
        public void ShouldNotSelectQuadruplesBySubjectPredicateObject()
        {
            File.Delete(Path.Combine(Environment.CurrentDirectory,"ShouldNotSelectQuadruplesBySubjectPredicateObject.fdb"));
            RDFQuadruple quadruple = new RDFQuadruple(new RDFContext(new Uri("ex:ctx")), new RDFResource("ex:subj"), new RDFResource("ex:pred"), new RDFResource("ex:obj"));

            RDFFirebirdStore store = new RDFFirebirdStore(GetConnectionString(Path.Combine(Environment.CurrentDirectory, "ShouldNotSelectQuadruplesBySubjectPredicateObject.fdb")), FirebirdVersion);
            store.AddQuadruple(quadruple);
            RDFMemoryStore result = store.SelectQuadruples(null, new RDFResource("ex:subj2"), new RDFResource("ex:pred"), new RDFResource("ex:obj"), null);

            Assert.IsNotNull(result);
            Assert.IsFalse(result.ContainsQuadruple(quadruple));
        }

        [TestMethod]
        public void ShouldSelectQuadruplesBySubjectPredicateLiteral()
        {
            File.Delete(Path.Combine(Environment.CurrentDirectory,"ShouldSelectQuadruplesBySubjectPredicateLiteral.fdb"));
            RDFQuadruple quadruple = new RDFQuadruple(new RDFContext(new Uri("ex:ctx")), new RDFResource("ex:subj"), new RDFResource("ex:pred"), new RDFPlainLiteral("hello"));

            RDFFirebirdStore store = new RDFFirebirdStore(GetConnectionString(Path.Combine(Environment.CurrentDirectory, "ShouldSelectQuadruplesBySubjectPredicateLiteral.fdb")), FirebirdVersion);
            store.AddQuadruple(quadruple);
            RDFMemoryStore result = store.SelectQuadruples(null, new RDFResource("ex:subj"), new RDFResource("ex:pred"), null, new RDFPlainLiteral("hello"));

            Assert.IsNotNull(result);
            Assert.IsTrue(result.ContainsQuadruple(quadruple));
        }

        [TestMethod]
        public void ShouldNotSelectQuadruplesBySubjectPredicateLiteral()
        {
            File.Delete(Path.Combine(Environment.CurrentDirectory,"ShouldNotSelectQuadruplesBySubjectPredicateLiteral.fdb"));
            RDFQuadruple quadruple = new RDFQuadruple(new RDFContext(new Uri("ex:ctx")), new RDFResource("ex:subj"), new RDFResource("ex:pred"), new RDFPlainLiteral("hello"));

            RDFFirebirdStore store = new RDFFirebirdStore(GetConnectionString(Path.Combine(Environment.CurrentDirectory, "ShouldNotSelectQuadruplesBySubjectPredicateLiteral.fdb")), FirebirdVersion);
            store.AddQuadruple(quadruple);
            RDFMemoryStore result = store.SelectQuadruples(null, new RDFResource("ex:subj2"), new RDFResource("ex:pred"), null, new RDFPlainLiteral("hello"));

            Assert.IsNotNull(result);
            Assert.IsFalse(result.ContainsQuadruple(quadruple));
        }
        #endregion        
    }
}
