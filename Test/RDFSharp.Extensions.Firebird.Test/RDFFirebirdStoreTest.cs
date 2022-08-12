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
using RDFSharp.Store;
using System.Reflection;
using FirebirdSql.Data.FirebirdClient;

namespace RDFSharp.Extensions.Firebird.Test
{
    [TestClass]
    public class RDFFirebirdStoreTest
    {
        //This test suite is based on a local installation of Firebird4 having default SYSDBA credentials
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

        private T GetPropertyValue<T>(string propertyName, object workingObject)
        {
            List<string> propertyNameParts = propertyName.Split('.').ToList();
            PropertyInfo currentProperty = workingObject.GetType().GetProperty(propertyNameParts[0]);
            if (currentProperty == null) 
                return default;

            if (propertyName.IndexOf('.') > -1)
            {
                propertyNameParts.RemoveAt(0);
                return GetPropertyValue<T>(string.Join('.', propertyNameParts), currentProperty.GetValue(workingObject, null));
            }
            else
            {
                object result = currentProperty.GetValue(workingObject, null);
                return (result == null ?  default : (T)currentProperty.GetValue(workingObject, null));
            }   
        }

        #region Tests        
        [TestMethod]
        public void ShouldCreateStore()
        {
            File.Delete(Path.Combine(Environment.CurrentDirectory,"RDFFirebirdStoreTest_ShouldCreateStore.fdb"));
            RDFFirebirdStore store = new RDFFirebirdStore(
                GetConnectionString(Path.Combine(Environment.CurrentDirectory,"RDFFirebirdStoreTest_ShouldCreateStore.fdb")), FirebirdVersion);

            Assert.IsNotNull(store);
            Assert.IsTrue(string.Equals(store.StoreType, "FIREBIRD"));
            Assert.IsTrue(store.StoreID.Equals(RDFModelUtilities.CreateHash(store.ToString())));
            Assert.IsTrue(string.Equals(store.ToString(), string.Concat("FIREBIRD|SERVER=", GetPropertyValue<string>("Connection.DataSource", store), ";DATABASE=", GetPropertyValue<string>("Connection.Database", store))));
            Assert.IsTrue(File.Exists(Path.Combine(Environment.CurrentDirectory, "RDFFirebirdStoreTest_ShouldCreateStore.fdb")));
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
            File.Delete(Path.Combine(Environment.CurrentDirectory,"RDFFirebirdStoreTest_ShouldCreateStoreUsingDispose.fdb"));
            RDFFirebirdStore store = default;
            using(store = new RDFFirebirdStore(GetConnectionString(Path.Combine(Environment.CurrentDirectory, "RDFFirebirdStoreTest_ShouldCreateStoreUsingDispose.fdb")), FirebirdVersion))
            {
                Assert.IsNotNull(store);
                Assert.IsTrue(string.Equals(store.StoreType, "FIREBIRD"));
                Assert.IsTrue(store.StoreID.Equals(RDFModelUtilities.CreateHash(store.ToString())));
                Assert.IsTrue(string.Equals(store.ToString(), string.Concat("FIREBIRD|SERVER=", GetPropertyValue<string>("Connection.DataSource", store), ";DATABASE=", GetPropertyValue<string>("Connection.Database", store))));
                Assert.IsTrue(File.Exists(Path.Combine(Environment.CurrentDirectory, "RDFFirebirdStoreTest_ShouldCreateStoreUsingDispose.fdb")));
                Assert.IsFalse(GetPropertyValue<bool>("Disposed", store));
            }

            Assert.IsTrue(GetPropertyValue<bool>("Disposed", store));
            Assert.IsNull(GetPropertyValue<FbConnection>("Connection", store));
        }

        [TestMethod]
        public void ShouldCreateStoreInvokingDispose()
        {
            File.Delete(Path.Combine(Environment.CurrentDirectory,"RDFFirebirdStoreTest_ShouldCreateStoreInvokingDispose.fdb"));
            RDFFirebirdStore store = new RDFFirebirdStore(GetConnectionString(Path.Combine(Environment.CurrentDirectory, "RDFFirebirdStoreTest_ShouldCreateStoreInvokingDispose.fdb")), FirebirdVersion);

            Assert.IsNotNull(store);
            Assert.IsTrue(string.Equals(store.StoreType, "FIREBIRD"));
            Assert.IsTrue(store.StoreID.Equals(RDFModelUtilities.CreateHash(store.ToString())));
            Assert.IsTrue(string.Equals(store.ToString(), string.Concat("FIREBIRD|SERVER=", GetPropertyValue<string>("Connection.DataSource", store), ";DATABASE=", GetPropertyValue<string>("Connection.Database", store))));
            Assert.IsTrue(File.Exists(Path.Combine(Environment.CurrentDirectory, "RDFFirebirdStoreTest_ShouldCreateStoreInvokingDispose.fdb")));
            Assert.IsFalse(GetPropertyValue<bool>("Disposed", store));

            store.Dispose();
            Assert.IsTrue(GetPropertyValue<bool>("Disposed", store));
            Assert.IsNull(GetPropertyValue<FbConnection>("Connection", store));
        }

        [TestMethod]
        public void ShouldAddQuadruple()
        {
            File.Delete(Path.Combine(Environment.CurrentDirectory,"RDFFirebirdStoreTest_ShouldAddQuadruple.fdb"));
            RDFQuadruple quadruple = new RDFQuadruple(new RDFContext(new Uri("ex:ctx")), new RDFResource("ex:subj"), new RDFResource("ex:pred"), new RDFResource("ex:obj"));

            RDFFirebirdStore store = new RDFFirebirdStore(GetConnectionString(Path.Combine(Environment.CurrentDirectory, "RDFFirebirdStoreTest_ShouldAddQuadruple.fdb")), FirebirdVersion);
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
            File.Delete(Path.Combine(Environment.CurrentDirectory,"RDFFirebirdStoreTest_ShouldMergeGraph.fdb"));
            RDFGraph graph = new RDFGraph(new List<RDFTriple>() {
                new RDFTriple(new RDFResource("ex:subj"), new RDFResource("ex:pred"), new RDFResource("ex:obj"))
            }).SetContext(new Uri("ex:ctx"));

            RDFFirebirdStore store = new RDFFirebirdStore(GetConnectionString(Path.Combine(Environment.CurrentDirectory, "RDFFirebirdStoreTest_ShouldMergeGraph.fdb")), FirebirdVersion);
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
            File.Delete(Path.Combine(Environment.CurrentDirectory,"RDFFirebirdStoreTest_ShouldRemoveQuadruple.fdb"));
            RDFQuadruple quadruple = new RDFQuadruple(new RDFContext(new Uri("ex:ctx")), new RDFResource("ex:subj"), new RDFResource("ex:pred"), new RDFResource("ex:obj"));

            RDFFirebirdStore store = new RDFFirebirdStore(GetConnectionString(Path.Combine(Environment.CurrentDirectory, "RDFFirebirdStoreTest_ShouldRemoveQuadruple.fdb")), FirebirdVersion);
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
            File.Delete(Path.Combine(Environment.CurrentDirectory,"RDFFirebirdStoreTest_ShouldNotRemoveQuadruple.fdb"));
            RDFQuadruple quadruple = new RDFQuadruple(new RDFContext(new Uri("ex:ctx")), new RDFResource("ex:subj"), new RDFResource("ex:pred"), new RDFResource("ex:obj"));
            RDFQuadruple quadruple2 = new RDFQuadruple(new RDFContext(new Uri("ex:ctx2")), new RDFResource("ex:subj"), new RDFResource("ex:pred"), new RDFResource("ex:obj"));

            RDFFirebirdStore store = new RDFFirebirdStore(GetConnectionString(Path.Combine(Environment.CurrentDirectory, "RDFFirebirdStoreTest_ShouldNotRemoveQuadruple.fdb")), FirebirdVersion);
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
            File.Delete(Path.Combine(Environment.CurrentDirectory,"RDFFirebirdStoreTest_ShouldRemoveQuadruplesByContext.fdb"));
            RDFQuadruple quadruple = new RDFQuadruple(new RDFContext(new Uri("ex:ctx")), new RDFResource("ex:subj"), new RDFResource("ex:pred"), new RDFResource("ex:obj"));

            RDFFirebirdStore store = new RDFFirebirdStore(GetConnectionString(Path.Combine(Environment.CurrentDirectory, "RDFFirebirdStoreTest_ShouldRemoveQuadruplesByContext.fdb")), FirebirdVersion);
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
            File.Delete(Path.Combine(Environment.CurrentDirectory,"RDFFirebirdStoreTest_ShouldNotRemoveQuadruplesByContext.fdb"));
            RDFQuadruple quadruple = new RDFQuadruple(new RDFContext(new Uri("ex:ctx")), new RDFResource("ex:subj"), new RDFResource("ex:pred"), new RDFResource("ex:obj"));

            RDFFirebirdStore store = new RDFFirebirdStore(GetConnectionString(Path.Combine(Environment.CurrentDirectory, "RDFFirebirdStoreTest_ShouldNotRemoveQuadruplesByContext.fdb")), FirebirdVersion);
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
            File.Delete(Path.Combine(Environment.CurrentDirectory,"RDFFirebirdStoreTest_ShouldRemoveQuadruplesBySubject.fdb"));
            RDFQuadruple quadruple = new RDFQuadruple(new RDFContext(new Uri("ex:ctx")), new RDFResource("ex:subj"), new RDFResource("ex:pred"), new RDFResource("ex:obj"));

            RDFFirebirdStore store = new RDFFirebirdStore(GetConnectionString(Path.Combine(Environment.CurrentDirectory, "RDFFirebirdStoreTest_ShouldRemoveQuadruplesBySubject.fdb")), FirebirdVersion);
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
            File.Delete(Path.Combine(Environment.CurrentDirectory,"RDFFirebirdStoreTest_ShouldNotRemoveQuadruplesBySubject.fdb"));
            RDFQuadruple quadruple = new RDFQuadruple(new RDFContext(new Uri("ex:ctx")), new RDFResource("ex:subj"), new RDFResource("ex:pred"), new RDFResource("ex:obj"));

            RDFFirebirdStore store = new RDFFirebirdStore(GetConnectionString(Path.Combine(Environment.CurrentDirectory, "RDFFirebirdStoreTest_ShouldNotRemoveQuadruplesBySubject.fdb")), FirebirdVersion);
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
            File.Delete(Path.Combine(Environment.CurrentDirectory,"RDFFirebirdStoreTest_ShouldRemoveQuadruplesByPredicate.fdb"));
            RDFQuadruple quadruple = new RDFQuadruple(new RDFContext(new Uri("ex:ctx")), new RDFResource("ex:subj"), new RDFResource("ex:pred"), new RDFResource("ex:obj"));

            RDFFirebirdStore store = new RDFFirebirdStore(GetConnectionString(Path.Combine(Environment.CurrentDirectory, "RDFFirebirdStoreTest_ShouldRemoveQuadruplesByPredicate.fdb")), FirebirdVersion);
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
            File.Delete(Path.Combine(Environment.CurrentDirectory,"RDFFirebirdStoreTest_ShouldNotRemoveQuadruplesByPredicate.fdb"));
            RDFQuadruple quadruple = new RDFQuadruple(new RDFContext(new Uri("ex:ctx")), new RDFResource("ex:subj"), new RDFResource("ex:pred"), new RDFResource("ex:obj"));

            RDFFirebirdStore store = new RDFFirebirdStore(GetConnectionString(Path.Combine(Environment.CurrentDirectory, "RDFFirebirdStoreTest_ShouldNotRemoveQuadruplesByPredicate.fdb")), FirebirdVersion);
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
            File.Delete(Path.Combine(Environment.CurrentDirectory,"RDFFirebirdStoreTest_ShouldRemoveQuadruplesByObject.fdb"));
            RDFQuadruple quadruple = new RDFQuadruple(new RDFContext(new Uri("ex:ctx")), new RDFResource("ex:subj"), new RDFResource("ex:pred"), new RDFResource("ex:obj"));

            RDFFirebirdStore store = new RDFFirebirdStore(GetConnectionString(Path.Combine(Environment.CurrentDirectory, "RDFFirebirdStoreTest_ShouldRemoveQuadruplesByObject.fdb")), FirebirdVersion);
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
            File.Delete(Path.Combine(Environment.CurrentDirectory,"RDFFirebirdStoreTest_ShouldNotRemoveQuadruplesByObject.fdb"));
            RDFQuadruple quadruple = new RDFQuadruple(new RDFContext(new Uri("ex:ctx")), new RDFResource("ex:subj"), new RDFResource("ex:pred"), new RDFResource("ex:obj"));

            RDFFirebirdStore store = new RDFFirebirdStore(GetConnectionString(Path.Combine(Environment.CurrentDirectory, "RDFFirebirdStoreTest_ShouldNotRemoveQuadruplesByObject.fdb")), FirebirdVersion);
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
            File.Delete(Path.Combine(Environment.CurrentDirectory,"RDFFirebirdStoreTest_ShouldRemoveQuadruplesByLiteral.fdb"));
            RDFQuadruple quadruple = new RDFQuadruple(new RDFContext(new Uri("ex:ctx")), new RDFResource("ex:subj"), new RDFResource("ex:pred"), new RDFPlainLiteral("hello"));

            RDFFirebirdStore store = new RDFFirebirdStore(GetConnectionString(Path.Combine(Environment.CurrentDirectory, "RDFFirebirdStoreTest_ShouldRemoveQuadruplesByLiteral.fdb")), FirebirdVersion);
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
            File.Delete(Path.Combine(Environment.CurrentDirectory,"RDFFirebirdStoreTest_ShouldNotRemoveQuadruplesByLiteral.fdb"));
            RDFQuadruple quadruple = new RDFQuadruple(new RDFContext(new Uri("ex:ctx")), new RDFResource("ex:subj"), new RDFResource("ex:pred"), new RDFPlainLiteral("hello"));

            RDFFirebirdStore store = new RDFFirebirdStore(GetConnectionString(Path.Combine(Environment.CurrentDirectory, "RDFFirebirdStoreTest_ShouldNotRemoveQuadruplesByLiteral.fdb")), FirebirdVersion);
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
            File.Delete(Path.Combine(Environment.CurrentDirectory,"RDFFirebirdStoreTest_ShouldRemoveQuadruplesByContextSubject.fdb"));
            RDFQuadruple quadruple = new RDFQuadruple(new RDFContext(new Uri("ex:ctx")), new RDFResource("ex:subj"), new RDFResource("ex:pred"), new RDFResource("ex:obj"));

            RDFFirebirdStore store = new RDFFirebirdStore(GetConnectionString(Path.Combine(Environment.CurrentDirectory, "RDFFirebirdStoreTest_ShouldRemoveQuadruplesByContextSubject.fdb")), FirebirdVersion);
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
            File.Delete(Path.Combine(Environment.CurrentDirectory,"RDFFirebirdStoreTest_ShouldNotRemoveQuadruplesByContextSubject.fdb"));
            RDFQuadruple quadruple = new RDFQuadruple(new RDFContext(new Uri("ex:ctx")), new RDFResource("ex:subj"), new RDFResource("ex:pred"), new RDFResource("ex:obj"));

            RDFFirebirdStore store = new RDFFirebirdStore(GetConnectionString(Path.Combine(Environment.CurrentDirectory, "RDFFirebirdStoreTest_ShouldNotRemoveQuadruplesByContextSubject.fdb")), FirebirdVersion);
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
            File.Delete(Path.Combine(Environment.CurrentDirectory,"RDFFirebirdStoreTest_ShouldRemoveQuadruplesByContextPredicate.fdb"));
            RDFQuadruple quadruple = new RDFQuadruple(new RDFContext(new Uri("ex:ctx")), new RDFResource("ex:subj"), new RDFResource("ex:pred"), new RDFResource("ex:obj"));

            RDFFirebirdStore store = new RDFFirebirdStore(GetConnectionString(Path.Combine(Environment.CurrentDirectory, "RDFFirebirdStoreTest_ShouldRemoveQuadruplesByContextPredicate.fdb")), FirebirdVersion);
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
            File.Delete(Path.Combine(Environment.CurrentDirectory,"RDFFirebirdStoreTest_ShouldNotRemoveQuadruplesByContextPredicate.fdb"));
            RDFQuadruple quadruple = new RDFQuadruple(new RDFContext(new Uri("ex:ctx")), new RDFResource("ex:subj"), new RDFResource("ex:pred"), new RDFResource("ex:obj"));

            RDFFirebirdStore store = new RDFFirebirdStore(GetConnectionString(Path.Combine(Environment.CurrentDirectory, "RDFFirebirdStoreTest_ShouldNotRemoveQuadruplesByContextPredicate.fdb")), FirebirdVersion);
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
            File.Delete(Path.Combine(Environment.CurrentDirectory,"RDFFirebirdStoreTest_ShouldRemoveQuadruplesByContextObject.fdb"));
            RDFQuadruple quadruple = new RDFQuadruple(new RDFContext(new Uri("ex:ctx")), new RDFResource("ex:subj"), new RDFResource("ex:pred"), new RDFResource("ex:obj"));

            RDFFirebirdStore store = new RDFFirebirdStore(GetConnectionString(Path.Combine(Environment.CurrentDirectory, "RDFFirebirdStoreTest_ShouldRemoveQuadruplesByContextObject.fdb")), FirebirdVersion);
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
            File.Delete(Path.Combine(Environment.CurrentDirectory,"RDFFirebirdStoreTest_ShouldNotRemoveQuadruplesByContextObject.fdb"));
            RDFQuadruple quadruple = new RDFQuadruple(new RDFContext(new Uri("ex:ctx")), new RDFResource("ex:subj"), new RDFResource("ex:pred"), new RDFResource("ex:obj"));

            RDFFirebirdStore store = new RDFFirebirdStore(GetConnectionString(Path.Combine(Environment.CurrentDirectory, "RDFFirebirdStoreTest_ShouldNotRemoveQuadruplesByContextObject.fdb")), FirebirdVersion);
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
            File.Delete(Path.Combine(Environment.CurrentDirectory,"RDFFirebirdStoreTest_ShouldRemoveQuadruplesByContextLiteral.fdb"));
            RDFQuadruple quadruple = new RDFQuadruple(new RDFContext(new Uri("ex:ctx")), new RDFResource("ex:subj"), new RDFResource("ex:pred"), new RDFPlainLiteral("hello"));

            RDFFirebirdStore store = new RDFFirebirdStore(GetConnectionString(Path.Combine(Environment.CurrentDirectory, "RDFFirebirdStoreTest_ShouldRemoveQuadruplesByContextLiteral.fdb")), FirebirdVersion);
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
            File.Delete(Path.Combine(Environment.CurrentDirectory,"RDFFirebirdStoreTest_ShouldNotRemoveQuadruplesByContextLiteral.fdb"));
            RDFQuadruple quadruple = new RDFQuadruple(new RDFContext(new Uri("ex:ctx")), new RDFResource("ex:subj"), new RDFResource("ex:pred"), new RDFPlainLiteral("hello"));

            RDFFirebirdStore store = new RDFFirebirdStore(GetConnectionString(Path.Combine(Environment.CurrentDirectory, "RDFFirebirdStoreTest_ShouldNotRemoveQuadruplesByContextLiteral.fdb")), FirebirdVersion);
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
            File.Delete(Path.Combine(Environment.CurrentDirectory,"RDFFirebirdStoreTest_ShouldRemoveQuadruplesByContextSubjectPredicate.fdb"));
            RDFQuadruple quadruple = new RDFQuadruple(new RDFContext(new Uri("ex:ctx")), new RDFResource("ex:subj"), new RDFResource("ex:pred"), new RDFResource("ex:obj"));

            RDFFirebirdStore store = new RDFFirebirdStore(GetConnectionString(Path.Combine(Environment.CurrentDirectory, "RDFFirebirdStoreTest_ShouldRemoveQuadruplesByContextSubjectPredicate.fdb")), FirebirdVersion);
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
            File.Delete(Path.Combine(Environment.CurrentDirectory,"RDFFirebirdStoreTest_ShouldNotRemoveQuadruplesByContextSubjectPredicate.fdb"));
            RDFQuadruple quadruple = new RDFQuadruple(new RDFContext(new Uri("ex:ctx")), new RDFResource("ex:subj"), new RDFResource("ex:pred"), new RDFResource("ex:obj"));

            RDFFirebirdStore store = new RDFFirebirdStore(GetConnectionString(Path.Combine(Environment.CurrentDirectory, "RDFFirebirdStoreTest_ShouldNotRemoveQuadruplesByContextSubjectPredicate.fdb")), FirebirdVersion);
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
            File.Delete(Path.Combine(Environment.CurrentDirectory,"RDFFirebirdStoreTest_ShouldRemoveQuadruplesByContextSubjectObject.fdb"));
            RDFQuadruple quadruple = new RDFQuadruple(new RDFContext(new Uri("ex:ctx")), new RDFResource("ex:subj"), new RDFResource("ex:pred"), new RDFResource("ex:obj"));

            RDFFirebirdStore store = new RDFFirebirdStore(GetConnectionString(Path.Combine(Environment.CurrentDirectory, "RDFFirebirdStoreTest_ShouldRemoveQuadruplesByContextSubjectObject.fdb")), FirebirdVersion);
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
            File.Delete(Path.Combine(Environment.CurrentDirectory,"RDFFirebirdStoreTest_ShouldNotRemoveQuadruplesByContextSubjectObject.fdb"));
            RDFQuadruple quadruple = new RDFQuadruple(new RDFContext(new Uri("ex:ctx")), new RDFResource("ex:subj"), new RDFResource("ex:pred"), new RDFResource("ex:obj"));

            RDFFirebirdStore store = new RDFFirebirdStore(GetConnectionString(Path.Combine(Environment.CurrentDirectory, "RDFFirebirdStoreTest_ShouldNotRemoveQuadruplesByContextSubjectObject.fdb")), FirebirdVersion);
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
            File.Delete(Path.Combine(Environment.CurrentDirectory,"RDFFirebirdStoreTest_ShouldRemoveQuadruplesByContextSubjectLiteral.fdb"));
            RDFQuadruple quadruple = new RDFQuadruple(new RDFContext(new Uri("ex:ctx")), new RDFResource("ex:subj"), new RDFResource("ex:pred"), new RDFPlainLiteral("hello"));

            RDFFirebirdStore store = new RDFFirebirdStore(GetConnectionString(Path.Combine(Environment.CurrentDirectory, "RDFFirebirdStoreTest_ShouldRemoveQuadruplesByContextSubjectLiteral.fdb")), FirebirdVersion);
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
            File.Delete(Path.Combine(Environment.CurrentDirectory,"RDFFirebirdStoreTest_ShouldNotRemoveQuadruplesByContextSubjectLiteral.fdb"));
            RDFQuadruple quadruple = new RDFQuadruple(new RDFContext(new Uri("ex:ctx")), new RDFResource("ex:subj"), new RDFResource("ex:pred"), new RDFPlainLiteral("hello"));

            RDFFirebirdStore store = new RDFFirebirdStore(GetConnectionString(Path.Combine(Environment.CurrentDirectory, "RDFFirebirdStoreTest_ShouldNotRemoveQuadruplesByContextSubjectLiteral.fdb")), FirebirdVersion);
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
            File.Delete(Path.Combine(Environment.CurrentDirectory,"RDFFirebirdStoreTest_ShouldRemoveQuadruplesByContextPredicateObject.fdb"));
            RDFQuadruple quadruple = new RDFQuadruple(new RDFContext(new Uri("ex:ctx")), new RDFResource("ex:subj"), new RDFResource("ex:pred"), new RDFResource("ex:obj"));

            RDFFirebirdStore store = new RDFFirebirdStore(GetConnectionString(Path.Combine(Environment.CurrentDirectory, "RDFFirebirdStoreTest_ShouldRemoveQuadruplesByContextPredicateObject.fdb")), FirebirdVersion);
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
            File.Delete(Path.Combine(Environment.CurrentDirectory,"RDFFirebirdStoreTest_ShouldNotRemoveQuadruplesByContextPredicateObject.fdb"));
            RDFQuadruple quadruple = new RDFQuadruple(new RDFContext(new Uri("ex:ctx")), new RDFResource("ex:subj"), new RDFResource("ex:pred"), new RDFResource("ex:obj"));

            RDFFirebirdStore store = new RDFFirebirdStore(GetConnectionString(Path.Combine(Environment.CurrentDirectory, "RDFFirebirdStoreTest_ShouldNotRemoveQuadruplesByContextPredicateObject.fdb")), FirebirdVersion);
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
            File.Delete(Path.Combine(Environment.CurrentDirectory,"RDFFirebirdStoreTest_ShouldRemoveQuadruplesByContextPredicateLiteral.fdb"));
            RDFQuadruple quadruple = new RDFQuadruple(new RDFContext(new Uri("ex:ctx")), new RDFResource("ex:subj"), new RDFResource("ex:pred"), new RDFPlainLiteral("hello"));

            RDFFirebirdStore store = new RDFFirebirdStore(GetConnectionString(Path.Combine(Environment.CurrentDirectory, "RDFFirebirdStoreTest_ShouldRemoveQuadruplesByContextPredicateLiteral.fdb")), FirebirdVersion);
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
            File.Delete(Path.Combine(Environment.CurrentDirectory,"RDFFirebirdStoreTest_ShouldNotRemoveQuadruplesByContextPredicateLiteral.fdb"));
            RDFQuadruple quadruple = new RDFQuadruple(new RDFContext(new Uri("ex:ctx")), new RDFResource("ex:subj"), new RDFResource("ex:pred"), new RDFPlainLiteral("hello"));

            RDFFirebirdStore store = new RDFFirebirdStore(GetConnectionString(Path.Combine(Environment.CurrentDirectory, "RDFFirebirdStoreTest_ShouldNotRemoveQuadruplesByContextPredicateLiteral.fdb")), FirebirdVersion);
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
            File.Delete(Path.Combine(Environment.CurrentDirectory,"RDFFirebirdStoreTest_ShouldRemoveQuadruplesBySubjectPredicate.fdb"));
            RDFQuadruple quadruple = new RDFQuadruple(new RDFContext(new Uri("ex:ctx")), new RDFResource("ex:subj"), new RDFResource("ex:pred"), new RDFResource("ex:obj"));

            RDFFirebirdStore store = new RDFFirebirdStore(GetConnectionString(Path.Combine(Environment.CurrentDirectory, "RDFFirebirdStoreTest_ShouldRemoveQuadruplesBySubjectPredicate.fdb")), FirebirdVersion);
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
            File.Delete(Path.Combine(Environment.CurrentDirectory,"RDFFirebirdStoreTest_ShouldNotRemoveQuadruplesBySubjectPredicate.fdb"));
            RDFQuadruple quadruple = new RDFQuadruple(new RDFContext(new Uri("ex:ctx")), new RDFResource("ex:subj"), new RDFResource("ex:pred"), new RDFResource("ex:obj"));

            RDFFirebirdStore store = new RDFFirebirdStore(GetConnectionString(Path.Combine(Environment.CurrentDirectory, "RDFFirebirdStoreTest_ShouldNotRemoveQuadruplesBySubjectPredicate.fdb")), FirebirdVersion);
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
            File.Delete(Path.Combine(Environment.CurrentDirectory,"RDFFirebirdStoreTest_ShouldRemoveQuadruplesBySubjectObject.fdb"));
            RDFQuadruple quadruple = new RDFQuadruple(new RDFContext(new Uri("ex:ctx")), new RDFResource("ex:subj"), new RDFResource("ex:pred"), new RDFResource("ex:obj"));

            RDFFirebirdStore store = new RDFFirebirdStore(GetConnectionString(Path.Combine(Environment.CurrentDirectory, "RDFFirebirdStoreTest_ShouldRemoveQuadruplesBySubjectObject.fdb")), FirebirdVersion);
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
            File.Delete(Path.Combine(Environment.CurrentDirectory,"RDFFirebirdStoreTest_ShouldNotRemoveQuadruplesBySubjectObject.fdb"));
            RDFQuadruple quadruple = new RDFQuadruple(new RDFContext(new Uri("ex:ctx")), new RDFResource("ex:subj"), new RDFResource("ex:pred"), new RDFResource("ex:obj"));

            RDFFirebirdStore store = new RDFFirebirdStore(GetConnectionString(Path.Combine(Environment.CurrentDirectory, "RDFFirebirdStoreTest_ShouldNotRemoveQuadruplesBySubjectObject.fdb")), FirebirdVersion);
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
            File.Delete(Path.Combine(Environment.CurrentDirectory,"RDFFirebirdStoreTest_ShouldRemoveQuadruplesBySubjectLiteral.fdb"));
            RDFQuadruple quadruple = new RDFQuadruple(new RDFContext(new Uri("ex:ctx")), new RDFResource("ex:subj"), new RDFResource("ex:pred"), new RDFPlainLiteral("hello"));

            RDFFirebirdStore store = new RDFFirebirdStore(GetConnectionString(Path.Combine(Environment.CurrentDirectory, "RDFFirebirdStoreTest_ShouldRemoveQuadruplesBySubjectLiteral.fdb")), FirebirdVersion);
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
            File.Delete(Path.Combine(Environment.CurrentDirectory,"RDFFirebirdStoreTest_ShouldNotRemoveQuadruplesBySubjectLiteral.fdb"));
            RDFQuadruple quadruple = new RDFQuadruple(new RDFContext(new Uri("ex:ctx")), new RDFResource("ex:subj"), new RDFResource("ex:pred"), new RDFPlainLiteral("hello"));

            RDFFirebirdStore store = new RDFFirebirdStore(GetConnectionString(Path.Combine(Environment.CurrentDirectory, "RDFFirebirdStoreTest_ShouldNotRemoveQuadruplesBySubjectLiteral.fdb")), FirebirdVersion);
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
            File.Delete(Path.Combine(Environment.CurrentDirectory,"RDFFirebirdStoreTest_ShouldRemoveQuadruplesByPredicateObject.fdb"));
            RDFQuadruple quadruple = new RDFQuadruple(new RDFContext(new Uri("ex:ctx")), new RDFResource("ex:subj"), new RDFResource("ex:pred"), new RDFResource("ex:obj"));

            RDFFirebirdStore store = new RDFFirebirdStore(GetConnectionString(Path.Combine(Environment.CurrentDirectory, "RDFFirebirdStoreTest_ShouldRemoveQuadruplesByPredicateObject.fdb")), FirebirdVersion);
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
            File.Delete(Path.Combine(Environment.CurrentDirectory,"RDFFirebirdStoreTest_ShouldNotRemoveQuadruplesByPredicateObject.fdb"));
            RDFQuadruple quadruple = new RDFQuadruple(new RDFContext(new Uri("ex:ctx")), new RDFResource("ex:subj"), new RDFResource("ex:pred"), new RDFResource("ex:obj"));

            RDFFirebirdStore store = new RDFFirebirdStore(GetConnectionString(Path.Combine(Environment.CurrentDirectory, "RDFFirebirdStoreTest_ShouldNotRemoveQuadruplesByPredicateObject.fdb")), FirebirdVersion);
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
            File.Delete(Path.Combine(Environment.CurrentDirectory,"RDFFirebirdStoreTest_ShouldRemoveQuadruplesByPredicateLiteral.fdb"));
            RDFQuadruple quadruple = new RDFQuadruple(new RDFContext(new Uri("ex:ctx")), new RDFResource("ex:subj"), new RDFResource("ex:pred"), new RDFPlainLiteral("hello"));

            RDFFirebirdStore store = new RDFFirebirdStore(GetConnectionString(Path.Combine(Environment.CurrentDirectory, "RDFFirebirdStoreTest_ShouldRemoveQuadruplesByPredicateLiteral.fdb")), FirebirdVersion);
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
            File.Delete(Path.Combine(Environment.CurrentDirectory,"RDFFirebirdStoreTest_ShouldNotRemoveQuadruplesByPredicateLiteral.fdb"));
            RDFQuadruple quadruple = new RDFQuadruple(new RDFContext(new Uri("ex:ctx")), new RDFResource("ex:subj"), new RDFResource("ex:pred"), new RDFPlainLiteral("hello"));

            RDFFirebirdStore store = new RDFFirebirdStore(GetConnectionString(Path.Combine(Environment.CurrentDirectory, "RDFFirebirdStoreTest_ShouldNotRemoveQuadruplesByPredicateLiteral.fdb")), FirebirdVersion);
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
            File.Delete(Path.Combine(Environment.CurrentDirectory,"RDFFirebirdStoreTest_ShouldClearQuadruples.fdb"));
            RDFQuadruple quadruple = new RDFQuadruple(new RDFContext(new Uri("ex:ctx")), new RDFResource("ex:subj"), new RDFResource("ex:pred"), new RDFPlainLiteral("hello"));

            RDFFirebirdStore store = new RDFFirebirdStore(GetConnectionString(Path.Combine(Environment.CurrentDirectory, "RDFFirebirdStoreTest_ShouldClearQuadruples.fdb")), FirebirdVersion);
            store.ClearQuadruples();

            RDFMemoryStore memStore = store.SelectAllQuadruples();

            Assert.IsNotNull(memStore);
            Assert.IsTrue(memStore.QuadruplesCount == 0);
        }

        [TestMethod]
        public void ShouldContainQuadruple()
        {
            File.Delete(Path.Combine(Environment.CurrentDirectory,"RDFFirebirdStoreTest_ShouldContainQuadruple.fdb"));
            RDFQuadruple quadruple1 = new RDFQuadruple(new RDFContext(new Uri("ex:ctx")), new RDFResource("ex:subj"), new RDFResource("ex:pred"), new RDFPlainLiteral("hello"));
            RDFQuadruple quadruple2 = new RDFQuadruple(new RDFContext(new Uri("ex:ctx2")), new RDFResource("ex:subj"), new RDFResource("ex:pred"), new RDFPlainLiteral("hello"));

            RDFFirebirdStore store = new RDFFirebirdStore(GetConnectionString(Path.Combine(Environment.CurrentDirectory, "RDFFirebirdStoreTest_ShouldContainQuadruple.fdb")), FirebirdVersion);
            store.AddQuadruple(quadruple1);
            store.AddQuadruple(quadruple2);

            Assert.IsTrue(store.ContainsQuadruple(quadruple1));
            Assert.IsTrue(store.ContainsQuadruple(quadruple2));
        }

        [TestMethod]
        public void ShouldNotContainQuadruple()
        {
            File.Delete(Path.Combine(Environment.CurrentDirectory,"RDFFirebirdStoreTest_ShouldNotContainQuadruple.fdb"));
            RDFQuadruple quadruple = new RDFQuadruple(new RDFContext(new Uri("ex:ctx")), new RDFResource("ex:subj"), new RDFResource("ex:pred"), new RDFPlainLiteral("hello"));
            RDFQuadruple quadruple2 = new RDFQuadruple(new RDFContext(new Uri("ex:ctx2")), new RDFResource("ex:subj"), new RDFResource("ex:pred"), new RDFPlainLiteral("hello"));

            RDFFirebirdStore store = new RDFFirebirdStore(GetConnectionString(Path.Combine(Environment.CurrentDirectory, "RDFFirebirdStoreTest_ShouldNotContainQuadruple.fdb")), FirebirdVersion);
            store.AddQuadruple(quadruple);

            Assert.IsFalse(store.ContainsQuadruple(quadruple2));
            Assert.IsFalse(store.ContainsQuadruple(null));
        }

        [TestMethod]
        public void ShouldSelectQuadruplesByContext()
        {
            File.Delete(Path.Combine(Environment.CurrentDirectory,"RDFFirebirdStoreTest_ShouldSelectQuadruplesByContext.fdb"));
            RDFQuadruple quadruple = new RDFQuadruple(new RDFContext(new Uri("ex:ctx")), new RDFResource("ex:subj"), new RDFResource("ex:pred"), new RDFResource("ex:obj"));

            RDFFirebirdStore store = new RDFFirebirdStore(GetConnectionString(Path.Combine(Environment.CurrentDirectory, "RDFFirebirdStoreTest_ShouldSelectQuadruplesByContext.fdb")), FirebirdVersion);
            store.AddQuadruple(quadruple);
            RDFMemoryStore result = store.SelectQuadruplesByContext(new RDFContext(new Uri("ex:ctx")));

            Assert.IsNotNull(result);
            Assert.IsTrue(result.ContainsQuadruple(quadruple));
        }

        [TestMethod]
        public void ShouldNotSelectQuadruplesByContext()
        {
            File.Delete(Path.Combine(Environment.CurrentDirectory,"RDFFirebirdStoreTest_ShouldNotSelectQuadruplesByContext.fdb"));
            RDFQuadruple quadruple = new RDFQuadruple(new RDFContext(new Uri("ex:ctx")), new RDFResource("ex:subj"), new RDFResource("ex:pred"), new RDFResource("ex:obj"));

            RDFFirebirdStore store = new RDFFirebirdStore(GetConnectionString(Path.Combine(Environment.CurrentDirectory, "RDFFirebirdStoreTest_ShouldNotSelectQuadruplesByContext.fdb")), FirebirdVersion);
            store.AddQuadruple(quadruple);
            RDFMemoryStore result = store.SelectQuadruplesByContext(new RDFContext(new Uri("ex:ctx2")));

            Assert.IsNotNull(result);
            Assert.IsFalse(result.ContainsQuadruple(quadruple));
        }

        [TestMethod]
        public void ShouldSelectQuadruplesBySubject()
        {
            File.Delete(Path.Combine(Environment.CurrentDirectory,"RDFFirebirdStoreTest_ShouldSelectQuadruplesBySubject.fdb"));
            RDFQuadruple quadruple = new RDFQuadruple(new RDFContext(new Uri("ex:ctx")), new RDFResource("ex:subj"), new RDFResource("ex:pred"), new RDFResource("ex:obj"));

            RDFFirebirdStore store = new RDFFirebirdStore(GetConnectionString(Path.Combine(Environment.CurrentDirectory, "RDFFirebirdStoreTest_ShouldSelectQuadruplesBySubject.fdb")), FirebirdVersion);
            store.AddQuadruple(quadruple);
            RDFMemoryStore result = store.SelectQuadruplesBySubject(new RDFResource("ex:subj"));

            Assert.IsNotNull(result);
            Assert.IsTrue(result.ContainsQuadruple(quadruple));
        }

        [TestMethod]
        public void ShouldNotSelectQuadruplesBySubject()
        {
            File.Delete(Path.Combine(Environment.CurrentDirectory,"RDFFirebirdStoreTest_ShouldNotSelectQuadruplesBySubject.fdb"));
            RDFQuadruple quadruple = new RDFQuadruple(new RDFContext(new Uri("ex:ctx")), new RDFResource("ex:subj"), new RDFResource("ex:pred"), new RDFResource("ex:obj"));

            RDFFirebirdStore store = new RDFFirebirdStore(GetConnectionString(Path.Combine(Environment.CurrentDirectory, "RDFFirebirdStoreTest_ShouldNotSelectQuadruplesBySubject.fdb")), FirebirdVersion);
            store.AddQuadruple(quadruple);
            RDFMemoryStore result = store.SelectQuadruplesBySubject(new RDFResource("ex:subj2"));

            Assert.IsNotNull(result);
            Assert.IsFalse(result.ContainsQuadruple(quadruple));
        }

        [TestMethod]
        public void ShouldSelectQuadruplesByPredicate()
        {
            File.Delete(Path.Combine(Environment.CurrentDirectory,"RDFFirebirdStoreTest_ShouldSelectQuadruplesByPredicate.fdb"));
            RDFQuadruple quadruple = new RDFQuadruple(new RDFContext(new Uri("ex:ctx")), new RDFResource("ex:subj"), new RDFResource("ex:pred"), new RDFResource("ex:obj"));

            RDFFirebirdStore store = new RDFFirebirdStore(GetConnectionString(Path.Combine(Environment.CurrentDirectory, "RDFFirebirdStoreTest_ShouldSelectQuadruplesByPredicate.fdb")), FirebirdVersion);
            store.AddQuadruple(quadruple);
            RDFMemoryStore result = store.SelectQuadruplesByPredicate(new RDFResource("ex:pred"));

            Assert.IsNotNull(result);
            Assert.IsTrue(result.ContainsQuadruple(quadruple));
        }

        [TestMethod]
        public void ShouldNotSelectQuadruplesByPredicate()
        {
            File.Delete(Path.Combine(Environment.CurrentDirectory,"RDFFirebirdStoreTest_ShouldNotSelectQuadruplesByPredicate.fdb"));
            RDFQuadruple quadruple = new RDFQuadruple(new RDFContext(new Uri("ex:ctx")), new RDFResource("ex:subj"), new RDFResource("ex:pred"), new RDFResource("ex:obj"));

            RDFFirebirdStore store = new RDFFirebirdStore(GetConnectionString(Path.Combine(Environment.CurrentDirectory, "RDFFirebirdStoreTest_ShouldNotSelectQuadruplesByPredicate.fdb")), FirebirdVersion);
            store.AddQuadruple(quadruple);
            RDFMemoryStore result = store.SelectQuadruplesByPredicate(new RDFResource("ex:pred2"));

            Assert.IsNotNull(result);
            Assert.IsFalse(result.ContainsQuadruple(quadruple));
        }

        [TestMethod]
        public void ShouldSelectQuadruplesByObject()
        {
            File.Delete(Path.Combine(Environment.CurrentDirectory,"RDFFirebirdStoreTest_ShouldSelectQuadruplesByObject.fdb"));
            RDFQuadruple quadruple = new RDFQuadruple(new RDFContext(new Uri("ex:ctx")), new RDFResource("ex:subj"), new RDFResource("ex:pred"), new RDFResource("ex:obj"));

            RDFFirebirdStore store = new RDFFirebirdStore(GetConnectionString(Path.Combine(Environment.CurrentDirectory, "RDFFirebirdStoreTest_ShouldSelectQuadruplesByObject.fdb")), FirebirdVersion);
            store.AddQuadruple(quadruple);
            RDFMemoryStore result = store.SelectQuadruplesByObject(new RDFResource("ex:obj"));

            Assert.IsNotNull(result);
            Assert.IsTrue(result.ContainsQuadruple(quadruple));
        }

        [TestMethod]
        public void ShouldNotSelectQuadruplesByObject()
        {
            File.Delete(Path.Combine(Environment.CurrentDirectory,"RDFFirebirdStoreTest_ShouldNotSelectQuadruplesByObject.fdb"));
            RDFQuadruple quadruple = new RDFQuadruple(new RDFContext(new Uri("ex:ctx")), new RDFResource("ex:subj"), new RDFResource("ex:pred"), new RDFResource("ex:obj"));

            RDFFirebirdStore store = new RDFFirebirdStore(GetConnectionString(Path.Combine(Environment.CurrentDirectory, "RDFFirebirdStoreTest_ShouldNotSelectQuadruplesByObject.fdb")), FirebirdVersion);
            store.AddQuadruple(quadruple);
            RDFMemoryStore result = store.SelectQuadruplesByObject(new RDFResource("ex:obj2"));

            Assert.IsNotNull(result);
            Assert.IsFalse(result.ContainsQuadruple(quadruple));
        }

        [TestMethod]
        public void ShouldSelectQuadruplesByLiteral()
        {
            File.Delete(Path.Combine(Environment.CurrentDirectory,"RDFFirebirdStoreTest_ShouldSelectQuadruplesByLiteral.fdb"));
            RDFQuadruple quadruple = new RDFQuadruple(new RDFContext(new Uri("ex:ctx")), new RDFResource("ex:subj"), new RDFResource("ex:pred"), new RDFPlainLiteral("hello"));

            RDFFirebirdStore store = new RDFFirebirdStore(GetConnectionString(Path.Combine(Environment.CurrentDirectory, "RDFFirebirdStoreTest_ShouldSelectQuadruplesByLiteral.fdb")), FirebirdVersion);
            store.AddQuadruple(quadruple);
            RDFMemoryStore result = store.SelectQuadruplesByLiteral(new RDFPlainLiteral("hello"));

            Assert.IsNotNull(result);
            Assert.IsTrue(result.ContainsQuadruple(quadruple));
        }

        [TestMethod]
        public void ShouldNotSelectQuadruplesByLiteral()
        {
            File.Delete(Path.Combine(Environment.CurrentDirectory,"RDFFirebirdStoreTest_ShouldNotSelectQuadruplesByLiteral.fdb"));
            RDFQuadruple quadruple = new RDFQuadruple(new RDFContext(new Uri("ex:ctx")), new RDFResource("ex:subj"), new RDFResource("ex:pred"), new RDFPlainLiteral("hello"));

            RDFFirebirdStore store = new RDFFirebirdStore(GetConnectionString(Path.Combine(Environment.CurrentDirectory, "RDFFirebirdStoreTest_ShouldNotSelectQuadruplesByLiteral.fdb")), FirebirdVersion);
            store.AddQuadruple(quadruple);
            RDFMemoryStore result = store.SelectQuadruplesByLiteral(new RDFPlainLiteral("hello","en"));

            Assert.IsNotNull(result);
            Assert.IsFalse(result.ContainsQuadruple(quadruple));
        }

        [TestMethod]
        public void ShouldSelectQuadruplesByContextSubject()
        {
            File.Delete(Path.Combine(Environment.CurrentDirectory,"RDFFirebirdStoreTest_ShouldSelectQuadruplesByContextSubject.fdb"));
            RDFQuadruple quadruple = new RDFQuadruple(new RDFContext(new Uri("ex:ctx")), new RDFResource("ex:subj"), new RDFResource("ex:pred"), new RDFResource("ex:obj"));

            RDFFirebirdStore store = new RDFFirebirdStore(GetConnectionString(Path.Combine(Environment.CurrentDirectory, "RDFFirebirdStoreTest_ShouldSelectQuadruplesByContextSubject.fdb")), FirebirdVersion);
            store.AddQuadruple(quadruple);
            RDFMemoryStore result = store.SelectQuadruples(new RDFContext(new Uri("ex:ctx")), new RDFResource("ex:subj"), null, null, null);

            Assert.IsNotNull(result);
            Assert.IsTrue(result.ContainsQuadruple(quadruple));
        }

        [TestMethod]
        public void ShouldNotSelectQuadruplesByContextSubject()
        {
            File.Delete(Path.Combine(Environment.CurrentDirectory,"RDFFirebirdStoreTest_ShouldNotSelectQuadruplesByContextSubject.fdb"));
            RDFQuadruple quadruple = new RDFQuadruple(new RDFContext(new Uri("ex:ctx")), new RDFResource("ex:subj"), new RDFResource("ex:pred"), new RDFResource("ex:obj"));

            RDFFirebirdStore store = new RDFFirebirdStore(GetConnectionString(Path.Combine(Environment.CurrentDirectory, "RDFFirebirdStoreTest_ShouldNotSelectQuadruplesByContextSubject.fdb")), FirebirdVersion);
            store.AddQuadruple(quadruple);
            RDFMemoryStore result = store.SelectQuadruples(new RDFContext(new Uri("ex:ctx2")), new RDFResource("ex:subj"), null, null, null);

            Assert.IsNotNull(result);
            Assert.IsFalse(result.ContainsQuadruple(quadruple));
        }

        [TestMethod]
        public void ShouldSelectQuadruplesByContextPredicate()
        {
            File.Delete(Path.Combine(Environment.CurrentDirectory,"RDFFirebirdStoreTest_ShouldSelectQuadruplesByContextPredicate.fdb"));
            RDFQuadruple quadruple = new RDFQuadruple(new RDFContext(new Uri("ex:ctx")), new RDFResource("ex:subj"), new RDFResource("ex:pred"), new RDFResource("ex:obj"));

            RDFFirebirdStore store = new RDFFirebirdStore(GetConnectionString(Path.Combine(Environment.CurrentDirectory, "RDFFirebirdStoreTest_ShouldSelectQuadruplesByContextPredicate.fdb")), FirebirdVersion);
            store.AddQuadruple(quadruple);
            RDFMemoryStore result = store.SelectQuadruples(new RDFContext(new Uri("ex:ctx")), null, new RDFResource("ex:pred"), null, null);

            Assert.IsNotNull(result);
            Assert.IsTrue(result.ContainsQuadruple(quadruple));
        }

        [TestMethod]
        public void ShouldNotSelectQuadruplesByContextPredicate()
        {
            File.Delete(Path.Combine(Environment.CurrentDirectory,"RDFFirebirdStoreTest_ShouldNotSelectQuadruplesByContextPredicate.fdb"));
            RDFQuadruple quadruple = new RDFQuadruple(new RDFContext(new Uri("ex:ctx")), new RDFResource("ex:subj"), new RDFResource("ex:pred"), new RDFResource("ex:obj"));

            RDFFirebirdStore store = new RDFFirebirdStore(GetConnectionString(Path.Combine(Environment.CurrentDirectory, "RDFFirebirdStoreTest_ShouldNotSelectQuadruplesByContextPredicate.fdb")), FirebirdVersion);
            store.AddQuadruple(quadruple);
            RDFMemoryStore result = store.SelectQuadruples(new RDFContext(new Uri("ex:ctx2")), null, new RDFResource("ex:pred"), null, null);

            Assert.IsNotNull(result);
            Assert.IsFalse(result.ContainsQuadruple(quadruple));
        }

        [TestMethod]
        public void ShouldSelectQuadruplesByContextObject()
        {
            File.Delete(Path.Combine(Environment.CurrentDirectory,"RDFFirebirdStoreTest_ShouldSelectQuadruplesByContextObject.fdb"));
            RDFQuadruple quadruple = new RDFQuadruple(new RDFContext(new Uri("ex:ctx")), new RDFResource("ex:subj"), new RDFResource("ex:pred"), new RDFResource("ex:obj"));

            RDFFirebirdStore store = new RDFFirebirdStore(GetConnectionString(Path.Combine(Environment.CurrentDirectory, "RDFFirebirdStoreTest_ShouldSelectQuadruplesByContextObject.fdb")), FirebirdVersion);
            store.AddQuadruple(quadruple);
            RDFMemoryStore result = store.SelectQuadruples(new RDFContext(new Uri("ex:ctx")), null, null, new RDFResource("ex:obj"), null);

            Assert.IsNotNull(result);
            Assert.IsTrue(result.ContainsQuadruple(quadruple));
        }

        [TestMethod]
        public void ShouldNotSelectQuadruplesByContextObject()
        {
            File.Delete(Path.Combine(Environment.CurrentDirectory,"RDFFirebirdStoreTest_ShouldNotSelectQuadruplesByContextObject.fdb"));
            RDFQuadruple quadruple = new RDFQuadruple(new RDFContext(new Uri("ex:ctx")), new RDFResource("ex:subj"), new RDFResource("ex:pred"), new RDFResource("ex:obj"));

            RDFFirebirdStore store = new RDFFirebirdStore(GetConnectionString(Path.Combine(Environment.CurrentDirectory, "RDFFirebirdStoreTest_ShouldNotSelectQuadruplesByContextObject.fdb")), FirebirdVersion);
            store.AddQuadruple(quadruple);
            RDFMemoryStore result = store.SelectQuadruples(new RDFContext(new Uri("ex:ctx2")), null, null, new RDFResource("ex:obj"), null);

            Assert.IsNotNull(result);
            Assert.IsFalse(result.ContainsQuadruple(quadruple));
        }

        [TestMethod]
        public void ShouldSelectQuadruplesByContextLiteral()
        {
            File.Delete(Path.Combine(Environment.CurrentDirectory,"RDFFirebirdStoreTest_ShouldSelectQuadruplesByContextLiteral.fdb"));
            RDFQuadruple quadruple = new RDFQuadruple(new RDFContext(new Uri("ex:ctx")), new RDFResource("ex:subj"), new RDFResource("ex:pred"), new RDFPlainLiteral("hello"));

            RDFFirebirdStore store = new RDFFirebirdStore(GetConnectionString(Path.Combine(Environment.CurrentDirectory, "RDFFirebirdStoreTest_ShouldSelectQuadruplesByContextLiteral.fdb")), FirebirdVersion);
            store.AddQuadruple(quadruple);
            RDFMemoryStore result = store.SelectQuadruples(new RDFContext(new Uri("ex:ctx")), null, null, null, new RDFPlainLiteral("hello"));

            Assert.IsNotNull(result);
            Assert.IsTrue(result.ContainsQuadruple(quadruple));
        }

        [TestMethod]
        public void ShouldNotSelectQuadruplesByContextLiteral()
        {
            File.Delete(Path.Combine(Environment.CurrentDirectory,"RDFFirebirdStoreTest_ShouldNotSelectQuadruplesByContextLiteral.fdb"));
            RDFQuadruple quadruple = new RDFQuadruple(new RDFContext(new Uri("ex:ctx")), new RDFResource("ex:subj"), new RDFResource("ex:pred"), new RDFPlainLiteral("hello"));

            RDFFirebirdStore store = new RDFFirebirdStore(GetConnectionString(Path.Combine(Environment.CurrentDirectory, "RDFFirebirdStoreTest_ShouldNotSelectQuadruplesByContextLiteral.fdb")), FirebirdVersion);
            store.AddQuadruple(quadruple);
            RDFMemoryStore result = store.SelectQuadruples(new RDFContext(new Uri("ex:ctx2")), null, null, null, new RDFPlainLiteral("hello"));

            Assert.IsNotNull(result);
            Assert.IsFalse(result.ContainsQuadruple(quadruple));
        }

        [TestMethod]
        public void ShouldSelectQuadruplesByContextSubjectPredicate()
        {
            File.Delete(Path.Combine(Environment.CurrentDirectory,"RDFFirebirdStoreTest_ShouldSelectQuadruplesByContextSubjectPredicate.fdb"));
            RDFQuadruple quadruple = new RDFQuadruple(new RDFContext(new Uri("ex:ctx")), new RDFResource("ex:subj"), new RDFResource("ex:pred"), new RDFResource("ex:obj"));

            RDFFirebirdStore store = new RDFFirebirdStore(GetConnectionString(Path.Combine(Environment.CurrentDirectory, "RDFFirebirdStoreTest_ShouldSelectQuadruplesByContextSubjectPredicate.fdb")), FirebirdVersion);
            store.AddQuadruple(quadruple);
            RDFMemoryStore result = store.SelectQuadruples(new RDFContext(new Uri("ex:ctx")), new RDFResource("ex:subj"), new RDFResource("ex:pred"), null, null);

            Assert.IsNotNull(result);
            Assert.IsTrue(result.ContainsQuadruple(quadruple));
        }

        [TestMethod]
        public void ShouldNotSelectQuadruplesByContextSubjectPredicate()
        {
            File.Delete(Path.Combine(Environment.CurrentDirectory,"RDFFirebirdStoreTest_ShouldNotSelectQuadruplesByContextSubjectPredicate.fdb"));
            RDFQuadruple quadruple = new RDFQuadruple(new RDFContext(new Uri("ex:ctx")), new RDFResource("ex:subj"), new RDFResource("ex:pred"), new RDFResource("ex:obj"));

            RDFFirebirdStore store = new RDFFirebirdStore(GetConnectionString(Path.Combine(Environment.CurrentDirectory, "RDFFirebirdStoreTest_ShouldNotSelectQuadruplesByContextSubjectPredicate.fdb")), FirebirdVersion);
            store.AddQuadruple(quadruple);
            RDFMemoryStore result = store.SelectQuadruples(new RDFContext(new Uri("ex:ctx2")), new RDFResource("ex:subj"), new RDFResource("ex:pred"), null, null);

            Assert.IsNotNull(result);
            Assert.IsFalse(result.ContainsQuadruple(quadruple));
        }

        [TestMethod]
        public void ShouldSelectQuadruplesByContextSubjectObject()
        {
            File.Delete(Path.Combine(Environment.CurrentDirectory,"RDFFirebirdStoreTest_ShouldSelectQuadruplesByContextSubjectObject.fdb"));
            RDFQuadruple quadruple = new RDFQuadruple(new RDFContext(new Uri("ex:ctx")), new RDFResource("ex:subj"), new RDFResource("ex:pred"), new RDFResource("ex:obj"));

            RDFFirebirdStore store = new RDFFirebirdStore(GetConnectionString(Path.Combine(Environment.CurrentDirectory, "RDFFirebirdStoreTest_ShouldSelectQuadruplesByContextSubjectObject.fdb")), FirebirdVersion);
            store.AddQuadruple(quadruple);
            RDFMemoryStore result = store.SelectQuadruples(new RDFContext(new Uri("ex:ctx")), new RDFResource("ex:subj"), null, new RDFResource("ex:obj"), null);

            Assert.IsNotNull(result);
            Assert.IsTrue(result.ContainsQuadruple(quadruple));
        }

        [TestMethod]
        public void ShouldNotSelectQuadruplesByContextSubjectObject()
        {
            File.Delete(Path.Combine(Environment.CurrentDirectory,"RDFFirebirdStoreTest_ShouldNotSelectQuadruplesByContextSubjectObject.fdb"));
            RDFQuadruple quadruple = new RDFQuadruple(new RDFContext(new Uri("ex:ctx")), new RDFResource("ex:subj"), new RDFResource("ex:pred"), new RDFResource("ex:obj"));

            RDFFirebirdStore store = new RDFFirebirdStore(GetConnectionString(Path.Combine(Environment.CurrentDirectory, "RDFFirebirdStoreTest_ShouldNotSelectQuadruplesByContextSubjectObject.fdb")), FirebirdVersion);
            store.AddQuadruple(quadruple);
            RDFMemoryStore result = store.SelectQuadruples(new RDFContext(new Uri("ex:ctx2")), new RDFResource("ex:subj"), null, new RDFResource("ex:obj"), null);

            Assert.IsNotNull(result);
            Assert.IsFalse(result.ContainsQuadruple(quadruple));
        }

        [TestMethod]
        public void ShouldSelectQuadruplesByContextSubjectLiteral()
        {
            File.Delete(Path.Combine(Environment.CurrentDirectory,"RDFFirebirdStoreTest_ShouldSelectQuadruplesByContextSubjectLiteral.fdb"));
            RDFQuadruple quadruple = new RDFQuadruple(new RDFContext(new Uri("ex:ctx")), new RDFResource("ex:subj"), new RDFResource("ex:pred"), new RDFPlainLiteral("hello"));

            RDFFirebirdStore store = new RDFFirebirdStore(GetConnectionString(Path.Combine(Environment.CurrentDirectory, "RDFFirebirdStoreTest_ShouldSelectQuadruplesByContextSubjectLiteral.fdb")), FirebirdVersion);
            store.AddQuadruple(quadruple);
            RDFMemoryStore result = store.SelectQuadruples(new RDFContext(new Uri("ex:ctx")), new RDFResource("ex:subj"), null, null, new RDFPlainLiteral("hello"));

            Assert.IsNotNull(result);
            Assert.IsTrue(result.ContainsQuadruple(quadruple));
        }

        [TestMethod]
        public void ShouldNotSelectQuadruplesByContextSubjectLiteral()
        {
            File.Delete(Path.Combine(Environment.CurrentDirectory,"RDFFirebirdStoreTest_ShouldNotSelectQuadruplesByContextSubjectLiteral.fdb"));
            RDFQuadruple quadruple = new RDFQuadruple(new RDFContext(new Uri("ex:ctx")), new RDFResource("ex:subj"), new RDFResource("ex:pred"), new RDFPlainLiteral("hello"));

            RDFFirebirdStore store = new RDFFirebirdStore(GetConnectionString(Path.Combine(Environment.CurrentDirectory, "RDFFirebirdStoreTest_ShouldNotSelectQuadruplesByContextSubjectLiteral.fdb")), FirebirdVersion);
            store.AddQuadruple(quadruple);
            RDFMemoryStore result = store.SelectQuadruples(new RDFContext(new Uri("ex:ctx2")), new RDFResource("ex:subj"), null, null, new RDFPlainLiteral("hello"));

            Assert.IsNotNull(result);
            Assert.IsFalse(result.ContainsQuadruple(quadruple));
        }

        [TestMethod]
        public void ShouldSelectQuadruplesByContextPredicateObject()
        {
            File.Delete(Path.Combine(Environment.CurrentDirectory,"RDFFirebirdStoreTest_ShouldSelectQuadruplesByContextPredicateObject.fdb"));
            RDFQuadruple quadruple = new RDFQuadruple(new RDFContext(new Uri("ex:ctx")), new RDFResource("ex:subj"), new RDFResource("ex:pred"), new RDFResource("ex:obj"));

            RDFFirebirdStore store = new RDFFirebirdStore(GetConnectionString(Path.Combine(Environment.CurrentDirectory, "RDFFirebirdStoreTest_ShouldSelectQuadruplesByContextPredicateObject.fdb")), FirebirdVersion);
            store.AddQuadruple(quadruple);
            RDFMemoryStore result = store.SelectQuadruples(new RDFContext(new Uri("ex:ctx")), null, new RDFResource("ex:pred"), new RDFResource("ex:obj"), null);

            Assert.IsNotNull(result);
            Assert.IsTrue(result.ContainsQuadruple(quadruple));
        }

        [TestMethod]
        public void ShouldNotSelectQuadruplesByContextPredicateObject()
        {
            File.Delete(Path.Combine(Environment.CurrentDirectory,"RDFFirebirdStoreTest_ShouldNotSelectQuadruplesByContextPredicateObject.fdb"));
            RDFQuadruple quadruple = new RDFQuadruple(new RDFContext(new Uri("ex:ctx")), new RDFResource("ex:subj"), new RDFResource("ex:pred"), new RDFResource("ex:obj"));

            RDFFirebirdStore store = new RDFFirebirdStore(GetConnectionString(Path.Combine(Environment.CurrentDirectory, "RDFFirebirdStoreTest_ShouldNotSelectQuadruplesByContextPredicateObject.fdb")), FirebirdVersion);
            store.AddQuadruple(quadruple);
            RDFMemoryStore result = store.SelectQuadruples(new RDFContext(new Uri("ex:ctx2")), null, new RDFResource("ex:pred"), new RDFResource("ex:obj"), null);

            Assert.IsNotNull(result);
            Assert.IsFalse(result.ContainsQuadruple(quadruple));
        }

        [TestMethod]
        public void ShouldSelectQuadruplesByContextPredicateLiteral()
        {
            File.Delete(Path.Combine(Environment.CurrentDirectory,"RDFFirebirdStoreTest_ShouldSelectQuadruplesByContextPredicateLiteral.fdb"));
            RDFQuadruple quadruple = new RDFQuadruple(new RDFContext(new Uri("ex:ctx")), new RDFResource("ex:subj"), new RDFResource("ex:pred"), new RDFPlainLiteral("hello"));

            RDFFirebirdStore store = new RDFFirebirdStore(GetConnectionString(Path.Combine(Environment.CurrentDirectory, "RDFFirebirdStoreTest_ShouldSelectQuadruplesByContextPredicateLiteral.fdb")), FirebirdVersion);
            store.AddQuadruple(quadruple);
            RDFMemoryStore result = store.SelectQuadruples(new RDFContext(new Uri("ex:ctx")), null, new RDFResource("ex:pred"), null, new RDFPlainLiteral("hello"));

            Assert.IsNotNull(result);
            Assert.IsTrue(result.ContainsQuadruple(quadruple));
        }

        [TestMethod]
        public void ShouldNotSelectQuadruplesByContextPredicateLiteral()
        {
            File.Delete(Path.Combine(Environment.CurrentDirectory,"RDFFirebirdStoreTest_ShouldNotSelectQuadruplesByContextPredicateLiteral.fdb"));
            RDFQuadruple quadruple = new RDFQuadruple(new RDFContext(new Uri("ex:ctx")), new RDFResource("ex:subj"), new RDFResource("ex:pred"), new RDFPlainLiteral("hello"));

            RDFFirebirdStore store = new RDFFirebirdStore(GetConnectionString(Path.Combine(Environment.CurrentDirectory, "RDFFirebirdStoreTest_ShouldNotSelectQuadruplesByContextPredicateLiteral.fdb")), FirebirdVersion);
            store.AddQuadruple(quadruple);
            RDFMemoryStore result = store.SelectQuadruples(new RDFContext(new Uri("ex:ctx2")), null, new RDFResource("ex:pred"), null, new RDFPlainLiteral("hello"));

            Assert.IsNotNull(result);
            Assert.IsFalse(result.ContainsQuadruple(quadruple));
        }

        [TestMethod]
        public void ShouldSelectQuadruplesByContextSubjectPredicateObject()
        {
            File.Delete(Path.Combine(Environment.CurrentDirectory,"RDFFirebirdStoreTest_ShouldSelectQuadruplesByContextSubjectPredicateObject.fdb"));
            RDFQuadruple quadruple = new RDFQuadruple(new RDFContext(new Uri("ex:ctx")), new RDFResource("ex:subj"), new RDFResource("ex:pred"), new RDFResource("ex:obj"));

            RDFFirebirdStore store = new RDFFirebirdStore(GetConnectionString(Path.Combine(Environment.CurrentDirectory, "RDFFirebirdStoreTest_ShouldSelectQuadruplesByContextSubjectPredicateObject.fdb")), FirebirdVersion);
            store.AddQuadruple(quadruple);
            RDFMemoryStore result = store.SelectQuadruples(new RDFContext(new Uri("ex:ctx")), new RDFResource("ex:subj"), new RDFResource("ex:pred"), new RDFResource("ex:obj"), null);

            Assert.IsNotNull(result);
            Assert.IsTrue(result.ContainsQuadruple(quadruple));
        }

        [TestMethod]
        public void ShouldNotSelectQuadruplesByContextSubjectPredicateObject()
        {
            File.Delete(Path.Combine(Environment.CurrentDirectory,"RDFFirebirdStoreTest_ShouldNotSelectQuadruplesByContextSubjectPredicateObject.fdb"));
            RDFQuadruple quadruple = new RDFQuadruple(new RDFContext(new Uri("ex:ctx")), new RDFResource("ex:subj"), new RDFResource("ex:pred"), new RDFResource("ex:obj"));

            RDFFirebirdStore store = new RDFFirebirdStore(GetConnectionString(Path.Combine(Environment.CurrentDirectory, "RDFFirebirdStoreTest_ShouldNotSelectQuadruplesByContextSubjectPredicateObject.fdb")), FirebirdVersion);
            store.AddQuadruple(quadruple);
            RDFMemoryStore result = store.SelectQuadruples(new RDFContext(new Uri("ex:ctx2")), new RDFResource("ex:subj"), new RDFResource("ex:pred"), new RDFResource("ex:obj"), null);

            Assert.IsNotNull(result);
            Assert.IsFalse(result.ContainsQuadruple(quadruple));
        }

        [TestMethod]
        public void ShouldSelectQuadruplesByContextSubjectPredicateLiteral()
        {
            File.Delete(Path.Combine(Environment.CurrentDirectory,"RDFFirebirdStoreTest_ShouldSelectQuadruplesByContextSubjectPredicateLiteral.fdb"));
            RDFQuadruple quadruple = new RDFQuadruple(new RDFContext(new Uri("ex:ctx")), new RDFResource("ex:subj"), new RDFResource("ex:pred"), new RDFPlainLiteral("hello"));

            RDFFirebirdStore store = new RDFFirebirdStore(GetConnectionString(Path.Combine(Environment.CurrentDirectory, "RDFFirebirdStoreTest_ShouldSelectQuadruplesByContextSubjectPredicateLiteral.fdb")), FirebirdVersion);
            store.AddQuadruple(quadruple);
            RDFMemoryStore result = store.SelectQuadruples(new RDFContext(new Uri("ex:ctx")), new RDFResource("ex:subj"), new RDFResource("ex:pred"), null, new RDFPlainLiteral("hello"));

            Assert.IsNotNull(result);
            Assert.IsTrue(result.ContainsQuadruple(quadruple));
        }

        [TestMethod]
        public void ShouldNotSelectQuadruplesByContextSubjectPredicateLiteral()
        {
            File.Delete(Path.Combine(Environment.CurrentDirectory,"RDFFirebirdStoreTest_ShouldNotSelectQuadruplesByContextSubjectPredicateLiteral.fdb"));
            RDFQuadruple quadruple = new RDFQuadruple(new RDFContext(new Uri("ex:ctx")), new RDFResource("ex:subj"), new RDFResource("ex:pred"), new RDFPlainLiteral("hello"));

            RDFFirebirdStore store = new RDFFirebirdStore(GetConnectionString(Path.Combine(Environment.CurrentDirectory, "RDFFirebirdStoreTest_ShouldNotSelectQuadruplesByContextSubjectPredicateLiteral.fdb")), FirebirdVersion);
            store.AddQuadruple(quadruple);
            RDFMemoryStore result = store.SelectQuadruples(new RDFContext(new Uri("ex:ctx2")), new RDFResource("ex:subj"), new RDFResource("ex:pred"), null, new RDFPlainLiteral("hello"));

            Assert.IsNotNull(result);
            Assert.IsFalse(result.ContainsQuadruple(quadruple));
        }

        [TestMethod]
        public void ShouldSelectQuadruplesBySubjectPredicate()
        {
            File.Delete(Path.Combine(Environment.CurrentDirectory,"RDFFirebirdStoreTest_ShouldSelectQuadruplesBySubjectPredicate.fdb"));
            RDFQuadruple quadruple = new RDFQuadruple(new RDFContext(new Uri("ex:ctx")), new RDFResource("ex:subj"), new RDFResource("ex:pred"), new RDFResource("ex:obj"));

            RDFFirebirdStore store = new RDFFirebirdStore(GetConnectionString(Path.Combine(Environment.CurrentDirectory, "RDFFirebirdStoreTest_ShouldSelectQuadruplesBySubjectPredicate.fdb")), FirebirdVersion);
            store.AddQuadruple(quadruple);
            RDFMemoryStore result = store.SelectQuadruples(null, new RDFResource("ex:subj"), new RDFResource("ex:pred"), null, null);

            Assert.IsNotNull(result);
            Assert.IsTrue(result.ContainsQuadruple(quadruple));
        }

        [TestMethod]
        public void ShouldNotSelectQuadruplesBySubjectPredicate()
        {
            File.Delete(Path.Combine(Environment.CurrentDirectory,"RDFFirebirdStoreTest_ShouldNotSelectQuadruplesBySubjectPredicate.fdb"));
            RDFQuadruple quadruple = new RDFQuadruple(new RDFContext(new Uri("ex:ctx")), new RDFResource("ex:subj"), new RDFResource("ex:pred"), new RDFResource("ex:obj"));

            RDFFirebirdStore store = new RDFFirebirdStore(GetConnectionString(Path.Combine(Environment.CurrentDirectory, "RDFFirebirdStoreTest_ShouldNotSelectQuadruplesBySubjectPredicate.fdb")), FirebirdVersion);
            store.AddQuadruple(quadruple);
            RDFMemoryStore result = store.SelectQuadruples(null, new RDFResource("ex:subj2"), new RDFResource("ex:pred"), null, null);

            Assert.IsNotNull(result);
            Assert.IsFalse(result.ContainsQuadruple(quadruple));
        }

        [TestMethod]
        public void ShouldSelectQuadruplesBySubjectObject()
        {
            File.Delete(Path.Combine(Environment.CurrentDirectory,"RDFFirebirdStoreTest_ShouldSelectQuadruplesBySubjectObject.fdb"));
            RDFQuadruple quadruple = new RDFQuadruple(new RDFContext(new Uri("ex:ctx")), new RDFResource("ex:subj"), new RDFResource("ex:pred"), new RDFResource("ex:obj"));

            RDFFirebirdStore store = new RDFFirebirdStore(GetConnectionString(Path.Combine(Environment.CurrentDirectory, "RDFFirebirdStoreTest_ShouldSelectQuadruplesBySubjectObject.fdb")), FirebirdVersion);
            store.AddQuadruple(quadruple);
            RDFMemoryStore result = store.SelectQuadruples(null, new RDFResource("ex:subj"), null, new RDFResource("ex:obj"), null);

            Assert.IsNotNull(result);
            Assert.IsTrue(result.ContainsQuadruple(quadruple));
        }

        [TestMethod]
        public void ShouldNotSelectQuadruplesBySubjectObject()
        {
            File.Delete(Path.Combine(Environment.CurrentDirectory,"RDFFirebirdStoreTest_ShouldNotSelectQuadruplesBySubjectObject.fdb"));
            RDFQuadruple quadruple = new RDFQuadruple(new RDFContext(new Uri("ex:ctx")), new RDFResource("ex:subj"), new RDFResource("ex:pred"), new RDFResource("ex:obj"));

            RDFFirebirdStore store = new RDFFirebirdStore(GetConnectionString(Path.Combine(Environment.CurrentDirectory, "RDFFirebirdStoreTest_ShouldNotSelectQuadruplesBySubjectObject.fdb")), FirebirdVersion);
            store.AddQuadruple(quadruple);
            RDFMemoryStore result = store.SelectQuadruples(null, new RDFResource("ex:subj2"), null, new RDFResource("ex:obj"), null);

            Assert.IsNotNull(result);
            Assert.IsFalse(result.ContainsQuadruple(quadruple));
        }

        [TestMethod]
        public void ShouldSelectQuadruplesBySubjectLiteral()
        {
            File.Delete(Path.Combine(Environment.CurrentDirectory,"RDFFirebirdStoreTest_ShouldSelectQuadruplesBySubjectLiteral.fdb"));
            RDFQuadruple quadruple = new RDFQuadruple(new RDFContext(new Uri("ex:ctx")), new RDFResource("ex:subj"), new RDFResource("ex:pred"), new RDFPlainLiteral("hello"));

            RDFFirebirdStore store = new RDFFirebirdStore(GetConnectionString(Path.Combine(Environment.CurrentDirectory, "RDFFirebirdStoreTest_ShouldSelectQuadruplesBySubjectLiteral.fdb")), FirebirdVersion);
            store.AddQuadruple(quadruple);
            RDFMemoryStore result = store.SelectQuadruples(null, new RDFResource("ex:subj"), null, null, new RDFPlainLiteral("hello"));

            Assert.IsNotNull(result);
            Assert.IsTrue(result.ContainsQuadruple(quadruple));
        }

        [TestMethod]
        public void ShouldNotSelectQuadruplesBySubjectLiteral()
        {
            File.Delete(Path.Combine(Environment.CurrentDirectory,"RDFFirebirdStoreTest_ShouldNotSelectQuadruplesBySubjectLiteral.fdb"));
            RDFQuadruple quadruple = new RDFQuadruple(new RDFContext(new Uri("ex:ctx")), new RDFResource("ex:subj"), new RDFResource("ex:pred"), new RDFPlainLiteral("hello"));

            RDFFirebirdStore store = new RDFFirebirdStore(GetConnectionString(Path.Combine(Environment.CurrentDirectory, "RDFFirebirdStoreTest_ShouldNotSelectQuadruplesBySubjectLiteral.fdb")), FirebirdVersion);
            store.AddQuadruple(quadruple);
            RDFMemoryStore result = store.SelectQuadruples(null, new RDFResource("ex:subj2"), null, null, new RDFPlainLiteral("hello"));

            Assert.IsNotNull(result);
            Assert.IsFalse(result.ContainsQuadruple(quadruple));
        }

        [TestMethod]
        public void ShouldSelectQuadruplesByPredicateObject()
        {
            File.Delete(Path.Combine(Environment.CurrentDirectory,"RDFFirebirdStoreTest_ShouldSelectQuadruplesByPredicateObject.fdb"));
            RDFQuadruple quadruple = new RDFQuadruple(new RDFContext(new Uri("ex:ctx")), new RDFResource("ex:subj"), new RDFResource("ex:pred"), new RDFResource("ex:obj"));

            RDFFirebirdStore store = new RDFFirebirdStore(GetConnectionString(Path.Combine(Environment.CurrentDirectory, "RDFFirebirdStoreTest_ShouldSelectQuadruplesByPredicateObject.fdb")), FirebirdVersion);
            store.AddQuadruple(quadruple);
            RDFMemoryStore result = store.SelectQuadruples(null, null, new RDFResource("ex:pred"), new RDFResource("ex:obj"), null);

            Assert.IsNotNull(result);
            Assert.IsTrue(result.ContainsQuadruple(quadruple));
        }

        [TestMethod]
        public void ShouldNotSelectQuadruplesByPredicateObject()
        {
            File.Delete(Path.Combine(Environment.CurrentDirectory,"RDFFirebirdStoreTest_ShouldNotSelectQuadruplesByPredicateObject.fdb"));
            RDFQuadruple quadruple = new RDFQuadruple(new RDFContext(new Uri("ex:ctx")), new RDFResource("ex:subj"), new RDFResource("ex:pred"), new RDFResource("ex:obj"));

            RDFFirebirdStore store = new RDFFirebirdStore(GetConnectionString(Path.Combine(Environment.CurrentDirectory, "RDFFirebirdStoreTest_ShouldNotSelectQuadruplesByPredicateObject.fdb")), FirebirdVersion);
            store.AddQuadruple(quadruple);
            RDFMemoryStore result = store.SelectQuadruples(null, null, new RDFResource("ex:pred2"), new RDFResource("ex:obj"), null);

            Assert.IsNotNull(result);
            Assert.IsFalse(result.ContainsQuadruple(quadruple));
        }

        [TestMethod]
        public void ShouldSelectQuadruplesByPredicateLiteral()
        {
            File.Delete(Path.Combine(Environment.CurrentDirectory,"RDFFirebirdStoreTest_ShouldSelectQuadruplesByPredicateLiteral.fdb"));
            RDFQuadruple quadruple = new RDFQuadruple(new RDFContext(new Uri("ex:ctx")), new RDFResource("ex:subj"), new RDFResource("ex:pred"), new RDFPlainLiteral("hello"));

            RDFFirebirdStore store = new RDFFirebirdStore(GetConnectionString(Path.Combine(Environment.CurrentDirectory, "RDFFirebirdStoreTest_ShouldSelectQuadruplesByPredicateLiteral.fdb")), FirebirdVersion);
            store.AddQuadruple(quadruple);
            RDFMemoryStore result = store.SelectQuadruples(null, null, new RDFResource("ex:pred"), null, new RDFPlainLiteral("hello"));

            Assert.IsNotNull(result);
            Assert.IsTrue(result.ContainsQuadruple(quadruple));
        }

        [TestMethod]
        public void ShouldNotSelectQuadruplesByPredicateLiteral()
        {
            File.Delete(Path.Combine(Environment.CurrentDirectory,"RDFFirebirdStoreTest_ShouldNotSelectQuadruplesByPredicateLiteral.fdb"));
            RDFQuadruple quadruple = new RDFQuadruple(new RDFContext(new Uri("ex:ctx")), new RDFResource("ex:subj"), new RDFResource("ex:pred"), new RDFPlainLiteral("hello"));

            RDFFirebirdStore store = new RDFFirebirdStore(GetConnectionString(Path.Combine(Environment.CurrentDirectory, "RDFFirebirdStoreTest_ShouldNotSelectQuadruplesByPredicateLiteral.fdb")), FirebirdVersion);
            store.AddQuadruple(quadruple);
            RDFMemoryStore result = store.SelectQuadruples(null, null, new RDFResource("ex:pred2"), null, new RDFPlainLiteral("hello"));

            Assert.IsNotNull(result);
            Assert.IsFalse(result.ContainsQuadruple(quadruple));
        }

        [TestMethod]
        public void ShouldSelectQuadruplesBySubjectPredicateObject()
        {
            File.Delete(Path.Combine(Environment.CurrentDirectory,"RDFFirebirdStoreTest_ShouldSelectQuadruplesBySubjectPredicateObject.fdb"));
            RDFQuadruple quadruple = new RDFQuadruple(new RDFContext(new Uri("ex:ctx")), new RDFResource("ex:subj"), new RDFResource("ex:pred"), new RDFResource("ex:obj"));

            RDFFirebirdStore store = new RDFFirebirdStore(GetConnectionString(Path.Combine(Environment.CurrentDirectory, "RDFFirebirdStoreTest_ShouldSelectQuadruplesBySubjectPredicateObject.fdb")), FirebirdVersion);
            store.AddQuadruple(quadruple);
            RDFMemoryStore result = store.SelectQuadruples(null, new RDFResource("ex:subj"), new RDFResource("ex:pred"), new RDFResource("ex:obj"), null);

            Assert.IsNotNull(result);
            Assert.IsTrue(result.ContainsQuadruple(quadruple));
        }

        [TestMethod]
        public void ShouldNotSelectQuadruplesBySubjectPredicateObject()
        {
            File.Delete(Path.Combine(Environment.CurrentDirectory,"RDFFirebirdStoreTest_ShouldNotSelectQuadruplesBySubjectPredicateObject.fdb"));
            RDFQuadruple quadruple = new RDFQuadruple(new RDFContext(new Uri("ex:ctx")), new RDFResource("ex:subj"), new RDFResource("ex:pred"), new RDFResource("ex:obj"));

            RDFFirebirdStore store = new RDFFirebirdStore(GetConnectionString(Path.Combine(Environment.CurrentDirectory, "RDFFirebirdStoreTest_ShouldNotSelectQuadruplesBySubjectPredicateObject.fdb")), FirebirdVersion);
            store.AddQuadruple(quadruple);
            RDFMemoryStore result = store.SelectQuadruples(null, new RDFResource("ex:subj2"), new RDFResource("ex:pred"), new RDFResource("ex:obj"), null);

            Assert.IsNotNull(result);
            Assert.IsFalse(result.ContainsQuadruple(quadruple));
        }

        [TestMethod]
        public void ShouldSelectQuadruplesBySubjectPredicateLiteral()
        {
            File.Delete(Path.Combine(Environment.CurrentDirectory,"RDFFirebirdStoreTest_ShouldSelectQuadruplesBySubjectPredicateLiteral.fdb"));
            RDFQuadruple quadruple = new RDFQuadruple(new RDFContext(new Uri("ex:ctx")), new RDFResource("ex:subj"), new RDFResource("ex:pred"), new RDFPlainLiteral("hello"));

            RDFFirebirdStore store = new RDFFirebirdStore(GetConnectionString(Path.Combine(Environment.CurrentDirectory, "RDFFirebirdStoreTest_ShouldSelectQuadruplesBySubjectPredicateLiteral.fdb")), FirebirdVersion);
            store.AddQuadruple(quadruple);
            RDFMemoryStore result = store.SelectQuadruples(null, new RDFResource("ex:subj"), new RDFResource("ex:pred"), null, new RDFPlainLiteral("hello"));

            Assert.IsNotNull(result);
            Assert.IsTrue(result.ContainsQuadruple(quadruple));
        }

        [TestMethod]
        public void ShouldNotSelectQuadruplesBySubjectPredicateLiteral()
        {
            File.Delete(Path.Combine(Environment.CurrentDirectory,"RDFFirebirdStoreTest_ShouldNotSelectQuadruplesBySubjectPredicateLiteral.fdb"));
            RDFQuadruple quadruple = new RDFQuadruple(new RDFContext(new Uri("ex:ctx")), new RDFResource("ex:subj"), new RDFResource("ex:pred"), new RDFPlainLiteral("hello"));

            RDFFirebirdStore store = new RDFFirebirdStore(GetConnectionString(Path.Combine(Environment.CurrentDirectory, "RDFFirebirdStoreTest_ShouldNotSelectQuadruplesBySubjectPredicateLiteral.fdb")), FirebirdVersion);
            store.AddQuadruple(quadruple);
            RDFMemoryStore result = store.SelectQuadruples(null, new RDFResource("ex:subj2"), new RDFResource("ex:pred"), null, new RDFPlainLiteral("hello"));

            Assert.IsNotNull(result);
            Assert.IsFalse(result.ContainsQuadruple(quadruple));
        }
        #endregion        
    }
}