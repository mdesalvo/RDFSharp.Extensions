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
using Microsoft.Data.SqlClient;
using System;
using System.Collections.Generic;
using System.Data;
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
            => $"Server=.\\SQLEXPRESS;Database={database};Trusted_Connection=True;Encrypt=False;";

        private void CreateDatabase(string database)
        {
            SqlConnection connection = default;
            try
            {
                //Create connection
                connection = new SqlConnection(GetConnectionString("master"));

                //Open connection
                connection.Open();

                //Create command
                SqlCommand command = new SqlCommand($"CREATE DATABASE {database};", connection);

                //Execute command
                command.ExecuteNonQuery();

                //Close connection
                connection.Close();
            }
            catch
            {
                //Close connection
                connection.Close();

                //Propagate exception
                throw;
            }
        }

        private void DropDatabase(string database)
        {
            SqlConnection connection = default;
            try
            {
                //Create connection
                connection = new SqlConnection(GetConnectionString("master"));

                //Open connection
                connection.Open();

                //Create command
                SqlCommand command = new SqlCommand($"DROP DATABASE IF EXISTS {database};", connection);

                //Execute command
                command.ExecuteNonQuery();

                //Close connection
                connection.Close();
            }
            catch
            {
                //Close connection
                connection.Close();

                //Propagate exception
                throw;
            }
        }

        #region Tests        
        [TestMethod]
        public void ShouldCreateStore()
        {
            DropDatabase("RDFSQLServerStoreTest_ShouldCreateStore");
            CreateDatabase("RDFSQLServerStoreTest_ShouldCreateStore");

            RDFSQLServerStore store = new RDFSQLServerStore(GetConnectionString("RDFSQLServerStoreTest_ShouldCreateStore"));

            Assert.IsNotNull(store);
            Assert.IsTrue(string.Equals(store.StoreType, "SQLSERVER"));
            Assert.IsTrue(store.StoreID.Equals(RDFModelUtilities.CreateHash(store.ToString())));
            Assert.IsTrue(string.Equals(store.ToString(), string.Concat("SQLSERVER|SERVER=", store.Connection.DataSource, ";DATABASE=", store.Connection.Database)));
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
            DropDatabase("RDFSQLServerStoreTest_ShouldCreateStoreUsingDispose");
            CreateDatabase("RDFSQLServerStoreTest_ShouldCreateStoreUsingDispose");

            RDFSQLServerStore store = default;
            using(store = new RDFSQLServerStore(GetConnectionString("RDFSQLServerStoreTest_ShouldCreateStoreUsingDispose")))
            {
                Assert.IsNotNull(store);
                Assert.IsTrue(string.Equals(store.StoreType, "SQLSERVER"));
                Assert.IsTrue(store.StoreID.Equals(RDFModelUtilities.CreateHash(store.ToString())));
                Assert.IsTrue(string.Equals(store.ToString(), string.Concat("SQLSERVER|SERVER=", store.Connection.DataSource, ";DATABASE=", store.Connection.Database)));
                Assert.IsFalse(store.Disposed);
            }

            Assert.IsTrue(store.Disposed);
            Assert.IsNull(store.Connection);
        }

        [TestMethod]
        public void ShouldCreateStoreInvokingDispose()
        {
            DropDatabase("RDFSQLServerStoreTest_ShouldCreateStoreInvokingDispose");
            CreateDatabase("RDFSQLServerStoreTest_ShouldCreateStoreInvokingDispose");

            RDFSQLServerStore store = new RDFSQLServerStore(GetConnectionString("RDFSQLServerStoreTest_ShouldCreateStoreInvokingDispose"));

            Assert.IsNotNull(store);
            Assert.IsTrue(string.Equals(store.StoreType, "SQLSERVER"));
            Assert.IsTrue(store.StoreID.Equals(RDFModelUtilities.CreateHash(store.ToString())));
            Assert.IsTrue(string.Equals(store.ToString(), string.Concat("SQLSERVER|SERVER=", store.Connection.DataSource, ";DATABASE=", store.Connection.Database)));
            Assert.IsFalse(store.Disposed);

            store.Dispose();
            Assert.IsTrue(store.Disposed);
            Assert.IsNull(store.Connection);
        }

        [TestMethod]
        public void ShouldAddQuadruple()
        {
            DropDatabase("RDFSQLServerStoreTest_ShouldAddQuadruple");
            CreateDatabase("RDFSQLServerStoreTest_ShouldAddQuadruple");

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
            DropDatabase("RDFSQLServerStoreTest_ShouldMergeGraph");
            CreateDatabase("RDFSQLServerStoreTest_ShouldMergeGraph");

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
            DropDatabase("RDFSQLServerStoreTest_ShouldRemoveQuadruple");
            CreateDatabase("RDFSQLServerStoreTest_ShouldRemoveQuadruple");

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
            DropDatabase("RDFSQLServerStoreTest_ShouldNotRemoveQuadruple");
            CreateDatabase("RDFSQLServerStoreTest_ShouldNotRemoveQuadruple");

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
            DropDatabase("RDFSQLServerStoreTest_ShouldRemoveQuadruplesByContext");
            CreateDatabase("RDFSQLServerStoreTest_ShouldRemoveQuadruplesByContext");

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
            DropDatabase("RDFSQLServerStoreTest_ShouldNotRemoveQuadruplesByContext");
            CreateDatabase("RDFSQLServerStoreTest_ShouldNotRemoveQuadruplesByContext");

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
            DropDatabase("RDFSQLServerStoreTest_ShouldRemoveQuadruplesBySubject");
            CreateDatabase("RDFSQLServerStoreTest_ShouldRemoveQuadruplesBySubject");

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
            DropDatabase("RDFSQLServerStoreTest_ShouldNotRemoveQuadruplesBySubject");
            CreateDatabase("RDFSQLServerStoreTest_ShouldNotRemoveQuadruplesBySubject");

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
            DropDatabase("RDFSQLServerStoreTest_ShouldRemoveQuadruplesByPredicate");
            CreateDatabase("RDFSQLServerStoreTest_ShouldRemoveQuadruplesByPredicate");

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
            DropDatabase("RDFSQLServerStoreTest_ShouldNotRemoveQuadruplesByPredicate");
            CreateDatabase("RDFSQLServerStoreTest_ShouldNotRemoveQuadruplesByPredicate");

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
            DropDatabase("RDFSQLServerStoreTest_ShouldRemoveQuadruplesByObject");
            CreateDatabase("RDFSQLServerStoreTest_ShouldRemoveQuadruplesByObject");

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
            DropDatabase("RDFSQLServerStoreTest_ShouldNotRemoveQuadruplesByObject");
            CreateDatabase("RDFSQLServerStoreTest_ShouldNotRemoveQuadruplesByObject");

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
            DropDatabase("RDFSQLServerStoreTest_ShouldRemoveQuadruplesByLiteral");
            CreateDatabase("RDFSQLServerStoreTest_ShouldRemoveQuadruplesByLiteral");

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
            DropDatabase("RDFSQLServerStoreTest_ShouldNotRemoveQuadruplesByLiteral");
            CreateDatabase("RDFSQLServerStoreTest_ShouldNotRemoveQuadruplesByLiteral");

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
            DropDatabase("RDFSQLServerStoreTest_ShouldRemoveQuadruplesByContextSubject");
            CreateDatabase("RDFSQLServerStoreTest_ShouldRemoveQuadruplesByContextSubject");

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
            DropDatabase("RDFSQLServerStoreTest_ShouldNotRemoveQuadruplesByContextSubject");
            CreateDatabase("RDFSQLServerStoreTest_ShouldNotRemoveQuadruplesByContextSubject");

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
            DropDatabase("RDFSQLServerStoreTest_ShouldRemoveQuadruplesByContextPredicate");
            CreateDatabase("RDFSQLServerStoreTest_ShouldRemoveQuadruplesByContextPredicate");

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
            DropDatabase("RDFSQLServerStoreTest_ShouldNotRemoveQuadruplesByContextPredicate");
            CreateDatabase("RDFSQLServerStoreTest_ShouldNotRemoveQuadruplesByContextPredicate");

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
            DropDatabase("RDFSQLServerStoreTest_ShouldRemoveQuadruplesByContextObject");
            CreateDatabase("RDFSQLServerStoreTest_ShouldRemoveQuadruplesByContextObject");

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
            DropDatabase("RDFSQLServerStoreTest_ShouldNotRemoveQuadruplesByContextObject");
            CreateDatabase("RDFSQLServerStoreTest_ShouldNotRemoveQuadruplesByContextObject");

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
            DropDatabase("RDFSQLServerStoreTest_ShouldRemoveQuadruplesByContextLiteral");
            CreateDatabase("RDFSQLServerStoreTest_ShouldRemoveQuadruplesByContextLiteral");

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
            DropDatabase("RDFSQLServerStoreTest_ShouldNotRemoveQuadruplesByContextLiteral");
            CreateDatabase("RDFSQLServerStoreTest_ShouldNotRemoveQuadruplesByContextLiteral");

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
            DropDatabase("RDFSQLServerStoreTest_ShouldRemoveQuadruplesByContextSubjectPredicate");
            CreateDatabase("RDFSQLServerStoreTest_ShouldRemoveQuadruplesByContextSubjectPredicate");

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
            DropDatabase("RDFSQLServerStoreTest_ShouldNotRemoveQuadruplesByContextSubjectPredicate");
            CreateDatabase("RDFSQLServerStoreTest_ShouldNotRemoveQuadruplesByContextSubjectPredicate");

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
            DropDatabase("RDFSQLServerStoreTest_ShouldRemoveQuadruplesByContextSubjectObject");
            CreateDatabase("RDFSQLServerStoreTest_ShouldRemoveQuadruplesByContextSubjectObject");

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
            DropDatabase("RDFSQLServerStoreTest_ShouldNotRemoveQuadruplesByContextSubjectObject");
            CreateDatabase("RDFSQLServerStoreTest_ShouldNotRemoveQuadruplesByContextSubjectObject");

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
            DropDatabase("RDFSQLServerStoreTest_ShouldRemoveQuadruplesByContextSubjectLiteral");
            CreateDatabase("RDFSQLServerStoreTest_ShouldRemoveQuadruplesByContextSubjectLiteral");

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
            DropDatabase("RDFSQLServerStoreTest_ShouldNotRemoveQuadruplesByContextSubjectLiteral");
            CreateDatabase("RDFSQLServerStoreTest_ShouldNotRemoveQuadruplesByContextSubjectLiteral");

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
            DropDatabase("RDFSQLServerStoreTest_ShouldRemoveQuadruplesByContextPredicateObject");
            CreateDatabase("RDFSQLServerStoreTest_ShouldRemoveQuadruplesByContextPredicateObject");

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
            DropDatabase("RDFSQLServerStoreTest_ShouldNotRemoveQuadruplesByContextPredicateObject");
            CreateDatabase("RDFSQLServerStoreTest_ShouldNotRemoveQuadruplesByContextPredicateObject");

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
            DropDatabase("RDFSQLServerStoreTest_ShouldRemoveQuadruplesByContextPredicateLiteral");
            CreateDatabase("RDFSQLServerStoreTest_ShouldRemoveQuadruplesByContextPredicateLiteral");

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
            DropDatabase("RDFSQLServerStoreTest_ShouldNotRemoveQuadruplesByContextPredicateLiteral");
            CreateDatabase("RDFSQLServerStoreTest_ShouldNotRemoveQuadruplesByContextPredicateLiteral");

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
            DropDatabase("RDFSQLServerStoreTest_ShouldRemoveQuadruplesBySubjectPredicate");
            CreateDatabase("RDFSQLServerStoreTest_ShouldRemoveQuadruplesBySubjectPredicate");

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
            DropDatabase("RDFSQLServerStoreTest_ShouldNotRemoveQuadruplesBySubjectPredicate");
            CreateDatabase("RDFSQLServerStoreTest_ShouldNotRemoveQuadruplesBySubjectPredicate");

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
            DropDatabase("RDFSQLServerStoreTest_ShouldRemoveQuadruplesBySubjectObject");
            CreateDatabase("RDFSQLServerStoreTest_ShouldRemoveQuadruplesBySubjectObject");

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
            DropDatabase("RDFSQLServerStoreTest_ShouldNotRemoveQuadruplesBySubjectObject");
            CreateDatabase("RDFSQLServerStoreTest_ShouldNotRemoveQuadruplesBySubjectObject");

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
            DropDatabase("RDFSQLServerStoreTest_ShouldRemoveQuadruplesBySubjectLiteral");
            CreateDatabase("RDFSQLServerStoreTest_ShouldRemoveQuadruplesBySubjectLiteral");

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
            DropDatabase("RDFSQLServerStoreTest_ShouldNotRemoveQuadruplesBySubjectLiteral");
            CreateDatabase("RDFSQLServerStoreTest_ShouldNotRemoveQuadruplesBySubjectLiteral");

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
            DropDatabase("RDFSQLServerStoreTest_ShouldRemoveQuadruplesByPredicateObject");
            CreateDatabase("RDFSQLServerStoreTest_ShouldRemoveQuadruplesByPredicateObject");

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
            DropDatabase("RDFSQLServerStoreTest_ShouldNotRemoveQuadruplesByPredicateObject");
            CreateDatabase("RDFSQLServerStoreTest_ShouldNotRemoveQuadruplesByPredicateObject");

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
            DropDatabase("RDFSQLServerStoreTest_ShouldRemoveQuadruplesByPredicateLiteral");
            CreateDatabase("RDFSQLServerStoreTest_ShouldRemoveQuadruplesByPredicateLiteral");

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
            DropDatabase("RDFSQLServerStoreTest_ShouldNotRemoveQuadruplesByPredicateLiteral");
            CreateDatabase("RDFSQLServerStoreTest_ShouldNotRemoveQuadruplesByPredicateLiteral");

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
            DropDatabase("RDFSQLServerStoreTest_ShouldClearQuadruples");
            CreateDatabase("RDFSQLServerStoreTest_ShouldClearQuadruples");

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
            DropDatabase("RDFSQLServerStoreTest_ShouldContainQuadruple");
            CreateDatabase("RDFSQLServerStoreTest_ShouldContainQuadruple");

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
            DropDatabase("RDFSQLServerStoreTest_ShouldNotContainQuadruple");
            CreateDatabase("RDFSQLServerStoreTest_ShouldNotContainQuadruple");

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
            DropDatabase("RDFSQLServerStoreTest_ShouldSelectQuadruplesByContext");
            CreateDatabase("RDFSQLServerStoreTest_ShouldSelectQuadruplesByContext");

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
            DropDatabase("RDFSQLServerStoreTest_ShouldNotSelectQuadruplesByContext");
            CreateDatabase("RDFSQLServerStoreTest_ShouldNotSelectQuadruplesByContext");

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
            DropDatabase("RDFSQLServerStoreTest_ShouldSelectQuadruplesBySubject");
            CreateDatabase("RDFSQLServerStoreTest_ShouldSelectQuadruplesBySubject");

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
            DropDatabase("RDFSQLServerStoreTest_ShouldNotSelectQuadruplesBySubject");
            CreateDatabase("RDFSQLServerStoreTest_ShouldNotSelectQuadruplesBySubject");

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
            DropDatabase("RDFSQLServerStoreTest_ShouldSelectQuadruplesByPredicate");
            CreateDatabase("RDFSQLServerStoreTest_ShouldSelectQuadruplesByPredicate");

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
            DropDatabase("RDFSQLServerStoreTest_ShouldNotSelectQuadruplesByPredicate");
            CreateDatabase("RDFSQLServerStoreTest_ShouldNotSelectQuadruplesByPredicate");

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
            DropDatabase("RDFSQLServerStoreTest_ShouldSelectQuadruplesByObject");
            CreateDatabase("RDFSQLServerStoreTest_ShouldSelectQuadruplesByObject");

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
            DropDatabase("RDFSQLServerStoreTest_ShouldNotSelectQuadruplesByObject");
            CreateDatabase("RDFSQLServerStoreTest_ShouldNotSelectQuadruplesByObject");

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
            DropDatabase("RDFSQLServerStoreTest_ShouldSelectQuadruplesByLiteral");
            CreateDatabase("RDFSQLServerStoreTest_ShouldSelectQuadruplesByLiteral");

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
            DropDatabase("RDFSQLServerStoreTest_ShouldNotSelectQuadruplesByLiteral");
            CreateDatabase("RDFSQLServerStoreTest_ShouldNotSelectQuadruplesByLiteral");

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
            DropDatabase("RDFSQLServerStoreTest_ShouldSelectQuadruplesByContextSubject");
            CreateDatabase("RDFSQLServerStoreTest_ShouldSelectQuadruplesByContextSubject");

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
            DropDatabase("RDFSQLServerStoreTest_ShouldNotSelectQuadruplesByContextSubject");
            CreateDatabase("RDFSQLServerStoreTest_ShouldNotSelectQuadruplesByContextSubject");

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
            DropDatabase("RDFSQLServerStoreTest_ShouldSelectQuadruplesByContextPredicate");
            CreateDatabase("RDFSQLServerStoreTest_ShouldSelectQuadruplesByContextPredicate");

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
            DropDatabase("RDFSQLServerStoreTest_ShouldNotSelectQuadruplesByContextPredicate");
            CreateDatabase("RDFSQLServerStoreTest_ShouldNotSelectQuadruplesByContextPredicate");

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
            DropDatabase("RDFSQLServerStoreTest_ShouldSelectQuadruplesByContextObject");
            CreateDatabase("RDFSQLServerStoreTest_ShouldSelectQuadruplesByContextObject");

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
            DropDatabase("RDFSQLServerStoreTest_ShouldNotSelectQuadruplesByContextObject");
            CreateDatabase("RDFSQLServerStoreTest_ShouldNotSelectQuadruplesByContextObject");

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
            DropDatabase("RDFSQLServerStoreTest_ShouldSelectQuadruplesByContextLiteral");
            CreateDatabase("RDFSQLServerStoreTest_ShouldSelectQuadruplesByContextLiteral");

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
            DropDatabase("RDFSQLServerStoreTest_ShouldNotSelectQuadruplesByContextLiteral");
            CreateDatabase("RDFSQLServerStoreTest_ShouldNotSelectQuadruplesByContextLiteral");

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
            DropDatabase("RDFSQLServerStoreTest_ShouldSelectQuadruplesByContextSubjectPredicate");
            CreateDatabase("RDFSQLServerStoreTest_ShouldSelectQuadruplesByContextSubjectPredicate");

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
            DropDatabase("RDFSQLServerStoreTest_ShouldNotSelectQuadruplesByContextSubjectPredicate");
            CreateDatabase("RDFSQLServerStoreTest_ShouldNotSelectQuadruplesByContextSubjectPredicate");

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
            DropDatabase("RDFSQLServerStoreTest_ShouldSelectQuadruplesByContextSubjectObject");
            CreateDatabase("RDFSQLServerStoreTest_ShouldSelectQuadruplesByContextSubjectObject");

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
            DropDatabase("RDFSQLServerStoreTest_ShouldNotSelectQuadruplesByContextSubjectObject");
            CreateDatabase("RDFSQLServerStoreTest_ShouldNotSelectQuadruplesByContextSubjectObject");

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
            DropDatabase("RDFSQLServerStoreTest_ShouldSelectQuadruplesByContextSubjectLiteral");
            CreateDatabase("RDFSQLServerStoreTest_ShouldSelectQuadruplesByContextSubjectLiteral");

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
            DropDatabase("RDFSQLServerStoreTest_ShouldNotSelectQuadruplesByContextSubjectLiteral");
            CreateDatabase("RDFSQLServerStoreTest_ShouldNotSelectQuadruplesByContextSubjectLiteral");

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
            DropDatabase("RDFSQLServerStoreTest_ShouldSelectQuadruplesByContextPredicateObject");
            CreateDatabase("RDFSQLServerStoreTest_ShouldSelectQuadruplesByContextPredicateObject");

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
            DropDatabase("RDFSQLServerStoreTest_ShouldNotSelectQuadruplesByContextPredicateObject");
            CreateDatabase("RDFSQLServerStoreTest_ShouldNotSelectQuadruplesByContextPredicateObject");

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
            DropDatabase("RDFSQLServerStoreTest_ShouldSelectQuadruplesByContextPredicateLiteral");
            CreateDatabase("RDFSQLServerStoreTest_ShouldSelectQuadruplesByContextPredicateLiteral");

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
            DropDatabase("RDFSQLServerStoreTest_ShouldNotSelectQuadruplesByContextPredicateLiteral");
            CreateDatabase("RDFSQLServerStoreTest_ShouldNotSelectQuadruplesByContextPredicateLiteral");

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
            DropDatabase("RDFSQLServerStoreTest_ShouldSelectQuadruplesByContextSubjectPredicateObject");
            CreateDatabase("RDFSQLServerStoreTest_ShouldSelectQuadruplesByContextSubjectPredicateObject");

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
            DropDatabase("RDFSQLServerStoreTest_ShouldNotSelectQuadruplesByContextSubjectPredicateObject");
            CreateDatabase("RDFSQLServerStoreTest_ShouldNotSelectQuadruplesByContextSubjectPredicateObject");

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
            DropDatabase("RDFSQLServerStoreTest_ShouldSelectQuadruplesByContextSubjectPredicateLiteral");
            CreateDatabase("RDFSQLServerStoreTest_ShouldSelectQuadruplesByContextSubjectPredicateLiteral");

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
            DropDatabase("RDFSQLServerStoreTest_ShouldNotSelectQuadruplesByContextSubjectPredicateLiteral");
            CreateDatabase("RDFSQLServerStoreTest_ShouldNotSelectQuadruplesByContextSubjectPredicateLiteral");

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
            DropDatabase("RDFSQLServerStoreTest_ShouldSelectQuadruplesBySubjectPredicate");
            CreateDatabase("RDFSQLServerStoreTest_ShouldSelectQuadruplesBySubjectPredicate");

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
            DropDatabase("RDFSQLServerStoreTest_ShouldNotSelectQuadruplesBySubjectPredicate");
            CreateDatabase("RDFSQLServerStoreTest_ShouldNotSelectQuadruplesBySubjectPredicate");

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
            DropDatabase("RDFSQLServerStoreTest_ShouldSelectQuadruplesBySubjectObject");
            CreateDatabase("RDFSQLServerStoreTest_ShouldSelectQuadruplesBySubjectObject");

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
            DropDatabase("RDFSQLServerStoreTest_ShouldNotSelectQuadruplesBySubjectObject");
            CreateDatabase("RDFSQLServerStoreTest_ShouldNotSelectQuadruplesBySubjectObject");

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
            DropDatabase("RDFSQLServerStoreTest_ShouldSelectQuadruplesBySubjectLiteral");
            CreateDatabase("RDFSQLServerStoreTest_ShouldSelectQuadruplesBySubjectLiteral");

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
            DropDatabase("RDFSQLServerStoreTest_ShouldNotSelectQuadruplesBySubjectLiteral");
            CreateDatabase("RDFSQLServerStoreTest_ShouldNotSelectQuadruplesBySubjectLiteral");

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
            DropDatabase("RDFSQLServerStoreTest_ShouldSelectQuadruplesByPredicateObject");
            CreateDatabase("RDFSQLServerStoreTest_ShouldSelectQuadruplesByPredicateObject");

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
            DropDatabase("RDFSQLServerStoreTest_ShouldNotSelectQuadruplesByPredicateObject");
            CreateDatabase("RDFSQLServerStoreTest_ShouldNotSelectQuadruplesByPredicateObject");

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
            DropDatabase("RDFSQLServerStoreTest_ShouldSelectQuadruplesByPredicateLiteral");
            CreateDatabase("RDFSQLServerStoreTest_ShouldSelectQuadruplesByPredicateLiteral");

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
            DropDatabase("RDFSQLServerStoreTest_ShouldNotSelectQuadruplesByPredicateLiteral");
            CreateDatabase("RDFSQLServerStoreTest_ShouldNotSelectQuadruplesByPredicateLiteral");

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
            DropDatabase("RDFSQLServerStoreTest_ShouldSelectQuadruplesBySubjectPredicateObject");
            CreateDatabase("RDFSQLServerStoreTest_ShouldSelectQuadruplesBySubjectPredicateObject");

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
            DropDatabase("RDFSQLServerStoreTest_ShouldNotSelectQuadruplesBySubjectPredicateObject");
            CreateDatabase("RDFSQLServerStoreTest_ShouldNotSelectQuadruplesBySubjectPredicateObject");

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
            DropDatabase("RDFSQLServerStoreTest_ShouldSelectQuadruplesBySubjectPredicateLiteral");
            CreateDatabase("RDFSQLServerStoreTest_ShouldSelectQuadruplesBySubjectPredicateLiteral");

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
            DropDatabase("RDFSQLServerStoreTest_ShouldNotSelectQuadruplesBySubjectPredicateLiteral");
            CreateDatabase("RDFSQLServerStoreTest_ShouldNotSelectQuadruplesBySubjectPredicateLiteral");

            RDFQuadruple quadruple = new RDFQuadruple(new RDFContext(new Uri("ex:ctx")), new RDFResource("ex:subj"), new RDFResource("ex:pred"), new RDFPlainLiteral("hello"));

            RDFSQLServerStore store = new RDFSQLServerStore(GetConnectionString("RDFSQLServerStoreTest_ShouldNotSelectQuadruplesBySubjectPredicateLiteral"));
            store.AddQuadruple(quadruple);
            RDFMemoryStore result = store.SelectQuadruples(null, new RDFResource("ex:subj2"), new RDFResource("ex:pred"), null, new RDFPlainLiteral("hello"));

            Assert.IsNotNull(result);
            Assert.IsFalse(result.ContainsQuadruple(quadruple));
        }

        [TestMethod]
        public void ShouldOptimize()
        {
            DropDatabase("RDFSQLServerStoreTest_ShouldOptimize");
            CreateDatabase("RDFSQLServerStoreTest_ShouldOptimize");

            RDFQuadruple quadruple1 = new RDFQuadruple(new RDFContext(new Uri("ex:ctx1")), new RDFResource("ex:subj"), new RDFResource("ex:pred"), new RDFResource("ex:obj"));
            RDFQuadruple quadruple2 = new RDFQuadruple(new RDFContext(new Uri("ex:ctx1")), new RDFResource("ex:subj"), new RDFResource("ex:pred"), new RDFPlainLiteral("hello"));
            RDFQuadruple quadruple3 = new RDFQuadruple(new RDFContext(new Uri("ex:ctx2")), new RDFResource("ex:subj"), new RDFResource("ex:pred"), new RDFPlainLiteral("hello"));
            RDFQuadruple quadruple4 = new RDFQuadruple(new RDFContext(new Uri("ex:ctx3")), new RDFResource("ex:subj"), new RDFResource("ex:pred"), new RDFPlainLiteral("hello"));
            RDFQuadruple quadruple5 = new RDFQuadruple(new RDFContext(new Uri("ex:ctx4")), new RDFResource("ex:subj"), new RDFResource("ex:pred"), new RDFResource("ex:obj"));
            RDFQuadruple quadruple6 = new RDFQuadruple(new RDFContext(new Uri("ex:ctx4")), new RDFResource("ex:subj4"), new RDFResource("ex:pred4"), new RDFResource("ex:obj"));

            RDFSQLServerStore store = new RDFSQLServerStore(GetConnectionString("RDFSQLServerStoreTest_ShouldOptimize"));
            store.AddQuadruple(quadruple1);
            store.AddQuadruple(quadruple2);
            store.AddQuadruple(quadruple3);
            store.AddQuadruple(quadruple4);
            store.RemoveQuadruple(quadruple1);
            store.RemoveQuadruple(quadruple2);
            store.AddQuadruple(quadruple1);
            store.AddQuadruple(quadruple2);
            store.AddQuadruple(quadruple5);
            store.AddQuadruple(quadruple6);

            store.OptimizeStore();
            Assert.IsTrue(true);
        }
        #endregion        
    }
}
