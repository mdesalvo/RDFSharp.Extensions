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
using MySql.Data.MySqlClient;
using System;
using System.Collections.Generic;
using System.Linq;
using RDFSharp.Model;

namespace RDFSharp.Store.Test
{
    [TestClass]
    public class RDFMySQLStoreTest
    {
        //This test suite is based on a local installation of MySQL using unencrypted Windows authentication
        private string GetConnectionString(string database)
            => $"Server=.\\SQLEXPRESS;Database={database};Trusted_Connection=True;Encrypt=False;";

        private void CreateDatabase(string database)
        {
            MySqlConnection connection = default;
            try
            {
                //Create connection
                connection = new MySqlConnection(GetConnectionString("master"));

                //Open connection
                connection.Open();

                //Create command
                MySqlCommand command = new MySqlCommand($"CREATE DATABASE {database};", connection);

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
            MySqlConnection connection = default;
            try
            {
                //Create connection
                connection = new MySqlConnection(GetConnectionString("master"));

                //Open connection
                connection.Open();

                //Create command
                MySqlCommand command = new MySqlCommand($"DROP DATABASE IF EXISTS {database};", connection);

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
            DropDatabase("RDFMySQLStoreTest_ShouldCreateStore");
            CreateDatabase("RDFMySQLStoreTest_ShouldCreateStore");

            RDFMySQLStore store = new RDFMySQLStore(GetConnectionString("RDFMySQLStoreTest_ShouldCreateStore"));

            Assert.IsNotNull(store);
            Assert.IsTrue(string.Equals(store.StoreType, "MYSQL"));
            Assert.IsTrue(store.StoreID.Equals(RDFModelUtilities.CreateHash(store.ToString())));
            Assert.IsTrue(string.Equals(store.ToString(), string.Concat("MYSQL|SERVER=", store.Connection.DataSource, ";DATABASE=", store.Connection.Database)));
        }

        [TestMethod]
        public void ShouldThrowExceptionOnCreatingStoreBecauseNullOrEmptyPath()
            => Assert.ThrowsException<RDFStoreException>(() => new RDFMySQLStore(null));

        [TestMethod]
        public void ShouldThrowExceptionOnCreatingStoreBecauseUnaccessiblePath()
            => Assert.ThrowsException<RDFStoreException>(() => new RDFMySQLStore(GetConnectionString("http://example.org/file")));

        [TestMethod]
        public void ShouldCreateStoreUsingDispose()
        {
            DropDatabase("RDFMySQLStoreTest_ShouldCreateStoreUsingDispose");
            CreateDatabase("RDFMySQLStoreTest_ShouldCreateStoreUsingDispose");

            RDFMySQLStore store = default;
            using(store = new RDFMySQLStore(GetConnectionString("RDFMySQLStoreTest_ShouldCreateStoreUsingDispose")))
            {
                Assert.IsNotNull(store);
                Assert.IsTrue(string.Equals(store.StoreType, "MYSQL"));
                Assert.IsTrue(store.StoreID.Equals(RDFModelUtilities.CreateHash(store.ToString())));
                Assert.IsTrue(string.Equals(store.ToString(), string.Concat("MYSQL|SERVER=", store.Connection.DataSource, ";DATABASE=", store.Connection.Database)));
                Assert.IsFalse(store.Disposed);
            }

            Assert.IsTrue(store.Disposed);
            Assert.IsNull(store.Connection);
        }

        [TestMethod]
        public void ShouldCreateStoreInvokingDispose()
        {
            DropDatabase("RDFMySQLStoreTest_ShouldCreateStoreInvokingDispose");
            CreateDatabase("RDFMySQLStoreTest_ShouldCreateStoreInvokingDispose");

            RDFMySQLStore store = new RDFMySQLStore(GetConnectionString("RDFMySQLStoreTest_ShouldCreateStoreInvokingDispose"));

            Assert.IsNotNull(store);
            Assert.IsTrue(string.Equals(store.StoreType, "MYSQL"));
            Assert.IsTrue(store.StoreID.Equals(RDFModelUtilities.CreateHash(store.ToString())));
            Assert.IsTrue(string.Equals(store.ToString(), string.Concat("MYSQL|SERVER=", store.Connection.DataSource, ";DATABASE=", store.Connection.Database)));
            Assert.IsFalse(store.Disposed);

            store.Dispose();
            Assert.IsTrue(store.Disposed);
            Assert.IsNull(store.Connection);
        }

        [TestMethod]
        public void ShouldAddQuadruple()
        {
            DropDatabase("RDFMySQLStoreTest_ShouldAddQuadruple");
            CreateDatabase("RDFMySQLStoreTest_ShouldAddQuadruple");

            RDFQuadruple quadruple = new RDFQuadruple(new RDFContext(new Uri("ex:ctx")), new RDFResource("ex:subj"), new RDFResource("ex:pred"), new RDFResource("ex:obj"));

            RDFMySQLStore store = new RDFMySQLStore(GetConnectionString("RDFMySQLStoreTest_ShouldAddQuadruple"));
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
            DropDatabase("RDFMySQLStoreTest_ShouldMergeGraph");
            CreateDatabase("RDFMySQLStoreTest_ShouldMergeGraph");

            RDFGraph graph = new RDFGraph(new List<RDFTriple>() {
                new RDFTriple(new RDFResource("ex:subj"), new RDFResource("ex:pred"), new RDFResource("ex:obj"))
            }).SetContext(new Uri("ex:ctx"));

            RDFMySQLStore store = new RDFMySQLStore(GetConnectionString("RDFMySQLStoreTest_ShouldMergeGraph"));
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
            DropDatabase("RDFMySQLStoreTest_ShouldRemoveQuadruple");
            CreateDatabase("RDFMySQLStoreTest_ShouldRemoveQuadruple");

            RDFQuadruple quadruple = new RDFQuadruple(new RDFContext(new Uri("ex:ctx")), new RDFResource("ex:subj"), new RDFResource("ex:pred"), new RDFResource("ex:obj"));

            RDFMySQLStore store = new RDFMySQLStore(GetConnectionString("RDFMySQLStoreTest_ShouldRemoveQuadruple"));
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
            DropDatabase("RDFMySQLStoreTest_ShouldNotRemoveQuadruple");
            CreateDatabase("RDFMySQLStoreTest_ShouldNotRemoveQuadruple");

            RDFQuadruple quadruple = new RDFQuadruple(new RDFContext(new Uri("ex:ctx")), new RDFResource("ex:subj"), new RDFResource("ex:pred"), new RDFResource("ex:obj"));
            RDFQuadruple quadruple2 = new RDFQuadruple(new RDFContext(new Uri("ex:ctx2")), new RDFResource("ex:subj"), new RDFResource("ex:pred"), new RDFResource("ex:obj"));

            RDFMySQLStore store = new RDFMySQLStore(GetConnectionString("RDFMySQLStoreTest_ShouldNotRemoveQuadruple"));
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
            DropDatabase("RDFMySQLStoreTest_ShouldRemoveQuadruplesByContext");
            CreateDatabase("RDFMySQLStoreTest_ShouldRemoveQuadruplesByContext");

            RDFQuadruple quadruple = new RDFQuadruple(new RDFContext(new Uri("ex:ctx")), new RDFResource("ex:subj"), new RDFResource("ex:pred"), new RDFResource("ex:obj"));

            RDFMySQLStore store = new RDFMySQLStore(GetConnectionString("RDFMySQLStoreTest_ShouldRemoveQuadruplesByContext"));
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
            DropDatabase("RDFMySQLStoreTest_ShouldNotRemoveQuadruplesByContext");
            CreateDatabase("RDFMySQLStoreTest_ShouldNotRemoveQuadruplesByContext");

            RDFQuadruple quadruple = new RDFQuadruple(new RDFContext(new Uri("ex:ctx")), new RDFResource("ex:subj"), new RDFResource("ex:pred"), new RDFResource("ex:obj"));

            RDFMySQLStore store = new RDFMySQLStore(GetConnectionString("RDFMySQLStoreTest_ShouldNotRemoveQuadruplesByContext"));
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
            DropDatabase("RDFMySQLStoreTest_ShouldRemoveQuadruplesBySubject");
            CreateDatabase("RDFMySQLStoreTest_ShouldRemoveQuadruplesBySubject");

            RDFQuadruple quadruple = new RDFQuadruple(new RDFContext(new Uri("ex:ctx")), new RDFResource("ex:subj"), new RDFResource("ex:pred"), new RDFResource("ex:obj"));

            RDFMySQLStore store = new RDFMySQLStore(GetConnectionString("RDFMySQLStoreTest_ShouldRemoveQuadruplesBySubject"));
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
            DropDatabase("RDFMySQLStoreTest_ShouldNotRemoveQuadruplesBySubject");
            CreateDatabase("RDFMySQLStoreTest_ShouldNotRemoveQuadruplesBySubject");

            RDFQuadruple quadruple = new RDFQuadruple(new RDFContext(new Uri("ex:ctx")), new RDFResource("ex:subj"), new RDFResource("ex:pred"), new RDFResource("ex:obj"));

            RDFMySQLStore store = new RDFMySQLStore(GetConnectionString("RDFMySQLStoreTest_ShouldNotRemoveQuadruplesBySubject"));
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
            DropDatabase("RDFMySQLStoreTest_ShouldRemoveQuadruplesByPredicate");
            CreateDatabase("RDFMySQLStoreTest_ShouldRemoveQuadruplesByPredicate");

            RDFQuadruple quadruple = new RDFQuadruple(new RDFContext(new Uri("ex:ctx")), new RDFResource("ex:subj"), new RDFResource("ex:pred"), new RDFResource("ex:obj"));

            RDFMySQLStore store = new RDFMySQLStore(GetConnectionString("RDFMySQLStoreTest_ShouldRemoveQuadruplesByPredicate"));
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
            DropDatabase("RDFMySQLStoreTest_ShouldNotRemoveQuadruplesByPredicate");
            CreateDatabase("RDFMySQLStoreTest_ShouldNotRemoveQuadruplesByPredicate");

            RDFQuadruple quadruple = new RDFQuadruple(new RDFContext(new Uri("ex:ctx")), new RDFResource("ex:subj"), new RDFResource("ex:pred"), new RDFResource("ex:obj"));

            RDFMySQLStore store = new RDFMySQLStore(GetConnectionString("RDFMySQLStoreTest_ShouldNotRemoveQuadruplesByPredicate"));
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
            DropDatabase("RDFMySQLStoreTest_ShouldRemoveQuadruplesByObject");
            CreateDatabase("RDFMySQLStoreTest_ShouldRemoveQuadruplesByObject");

            RDFQuadruple quadruple = new RDFQuadruple(new RDFContext(new Uri("ex:ctx")), new RDFResource("ex:subj"), new RDFResource("ex:pred"), new RDFResource("ex:obj"));

            RDFMySQLStore store = new RDFMySQLStore(GetConnectionString("RDFMySQLStoreTest_ShouldRemoveQuadruplesByObject"));
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
            DropDatabase("RDFMySQLStoreTest_ShouldNotRemoveQuadruplesByObject");
            CreateDatabase("RDFMySQLStoreTest_ShouldNotRemoveQuadruplesByObject");

            RDFQuadruple quadruple = new RDFQuadruple(new RDFContext(new Uri("ex:ctx")), new RDFResource("ex:subj"), new RDFResource("ex:pred"), new RDFResource("ex:obj"));

            RDFMySQLStore store = new RDFMySQLStore(GetConnectionString("RDFMySQLStoreTest_ShouldNotRemoveQuadruplesByObject"));
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
            DropDatabase("RDFMySQLStoreTest_ShouldRemoveQuadruplesByLiteral");
            CreateDatabase("RDFMySQLStoreTest_ShouldRemoveQuadruplesByLiteral");

            RDFQuadruple quadruple = new RDFQuadruple(new RDFContext(new Uri("ex:ctx")), new RDFResource("ex:subj"), new RDFResource("ex:pred"), new RDFPlainLiteral("hello"));

            RDFMySQLStore store = new RDFMySQLStore(GetConnectionString("RDFMySQLStoreTest_ShouldRemoveQuadruplesByLiteral"));
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
            DropDatabase("RDFMySQLStoreTest_ShouldNotRemoveQuadruplesByLiteral");
            CreateDatabase("RDFMySQLStoreTest_ShouldNotRemoveQuadruplesByLiteral");

            RDFQuadruple quadruple = new RDFQuadruple(new RDFContext(new Uri("ex:ctx")), new RDFResource("ex:subj"), new RDFResource("ex:pred"), new RDFPlainLiteral("hello"));

            RDFMySQLStore store = new RDFMySQLStore(GetConnectionString("RDFMySQLStoreTest_ShouldNotRemoveQuadruplesByLiteral"));
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
            DropDatabase("RDFMySQLStoreTest_ShouldRemoveQuadruplesByContextSubject");
            CreateDatabase("RDFMySQLStoreTest_ShouldRemoveQuadruplesByContextSubject");

            RDFQuadruple quadruple = new RDFQuadruple(new RDFContext(new Uri("ex:ctx")), new RDFResource("ex:subj"), new RDFResource("ex:pred"), new RDFResource("ex:obj"));

            RDFMySQLStore store = new RDFMySQLStore(GetConnectionString("RDFMySQLStoreTest_ShouldRemoveQuadruplesByContextSubject"));
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
            DropDatabase("RDFMySQLStoreTest_ShouldNotRemoveQuadruplesByContextSubject");
            CreateDatabase("RDFMySQLStoreTest_ShouldNotRemoveQuadruplesByContextSubject");

            RDFQuadruple quadruple = new RDFQuadruple(new RDFContext(new Uri("ex:ctx")), new RDFResource("ex:subj"), new RDFResource("ex:pred"), new RDFResource("ex:obj"));

            RDFMySQLStore store = new RDFMySQLStore(GetConnectionString("RDFMySQLStoreTest_ShouldNotRemoveQuadruplesByContextSubject"));
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
            DropDatabase("RDFMySQLStoreTest_ShouldRemoveQuadruplesByContextPredicate");
            CreateDatabase("RDFMySQLStoreTest_ShouldRemoveQuadruplesByContextPredicate");

            RDFQuadruple quadruple = new RDFQuadruple(new RDFContext(new Uri("ex:ctx")), new RDFResource("ex:subj"), new RDFResource("ex:pred"), new RDFResource("ex:obj"));

            RDFMySQLStore store = new RDFMySQLStore(GetConnectionString("RDFMySQLStoreTest_ShouldRemoveQuadruplesByContextPredicate"));
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
            DropDatabase("RDFMySQLStoreTest_ShouldNotRemoveQuadruplesByContextPredicate");
            CreateDatabase("RDFMySQLStoreTest_ShouldNotRemoveQuadruplesByContextPredicate");

            RDFQuadruple quadruple = new RDFQuadruple(new RDFContext(new Uri("ex:ctx")), new RDFResource("ex:subj"), new RDFResource("ex:pred"), new RDFResource("ex:obj"));

            RDFMySQLStore store = new RDFMySQLStore(GetConnectionString("RDFMySQLStoreTest_ShouldNotRemoveQuadruplesByContextPredicate"));
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
            DropDatabase("RDFMySQLStoreTest_ShouldRemoveQuadruplesByContextObject");
            CreateDatabase("RDFMySQLStoreTest_ShouldRemoveQuadruplesByContextObject");

            RDFQuadruple quadruple = new RDFQuadruple(new RDFContext(new Uri("ex:ctx")), new RDFResource("ex:subj"), new RDFResource("ex:pred"), new RDFResource("ex:obj"));

            RDFMySQLStore store = new RDFMySQLStore(GetConnectionString("RDFMySQLStoreTest_ShouldRemoveQuadruplesByContextObject"));
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
            DropDatabase("RDFMySQLStoreTest_ShouldNotRemoveQuadruplesByContextObject");
            CreateDatabase("RDFMySQLStoreTest_ShouldNotRemoveQuadruplesByContextObject");

            RDFQuadruple quadruple = new RDFQuadruple(new RDFContext(new Uri("ex:ctx")), new RDFResource("ex:subj"), new RDFResource("ex:pred"), new RDFResource("ex:obj"));

            RDFMySQLStore store = new RDFMySQLStore(GetConnectionString("RDFMySQLStoreTest_ShouldNotRemoveQuadruplesByContextObject"));
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
            DropDatabase("RDFMySQLStoreTest_ShouldRemoveQuadruplesByContextLiteral");
            CreateDatabase("RDFMySQLStoreTest_ShouldRemoveQuadruplesByContextLiteral");

            RDFQuadruple quadruple = new RDFQuadruple(new RDFContext(new Uri("ex:ctx")), new RDFResource("ex:subj"), new RDFResource("ex:pred"), new RDFPlainLiteral("hello"));

            RDFMySQLStore store = new RDFMySQLStore(GetConnectionString("RDFMySQLStoreTest_ShouldRemoveQuadruplesByContextLiteral"));
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
            DropDatabase("RDFMySQLStoreTest_ShouldNotRemoveQuadruplesByContextLiteral");
            CreateDatabase("RDFMySQLStoreTest_ShouldNotRemoveQuadruplesByContextLiteral");

            RDFQuadruple quadruple = new RDFQuadruple(new RDFContext(new Uri("ex:ctx")), new RDFResource("ex:subj"), new RDFResource("ex:pred"), new RDFPlainLiteral("hello"));

            RDFMySQLStore store = new RDFMySQLStore(GetConnectionString("RDFMySQLStoreTest_ShouldNotRemoveQuadruplesByContextLiteral"));
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
            DropDatabase("RDFMySQLStoreTest_ShouldRemoveQuadruplesByContextSubjectPredicate");
            CreateDatabase("RDFMySQLStoreTest_ShouldRemoveQuadruplesByContextSubjectPredicate");

            RDFQuadruple quadruple = new RDFQuadruple(new RDFContext(new Uri("ex:ctx")), new RDFResource("ex:subj"), new RDFResource("ex:pred"), new RDFResource("ex:obj"));

            RDFMySQLStore store = new RDFMySQLStore(GetConnectionString("RDFMySQLStoreTest_ShouldRemoveQuadruplesByContextSubjectPredicate"));
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
            DropDatabase("RDFMySQLStoreTest_ShouldNotRemoveQuadruplesByContextSubjectPredicate");
            CreateDatabase("RDFMySQLStoreTest_ShouldNotRemoveQuadruplesByContextSubjectPredicate");

            RDFQuadruple quadruple = new RDFQuadruple(new RDFContext(new Uri("ex:ctx")), new RDFResource("ex:subj"), new RDFResource("ex:pred"), new RDFResource("ex:obj"));

            RDFMySQLStore store = new RDFMySQLStore(GetConnectionString("RDFMySQLStoreTest_ShouldNotRemoveQuadruplesByContextSubjectPredicate"));
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
            DropDatabase("RDFMySQLStoreTest_ShouldRemoveQuadruplesByContextSubjectObject");
            CreateDatabase("RDFMySQLStoreTest_ShouldRemoveQuadruplesByContextSubjectObject");

            RDFQuadruple quadruple = new RDFQuadruple(new RDFContext(new Uri("ex:ctx")), new RDFResource("ex:subj"), new RDFResource("ex:pred"), new RDFResource("ex:obj"));

            RDFMySQLStore store = new RDFMySQLStore(GetConnectionString("RDFMySQLStoreTest_ShouldRemoveQuadruplesByContextSubjectObject"));
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
            DropDatabase("RDFMySQLStoreTest_ShouldNotRemoveQuadruplesByContextSubjectObject");
            CreateDatabase("RDFMySQLStoreTest_ShouldNotRemoveQuadruplesByContextSubjectObject");

            RDFQuadruple quadruple = new RDFQuadruple(new RDFContext(new Uri("ex:ctx")), new RDFResource("ex:subj"), new RDFResource("ex:pred"), new RDFResource("ex:obj"));

            RDFMySQLStore store = new RDFMySQLStore(GetConnectionString("RDFMySQLStoreTest_ShouldNotRemoveQuadruplesByContextSubjectObject"));
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
            DropDatabase("RDFMySQLStoreTest_ShouldRemoveQuadruplesByContextSubjectLiteral");
            CreateDatabase("RDFMySQLStoreTest_ShouldRemoveQuadruplesByContextSubjectLiteral");

            RDFQuadruple quadruple = new RDFQuadruple(new RDFContext(new Uri("ex:ctx")), new RDFResource("ex:subj"), new RDFResource("ex:pred"), new RDFPlainLiteral("hello"));

            RDFMySQLStore store = new RDFMySQLStore(GetConnectionString("RDFMySQLStoreTest_ShouldRemoveQuadruplesByContextSubjectLiteral"));
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
            DropDatabase("RDFMySQLStoreTest_ShouldNotRemoveQuadruplesByContextSubjectLiteral");
            CreateDatabase("RDFMySQLStoreTest_ShouldNotRemoveQuadruplesByContextSubjectLiteral");

            RDFQuadruple quadruple = new RDFQuadruple(new RDFContext(new Uri("ex:ctx")), new RDFResource("ex:subj"), new RDFResource("ex:pred"), new RDFPlainLiteral("hello"));

            RDFMySQLStore store = new RDFMySQLStore(GetConnectionString("RDFMySQLStoreTest_ShouldNotRemoveQuadruplesByContextSubjectLiteral"));
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
            DropDatabase("RDFMySQLStoreTest_ShouldRemoveQuadruplesByContextPredicateObject");
            CreateDatabase("RDFMySQLStoreTest_ShouldRemoveQuadruplesByContextPredicateObject");

            RDFQuadruple quadruple = new RDFQuadruple(new RDFContext(new Uri("ex:ctx")), new RDFResource("ex:subj"), new RDFResource("ex:pred"), new RDFResource("ex:obj"));

            RDFMySQLStore store = new RDFMySQLStore(GetConnectionString("RDFMySQLStoreTest_ShouldRemoveQuadruplesByContextPredicateObject"));
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
            DropDatabase("RDFMySQLStoreTest_ShouldNotRemoveQuadruplesByContextPredicateObject");
            CreateDatabase("RDFMySQLStoreTest_ShouldNotRemoveQuadruplesByContextPredicateObject");

            RDFQuadruple quadruple = new RDFQuadruple(new RDFContext(new Uri("ex:ctx")), new RDFResource("ex:subj"), new RDFResource("ex:pred"), new RDFResource("ex:obj"));

            RDFMySQLStore store = new RDFMySQLStore(GetConnectionString("RDFMySQLStoreTest_ShouldNotRemoveQuadruplesByContextPredicateObject"));
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
            DropDatabase("RDFMySQLStoreTest_ShouldRemoveQuadruplesByContextPredicateLiteral");
            CreateDatabase("RDFMySQLStoreTest_ShouldRemoveQuadruplesByContextPredicateLiteral");

            RDFQuadruple quadruple = new RDFQuadruple(new RDFContext(new Uri("ex:ctx")), new RDFResource("ex:subj"), new RDFResource("ex:pred"), new RDFPlainLiteral("hello"));

            RDFMySQLStore store = new RDFMySQLStore(GetConnectionString("RDFMySQLStoreTest_ShouldRemoveQuadruplesByContextPredicateLiteral"));
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
            DropDatabase("RDFMySQLStoreTest_ShouldNotRemoveQuadruplesByContextPredicateLiteral");
            CreateDatabase("RDFMySQLStoreTest_ShouldNotRemoveQuadruplesByContextPredicateLiteral");

            RDFQuadruple quadruple = new RDFQuadruple(new RDFContext(new Uri("ex:ctx")), new RDFResource("ex:subj"), new RDFResource("ex:pred"), new RDFPlainLiteral("hello"));

            RDFMySQLStore store = new RDFMySQLStore(GetConnectionString("RDFMySQLStoreTest_ShouldNotRemoveQuadruplesByContextPredicateLiteral"));
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
            DropDatabase("RDFMySQLStoreTest_ShouldRemoveQuadruplesBySubjectPredicate");
            CreateDatabase("RDFMySQLStoreTest_ShouldRemoveQuadruplesBySubjectPredicate");

            RDFQuadruple quadruple = new RDFQuadruple(new RDFContext(new Uri("ex:ctx")), new RDFResource("ex:subj"), new RDFResource("ex:pred"), new RDFResource("ex:obj"));

            RDFMySQLStore store = new RDFMySQLStore(GetConnectionString("RDFMySQLStoreTest_ShouldRemoveQuadruplesBySubjectPredicate"));
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
            DropDatabase("RDFMySQLStoreTest_ShouldNotRemoveQuadruplesBySubjectPredicate");
            CreateDatabase("RDFMySQLStoreTest_ShouldNotRemoveQuadruplesBySubjectPredicate");

            RDFQuadruple quadruple = new RDFQuadruple(new RDFContext(new Uri("ex:ctx")), new RDFResource("ex:subj"), new RDFResource("ex:pred"), new RDFResource("ex:obj"));

            RDFMySQLStore store = new RDFMySQLStore(GetConnectionString("RDFMySQLStoreTest_ShouldNotRemoveQuadruplesBySubjectPredicate"));
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
            DropDatabase("RDFMySQLStoreTest_ShouldRemoveQuadruplesBySubjectObject");
            CreateDatabase("RDFMySQLStoreTest_ShouldRemoveQuadruplesBySubjectObject");

            RDFQuadruple quadruple = new RDFQuadruple(new RDFContext(new Uri("ex:ctx")), new RDFResource("ex:subj"), new RDFResource("ex:pred"), new RDFResource("ex:obj"));

            RDFMySQLStore store = new RDFMySQLStore(GetConnectionString("RDFMySQLStoreTest_ShouldRemoveQuadruplesBySubjectObject"));
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
            DropDatabase("RDFMySQLStoreTest_ShouldNotRemoveQuadruplesBySubjectObject");
            CreateDatabase("RDFMySQLStoreTest_ShouldNotRemoveQuadruplesBySubjectObject");

            RDFQuadruple quadruple = new RDFQuadruple(new RDFContext(new Uri("ex:ctx")), new RDFResource("ex:subj"), new RDFResource("ex:pred"), new RDFResource("ex:obj"));

            RDFMySQLStore store = new RDFMySQLStore(GetConnectionString("RDFMySQLStoreTest_ShouldNotRemoveQuadruplesBySubjectObject"));
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
            DropDatabase("RDFMySQLStoreTest_ShouldRemoveQuadruplesBySubjectLiteral");
            CreateDatabase("RDFMySQLStoreTest_ShouldRemoveQuadruplesBySubjectLiteral");

            RDFQuadruple quadruple = new RDFQuadruple(new RDFContext(new Uri("ex:ctx")), new RDFResource("ex:subj"), new RDFResource("ex:pred"), new RDFPlainLiteral("hello"));

            RDFMySQLStore store = new RDFMySQLStore(GetConnectionString("RDFMySQLStoreTest_ShouldRemoveQuadruplesBySubjectLiteral"));
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
            DropDatabase("RDFMySQLStoreTest_ShouldNotRemoveQuadruplesBySubjectLiteral");
            CreateDatabase("RDFMySQLStoreTest_ShouldNotRemoveQuadruplesBySubjectLiteral");

            RDFQuadruple quadruple = new RDFQuadruple(new RDFContext(new Uri("ex:ctx")), new RDFResource("ex:subj"), new RDFResource("ex:pred"), new RDFPlainLiteral("hello"));

            RDFMySQLStore store = new RDFMySQLStore(GetConnectionString("RDFMySQLStoreTest_ShouldNotRemoveQuadruplesBySubjectLiteral"));
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
            DropDatabase("RDFMySQLStoreTest_ShouldRemoveQuadruplesByPredicateObject");
            CreateDatabase("RDFMySQLStoreTest_ShouldRemoveQuadruplesByPredicateObject");

            RDFQuadruple quadruple = new RDFQuadruple(new RDFContext(new Uri("ex:ctx")), new RDFResource("ex:subj"), new RDFResource("ex:pred"), new RDFResource("ex:obj"));

            RDFMySQLStore store = new RDFMySQLStore(GetConnectionString("RDFMySQLStoreTest_ShouldRemoveQuadruplesByPredicateObject"));
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
            DropDatabase("RDFMySQLStoreTest_ShouldNotRemoveQuadruplesByPredicateObject");
            CreateDatabase("RDFMySQLStoreTest_ShouldNotRemoveQuadruplesByPredicateObject");

            RDFQuadruple quadruple = new RDFQuadruple(new RDFContext(new Uri("ex:ctx")), new RDFResource("ex:subj"), new RDFResource("ex:pred"), new RDFResource("ex:obj"));

            RDFMySQLStore store = new RDFMySQLStore(GetConnectionString("RDFMySQLStoreTest_ShouldNotRemoveQuadruplesByPredicateObject"));
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
            DropDatabase("RDFMySQLStoreTest_ShouldRemoveQuadruplesByPredicateLiteral");
            CreateDatabase("RDFMySQLStoreTest_ShouldRemoveQuadruplesByPredicateLiteral");

            RDFQuadruple quadruple = new RDFQuadruple(new RDFContext(new Uri("ex:ctx")), new RDFResource("ex:subj"), new RDFResource("ex:pred"), new RDFPlainLiteral("hello"));

            RDFMySQLStore store = new RDFMySQLStore(GetConnectionString("RDFMySQLStoreTest_ShouldRemoveQuadruplesByPredicateLiteral"));
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
            DropDatabase("RDFMySQLStoreTest_ShouldNotRemoveQuadruplesByPredicateLiteral");
            CreateDatabase("RDFMySQLStoreTest_ShouldNotRemoveQuadruplesByPredicateLiteral");

            RDFQuadruple quadruple = new RDFQuadruple(new RDFContext(new Uri("ex:ctx")), new RDFResource("ex:subj"), new RDFResource("ex:pred"), new RDFPlainLiteral("hello"));

            RDFMySQLStore store = new RDFMySQLStore(GetConnectionString("RDFMySQLStoreTest_ShouldNotRemoveQuadruplesByPredicateLiteral"));
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
            DropDatabase("RDFMySQLStoreTest_ShouldClearQuadruples");
            CreateDatabase("RDFMySQLStoreTest_ShouldClearQuadruples");

            RDFQuadruple quadruple = new RDFQuadruple(new RDFContext(new Uri("ex:ctx")), new RDFResource("ex:subj"), new RDFResource("ex:pred"), new RDFPlainLiteral("hello"));

            RDFMySQLStore store = new RDFMySQLStore(GetConnectionString("RDFMySQLStoreTest_ShouldClearQuadruples"));
            store.ClearQuadruples();

            RDFMemoryStore memStore = store.SelectAllQuadruples();

            Assert.IsNotNull(memStore);
            Assert.IsTrue(memStore.QuadruplesCount == 0);
        }

        [TestMethod]
        public void ShouldContainQuadruple()
        {
            DropDatabase("RDFMySQLStoreTest_ShouldContainQuadruple");
            CreateDatabase("RDFMySQLStoreTest_ShouldContainQuadruple");

            RDFQuadruple quadruple1 = new RDFQuadruple(new RDFContext(new Uri("ex:ctx")), new RDFResource("ex:subj"), new RDFResource("ex:pred"), new RDFPlainLiteral("hello"));
            RDFQuadruple quadruple2 = new RDFQuadruple(new RDFContext(new Uri("ex:ctx2")), new RDFResource("ex:subj"), new RDFResource("ex:pred"), new RDFPlainLiteral("hello"));

            RDFMySQLStore store = new RDFMySQLStore(GetConnectionString("RDFMySQLStoreTest_ShouldContainQuadruple"));
            store.AddQuadruple(quadruple1);
            store.AddQuadruple(quadruple2);

            Assert.IsTrue(store.ContainsQuadruple(quadruple1));
            Assert.IsTrue(store.ContainsQuadruple(quadruple2));
        }

        [TestMethod]
        public void ShouldNotContainQuadruple()
        {
            DropDatabase("RDFMySQLStoreTest_ShouldNotContainQuadruple");
            CreateDatabase("RDFMySQLStoreTest_ShouldNotContainQuadruple");

            RDFQuadruple quadruple = new RDFQuadruple(new RDFContext(new Uri("ex:ctx")), new RDFResource("ex:subj"), new RDFResource("ex:pred"), new RDFPlainLiteral("hello"));
            RDFQuadruple quadruple2 = new RDFQuadruple(new RDFContext(new Uri("ex:ctx2")), new RDFResource("ex:subj"), new RDFResource("ex:pred"), new RDFPlainLiteral("hello"));

            RDFMySQLStore store = new RDFMySQLStore(GetConnectionString("RDFMySQLStoreTest_ShouldNotContainQuadruple"));
            store.AddQuadruple(quadruple);

            Assert.IsFalse(store.ContainsQuadruple(quadruple2));
            Assert.IsFalse(store.ContainsQuadruple(null));
        }

        [TestMethod]
        public void ShouldSelectQuadruplesByContext()
        {
            DropDatabase("RDFMySQLStoreTest_ShouldSelectQuadruplesByContext");
            CreateDatabase("RDFMySQLStoreTest_ShouldSelectQuadruplesByContext");

            RDFQuadruple quadruple = new RDFQuadruple(new RDFContext(new Uri("ex:ctx")), new RDFResource("ex:subj"), new RDFResource("ex:pred"), new RDFResource("ex:obj"));

            RDFMySQLStore store = new RDFMySQLStore(GetConnectionString("RDFMySQLStoreTest_ShouldSelectQuadruplesByContext"));
            store.AddQuadruple(quadruple);
            RDFMemoryStore result = store.SelectQuadruplesByContext(new RDFContext(new Uri("ex:ctx")));

            Assert.IsNotNull(result);
            Assert.IsTrue(result.ContainsQuadruple(quadruple));
        }

        [TestMethod]
        public void ShouldNotSelectQuadruplesByContext()
        {
            DropDatabase("RDFMySQLStoreTest_ShouldNotSelectQuadruplesByContext");
            CreateDatabase("RDFMySQLStoreTest_ShouldNotSelectQuadruplesByContext");

            RDFQuadruple quadruple = new RDFQuadruple(new RDFContext(new Uri("ex:ctx")), new RDFResource("ex:subj"), new RDFResource("ex:pred"), new RDFResource("ex:obj"));

            RDFMySQLStore store = new RDFMySQLStore(GetConnectionString("RDFMySQLStoreTest_ShouldNotSelectQuadruplesByContext"));
            store.AddQuadruple(quadruple);
            RDFMemoryStore result = store.SelectQuadruplesByContext(new RDFContext(new Uri("ex:ctx2")));

            Assert.IsNotNull(result);
            Assert.IsFalse(result.ContainsQuadruple(quadruple));
        }

        [TestMethod]
        public void ShouldSelectQuadruplesBySubject()
        {
            DropDatabase("RDFMySQLStoreTest_ShouldSelectQuadruplesBySubject");
            CreateDatabase("RDFMySQLStoreTest_ShouldSelectQuadruplesBySubject");

            RDFQuadruple quadruple = new RDFQuadruple(new RDFContext(new Uri("ex:ctx")), new RDFResource("ex:subj"), new RDFResource("ex:pred"), new RDFResource("ex:obj"));

            RDFMySQLStore store = new RDFMySQLStore(GetConnectionString("RDFMySQLStoreTest_ShouldSelectQuadruplesBySubject"));
            store.AddQuadruple(quadruple);
            RDFMemoryStore result = store.SelectQuadruplesBySubject(new RDFResource("ex:subj"));

            Assert.IsNotNull(result);
            Assert.IsTrue(result.ContainsQuadruple(quadruple));
        }

        [TestMethod]
        public void ShouldNotSelectQuadruplesBySubject()
        {
            DropDatabase("RDFMySQLStoreTest_ShouldNotSelectQuadruplesBySubject");
            CreateDatabase("RDFMySQLStoreTest_ShouldNotSelectQuadruplesBySubject");

            RDFQuadruple quadruple = new RDFQuadruple(new RDFContext(new Uri("ex:ctx")), new RDFResource("ex:subj"), new RDFResource("ex:pred"), new RDFResource("ex:obj"));

            RDFMySQLStore store = new RDFMySQLStore(GetConnectionString("RDFMySQLStoreTest_ShouldNotSelectQuadruplesBySubject"));
            store.AddQuadruple(quadruple);
            RDFMemoryStore result = store.SelectQuadruplesBySubject(new RDFResource("ex:subj2"));

            Assert.IsNotNull(result);
            Assert.IsFalse(result.ContainsQuadruple(quadruple));
        }

        [TestMethod]
        public void ShouldSelectQuadruplesByPredicate()
        {
            DropDatabase("RDFMySQLStoreTest_ShouldSelectQuadruplesByPredicate");
            CreateDatabase("RDFMySQLStoreTest_ShouldSelectQuadruplesByPredicate");

            RDFQuadruple quadruple = new RDFQuadruple(new RDFContext(new Uri("ex:ctx")), new RDFResource("ex:subj"), new RDFResource("ex:pred"), new RDFResource("ex:obj"));

            RDFMySQLStore store = new RDFMySQLStore(GetConnectionString("RDFMySQLStoreTest_ShouldSelectQuadruplesByPredicate"));
            store.AddQuadruple(quadruple);
            RDFMemoryStore result = store.SelectQuadruplesByPredicate(new RDFResource("ex:pred"));

            Assert.IsNotNull(result);
            Assert.IsTrue(result.ContainsQuadruple(quadruple));
        }

        [TestMethod]
        public void ShouldNotSelectQuadruplesByPredicate()
        {
            DropDatabase("RDFMySQLStoreTest_ShouldNotSelectQuadruplesByPredicate");
            CreateDatabase("RDFMySQLStoreTest_ShouldNotSelectQuadruplesByPredicate");

            RDFQuadruple quadruple = new RDFQuadruple(new RDFContext(new Uri("ex:ctx")), new RDFResource("ex:subj"), new RDFResource("ex:pred"), new RDFResource("ex:obj"));

            RDFMySQLStore store = new RDFMySQLStore(GetConnectionString("RDFMySQLStoreTest_ShouldNotSelectQuadruplesByPredicate"));
            store.AddQuadruple(quadruple);
            RDFMemoryStore result = store.SelectQuadruplesByPredicate(new RDFResource("ex:pred2"));

            Assert.IsNotNull(result);
            Assert.IsFalse(result.ContainsQuadruple(quadruple));
        }

        [TestMethod]
        public void ShouldSelectQuadruplesByObject()
        {
            DropDatabase("RDFMySQLStoreTest_ShouldSelectQuadruplesByObject");
            CreateDatabase("RDFMySQLStoreTest_ShouldSelectQuadruplesByObject");

            RDFQuadruple quadruple = new RDFQuadruple(new RDFContext(new Uri("ex:ctx")), new RDFResource("ex:subj"), new RDFResource("ex:pred"), new RDFResource("ex:obj"));

            RDFMySQLStore store = new RDFMySQLStore(GetConnectionString("RDFMySQLStoreTest_ShouldSelectQuadruplesByObject"));
            store.AddQuadruple(quadruple);
            RDFMemoryStore result = store.SelectQuadruplesByObject(new RDFResource("ex:obj"));

            Assert.IsNotNull(result);
            Assert.IsTrue(result.ContainsQuadruple(quadruple));
        }

        [TestMethod]
        public void ShouldNotSelectQuadruplesByObject()
        {
            DropDatabase("RDFMySQLStoreTest_ShouldNotSelectQuadruplesByObject");
            CreateDatabase("RDFMySQLStoreTest_ShouldNotSelectQuadruplesByObject");

            RDFQuadruple quadruple = new RDFQuadruple(new RDFContext(new Uri("ex:ctx")), new RDFResource("ex:subj"), new RDFResource("ex:pred"), new RDFResource("ex:obj"));

            RDFMySQLStore store = new RDFMySQLStore(GetConnectionString("RDFMySQLStoreTest_ShouldNotSelectQuadruplesByObject"));
            store.AddQuadruple(quadruple);
            RDFMemoryStore result = store.SelectQuadruplesByObject(new RDFResource("ex:obj2"));

            Assert.IsNotNull(result);
            Assert.IsFalse(result.ContainsQuadruple(quadruple));
        }

        [TestMethod]
        public void ShouldSelectQuadruplesByLiteral()
        {
            DropDatabase("RDFMySQLStoreTest_ShouldSelectQuadruplesByLiteral");
            CreateDatabase("RDFMySQLStoreTest_ShouldSelectQuadruplesByLiteral");

            RDFQuadruple quadruple = new RDFQuadruple(new RDFContext(new Uri("ex:ctx")), new RDFResource("ex:subj"), new RDFResource("ex:pred"), new RDFPlainLiteral("hello"));

            RDFMySQLStore store = new RDFMySQLStore(GetConnectionString("RDFMySQLStoreTest_ShouldSelectQuadruplesByLiteral"));
            store.AddQuadruple(quadruple);
            RDFMemoryStore result = store.SelectQuadruplesByLiteral(new RDFPlainLiteral("hello"));

            Assert.IsNotNull(result);
            Assert.IsTrue(result.ContainsQuadruple(quadruple));
        }

        [TestMethod]
        public void ShouldNotSelectQuadruplesByLiteral()
        {
            DropDatabase("RDFMySQLStoreTest_ShouldNotSelectQuadruplesByLiteral");
            CreateDatabase("RDFMySQLStoreTest_ShouldNotSelectQuadruplesByLiteral");

            RDFQuadruple quadruple = new RDFQuadruple(new RDFContext(new Uri("ex:ctx")), new RDFResource("ex:subj"), new RDFResource("ex:pred"), new RDFPlainLiteral("hello"));

            RDFMySQLStore store = new RDFMySQLStore(GetConnectionString("RDFMySQLStoreTest_ShouldNotSelectQuadruplesByLiteral"));
            store.AddQuadruple(quadruple);
            RDFMemoryStore result = store.SelectQuadruplesByLiteral(new RDFPlainLiteral("hello","en"));

            Assert.IsNotNull(result);
            Assert.IsFalse(result.ContainsQuadruple(quadruple));
        }

        [TestMethod]
        public void ShouldSelectQuadruplesByContextSubject()
        {
            DropDatabase("RDFMySQLStoreTest_ShouldSelectQuadruplesByContextSubject");
            CreateDatabase("RDFMySQLStoreTest_ShouldSelectQuadruplesByContextSubject");

            RDFQuadruple quadruple = new RDFQuadruple(new RDFContext(new Uri("ex:ctx")), new RDFResource("ex:subj"), new RDFResource("ex:pred"), new RDFResource("ex:obj"));

            RDFMySQLStore store = new RDFMySQLStore(GetConnectionString("RDFMySQLStoreTest_ShouldSelectQuadruplesByContextSubject"));
            store.AddQuadruple(quadruple);
            RDFMemoryStore result = store.SelectQuadruples(new RDFContext(new Uri("ex:ctx")), new RDFResource("ex:subj"), null, null, null);

            Assert.IsNotNull(result);
            Assert.IsTrue(result.ContainsQuadruple(quadruple));
        }

        [TestMethod]
        public void ShouldNotSelectQuadruplesByContextSubject()
        {
            DropDatabase("RDFMySQLStoreTest_ShouldNotSelectQuadruplesByContextSubject");
            CreateDatabase("RDFMySQLStoreTest_ShouldNotSelectQuadruplesByContextSubject");

            RDFQuadruple quadruple = new RDFQuadruple(new RDFContext(new Uri("ex:ctx")), new RDFResource("ex:subj"), new RDFResource("ex:pred"), new RDFResource("ex:obj"));

            RDFMySQLStore store = new RDFMySQLStore(GetConnectionString("RDFMySQLStoreTest_ShouldNotSelectQuadruplesByContextSubject"));
            store.AddQuadruple(quadruple);
            RDFMemoryStore result = store.SelectQuadruples(new RDFContext(new Uri("ex:ctx2")), new RDFResource("ex:subj"), null, null, null);

            Assert.IsNotNull(result);
            Assert.IsFalse(result.ContainsQuadruple(quadruple));
        }

        [TestMethod]
        public void ShouldSelectQuadruplesByContextPredicate()
        {
            DropDatabase("RDFMySQLStoreTest_ShouldSelectQuadruplesByContextPredicate");
            CreateDatabase("RDFMySQLStoreTest_ShouldSelectQuadruplesByContextPredicate");

            RDFQuadruple quadruple = new RDFQuadruple(new RDFContext(new Uri("ex:ctx")), new RDFResource("ex:subj"), new RDFResource("ex:pred"), new RDFResource("ex:obj"));

            RDFMySQLStore store = new RDFMySQLStore(GetConnectionString("RDFMySQLStoreTest_ShouldSelectQuadruplesByContextPredicate"));
            store.AddQuadruple(quadruple);
            RDFMemoryStore result = store.SelectQuadruples(new RDFContext(new Uri("ex:ctx")), null, new RDFResource("ex:pred"), null, null);

            Assert.IsNotNull(result);
            Assert.IsTrue(result.ContainsQuadruple(quadruple));
        }

        [TestMethod]
        public void ShouldNotSelectQuadruplesByContextPredicate()
        {
            DropDatabase("RDFMySQLStoreTest_ShouldNotSelectQuadruplesByContextPredicate");
            CreateDatabase("RDFMySQLStoreTest_ShouldNotSelectQuadruplesByContextPredicate");

            RDFQuadruple quadruple = new RDFQuadruple(new RDFContext(new Uri("ex:ctx")), new RDFResource("ex:subj"), new RDFResource("ex:pred"), new RDFResource("ex:obj"));

            RDFMySQLStore store = new RDFMySQLStore(GetConnectionString("RDFMySQLStoreTest_ShouldNotSelectQuadruplesByContextPredicate"));
            store.AddQuadruple(quadruple);
            RDFMemoryStore result = store.SelectQuadruples(new RDFContext(new Uri("ex:ctx2")), null, new RDFResource("ex:pred"), null, null);

            Assert.IsNotNull(result);
            Assert.IsFalse(result.ContainsQuadruple(quadruple));
        }

        [TestMethod]
        public void ShouldSelectQuadruplesByContextObject()
        {
            DropDatabase("RDFMySQLStoreTest_ShouldSelectQuadruplesByContextObject");
            CreateDatabase("RDFMySQLStoreTest_ShouldSelectQuadruplesByContextObject");

            RDFQuadruple quadruple = new RDFQuadruple(new RDFContext(new Uri("ex:ctx")), new RDFResource("ex:subj"), new RDFResource("ex:pred"), new RDFResource("ex:obj"));

            RDFMySQLStore store = new RDFMySQLStore(GetConnectionString("RDFMySQLStoreTest_ShouldSelectQuadruplesByContextObject"));
            store.AddQuadruple(quadruple);
            RDFMemoryStore result = store.SelectQuadruples(new RDFContext(new Uri("ex:ctx")), null, null, new RDFResource("ex:obj"), null);

            Assert.IsNotNull(result);
            Assert.IsTrue(result.ContainsQuadruple(quadruple));
        }

        [TestMethod]
        public void ShouldNotSelectQuadruplesByContextObject()
        {
            DropDatabase("RDFMySQLStoreTest_ShouldNotSelectQuadruplesByContextObject");
            CreateDatabase("RDFMySQLStoreTest_ShouldNotSelectQuadruplesByContextObject");

            RDFQuadruple quadruple = new RDFQuadruple(new RDFContext(new Uri("ex:ctx")), new RDFResource("ex:subj"), new RDFResource("ex:pred"), new RDFResource("ex:obj"));

            RDFMySQLStore store = new RDFMySQLStore(GetConnectionString("RDFMySQLStoreTest_ShouldNotSelectQuadruplesByContextObject"));
            store.AddQuadruple(quadruple);
            RDFMemoryStore result = store.SelectQuadruples(new RDFContext(new Uri("ex:ctx2")), null, null, new RDFResource("ex:obj"), null);

            Assert.IsNotNull(result);
            Assert.IsFalse(result.ContainsQuadruple(quadruple));
        }

        [TestMethod]
        public void ShouldSelectQuadruplesByContextLiteral()
        {
            DropDatabase("RDFMySQLStoreTest_ShouldSelectQuadruplesByContextLiteral");
            CreateDatabase("RDFMySQLStoreTest_ShouldSelectQuadruplesByContextLiteral");

            RDFQuadruple quadruple = new RDFQuadruple(new RDFContext(new Uri("ex:ctx")), new RDFResource("ex:subj"), new RDFResource("ex:pred"), new RDFPlainLiteral("hello"));

            RDFMySQLStore store = new RDFMySQLStore(GetConnectionString("RDFMySQLStoreTest_ShouldSelectQuadruplesByContextLiteral"));
            store.AddQuadruple(quadruple);
            RDFMemoryStore result = store.SelectQuadruples(new RDFContext(new Uri("ex:ctx")), null, null, null, new RDFPlainLiteral("hello"));

            Assert.IsNotNull(result);
            Assert.IsTrue(result.ContainsQuadruple(quadruple));
        }

        [TestMethod]
        public void ShouldNotSelectQuadruplesByContextLiteral()
        {
            DropDatabase("RDFMySQLStoreTest_ShouldNotSelectQuadruplesByContextLiteral");
            CreateDatabase("RDFMySQLStoreTest_ShouldNotSelectQuadruplesByContextLiteral");

            RDFQuadruple quadruple = new RDFQuadruple(new RDFContext(new Uri("ex:ctx")), new RDFResource("ex:subj"), new RDFResource("ex:pred"), new RDFPlainLiteral("hello"));

            RDFMySQLStore store = new RDFMySQLStore(GetConnectionString("RDFMySQLStoreTest_ShouldNotSelectQuadruplesByContextLiteral"));
            store.AddQuadruple(quadruple);
            RDFMemoryStore result = store.SelectQuadruples(new RDFContext(new Uri("ex:ctx2")), null, null, null, new RDFPlainLiteral("hello"));

            Assert.IsNotNull(result);
            Assert.IsFalse(result.ContainsQuadruple(quadruple));
        }

        [TestMethod]
        public void ShouldSelectQuadruplesByContextSubjectPredicate()
        {
            DropDatabase("RDFMySQLStoreTest_ShouldSelectQuadruplesByContextSubjectPredicate");
            CreateDatabase("RDFMySQLStoreTest_ShouldSelectQuadruplesByContextSubjectPredicate");

            RDFQuadruple quadruple = new RDFQuadruple(new RDFContext(new Uri("ex:ctx")), new RDFResource("ex:subj"), new RDFResource("ex:pred"), new RDFResource("ex:obj"));

            RDFMySQLStore store = new RDFMySQLStore(GetConnectionString("RDFMySQLStoreTest_ShouldSelectQuadruplesByContextSubjectPredicate"));
            store.AddQuadruple(quadruple);
            RDFMemoryStore result = store.SelectQuadruples(new RDFContext(new Uri("ex:ctx")), new RDFResource("ex:subj"), new RDFResource("ex:pred"), null, null);

            Assert.IsNotNull(result);
            Assert.IsTrue(result.ContainsQuadruple(quadruple));
        }

        [TestMethod]
        public void ShouldNotSelectQuadruplesByContextSubjectPredicate()
        {
            DropDatabase("RDFMySQLStoreTest_ShouldNotSelectQuadruplesByContextSubjectPredicate");
            CreateDatabase("RDFMySQLStoreTest_ShouldNotSelectQuadruplesByContextSubjectPredicate");

            RDFQuadruple quadruple = new RDFQuadruple(new RDFContext(new Uri("ex:ctx")), new RDFResource("ex:subj"), new RDFResource("ex:pred"), new RDFResource("ex:obj"));

            RDFMySQLStore store = new RDFMySQLStore(GetConnectionString("RDFMySQLStoreTest_ShouldNotSelectQuadruplesByContextSubjectPredicate"));
            store.AddQuadruple(quadruple);
            RDFMemoryStore result = store.SelectQuadruples(new RDFContext(new Uri("ex:ctx2")), new RDFResource("ex:subj"), new RDFResource("ex:pred"), null, null);

            Assert.IsNotNull(result);
            Assert.IsFalse(result.ContainsQuadruple(quadruple));
        }

        [TestMethod]
        public void ShouldSelectQuadruplesByContextSubjectObject()
        {
            DropDatabase("RDFMySQLStoreTest_ShouldSelectQuadruplesByContextSubjectObject");
            CreateDatabase("RDFMySQLStoreTest_ShouldSelectQuadruplesByContextSubjectObject");

            RDFQuadruple quadruple = new RDFQuadruple(new RDFContext(new Uri("ex:ctx")), new RDFResource("ex:subj"), new RDFResource("ex:pred"), new RDFResource("ex:obj"));

            RDFMySQLStore store = new RDFMySQLStore(GetConnectionString("RDFMySQLStoreTest_ShouldSelectQuadruplesByContextSubjectObject"));
            store.AddQuadruple(quadruple);
            RDFMemoryStore result = store.SelectQuadruples(new RDFContext(new Uri("ex:ctx")), new RDFResource("ex:subj"), null, new RDFResource("ex:obj"), null);

            Assert.IsNotNull(result);
            Assert.IsTrue(result.ContainsQuadruple(quadruple));
        }

        [TestMethod]
        public void ShouldNotSelectQuadruplesByContextSubjectObject()
        {
            DropDatabase("RDFMySQLStoreTest_ShouldNotSelectQuadruplesByContextSubjectObject");
            CreateDatabase("RDFMySQLStoreTest_ShouldNotSelectQuadruplesByContextSubjectObject");

            RDFQuadruple quadruple = new RDFQuadruple(new RDFContext(new Uri("ex:ctx")), new RDFResource("ex:subj"), new RDFResource("ex:pred"), new RDFResource("ex:obj"));

            RDFMySQLStore store = new RDFMySQLStore(GetConnectionString("RDFMySQLStoreTest_ShouldNotSelectQuadruplesByContextSubjectObject"));
            store.AddQuadruple(quadruple);
            RDFMemoryStore result = store.SelectQuadruples(new RDFContext(new Uri("ex:ctx2")), new RDFResource("ex:subj"), null, new RDFResource("ex:obj"), null);

            Assert.IsNotNull(result);
            Assert.IsFalse(result.ContainsQuadruple(quadruple));
        }

        [TestMethod]
        public void ShouldSelectQuadruplesByContextSubjectLiteral()
        {
            DropDatabase("RDFMySQLStoreTest_ShouldSelectQuadruplesByContextSubjectLiteral");
            CreateDatabase("RDFMySQLStoreTest_ShouldSelectQuadruplesByContextSubjectLiteral");

            RDFQuadruple quadruple = new RDFQuadruple(new RDFContext(new Uri("ex:ctx")), new RDFResource("ex:subj"), new RDFResource("ex:pred"), new RDFPlainLiteral("hello"));

            RDFMySQLStore store = new RDFMySQLStore(GetConnectionString("RDFMySQLStoreTest_ShouldSelectQuadruplesByContextSubjectLiteral"));
            store.AddQuadruple(quadruple);
            RDFMemoryStore result = store.SelectQuadruples(new RDFContext(new Uri("ex:ctx")), new RDFResource("ex:subj"), null, null, new RDFPlainLiteral("hello"));

            Assert.IsNotNull(result);
            Assert.IsTrue(result.ContainsQuadruple(quadruple));
        }

        [TestMethod]
        public void ShouldNotSelectQuadruplesByContextSubjectLiteral()
        {
            DropDatabase("RDFMySQLStoreTest_ShouldNotSelectQuadruplesByContextSubjectLiteral");
            CreateDatabase("RDFMySQLStoreTest_ShouldNotSelectQuadruplesByContextSubjectLiteral");

            RDFQuadruple quadruple = new RDFQuadruple(new RDFContext(new Uri("ex:ctx")), new RDFResource("ex:subj"), new RDFResource("ex:pred"), new RDFPlainLiteral("hello"));

            RDFMySQLStore store = new RDFMySQLStore(GetConnectionString("RDFMySQLStoreTest_ShouldNotSelectQuadruplesByContextSubjectLiteral"));
            store.AddQuadruple(quadruple);
            RDFMemoryStore result = store.SelectQuadruples(new RDFContext(new Uri("ex:ctx2")), new RDFResource("ex:subj"), null, null, new RDFPlainLiteral("hello"));

            Assert.IsNotNull(result);
            Assert.IsFalse(result.ContainsQuadruple(quadruple));
        }

        [TestMethod]
        public void ShouldSelectQuadruplesByContextPredicateObject()
        {
            DropDatabase("RDFMySQLStoreTest_ShouldSelectQuadruplesByContextPredicateObject");
            CreateDatabase("RDFMySQLStoreTest_ShouldSelectQuadruplesByContextPredicateObject");

            RDFQuadruple quadruple = new RDFQuadruple(new RDFContext(new Uri("ex:ctx")), new RDFResource("ex:subj"), new RDFResource("ex:pred"), new RDFResource("ex:obj"));

            RDFMySQLStore store = new RDFMySQLStore(GetConnectionString("RDFMySQLStoreTest_ShouldSelectQuadruplesByContextPredicateObject"));
            store.AddQuadruple(quadruple);
            RDFMemoryStore result = store.SelectQuadruples(new RDFContext(new Uri("ex:ctx")), null, new RDFResource("ex:pred"), new RDFResource("ex:obj"), null);

            Assert.IsNotNull(result);
            Assert.IsTrue(result.ContainsQuadruple(quadruple));
        }

        [TestMethod]
        public void ShouldNotSelectQuadruplesByContextPredicateObject()
        {
            DropDatabase("RDFMySQLStoreTest_ShouldNotSelectQuadruplesByContextPredicateObject");
            CreateDatabase("RDFMySQLStoreTest_ShouldNotSelectQuadruplesByContextPredicateObject");

            RDFQuadruple quadruple = new RDFQuadruple(new RDFContext(new Uri("ex:ctx")), new RDFResource("ex:subj"), new RDFResource("ex:pred"), new RDFResource("ex:obj"));

            RDFMySQLStore store = new RDFMySQLStore(GetConnectionString("RDFMySQLStoreTest_ShouldNotSelectQuadruplesByContextPredicateObject"));
            store.AddQuadruple(quadruple);
            RDFMemoryStore result = store.SelectQuadruples(new RDFContext(new Uri("ex:ctx2")), null, new RDFResource("ex:pred"), new RDFResource("ex:obj"), null);

            Assert.IsNotNull(result);
            Assert.IsFalse(result.ContainsQuadruple(quadruple));
        }

        [TestMethod]
        public void ShouldSelectQuadruplesByContextPredicateLiteral()
        {
            DropDatabase("RDFMySQLStoreTest_ShouldSelectQuadruplesByContextPredicateLiteral");
            CreateDatabase("RDFMySQLStoreTest_ShouldSelectQuadruplesByContextPredicateLiteral");

            RDFQuadruple quadruple = new RDFQuadruple(new RDFContext(new Uri("ex:ctx")), new RDFResource("ex:subj"), new RDFResource("ex:pred"), new RDFPlainLiteral("hello"));

            RDFMySQLStore store = new RDFMySQLStore(GetConnectionString("RDFMySQLStoreTest_ShouldSelectQuadruplesByContextPredicateLiteral"));
            store.AddQuadruple(quadruple);
            RDFMemoryStore result = store.SelectQuadruples(new RDFContext(new Uri("ex:ctx")), null, new RDFResource("ex:pred"), null, new RDFPlainLiteral("hello"));

            Assert.IsNotNull(result);
            Assert.IsTrue(result.ContainsQuadruple(quadruple));
        }

        [TestMethod]
        public void ShouldNotSelectQuadruplesByContextPredicateLiteral()
        {
            DropDatabase("RDFMySQLStoreTest_ShouldNotSelectQuadruplesByContextPredicateLiteral");
            CreateDatabase("RDFMySQLStoreTest_ShouldNotSelectQuadruplesByContextPredicateLiteral");

            RDFQuadruple quadruple = new RDFQuadruple(new RDFContext(new Uri("ex:ctx")), new RDFResource("ex:subj"), new RDFResource("ex:pred"), new RDFPlainLiteral("hello"));

            RDFMySQLStore store = new RDFMySQLStore(GetConnectionString("RDFMySQLStoreTest_ShouldNotSelectQuadruplesByContextPredicateLiteral"));
            store.AddQuadruple(quadruple);
            RDFMemoryStore result = store.SelectQuadruples(new RDFContext(new Uri("ex:ctx2")), null, new RDFResource("ex:pred"), null, new RDFPlainLiteral("hello"));

            Assert.IsNotNull(result);
            Assert.IsFalse(result.ContainsQuadruple(quadruple));
        }

        [TestMethod]
        public void ShouldSelectQuadruplesByContextSubjectPredicateObject()
        {
            DropDatabase("RDFMySQLStoreTest_ShouldSelectQuadruplesByContextSubjectPredicateObject");
            CreateDatabase("RDFMySQLStoreTest_ShouldSelectQuadruplesByContextSubjectPredicateObject");

            RDFQuadruple quadruple = new RDFQuadruple(new RDFContext(new Uri("ex:ctx")), new RDFResource("ex:subj"), new RDFResource("ex:pred"), new RDFResource("ex:obj"));

            RDFMySQLStore store = new RDFMySQLStore(GetConnectionString("RDFMySQLStoreTest_ShouldSelectQuadruplesByContextSubjectPredicateObject"));
            store.AddQuadruple(quadruple);
            RDFMemoryStore result = store.SelectQuadruples(new RDFContext(new Uri("ex:ctx")), new RDFResource("ex:subj"), new RDFResource("ex:pred"), new RDFResource("ex:obj"), null);

            Assert.IsNotNull(result);
            Assert.IsTrue(result.ContainsQuadruple(quadruple));
        }

        [TestMethod]
        public void ShouldNotSelectQuadruplesByContextSubjectPredicateObject()
        {
            DropDatabase("RDFMySQLStoreTest_ShouldNotSelectQuadruplesByContextSubjectPredicateObject");
            CreateDatabase("RDFMySQLStoreTest_ShouldNotSelectQuadruplesByContextSubjectPredicateObject");

            RDFQuadruple quadruple = new RDFQuadruple(new RDFContext(new Uri("ex:ctx")), new RDFResource("ex:subj"), new RDFResource("ex:pred"), new RDFResource("ex:obj"));

            RDFMySQLStore store = new RDFMySQLStore(GetConnectionString("RDFMySQLStoreTest_ShouldNotSelectQuadruplesByContextSubjectPredicateObject"));
            store.AddQuadruple(quadruple);
            RDFMemoryStore result = store.SelectQuadruples(new RDFContext(new Uri("ex:ctx2")), new RDFResource("ex:subj"), new RDFResource("ex:pred"), new RDFResource("ex:obj"), null);

            Assert.IsNotNull(result);
            Assert.IsFalse(result.ContainsQuadruple(quadruple));
        }

        [TestMethod]
        public void ShouldSelectQuadruplesByContextSubjectPredicateLiteral()
        {
            DropDatabase("RDFMySQLStoreTest_ShouldSelectQuadruplesByContextSubjectPredicateLiteral");
            CreateDatabase("RDFMySQLStoreTest_ShouldSelectQuadruplesByContextSubjectPredicateLiteral");

            RDFQuadruple quadruple = new RDFQuadruple(new RDFContext(new Uri("ex:ctx")), new RDFResource("ex:subj"), new RDFResource("ex:pred"), new RDFPlainLiteral("hello"));

            RDFMySQLStore store = new RDFMySQLStore(GetConnectionString("RDFMySQLStoreTest_ShouldSelectQuadruplesByContextSubjectPredicateLiteral"));
            store.AddQuadruple(quadruple);
            RDFMemoryStore result = store.SelectQuadruples(new RDFContext(new Uri("ex:ctx")), new RDFResource("ex:subj"), new RDFResource("ex:pred"), null, new RDFPlainLiteral("hello"));

            Assert.IsNotNull(result);
            Assert.IsTrue(result.ContainsQuadruple(quadruple));
        }

        [TestMethod]
        public void ShouldNotSelectQuadruplesByContextSubjectPredicateLiteral()
        {
            DropDatabase("RDFMySQLStoreTest_ShouldNotSelectQuadruplesByContextSubjectPredicateLiteral");
            CreateDatabase("RDFMySQLStoreTest_ShouldNotSelectQuadruplesByContextSubjectPredicateLiteral");

            RDFQuadruple quadruple = new RDFQuadruple(new RDFContext(new Uri("ex:ctx")), new RDFResource("ex:subj"), new RDFResource("ex:pred"), new RDFPlainLiteral("hello"));

            RDFMySQLStore store = new RDFMySQLStore(GetConnectionString("RDFMySQLStoreTest_ShouldNotSelectQuadruplesByContextSubjectPredicateLiteral"));
            store.AddQuadruple(quadruple);
            RDFMemoryStore result = store.SelectQuadruples(new RDFContext(new Uri("ex:ctx2")), new RDFResource("ex:subj"), new RDFResource("ex:pred"), null, new RDFPlainLiteral("hello"));

            Assert.IsNotNull(result);
            Assert.IsFalse(result.ContainsQuadruple(quadruple));
        }

        [TestMethod]
        public void ShouldSelectQuadruplesBySubjectPredicate()
        {
            DropDatabase("RDFMySQLStoreTest_ShouldSelectQuadruplesBySubjectPredicate");
            CreateDatabase("RDFMySQLStoreTest_ShouldSelectQuadruplesBySubjectPredicate");

            RDFQuadruple quadruple = new RDFQuadruple(new RDFContext(new Uri("ex:ctx")), new RDFResource("ex:subj"), new RDFResource("ex:pred"), new RDFResource("ex:obj"));

            RDFMySQLStore store = new RDFMySQLStore(GetConnectionString("RDFMySQLStoreTest_ShouldSelectQuadruplesBySubjectPredicate"));
            store.AddQuadruple(quadruple);
            RDFMemoryStore result = store.SelectQuadruples(null, new RDFResource("ex:subj"), new RDFResource("ex:pred"), null, null);

            Assert.IsNotNull(result);
            Assert.IsTrue(result.ContainsQuadruple(quadruple));
        }

        [TestMethod]
        public void ShouldNotSelectQuadruplesBySubjectPredicate()
        {
            DropDatabase("RDFMySQLStoreTest_ShouldNotSelectQuadruplesBySubjectPredicate");
            CreateDatabase("RDFMySQLStoreTest_ShouldNotSelectQuadruplesBySubjectPredicate");

            RDFQuadruple quadruple = new RDFQuadruple(new RDFContext(new Uri("ex:ctx")), new RDFResource("ex:subj"), new RDFResource("ex:pred"), new RDFResource("ex:obj"));

            RDFMySQLStore store = new RDFMySQLStore(GetConnectionString("RDFMySQLStoreTest_ShouldNotSelectQuadruplesBySubjectPredicate"));
            store.AddQuadruple(quadruple);
            RDFMemoryStore result = store.SelectQuadruples(null, new RDFResource("ex:subj2"), new RDFResource("ex:pred"), null, null);

            Assert.IsNotNull(result);
            Assert.IsFalse(result.ContainsQuadruple(quadruple));
        }

        [TestMethod]
        public void ShouldSelectQuadruplesBySubjectObject()
        {
            DropDatabase("RDFMySQLStoreTest_ShouldSelectQuadruplesBySubjectObject");
            CreateDatabase("RDFMySQLStoreTest_ShouldSelectQuadruplesBySubjectObject");

            RDFQuadruple quadruple = new RDFQuadruple(new RDFContext(new Uri("ex:ctx")), new RDFResource("ex:subj"), new RDFResource("ex:pred"), new RDFResource("ex:obj"));

            RDFMySQLStore store = new RDFMySQLStore(GetConnectionString("RDFMySQLStoreTest_ShouldSelectQuadruplesBySubjectObject"));
            store.AddQuadruple(quadruple);
            RDFMemoryStore result = store.SelectQuadruples(null, new RDFResource("ex:subj"), null, new RDFResource("ex:obj"), null);

            Assert.IsNotNull(result);
            Assert.IsTrue(result.ContainsQuadruple(quadruple));
        }

        [TestMethod]
        public void ShouldNotSelectQuadruplesBySubjectObject()
        {
            DropDatabase("RDFMySQLStoreTest_ShouldNotSelectQuadruplesBySubjectObject");
            CreateDatabase("RDFMySQLStoreTest_ShouldNotSelectQuadruplesBySubjectObject");

            RDFQuadruple quadruple = new RDFQuadruple(new RDFContext(new Uri("ex:ctx")), new RDFResource("ex:subj"), new RDFResource("ex:pred"), new RDFResource("ex:obj"));

            RDFMySQLStore store = new RDFMySQLStore(GetConnectionString("RDFMySQLStoreTest_ShouldNotSelectQuadruplesBySubjectObject"));
            store.AddQuadruple(quadruple);
            RDFMemoryStore result = store.SelectQuadruples(null, new RDFResource("ex:subj2"), null, new RDFResource("ex:obj"), null);

            Assert.IsNotNull(result);
            Assert.IsFalse(result.ContainsQuadruple(quadruple));
        }

        [TestMethod]
        public void ShouldSelectQuadruplesBySubjectLiteral()
        {
            DropDatabase("RDFMySQLStoreTest_ShouldSelectQuadruplesBySubjectLiteral");
            CreateDatabase("RDFMySQLStoreTest_ShouldSelectQuadruplesBySubjectLiteral");

            RDFQuadruple quadruple = new RDFQuadruple(new RDFContext(new Uri("ex:ctx")), new RDFResource("ex:subj"), new RDFResource("ex:pred"), new RDFPlainLiteral("hello"));

            RDFMySQLStore store = new RDFMySQLStore(GetConnectionString("RDFMySQLStoreTest_ShouldSelectQuadruplesBySubjectLiteral"));
            store.AddQuadruple(quadruple);
            RDFMemoryStore result = store.SelectQuadruples(null, new RDFResource("ex:subj"), null, null, new RDFPlainLiteral("hello"));

            Assert.IsNotNull(result);
            Assert.IsTrue(result.ContainsQuadruple(quadruple));
        }

        [TestMethod]
        public void ShouldNotSelectQuadruplesBySubjectLiteral()
        {
            DropDatabase("RDFMySQLStoreTest_ShouldNotSelectQuadruplesBySubjectLiteral");
            CreateDatabase("RDFMySQLStoreTest_ShouldNotSelectQuadruplesBySubjectLiteral");

            RDFQuadruple quadruple = new RDFQuadruple(new RDFContext(new Uri("ex:ctx")), new RDFResource("ex:subj"), new RDFResource("ex:pred"), new RDFPlainLiteral("hello"));

            RDFMySQLStore store = new RDFMySQLStore(GetConnectionString("RDFMySQLStoreTest_ShouldNotSelectQuadruplesBySubjectLiteral"));
            store.AddQuadruple(quadruple);
            RDFMemoryStore result = store.SelectQuadruples(null, new RDFResource("ex:subj2"), null, null, new RDFPlainLiteral("hello"));

            Assert.IsNotNull(result);
            Assert.IsFalse(result.ContainsQuadruple(quadruple));
        }

        [TestMethod]
        public void ShouldSelectQuadruplesByPredicateObject()
        {
            DropDatabase("RDFMySQLStoreTest_ShouldSelectQuadruplesByPredicateObject");
            CreateDatabase("RDFMySQLStoreTest_ShouldSelectQuadruplesByPredicateObject");

            RDFQuadruple quadruple = new RDFQuadruple(new RDFContext(new Uri("ex:ctx")), new RDFResource("ex:subj"), new RDFResource("ex:pred"), new RDFResource("ex:obj"));

            RDFMySQLStore store = new RDFMySQLStore(GetConnectionString("RDFMySQLStoreTest_ShouldSelectQuadruplesByPredicateObject"));
            store.AddQuadruple(quadruple);
            RDFMemoryStore result = store.SelectQuadruples(null, null, new RDFResource("ex:pred"), new RDFResource("ex:obj"), null);

            Assert.IsNotNull(result);
            Assert.IsTrue(result.ContainsQuadruple(quadruple));
        }

        [TestMethod]
        public void ShouldNotSelectQuadruplesByPredicateObject()
        {
            DropDatabase("RDFMySQLStoreTest_ShouldNotSelectQuadruplesByPredicateObject");
            CreateDatabase("RDFMySQLStoreTest_ShouldNotSelectQuadruplesByPredicateObject");

            RDFQuadruple quadruple = new RDFQuadruple(new RDFContext(new Uri("ex:ctx")), new RDFResource("ex:subj"), new RDFResource("ex:pred"), new RDFResource("ex:obj"));

            RDFMySQLStore store = new RDFMySQLStore(GetConnectionString("RDFMySQLStoreTest_ShouldNotSelectQuadruplesByPredicateObject"));
            store.AddQuadruple(quadruple);
            RDFMemoryStore result = store.SelectQuadruples(null, null, new RDFResource("ex:pred2"), new RDFResource("ex:obj"), null);

            Assert.IsNotNull(result);
            Assert.IsFalse(result.ContainsQuadruple(quadruple));
        }

        [TestMethod]
        public void ShouldSelectQuadruplesByPredicateLiteral()
        {
            DropDatabase("RDFMySQLStoreTest_ShouldSelectQuadruplesByPredicateLiteral");
            CreateDatabase("RDFMySQLStoreTest_ShouldSelectQuadruplesByPredicateLiteral");

            RDFQuadruple quadruple = new RDFQuadruple(new RDFContext(new Uri("ex:ctx")), new RDFResource("ex:subj"), new RDFResource("ex:pred"), new RDFPlainLiteral("hello"));

            RDFMySQLStore store = new RDFMySQLStore(GetConnectionString("RDFMySQLStoreTest_ShouldSelectQuadruplesByPredicateLiteral"));
            store.AddQuadruple(quadruple);
            RDFMemoryStore result = store.SelectQuadruples(null, null, new RDFResource("ex:pred"), null, new RDFPlainLiteral("hello"));

            Assert.IsNotNull(result);
            Assert.IsTrue(result.ContainsQuadruple(quadruple));
        }

        [TestMethod]
        public void ShouldNotSelectQuadruplesByPredicateLiteral()
        {
            DropDatabase("RDFMySQLStoreTest_ShouldNotSelectQuadruplesByPredicateLiteral");
            CreateDatabase("RDFMySQLStoreTest_ShouldNotSelectQuadruplesByPredicateLiteral");

            RDFQuadruple quadruple = new RDFQuadruple(new RDFContext(new Uri("ex:ctx")), new RDFResource("ex:subj"), new RDFResource("ex:pred"), new RDFPlainLiteral("hello"));

            RDFMySQLStore store = new RDFMySQLStore(GetConnectionString("RDFMySQLStoreTest_ShouldNotSelectQuadruplesByPredicateLiteral"));
            store.AddQuadruple(quadruple);
            RDFMemoryStore result = store.SelectQuadruples(null, null, new RDFResource("ex:pred2"), null, new RDFPlainLiteral("hello"));

            Assert.IsNotNull(result);
            Assert.IsFalse(result.ContainsQuadruple(quadruple));
        }

        [TestMethod]
        public void ShouldSelectQuadruplesBySubjectPredicateObject()
        {
            DropDatabase("RDFMySQLStoreTest_ShouldSelectQuadruplesBySubjectPredicateObject");
            CreateDatabase("RDFMySQLStoreTest_ShouldSelectQuadruplesBySubjectPredicateObject");

            RDFQuadruple quadruple = new RDFQuadruple(new RDFContext(new Uri("ex:ctx")), new RDFResource("ex:subj"), new RDFResource("ex:pred"), new RDFResource("ex:obj"));

            RDFMySQLStore store = new RDFMySQLStore(GetConnectionString("RDFMySQLStoreTest_ShouldSelectQuadruplesBySubjectPredicateObject"));
            store.AddQuadruple(quadruple);
            RDFMemoryStore result = store.SelectQuadruples(null, new RDFResource("ex:subj"), new RDFResource("ex:pred"), new RDFResource("ex:obj"), null);

            Assert.IsNotNull(result);
            Assert.IsTrue(result.ContainsQuadruple(quadruple));
        }

        [TestMethod]
        public void ShouldNotSelectQuadruplesBySubjectPredicateObject()
        {
            DropDatabase("RDFMySQLStoreTest_ShouldNotSelectQuadruplesBySubjectPredicateObject");
            CreateDatabase("RDFMySQLStoreTest_ShouldNotSelectQuadruplesBySubjectPredicateObject");

            RDFQuadruple quadruple = new RDFQuadruple(new RDFContext(new Uri("ex:ctx")), new RDFResource("ex:subj"), new RDFResource("ex:pred"), new RDFResource("ex:obj"));

            RDFMySQLStore store = new RDFMySQLStore(GetConnectionString("RDFMySQLStoreTest_ShouldNotSelectQuadruplesBySubjectPredicateObject"));
            store.AddQuadruple(quadruple);
            RDFMemoryStore result = store.SelectQuadruples(null, new RDFResource("ex:subj2"), new RDFResource("ex:pred"), new RDFResource("ex:obj"), null);

            Assert.IsNotNull(result);
            Assert.IsFalse(result.ContainsQuadruple(quadruple));
        }

        [TestMethod]
        public void ShouldSelectQuadruplesBySubjectPredicateLiteral()
        {
            DropDatabase("RDFMySQLStoreTest_ShouldSelectQuadruplesBySubjectPredicateLiteral");
            CreateDatabase("RDFMySQLStoreTest_ShouldSelectQuadruplesBySubjectPredicateLiteral");

            RDFQuadruple quadruple = new RDFQuadruple(new RDFContext(new Uri("ex:ctx")), new RDFResource("ex:subj"), new RDFResource("ex:pred"), new RDFPlainLiteral("hello"));

            RDFMySQLStore store = new RDFMySQLStore(GetConnectionString("RDFMySQLStoreTest_ShouldSelectQuadruplesBySubjectPredicateLiteral"));
            store.AddQuadruple(quadruple);
            RDFMemoryStore result = store.SelectQuadruples(null, new RDFResource("ex:subj"), new RDFResource("ex:pred"), null, new RDFPlainLiteral("hello"));

            Assert.IsNotNull(result);
            Assert.IsTrue(result.ContainsQuadruple(quadruple));
        }

        [TestMethod]
        public void ShouldNotSelectQuadruplesBySubjectPredicateLiteral()
        {
            DropDatabase("RDFMySQLStoreTest_ShouldNotSelectQuadruplesBySubjectPredicateLiteral");
            CreateDatabase("RDFMySQLStoreTest_ShouldNotSelectQuadruplesBySubjectPredicateLiteral");

            RDFQuadruple quadruple = new RDFQuadruple(new RDFContext(new Uri("ex:ctx")), new RDFResource("ex:subj"), new RDFResource("ex:pred"), new RDFPlainLiteral("hello"));

            RDFMySQLStore store = new RDFMySQLStore(GetConnectionString("RDFMySQLStoreTest_ShouldNotSelectQuadruplesBySubjectPredicateLiteral"));
            store.AddQuadruple(quadruple);
            RDFMemoryStore result = store.SelectQuadruples(null, new RDFResource("ex:subj2"), new RDFResource("ex:pred"), null, new RDFPlainLiteral("hello"));

            Assert.IsNotNull(result);
            Assert.IsFalse(result.ContainsQuadruple(quadruple));
        }

        [TestMethod]
        public void ShouldOptimize()
        {
            DropDatabase("RDFMySQLStoreTest_ShouldOptimize");
            CreateDatabase("RDFMySQLStoreTest_ShouldOptimize");

            RDFQuadruple quadruple1 = new RDFQuadruple(new RDFContext(new Uri("ex:ctx1")), new RDFResource("ex:subj"), new RDFResource("ex:pred"), new RDFResource("ex:obj"));
            RDFQuadruple quadruple2 = new RDFQuadruple(new RDFContext(new Uri("ex:ctx1")), new RDFResource("ex:subj"), new RDFResource("ex:pred"), new RDFPlainLiteral("hello"));
            RDFQuadruple quadruple3 = new RDFQuadruple(new RDFContext(new Uri("ex:ctx2")), new RDFResource("ex:subj"), new RDFResource("ex:pred"), new RDFPlainLiteral("hello"));
            RDFQuadruple quadruple4 = new RDFQuadruple(new RDFContext(new Uri("ex:ctx3")), new RDFResource("ex:subj"), new RDFResource("ex:pred"), new RDFPlainLiteral("hello"));
            RDFQuadruple quadruple5 = new RDFQuadruple(new RDFContext(new Uri("ex:ctx4")), new RDFResource("ex:subj"), new RDFResource("ex:pred"), new RDFResource("ex:obj"));
            RDFQuadruple quadruple6 = new RDFQuadruple(new RDFContext(new Uri("ex:ctx4")), new RDFResource("ex:subj4"), new RDFResource("ex:pred4"), new RDFResource("ex:obj"));

            RDFMySQLStore store = new RDFMySQLStore(GetConnectionString("RDFMySQLStoreTest_ShouldOptimize"));
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
