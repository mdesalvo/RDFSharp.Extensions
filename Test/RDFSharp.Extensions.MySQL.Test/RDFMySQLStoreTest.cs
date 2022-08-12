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
using RDFSharp.Store;

namespace RDFSharp.Extensions.MySQL.Test
{
    [TestClass]
    public class RDFMySQLStoreTest
    {
        //This test suite is based on a local installation of MySQL using root/root credentials
        private string User { get; set; } = "root";
        private string Password { get; set; } = "root";
        private string Server { get; set; } = "localhost";
        private int Port { get; set; } = 3306;

        private string GetConnectionString(string database)
            => $"Server={Server};Database={database};Uid={User};Pwd={Password};Port={Port}";

        private void CreateDatabase(string database)
        {
            MySqlConnection connection = default;
            try
            {
                //Create connection
                connection = new MySqlConnection(GetConnectionString("mysql"));

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
                connection = new MySqlConnection(GetConnectionString("mysql"));

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
            DropDatabase("ShouldCreateStore");
            CreateDatabase("ShouldCreateStore");

            RDFMySQLStore store = new RDFMySQLStore(GetConnectionString("ShouldCreateStore"));

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
            DropDatabase("ShouldCreateStoreUsingDispose");
            CreateDatabase("ShouldCreateStoreUsingDispose");

            RDFMySQLStore store = default;
            using(store = new RDFMySQLStore(GetConnectionString("ShouldCreateStoreUsingDispose")))
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
            DropDatabase("ShouldCreateStoreInvokingDispose");
            CreateDatabase("ShouldCreateStoreInvokingDispose");

            RDFMySQLStore store = new RDFMySQLStore(GetConnectionString("ShouldCreateStoreInvokingDispose"));

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
            DropDatabase("ShouldAddQuadruple");
            CreateDatabase("ShouldAddQuadruple");

            RDFQuadruple quadruple = new RDFQuadruple(new RDFContext(new Uri("ex:ctx")), new RDFResource("ex:subj"), new RDFResource("ex:pred"), new RDFResource("ex:obj"));

            RDFMySQLStore store = new RDFMySQLStore(GetConnectionString("ShouldAddQuadruple"));
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
            DropDatabase("ShouldMergeGraph");
            CreateDatabase("ShouldMergeGraph");

            RDFGraph graph = new RDFGraph(new List<RDFTriple>() {
                new RDFTriple(new RDFResource("ex:subj"), new RDFResource("ex:pred"), new RDFResource("ex:obj"))
            }).SetContext(new Uri("ex:ctx"));

            RDFMySQLStore store = new RDFMySQLStore(GetConnectionString("ShouldMergeGraph"));
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
            DropDatabase("ShouldRemoveQuadruple");
            CreateDatabase("ShouldRemoveQuadruple");

            RDFQuadruple quadruple = new RDFQuadruple(new RDFContext(new Uri("ex:ctx")), new RDFResource("ex:subj"), new RDFResource("ex:pred"), new RDFResource("ex:obj"));

            RDFMySQLStore store = new RDFMySQLStore(GetConnectionString("ShouldRemoveQuadruple"));
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
            DropDatabase("ShouldNotRemoveQuadruple");
            CreateDatabase("ShouldNotRemoveQuadruple");

            RDFQuadruple quadruple = new RDFQuadruple(new RDFContext(new Uri("ex:ctx")), new RDFResource("ex:subj"), new RDFResource("ex:pred"), new RDFResource("ex:obj"));
            RDFQuadruple quadruple2 = new RDFQuadruple(new RDFContext(new Uri("ex:ctx2")), new RDFResource("ex:subj"), new RDFResource("ex:pred"), new RDFResource("ex:obj"));

            RDFMySQLStore store = new RDFMySQLStore(GetConnectionString("ShouldNotRemoveQuadruple"));
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
            DropDatabase("ShouldRemoveQuadruplesByContext");
            CreateDatabase("ShouldRemoveQuadruplesByContext");

            RDFQuadruple quadruple = new RDFQuadruple(new RDFContext(new Uri("ex:ctx")), new RDFResource("ex:subj"), new RDFResource("ex:pred"), new RDFResource("ex:obj"));

            RDFMySQLStore store = new RDFMySQLStore(GetConnectionString("ShouldRemoveQuadruplesByContext"));
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
            DropDatabase("ShouldNotRemoveQuadruplesByContext");
            CreateDatabase("ShouldNotRemoveQuadruplesByContext");

            RDFQuadruple quadruple = new RDFQuadruple(new RDFContext(new Uri("ex:ctx")), new RDFResource("ex:subj"), new RDFResource("ex:pred"), new RDFResource("ex:obj"));

            RDFMySQLStore store = new RDFMySQLStore(GetConnectionString("ShouldNotRemoveQuadruplesByContext"));
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
            DropDatabase("ShouldRemoveQuadruplesBySubject");
            CreateDatabase("ShouldRemoveQuadruplesBySubject");

            RDFQuadruple quadruple = new RDFQuadruple(new RDFContext(new Uri("ex:ctx")), new RDFResource("ex:subj"), new RDFResource("ex:pred"), new RDFResource("ex:obj"));

            RDFMySQLStore store = new RDFMySQLStore(GetConnectionString("ShouldRemoveQuadruplesBySubject"));
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
            DropDatabase("ShouldNotRemoveQuadruplesBySubject");
            CreateDatabase("ShouldNotRemoveQuadruplesBySubject");

            RDFQuadruple quadruple = new RDFQuadruple(new RDFContext(new Uri("ex:ctx")), new RDFResource("ex:subj"), new RDFResource("ex:pred"), new RDFResource("ex:obj"));

            RDFMySQLStore store = new RDFMySQLStore(GetConnectionString("ShouldNotRemoveQuadruplesBySubject"));
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
            DropDatabase("ShouldRemoveQuadruplesByPredicate");
            CreateDatabase("ShouldRemoveQuadruplesByPredicate");

            RDFQuadruple quadruple = new RDFQuadruple(new RDFContext(new Uri("ex:ctx")), new RDFResource("ex:subj"), new RDFResource("ex:pred"), new RDFResource("ex:obj"));

            RDFMySQLStore store = new RDFMySQLStore(GetConnectionString("ShouldRemoveQuadruplesByPredicate"));
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
            DropDatabase("ShouldNotRemoveQuadruplesByPredicate");
            CreateDatabase("ShouldNotRemoveQuadruplesByPredicate");

            RDFQuadruple quadruple = new RDFQuadruple(new RDFContext(new Uri("ex:ctx")), new RDFResource("ex:subj"), new RDFResource("ex:pred"), new RDFResource("ex:obj"));

            RDFMySQLStore store = new RDFMySQLStore(GetConnectionString("ShouldNotRemoveQuadruplesByPredicate"));
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
            DropDatabase("ShouldRemoveQuadruplesByObject");
            CreateDatabase("ShouldRemoveQuadruplesByObject");

            RDFQuadruple quadruple = new RDFQuadruple(new RDFContext(new Uri("ex:ctx")), new RDFResource("ex:subj"), new RDFResource("ex:pred"), new RDFResource("ex:obj"));

            RDFMySQLStore store = new RDFMySQLStore(GetConnectionString("ShouldRemoveQuadruplesByObject"));
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
            DropDatabase("ShouldNotRemoveQuadruplesByObject");
            CreateDatabase("ShouldNotRemoveQuadruplesByObject");

            RDFQuadruple quadruple = new RDFQuadruple(new RDFContext(new Uri("ex:ctx")), new RDFResource("ex:subj"), new RDFResource("ex:pred"), new RDFResource("ex:obj"));

            RDFMySQLStore store = new RDFMySQLStore(GetConnectionString("ShouldNotRemoveQuadruplesByObject"));
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
            DropDatabase("ShouldRemoveQuadruplesByLiteral");
            CreateDatabase("ShouldRemoveQuadruplesByLiteral");

            RDFQuadruple quadruple = new RDFQuadruple(new RDFContext(new Uri("ex:ctx")), new RDFResource("ex:subj"), new RDFResource("ex:pred"), new RDFPlainLiteral("hello"));

            RDFMySQLStore store = new RDFMySQLStore(GetConnectionString("ShouldRemoveQuadruplesByLiteral"));
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
            DropDatabase("ShouldNotRemoveQuadruplesByLiteral");
            CreateDatabase("ShouldNotRemoveQuadruplesByLiteral");

            RDFQuadruple quadruple = new RDFQuadruple(new RDFContext(new Uri("ex:ctx")), new RDFResource("ex:subj"), new RDFResource("ex:pred"), new RDFPlainLiteral("hello"));

            RDFMySQLStore store = new RDFMySQLStore(GetConnectionString("ShouldNotRemoveQuadruplesByLiteral"));
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
            DropDatabase("ShouldRemoveQuadruplesByContextSubject");
            CreateDatabase("ShouldRemoveQuadruplesByContextSubject");

            RDFQuadruple quadruple = new RDFQuadruple(new RDFContext(new Uri("ex:ctx")), new RDFResource("ex:subj"), new RDFResource("ex:pred"), new RDFResource("ex:obj"));

            RDFMySQLStore store = new RDFMySQLStore(GetConnectionString("ShouldRemoveQuadruplesByContextSubject"));
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
            DropDatabase("ShouldNotRemoveQuadruplesByContextSubject");
            CreateDatabase("ShouldNotRemoveQuadruplesByContextSubject");

            RDFQuadruple quadruple = new RDFQuadruple(new RDFContext(new Uri("ex:ctx")), new RDFResource("ex:subj"), new RDFResource("ex:pred"), new RDFResource("ex:obj"));

            RDFMySQLStore store = new RDFMySQLStore(GetConnectionString("ShouldNotRemoveQuadruplesByContextSubject"));
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
            DropDatabase("ShouldRemoveQuadruplesByContextPredicate");
            CreateDatabase("ShouldRemoveQuadruplesByContextPredicate");

            RDFQuadruple quadruple = new RDFQuadruple(new RDFContext(new Uri("ex:ctx")), new RDFResource("ex:subj"), new RDFResource("ex:pred"), new RDFResource("ex:obj"));

            RDFMySQLStore store = new RDFMySQLStore(GetConnectionString("ShouldRemoveQuadruplesByContextPredicate"));
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
            DropDatabase("ShouldNotRemoveQuadruplesByContextPredicate");
            CreateDatabase("ShouldNotRemoveQuadruplesByContextPredicate");

            RDFQuadruple quadruple = new RDFQuadruple(new RDFContext(new Uri("ex:ctx")), new RDFResource("ex:subj"), new RDFResource("ex:pred"), new RDFResource("ex:obj"));

            RDFMySQLStore store = new RDFMySQLStore(GetConnectionString("ShouldNotRemoveQuadruplesByContextPredicate"));
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
            DropDatabase("ShouldRemoveQuadruplesByContextObject");
            CreateDatabase("ShouldRemoveQuadruplesByContextObject");

            RDFQuadruple quadruple = new RDFQuadruple(new RDFContext(new Uri("ex:ctx")), new RDFResource("ex:subj"), new RDFResource("ex:pred"), new RDFResource("ex:obj"));

            RDFMySQLStore store = new RDFMySQLStore(GetConnectionString("ShouldRemoveQuadruplesByContextObject"));
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
            DropDatabase("ShouldNotRemoveQuadruplesByContextObject");
            CreateDatabase("ShouldNotRemoveQuadruplesByContextObject");

            RDFQuadruple quadruple = new RDFQuadruple(new RDFContext(new Uri("ex:ctx")), new RDFResource("ex:subj"), new RDFResource("ex:pred"), new RDFResource("ex:obj"));

            RDFMySQLStore store = new RDFMySQLStore(GetConnectionString("ShouldNotRemoveQuadruplesByContextObject"));
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
            DropDatabase("ShouldRemoveQuadruplesByContextLiteral");
            CreateDatabase("ShouldRemoveQuadruplesByContextLiteral");

            RDFQuadruple quadruple = new RDFQuadruple(new RDFContext(new Uri("ex:ctx")), new RDFResource("ex:subj"), new RDFResource("ex:pred"), new RDFPlainLiteral("hello"));

            RDFMySQLStore store = new RDFMySQLStore(GetConnectionString("ShouldRemoveQuadruplesByContextLiteral"));
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
            DropDatabase("ShouldNotRemoveQuadruplesByContextLiteral");
            CreateDatabase("ShouldNotRemoveQuadruplesByContextLiteral");

            RDFQuadruple quadruple = new RDFQuadruple(new RDFContext(new Uri("ex:ctx")), new RDFResource("ex:subj"), new RDFResource("ex:pred"), new RDFPlainLiteral("hello"));

            RDFMySQLStore store = new RDFMySQLStore(GetConnectionString("ShouldNotRemoveQuadruplesByContextLiteral"));
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
            DropDatabase("ShouldRemoveQuadruplesByContextSubjectPredicate");
            CreateDatabase("ShouldRemoveQuadruplesByContextSubjectPredicate");

            RDFQuadruple quadruple = new RDFQuadruple(new RDFContext(new Uri("ex:ctx")), new RDFResource("ex:subj"), new RDFResource("ex:pred"), new RDFResource("ex:obj"));

            RDFMySQLStore store = new RDFMySQLStore(GetConnectionString("ShouldRemoveQuadruplesByContextSubjectPredicate"));
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
            DropDatabase("ShouldNotRemoveQuadruplesByContextSubjectPredicate");
            CreateDatabase("ShouldNotRemoveQuadruplesByContextSubjectPredicate");

            RDFQuadruple quadruple = new RDFQuadruple(new RDFContext(new Uri("ex:ctx")), new RDFResource("ex:subj"), new RDFResource("ex:pred"), new RDFResource("ex:obj"));

            RDFMySQLStore store = new RDFMySQLStore(GetConnectionString("ShouldNotRemoveQuadruplesByContextSubjectPredicate"));
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
            DropDatabase("ShouldRemoveQuadruplesByContextSubjectObject");
            CreateDatabase("ShouldRemoveQuadruplesByContextSubjectObject");

            RDFQuadruple quadruple = new RDFQuadruple(new RDFContext(new Uri("ex:ctx")), new RDFResource("ex:subj"), new RDFResource("ex:pred"), new RDFResource("ex:obj"));

            RDFMySQLStore store = new RDFMySQLStore(GetConnectionString("ShouldRemoveQuadruplesByContextSubjectObject"));
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
            DropDatabase("ShouldNotRemoveQuadruplesByContextSubjectObject");
            CreateDatabase("ShouldNotRemoveQuadruplesByContextSubjectObject");

            RDFQuadruple quadruple = new RDFQuadruple(new RDFContext(new Uri("ex:ctx")), new RDFResource("ex:subj"), new RDFResource("ex:pred"), new RDFResource("ex:obj"));

            RDFMySQLStore store = new RDFMySQLStore(GetConnectionString("ShouldNotRemoveQuadruplesByContextSubjectObject"));
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
            DropDatabase("ShouldRemoveQuadruplesByContextSubjectLiteral");
            CreateDatabase("ShouldRemoveQuadruplesByContextSubjectLiteral");

            RDFQuadruple quadruple = new RDFQuadruple(new RDFContext(new Uri("ex:ctx")), new RDFResource("ex:subj"), new RDFResource("ex:pred"), new RDFPlainLiteral("hello"));

            RDFMySQLStore store = new RDFMySQLStore(GetConnectionString("ShouldRemoveQuadruplesByContextSubjectLiteral"));
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
            DropDatabase("ShouldNotRemoveQuadruplesByContextSubjectLiteral");
            CreateDatabase("ShouldNotRemoveQuadruplesByContextSubjectLiteral");

            RDFQuadruple quadruple = new RDFQuadruple(new RDFContext(new Uri("ex:ctx")), new RDFResource("ex:subj"), new RDFResource("ex:pred"), new RDFPlainLiteral("hello"));

            RDFMySQLStore store = new RDFMySQLStore(GetConnectionString("ShouldNotRemoveQuadruplesByContextSubjectLiteral"));
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
            DropDatabase("ShouldRemoveQuadruplesByContextPredicateObject");
            CreateDatabase("ShouldRemoveQuadruplesByContextPredicateObject");

            RDFQuadruple quadruple = new RDFQuadruple(new RDFContext(new Uri("ex:ctx")), new RDFResource("ex:subj"), new RDFResource("ex:pred"), new RDFResource("ex:obj"));

            RDFMySQLStore store = new RDFMySQLStore(GetConnectionString("ShouldRemoveQuadruplesByContextPredicateObject"));
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
            DropDatabase("ShouldNotRemoveQuadruplesByContextPredicateObject");
            CreateDatabase("ShouldNotRemoveQuadruplesByContextPredicateObject");

            RDFQuadruple quadruple = new RDFQuadruple(new RDFContext(new Uri("ex:ctx")), new RDFResource("ex:subj"), new RDFResource("ex:pred"), new RDFResource("ex:obj"));

            RDFMySQLStore store = new RDFMySQLStore(GetConnectionString("ShouldNotRemoveQuadruplesByContextPredicateObject"));
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
            DropDatabase("ShouldRemoveQuadruplesByContextPredicateLiteral");
            CreateDatabase("ShouldRemoveQuadruplesByContextPredicateLiteral");

            RDFQuadruple quadruple = new RDFQuadruple(new RDFContext(new Uri("ex:ctx")), new RDFResource("ex:subj"), new RDFResource("ex:pred"), new RDFPlainLiteral("hello"));

            RDFMySQLStore store = new RDFMySQLStore(GetConnectionString("ShouldRemoveQuadruplesByContextPredicateLiteral"));
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
            DropDatabase("ShouldNotRemoveQuadruplesByContextPredicateLiteral");
            CreateDatabase("ShouldNotRemoveQuadruplesByContextPredicateLiteral");

            RDFQuadruple quadruple = new RDFQuadruple(new RDFContext(new Uri("ex:ctx")), new RDFResource("ex:subj"), new RDFResource("ex:pred"), new RDFPlainLiteral("hello"));

            RDFMySQLStore store = new RDFMySQLStore(GetConnectionString("ShouldNotRemoveQuadruplesByContextPredicateLiteral"));
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
            DropDatabase("ShouldRemoveQuadruplesBySubjectPredicate");
            CreateDatabase("ShouldRemoveQuadruplesBySubjectPredicate");

            RDFQuadruple quadruple = new RDFQuadruple(new RDFContext(new Uri("ex:ctx")), new RDFResource("ex:subj"), new RDFResource("ex:pred"), new RDFResource("ex:obj"));

            RDFMySQLStore store = new RDFMySQLStore(GetConnectionString("ShouldRemoveQuadruplesBySubjectPredicate"));
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
            DropDatabase("ShouldNotRemoveQuadruplesBySubjectPredicate");
            CreateDatabase("ShouldNotRemoveQuadruplesBySubjectPredicate");

            RDFQuadruple quadruple = new RDFQuadruple(new RDFContext(new Uri("ex:ctx")), new RDFResource("ex:subj"), new RDFResource("ex:pred"), new RDFResource("ex:obj"));

            RDFMySQLStore store = new RDFMySQLStore(GetConnectionString("ShouldNotRemoveQuadruplesBySubjectPredicate"));
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
            DropDatabase("ShouldRemoveQuadruplesBySubjectObject");
            CreateDatabase("ShouldRemoveQuadruplesBySubjectObject");

            RDFQuadruple quadruple = new RDFQuadruple(new RDFContext(new Uri("ex:ctx")), new RDFResource("ex:subj"), new RDFResource("ex:pred"), new RDFResource("ex:obj"));

            RDFMySQLStore store = new RDFMySQLStore(GetConnectionString("ShouldRemoveQuadruplesBySubjectObject"));
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
            DropDatabase("ShouldNotRemoveQuadruplesBySubjectObject");
            CreateDatabase("ShouldNotRemoveQuadruplesBySubjectObject");

            RDFQuadruple quadruple = new RDFQuadruple(new RDFContext(new Uri("ex:ctx")), new RDFResource("ex:subj"), new RDFResource("ex:pred"), new RDFResource("ex:obj"));

            RDFMySQLStore store = new RDFMySQLStore(GetConnectionString("ShouldNotRemoveQuadruplesBySubjectObject"));
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
            DropDatabase("ShouldRemoveQuadruplesBySubjectLiteral");
            CreateDatabase("ShouldRemoveQuadruplesBySubjectLiteral");

            RDFQuadruple quadruple = new RDFQuadruple(new RDFContext(new Uri("ex:ctx")), new RDFResource("ex:subj"), new RDFResource("ex:pred"), new RDFPlainLiteral("hello"));

            RDFMySQLStore store = new RDFMySQLStore(GetConnectionString("ShouldRemoveQuadruplesBySubjectLiteral"));
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
            DropDatabase("ShouldNotRemoveQuadruplesBySubjectLiteral");
            CreateDatabase("ShouldNotRemoveQuadruplesBySubjectLiteral");

            RDFQuadruple quadruple = new RDFQuadruple(new RDFContext(new Uri("ex:ctx")), new RDFResource("ex:subj"), new RDFResource("ex:pred"), new RDFPlainLiteral("hello"));

            RDFMySQLStore store = new RDFMySQLStore(GetConnectionString("ShouldNotRemoveQuadruplesBySubjectLiteral"));
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
            DropDatabase("ShouldRemoveQuadruplesByPredicateObject");
            CreateDatabase("ShouldRemoveQuadruplesByPredicateObject");

            RDFQuadruple quadruple = new RDFQuadruple(new RDFContext(new Uri("ex:ctx")), new RDFResource("ex:subj"), new RDFResource("ex:pred"), new RDFResource("ex:obj"));

            RDFMySQLStore store = new RDFMySQLStore(GetConnectionString("ShouldRemoveQuadruplesByPredicateObject"));
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
            DropDatabase("ShouldNotRemoveQuadruplesByPredicateObject");
            CreateDatabase("ShouldNotRemoveQuadruplesByPredicateObject");

            RDFQuadruple quadruple = new RDFQuadruple(new RDFContext(new Uri("ex:ctx")), new RDFResource("ex:subj"), new RDFResource("ex:pred"), new RDFResource("ex:obj"));

            RDFMySQLStore store = new RDFMySQLStore(GetConnectionString("ShouldNotRemoveQuadruplesByPredicateObject"));
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
            DropDatabase("ShouldRemoveQuadruplesByPredicateLiteral");
            CreateDatabase("ShouldRemoveQuadruplesByPredicateLiteral");

            RDFQuadruple quadruple = new RDFQuadruple(new RDFContext(new Uri("ex:ctx")), new RDFResource("ex:subj"), new RDFResource("ex:pred"), new RDFPlainLiteral("hello"));

            RDFMySQLStore store = new RDFMySQLStore(GetConnectionString("ShouldRemoveQuadruplesByPredicateLiteral"));
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
            DropDatabase("ShouldNotRemoveQuadruplesByPredicateLiteral");
            CreateDatabase("ShouldNotRemoveQuadruplesByPredicateLiteral");

            RDFQuadruple quadruple = new RDFQuadruple(new RDFContext(new Uri("ex:ctx")), new RDFResource("ex:subj"), new RDFResource("ex:pred"), new RDFPlainLiteral("hello"));

            RDFMySQLStore store = new RDFMySQLStore(GetConnectionString("ShouldNotRemoveQuadruplesByPredicateLiteral"));
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
            DropDatabase("ShouldClearQuadruples");
            CreateDatabase("ShouldClearQuadruples");

            RDFQuadruple quadruple = new RDFQuadruple(new RDFContext(new Uri("ex:ctx")), new RDFResource("ex:subj"), new RDFResource("ex:pred"), new RDFPlainLiteral("hello"));

            RDFMySQLStore store = new RDFMySQLStore(GetConnectionString("ShouldClearQuadruples"));
            store.ClearQuadruples();

            RDFMemoryStore memStore = store.SelectAllQuadruples();

            Assert.IsNotNull(memStore);
            Assert.IsTrue(memStore.QuadruplesCount == 0);
        }

        [TestMethod]
        public void ShouldContainQuadruple()
        {
            DropDatabase("ShouldContainQuadruple");
            CreateDatabase("ShouldContainQuadruple");

            RDFQuadruple quadruple1 = new RDFQuadruple(new RDFContext(new Uri("ex:ctx")), new RDFResource("ex:subj"), new RDFResource("ex:pred"), new RDFPlainLiteral("hello"));
            RDFQuadruple quadruple2 = new RDFQuadruple(new RDFContext(new Uri("ex:ctx2")), new RDFResource("ex:subj"), new RDFResource("ex:pred"), new RDFPlainLiteral("hello"));

            RDFMySQLStore store = new RDFMySQLStore(GetConnectionString("ShouldContainQuadruple"));
            store.AddQuadruple(quadruple1);
            store.AddQuadruple(quadruple2);

            Assert.IsTrue(store.ContainsQuadruple(quadruple1));
            Assert.IsTrue(store.ContainsQuadruple(quadruple2));
        }

        [TestMethod]
        public void ShouldNotContainQuadruple()
        {
            DropDatabase("ShouldNotContainQuadruple");
            CreateDatabase("ShouldNotContainQuadruple");

            RDFQuadruple quadruple = new RDFQuadruple(new RDFContext(new Uri("ex:ctx")), new RDFResource("ex:subj"), new RDFResource("ex:pred"), new RDFPlainLiteral("hello"));
            RDFQuadruple quadruple2 = new RDFQuadruple(new RDFContext(new Uri("ex:ctx2")), new RDFResource("ex:subj"), new RDFResource("ex:pred"), new RDFPlainLiteral("hello"));

            RDFMySQLStore store = new RDFMySQLStore(GetConnectionString("ShouldNotContainQuadruple"));
            store.AddQuadruple(quadruple);

            Assert.IsFalse(store.ContainsQuadruple(quadruple2));
            Assert.IsFalse(store.ContainsQuadruple(null));
        }

        [TestMethod]
        public void ShouldSelectQuadruplesByContext()
        {
            DropDatabase("ShouldSelectQuadruplesByContext");
            CreateDatabase("ShouldSelectQuadruplesByContext");

            RDFQuadruple quadruple = new RDFQuadruple(new RDFContext(new Uri("ex:ctx")), new RDFResource("ex:subj"), new RDFResource("ex:pred"), new RDFResource("ex:obj"));

            RDFMySQLStore store = new RDFMySQLStore(GetConnectionString("ShouldSelectQuadruplesByContext"));
            store.AddQuadruple(quadruple);
            RDFMemoryStore result = store.SelectQuadruplesByContext(new RDFContext(new Uri("ex:ctx")));

            Assert.IsNotNull(result);
            Assert.IsTrue(result.ContainsQuadruple(quadruple));
        }

        [TestMethod]
        public void ShouldNotSelectQuadruplesByContext()
        {
            DropDatabase("ShouldNotSelectQuadruplesByContext");
            CreateDatabase("ShouldNotSelectQuadruplesByContext");

            RDFQuadruple quadruple = new RDFQuadruple(new RDFContext(new Uri("ex:ctx")), new RDFResource("ex:subj"), new RDFResource("ex:pred"), new RDFResource("ex:obj"));

            RDFMySQLStore store = new RDFMySQLStore(GetConnectionString("ShouldNotSelectQuadruplesByContext"));
            store.AddQuadruple(quadruple);
            RDFMemoryStore result = store.SelectQuadruplesByContext(new RDFContext(new Uri("ex:ctx2")));

            Assert.IsNotNull(result);
            Assert.IsFalse(result.ContainsQuadruple(quadruple));
        }

        [TestMethod]
        public void ShouldSelectQuadruplesBySubject()
        {
            DropDatabase("ShouldSelectQuadruplesBySubject");
            CreateDatabase("ShouldSelectQuadruplesBySubject");

            RDFQuadruple quadruple = new RDFQuadruple(new RDFContext(new Uri("ex:ctx")), new RDFResource("ex:subj"), new RDFResource("ex:pred"), new RDFResource("ex:obj"));

            RDFMySQLStore store = new RDFMySQLStore(GetConnectionString("ShouldSelectQuadruplesBySubject"));
            store.AddQuadruple(quadruple);
            RDFMemoryStore result = store.SelectQuadruplesBySubject(new RDFResource("ex:subj"));

            Assert.IsNotNull(result);
            Assert.IsTrue(result.ContainsQuadruple(quadruple));
        }

        [TestMethod]
        public void ShouldNotSelectQuadruplesBySubject()
        {
            DropDatabase("ShouldNotSelectQuadruplesBySubject");
            CreateDatabase("ShouldNotSelectQuadruplesBySubject");

            RDFQuadruple quadruple = new RDFQuadruple(new RDFContext(new Uri("ex:ctx")), new RDFResource("ex:subj"), new RDFResource("ex:pred"), new RDFResource("ex:obj"));

            RDFMySQLStore store = new RDFMySQLStore(GetConnectionString("ShouldNotSelectQuadruplesBySubject"));
            store.AddQuadruple(quadruple);
            RDFMemoryStore result = store.SelectQuadruplesBySubject(new RDFResource("ex:subj2"));

            Assert.IsNotNull(result);
            Assert.IsFalse(result.ContainsQuadruple(quadruple));
        }

        [TestMethod]
        public void ShouldSelectQuadruplesByPredicate()
        {
            DropDatabase("ShouldSelectQuadruplesByPredicate");
            CreateDatabase("ShouldSelectQuadruplesByPredicate");

            RDFQuadruple quadruple = new RDFQuadruple(new RDFContext(new Uri("ex:ctx")), new RDFResource("ex:subj"), new RDFResource("ex:pred"), new RDFResource("ex:obj"));

            RDFMySQLStore store = new RDFMySQLStore(GetConnectionString("ShouldSelectQuadruplesByPredicate"));
            store.AddQuadruple(quadruple);
            RDFMemoryStore result = store.SelectQuadruplesByPredicate(new RDFResource("ex:pred"));

            Assert.IsNotNull(result);
            Assert.IsTrue(result.ContainsQuadruple(quadruple));
        }

        [TestMethod]
        public void ShouldNotSelectQuadruplesByPredicate()
        {
            DropDatabase("ShouldNotSelectQuadruplesByPredicate");
            CreateDatabase("ShouldNotSelectQuadruplesByPredicate");

            RDFQuadruple quadruple = new RDFQuadruple(new RDFContext(new Uri("ex:ctx")), new RDFResource("ex:subj"), new RDFResource("ex:pred"), new RDFResource("ex:obj"));

            RDFMySQLStore store = new RDFMySQLStore(GetConnectionString("ShouldNotSelectQuadruplesByPredicate"));
            store.AddQuadruple(quadruple);
            RDFMemoryStore result = store.SelectQuadruplesByPredicate(new RDFResource("ex:pred2"));

            Assert.IsNotNull(result);
            Assert.IsFalse(result.ContainsQuadruple(quadruple));
        }

        [TestMethod]
        public void ShouldSelectQuadruplesByObject()
        {
            DropDatabase("ShouldSelectQuadruplesByObject");
            CreateDatabase("ShouldSelectQuadruplesByObject");

            RDFQuadruple quadruple = new RDFQuadruple(new RDFContext(new Uri("ex:ctx")), new RDFResource("ex:subj"), new RDFResource("ex:pred"), new RDFResource("ex:obj"));

            RDFMySQLStore store = new RDFMySQLStore(GetConnectionString("ShouldSelectQuadruplesByObject"));
            store.AddQuadruple(quadruple);
            RDFMemoryStore result = store.SelectQuadruplesByObject(new RDFResource("ex:obj"));

            Assert.IsNotNull(result);
            Assert.IsTrue(result.ContainsQuadruple(quadruple));
        }

        [TestMethod]
        public void ShouldNotSelectQuadruplesByObject()
        {
            DropDatabase("ShouldNotSelectQuadruplesByObject");
            CreateDatabase("ShouldNotSelectQuadruplesByObject");

            RDFQuadruple quadruple = new RDFQuadruple(new RDFContext(new Uri("ex:ctx")), new RDFResource("ex:subj"), new RDFResource("ex:pred"), new RDFResource("ex:obj"));

            RDFMySQLStore store = new RDFMySQLStore(GetConnectionString("ShouldNotSelectQuadruplesByObject"));
            store.AddQuadruple(quadruple);
            RDFMemoryStore result = store.SelectQuadruplesByObject(new RDFResource("ex:obj2"));

            Assert.IsNotNull(result);
            Assert.IsFalse(result.ContainsQuadruple(quadruple));
        }

        [TestMethod]
        public void ShouldSelectQuadruplesByLiteral()
        {
            DropDatabase("ShouldSelectQuadruplesByLiteral");
            CreateDatabase("ShouldSelectQuadruplesByLiteral");

            RDFQuadruple quadruple = new RDFQuadruple(new RDFContext(new Uri("ex:ctx")), new RDFResource("ex:subj"), new RDFResource("ex:pred"), new RDFPlainLiteral("hello"));

            RDFMySQLStore store = new RDFMySQLStore(GetConnectionString("ShouldSelectQuadruplesByLiteral"));
            store.AddQuadruple(quadruple);
            RDFMemoryStore result = store.SelectQuadruplesByLiteral(new RDFPlainLiteral("hello"));

            Assert.IsNotNull(result);
            Assert.IsTrue(result.ContainsQuadruple(quadruple));
        }

        [TestMethod]
        public void ShouldNotSelectQuadruplesByLiteral()
        {
            DropDatabase("ShouldNotSelectQuadruplesByLiteral");
            CreateDatabase("ShouldNotSelectQuadruplesByLiteral");

            RDFQuadruple quadruple = new RDFQuadruple(new RDFContext(new Uri("ex:ctx")), new RDFResource("ex:subj"), new RDFResource("ex:pred"), new RDFPlainLiteral("hello"));

            RDFMySQLStore store = new RDFMySQLStore(GetConnectionString("ShouldNotSelectQuadruplesByLiteral"));
            store.AddQuadruple(quadruple);
            RDFMemoryStore result = store.SelectQuadruplesByLiteral(new RDFPlainLiteral("hello","en"));

            Assert.IsNotNull(result);
            Assert.IsFalse(result.ContainsQuadruple(quadruple));
        }

        [TestMethod]
        public void ShouldSelectQuadruplesByContextSubject()
        {
            DropDatabase("ShouldSelectQuadruplesByContextSubject");
            CreateDatabase("ShouldSelectQuadruplesByContextSubject");

            RDFQuadruple quadruple = new RDFQuadruple(new RDFContext(new Uri("ex:ctx")), new RDFResource("ex:subj"), new RDFResource("ex:pred"), new RDFResource("ex:obj"));

            RDFMySQLStore store = new RDFMySQLStore(GetConnectionString("ShouldSelectQuadruplesByContextSubject"));
            store.AddQuadruple(quadruple);
            RDFMemoryStore result = store.SelectQuadruples(new RDFContext(new Uri("ex:ctx")), new RDFResource("ex:subj"), null, null, null);

            Assert.IsNotNull(result);
            Assert.IsTrue(result.ContainsQuadruple(quadruple));
        }

        [TestMethod]
        public void ShouldNotSelectQuadruplesByContextSubject()
        {
            DropDatabase("ShouldNotSelectQuadruplesByContextSubject");
            CreateDatabase("ShouldNotSelectQuadruplesByContextSubject");

            RDFQuadruple quadruple = new RDFQuadruple(new RDFContext(new Uri("ex:ctx")), new RDFResource("ex:subj"), new RDFResource("ex:pred"), new RDFResource("ex:obj"));

            RDFMySQLStore store = new RDFMySQLStore(GetConnectionString("ShouldNotSelectQuadruplesByContextSubject"));
            store.AddQuadruple(quadruple);
            RDFMemoryStore result = store.SelectQuadruples(new RDFContext(new Uri("ex:ctx2")), new RDFResource("ex:subj"), null, null, null);

            Assert.IsNotNull(result);
            Assert.IsFalse(result.ContainsQuadruple(quadruple));
        }

        [TestMethod]
        public void ShouldSelectQuadruplesByContextPredicate()
        {
            DropDatabase("ShouldSelectQuadruplesByContextPredicate");
            CreateDatabase("ShouldSelectQuadruplesByContextPredicate");

            RDFQuadruple quadruple = new RDFQuadruple(new RDFContext(new Uri("ex:ctx")), new RDFResource("ex:subj"), new RDFResource("ex:pred"), new RDFResource("ex:obj"));

            RDFMySQLStore store = new RDFMySQLStore(GetConnectionString("ShouldSelectQuadruplesByContextPredicate"));
            store.AddQuadruple(quadruple);
            RDFMemoryStore result = store.SelectQuadruples(new RDFContext(new Uri("ex:ctx")), null, new RDFResource("ex:pred"), null, null);

            Assert.IsNotNull(result);
            Assert.IsTrue(result.ContainsQuadruple(quadruple));
        }

        [TestMethod]
        public void ShouldNotSelectQuadruplesByContextPredicate()
        {
            DropDatabase("ShouldNotSelectQuadruplesByContextPredicate");
            CreateDatabase("ShouldNotSelectQuadruplesByContextPredicate");

            RDFQuadruple quadruple = new RDFQuadruple(new RDFContext(new Uri("ex:ctx")), new RDFResource("ex:subj"), new RDFResource("ex:pred"), new RDFResource("ex:obj"));

            RDFMySQLStore store = new RDFMySQLStore(GetConnectionString("ShouldNotSelectQuadruplesByContextPredicate"));
            store.AddQuadruple(quadruple);
            RDFMemoryStore result = store.SelectQuadruples(new RDFContext(new Uri("ex:ctx2")), null, new RDFResource("ex:pred"), null, null);

            Assert.IsNotNull(result);
            Assert.IsFalse(result.ContainsQuadruple(quadruple));
        }

        [TestMethod]
        public void ShouldSelectQuadruplesByContextObject()
        {
            DropDatabase("ShouldSelectQuadruplesByContextObject");
            CreateDatabase("ShouldSelectQuadruplesByContextObject");

            RDFQuadruple quadruple = new RDFQuadruple(new RDFContext(new Uri("ex:ctx")), new RDFResource("ex:subj"), new RDFResource("ex:pred"), new RDFResource("ex:obj"));

            RDFMySQLStore store = new RDFMySQLStore(GetConnectionString("ShouldSelectQuadruplesByContextObject"));
            store.AddQuadruple(quadruple);
            RDFMemoryStore result = store.SelectQuadruples(new RDFContext(new Uri("ex:ctx")), null, null, new RDFResource("ex:obj"), null);

            Assert.IsNotNull(result);
            Assert.IsTrue(result.ContainsQuadruple(quadruple));
        }

        [TestMethod]
        public void ShouldNotSelectQuadruplesByContextObject()
        {
            DropDatabase("ShouldNotSelectQuadruplesByContextObject");
            CreateDatabase("ShouldNotSelectQuadruplesByContextObject");

            RDFQuadruple quadruple = new RDFQuadruple(new RDFContext(new Uri("ex:ctx")), new RDFResource("ex:subj"), new RDFResource("ex:pred"), new RDFResource("ex:obj"));

            RDFMySQLStore store = new RDFMySQLStore(GetConnectionString("ShouldNotSelectQuadruplesByContextObject"));
            store.AddQuadruple(quadruple);
            RDFMemoryStore result = store.SelectQuadruples(new RDFContext(new Uri("ex:ctx2")), null, null, new RDFResource("ex:obj"), null);

            Assert.IsNotNull(result);
            Assert.IsFalse(result.ContainsQuadruple(quadruple));
        }

        [TestMethod]
        public void ShouldSelectQuadruplesByContextLiteral()
        {
            DropDatabase("ShouldSelectQuadruplesByContextLiteral");
            CreateDatabase("ShouldSelectQuadruplesByContextLiteral");

            RDFQuadruple quadruple = new RDFQuadruple(new RDFContext(new Uri("ex:ctx")), new RDFResource("ex:subj"), new RDFResource("ex:pred"), new RDFPlainLiteral("hello"));

            RDFMySQLStore store = new RDFMySQLStore(GetConnectionString("ShouldSelectQuadruplesByContextLiteral"));
            store.AddQuadruple(quadruple);
            RDFMemoryStore result = store.SelectQuadruples(new RDFContext(new Uri("ex:ctx")), null, null, null, new RDFPlainLiteral("hello"));

            Assert.IsNotNull(result);
            Assert.IsTrue(result.ContainsQuadruple(quadruple));
        }

        [TestMethod]
        public void ShouldNotSelectQuadruplesByContextLiteral()
        {
            DropDatabase("ShouldNotSelectQuadruplesByContextLiteral");
            CreateDatabase("ShouldNotSelectQuadruplesByContextLiteral");

            RDFQuadruple quadruple = new RDFQuadruple(new RDFContext(new Uri("ex:ctx")), new RDFResource("ex:subj"), new RDFResource("ex:pred"), new RDFPlainLiteral("hello"));

            RDFMySQLStore store = new RDFMySQLStore(GetConnectionString("ShouldNotSelectQuadruplesByContextLiteral"));
            store.AddQuadruple(quadruple);
            RDFMemoryStore result = store.SelectQuadruples(new RDFContext(new Uri("ex:ctx2")), null, null, null, new RDFPlainLiteral("hello"));

            Assert.IsNotNull(result);
            Assert.IsFalse(result.ContainsQuadruple(quadruple));
        }

        [TestMethod]
        public void ShouldSelectQuadruplesByContextSubjectPredicate()
        {
            DropDatabase("ShouldSelectQuadruplesByContextSubjectPredicate");
            CreateDatabase("ShouldSelectQuadruplesByContextSubjectPredicate");

            RDFQuadruple quadruple = new RDFQuadruple(new RDFContext(new Uri("ex:ctx")), new RDFResource("ex:subj"), new RDFResource("ex:pred"), new RDFResource("ex:obj"));

            RDFMySQLStore store = new RDFMySQLStore(GetConnectionString("ShouldSelectQuadruplesByContextSubjectPredicate"));
            store.AddQuadruple(quadruple);
            RDFMemoryStore result = store.SelectQuadruples(new RDFContext(new Uri("ex:ctx")), new RDFResource("ex:subj"), new RDFResource("ex:pred"), null, null);

            Assert.IsNotNull(result);
            Assert.IsTrue(result.ContainsQuadruple(quadruple));
        }

        [TestMethod]
        public void ShouldNotSelectQuadruplesByContextSubjectPredicate()
        {
            DropDatabase("ShouldNotSelectQuadruplesByContextSubjectPredicate");
            CreateDatabase("ShouldNotSelectQuadruplesByContextSubjectPredicate");

            RDFQuadruple quadruple = new RDFQuadruple(new RDFContext(new Uri("ex:ctx")), new RDFResource("ex:subj"), new RDFResource("ex:pred"), new RDFResource("ex:obj"));

            RDFMySQLStore store = new RDFMySQLStore(GetConnectionString("ShouldNotSelectQuadruplesByContextSubjectPredicate"));
            store.AddQuadruple(quadruple);
            RDFMemoryStore result = store.SelectQuadruples(new RDFContext(new Uri("ex:ctx2")), new RDFResource("ex:subj"), new RDFResource("ex:pred"), null, null);

            Assert.IsNotNull(result);
            Assert.IsFalse(result.ContainsQuadruple(quadruple));
        }

        [TestMethod]
        public void ShouldSelectQuadruplesByContextSubjectObject()
        {
            DropDatabase("ShouldSelectQuadruplesByContextSubjectObject");
            CreateDatabase("ShouldSelectQuadruplesByContextSubjectObject");

            RDFQuadruple quadruple = new RDFQuadruple(new RDFContext(new Uri("ex:ctx")), new RDFResource("ex:subj"), new RDFResource("ex:pred"), new RDFResource("ex:obj"));

            RDFMySQLStore store = new RDFMySQLStore(GetConnectionString("ShouldSelectQuadruplesByContextSubjectObject"));
            store.AddQuadruple(quadruple);
            RDFMemoryStore result = store.SelectQuadruples(new RDFContext(new Uri("ex:ctx")), new RDFResource("ex:subj"), null, new RDFResource("ex:obj"), null);

            Assert.IsNotNull(result);
            Assert.IsTrue(result.ContainsQuadruple(quadruple));
        }

        [TestMethod]
        public void ShouldNotSelectQuadruplesByContextSubjectObject()
        {
            DropDatabase("ShouldNotSelectQuadruplesByContextSubjectObject");
            CreateDatabase("ShouldNotSelectQuadruplesByContextSubjectObject");

            RDFQuadruple quadruple = new RDFQuadruple(new RDFContext(new Uri("ex:ctx")), new RDFResource("ex:subj"), new RDFResource("ex:pred"), new RDFResource("ex:obj"));

            RDFMySQLStore store = new RDFMySQLStore(GetConnectionString("ShouldNotSelectQuadruplesByContextSubjectObject"));
            store.AddQuadruple(quadruple);
            RDFMemoryStore result = store.SelectQuadruples(new RDFContext(new Uri("ex:ctx2")), new RDFResource("ex:subj"), null, new RDFResource("ex:obj"), null);

            Assert.IsNotNull(result);
            Assert.IsFalse(result.ContainsQuadruple(quadruple));
        }

        [TestMethod]
        public void ShouldSelectQuadruplesByContextSubjectLiteral()
        {
            DropDatabase("ShouldSelectQuadruplesByContextSubjectLiteral");
            CreateDatabase("ShouldSelectQuadruplesByContextSubjectLiteral");

            RDFQuadruple quadruple = new RDFQuadruple(new RDFContext(new Uri("ex:ctx")), new RDFResource("ex:subj"), new RDFResource("ex:pred"), new RDFPlainLiteral("hello"));

            RDFMySQLStore store = new RDFMySQLStore(GetConnectionString("ShouldSelectQuadruplesByContextSubjectLiteral"));
            store.AddQuadruple(quadruple);
            RDFMemoryStore result = store.SelectQuadruples(new RDFContext(new Uri("ex:ctx")), new RDFResource("ex:subj"), null, null, new RDFPlainLiteral("hello"));

            Assert.IsNotNull(result);
            Assert.IsTrue(result.ContainsQuadruple(quadruple));
        }

        [TestMethod]
        public void ShouldNotSelectQuadruplesByContextSubjectLiteral()
        {
            DropDatabase("ShouldNotSelectQuadruplesByContextSubjectLiteral");
            CreateDatabase("ShouldNotSelectQuadruplesByContextSubjectLiteral");

            RDFQuadruple quadruple = new RDFQuadruple(new RDFContext(new Uri("ex:ctx")), new RDFResource("ex:subj"), new RDFResource("ex:pred"), new RDFPlainLiteral("hello"));

            RDFMySQLStore store = new RDFMySQLStore(GetConnectionString("ShouldNotSelectQuadruplesByContextSubjectLiteral"));
            store.AddQuadruple(quadruple);
            RDFMemoryStore result = store.SelectQuadruples(new RDFContext(new Uri("ex:ctx2")), new RDFResource("ex:subj"), null, null, new RDFPlainLiteral("hello"));

            Assert.IsNotNull(result);
            Assert.IsFalse(result.ContainsQuadruple(quadruple));
        }

        [TestMethod]
        public void ShouldSelectQuadruplesByContextPredicateObject()
        {
            DropDatabase("ShouldSelectQuadruplesByContextPredicateObject");
            CreateDatabase("ShouldSelectQuadruplesByContextPredicateObject");

            RDFQuadruple quadruple = new RDFQuadruple(new RDFContext(new Uri("ex:ctx")), new RDFResource("ex:subj"), new RDFResource("ex:pred"), new RDFResource("ex:obj"));

            RDFMySQLStore store = new RDFMySQLStore(GetConnectionString("ShouldSelectQuadruplesByContextPredicateObject"));
            store.AddQuadruple(quadruple);
            RDFMemoryStore result = store.SelectQuadruples(new RDFContext(new Uri("ex:ctx")), null, new RDFResource("ex:pred"), new RDFResource("ex:obj"), null);

            Assert.IsNotNull(result);
            Assert.IsTrue(result.ContainsQuadruple(quadruple));
        }

        [TestMethod]
        public void ShouldNotSelectQuadruplesByContextPredicateObject()
        {
            DropDatabase("ShouldNotSelectQuadruplesByContextPredicateObject");
            CreateDatabase("ShouldNotSelectQuadruplesByContextPredicateObject");

            RDFQuadruple quadruple = new RDFQuadruple(new RDFContext(new Uri("ex:ctx")), new RDFResource("ex:subj"), new RDFResource("ex:pred"), new RDFResource("ex:obj"));

            RDFMySQLStore store = new RDFMySQLStore(GetConnectionString("ShouldNotSelectQuadruplesByContextPredicateObject"));
            store.AddQuadruple(quadruple);
            RDFMemoryStore result = store.SelectQuadruples(new RDFContext(new Uri("ex:ctx2")), null, new RDFResource("ex:pred"), new RDFResource("ex:obj"), null);

            Assert.IsNotNull(result);
            Assert.IsFalse(result.ContainsQuadruple(quadruple));
        }

        [TestMethod]
        public void ShouldSelectQuadruplesByContextPredicateLiteral()
        {
            DropDatabase("ShouldSelectQuadruplesByContextPredicateLiteral");
            CreateDatabase("ShouldSelectQuadruplesByContextPredicateLiteral");

            RDFQuadruple quadruple = new RDFQuadruple(new RDFContext(new Uri("ex:ctx")), new RDFResource("ex:subj"), new RDFResource("ex:pred"), new RDFPlainLiteral("hello"));

            RDFMySQLStore store = new RDFMySQLStore(GetConnectionString("ShouldSelectQuadruplesByContextPredicateLiteral"));
            store.AddQuadruple(quadruple);
            RDFMemoryStore result = store.SelectQuadruples(new RDFContext(new Uri("ex:ctx")), null, new RDFResource("ex:pred"), null, new RDFPlainLiteral("hello"));

            Assert.IsNotNull(result);
            Assert.IsTrue(result.ContainsQuadruple(quadruple));
        }

        [TestMethod]
        public void ShouldNotSelectQuadruplesByContextPredicateLiteral()
        {
            DropDatabase("ShouldNotSelectQuadruplesByContextPredicateLiteral");
            CreateDatabase("ShouldNotSelectQuadruplesByContextPredicateLiteral");

            RDFQuadruple quadruple = new RDFQuadruple(new RDFContext(new Uri("ex:ctx")), new RDFResource("ex:subj"), new RDFResource("ex:pred"), new RDFPlainLiteral("hello"));

            RDFMySQLStore store = new RDFMySQLStore(GetConnectionString("ShouldNotSelectQuadruplesByContextPredicateLiteral"));
            store.AddQuadruple(quadruple);
            RDFMemoryStore result = store.SelectQuadruples(new RDFContext(new Uri("ex:ctx2")), null, new RDFResource("ex:pred"), null, new RDFPlainLiteral("hello"));

            Assert.IsNotNull(result);
            Assert.IsFalse(result.ContainsQuadruple(quadruple));
        }

        [TestMethod]
        public void ShouldSelectQuadruplesByContextSubjectPredicateObject()
        {
            DropDatabase("ShouldSelectQuadruplesByContextSubjectPredicateObject");
            CreateDatabase("ShouldSelectQuadruplesByContextSubjectPredicateObject");

            RDFQuadruple quadruple = new RDFQuadruple(new RDFContext(new Uri("ex:ctx")), new RDFResource("ex:subj"), new RDFResource("ex:pred"), new RDFResource("ex:obj"));

            RDFMySQLStore store = new RDFMySQLStore(GetConnectionString("ShouldSelectQuadruplesByContextSubjectPredicateObject"));
            store.AddQuadruple(quadruple);
            RDFMemoryStore result = store.SelectQuadruples(new RDFContext(new Uri("ex:ctx")), new RDFResource("ex:subj"), new RDFResource("ex:pred"), new RDFResource("ex:obj"), null);

            Assert.IsNotNull(result);
            Assert.IsTrue(result.ContainsQuadruple(quadruple));
        }

        [TestMethod]
        public void ShouldNotSelectQuadruplesByContextSubjectPredicateObject()
        {
            DropDatabase("ShouldNotSelectQuadruplesByContextSubjectPredicateObject");
            CreateDatabase("ShouldNotSelectQuadruplesByContextSubjectPredicateObject");

            RDFQuadruple quadruple = new RDFQuadruple(new RDFContext(new Uri("ex:ctx")), new RDFResource("ex:subj"), new RDFResource("ex:pred"), new RDFResource("ex:obj"));

            RDFMySQLStore store = new RDFMySQLStore(GetConnectionString("ShouldNotSelectQuadruplesByContextSubjectPredicateObject"));
            store.AddQuadruple(quadruple);
            RDFMemoryStore result = store.SelectQuadruples(new RDFContext(new Uri("ex:ctx2")), new RDFResource("ex:subj"), new RDFResource("ex:pred"), new RDFResource("ex:obj"), null);

            Assert.IsNotNull(result);
            Assert.IsFalse(result.ContainsQuadruple(quadruple));
        }

        [TestMethod]
        public void ShouldSelectQuadruplesByContextSubjectPredicateLiteral()
        {
            DropDatabase("ShouldSelectQuadruplesByContextSubjectPredicateLiteral");
            CreateDatabase("ShouldSelectQuadruplesByContextSubjectPredicateLiteral");

            RDFQuadruple quadruple = new RDFQuadruple(new RDFContext(new Uri("ex:ctx")), new RDFResource("ex:subj"), new RDFResource("ex:pred"), new RDFPlainLiteral("hello"));

            RDFMySQLStore store = new RDFMySQLStore(GetConnectionString("ShouldSelectQuadruplesByContextSubjectPredicateLiteral"));
            store.AddQuadruple(quadruple);
            RDFMemoryStore result = store.SelectQuadruples(new RDFContext(new Uri("ex:ctx")), new RDFResource("ex:subj"), new RDFResource("ex:pred"), null, new RDFPlainLiteral("hello"));

            Assert.IsNotNull(result);
            Assert.IsTrue(result.ContainsQuadruple(quadruple));
        }

        [TestMethod]
        public void ShouldNotSelectQuadruplesByContextSubjectPredicateLiteral()
        {
            DropDatabase("ShouldNotSelectQuadruplesByContextSubjectPredicateLiteral");
            CreateDatabase("ShouldNotSelectQuadruplesByContextSubjectPredicateLiteral");

            RDFQuadruple quadruple = new RDFQuadruple(new RDFContext(new Uri("ex:ctx")), new RDFResource("ex:subj"), new RDFResource("ex:pred"), new RDFPlainLiteral("hello"));

            RDFMySQLStore store = new RDFMySQLStore(GetConnectionString("ShouldNotSelectQuadruplesByContextSubjectPredicateLiteral"));
            store.AddQuadruple(quadruple);
            RDFMemoryStore result = store.SelectQuadruples(new RDFContext(new Uri("ex:ctx2")), new RDFResource("ex:subj"), new RDFResource("ex:pred"), null, new RDFPlainLiteral("hello"));

            Assert.IsNotNull(result);
            Assert.IsFalse(result.ContainsQuadruple(quadruple));
        }

        [TestMethod]
        public void ShouldSelectQuadruplesBySubjectPredicate()
        {
            DropDatabase("ShouldSelectQuadruplesBySubjectPredicate");
            CreateDatabase("ShouldSelectQuadruplesBySubjectPredicate");

            RDFQuadruple quadruple = new RDFQuadruple(new RDFContext(new Uri("ex:ctx")), new RDFResource("ex:subj"), new RDFResource("ex:pred"), new RDFResource("ex:obj"));

            RDFMySQLStore store = new RDFMySQLStore(GetConnectionString("ShouldSelectQuadruplesBySubjectPredicate"));
            store.AddQuadruple(quadruple);
            RDFMemoryStore result = store.SelectQuadruples(null, new RDFResource("ex:subj"), new RDFResource("ex:pred"), null, null);

            Assert.IsNotNull(result);
            Assert.IsTrue(result.ContainsQuadruple(quadruple));
        }

        [TestMethod]
        public void ShouldNotSelectQuadruplesBySubjectPredicate()
        {
            DropDatabase("ShouldNotSelectQuadruplesBySubjectPredicate");
            CreateDatabase("ShouldNotSelectQuadruplesBySubjectPredicate");

            RDFQuadruple quadruple = new RDFQuadruple(new RDFContext(new Uri("ex:ctx")), new RDFResource("ex:subj"), new RDFResource("ex:pred"), new RDFResource("ex:obj"));

            RDFMySQLStore store = new RDFMySQLStore(GetConnectionString("ShouldNotSelectQuadruplesBySubjectPredicate"));
            store.AddQuadruple(quadruple);
            RDFMemoryStore result = store.SelectQuadruples(null, new RDFResource("ex:subj2"), new RDFResource("ex:pred"), null, null);

            Assert.IsNotNull(result);
            Assert.IsFalse(result.ContainsQuadruple(quadruple));
        }

        [TestMethod]
        public void ShouldSelectQuadruplesBySubjectObject()
        {
            DropDatabase("ShouldSelectQuadruplesBySubjectObject");
            CreateDatabase("ShouldSelectQuadruplesBySubjectObject");

            RDFQuadruple quadruple = new RDFQuadruple(new RDFContext(new Uri("ex:ctx")), new RDFResource("ex:subj"), new RDFResource("ex:pred"), new RDFResource("ex:obj"));

            RDFMySQLStore store = new RDFMySQLStore(GetConnectionString("ShouldSelectQuadruplesBySubjectObject"));
            store.AddQuadruple(quadruple);
            RDFMemoryStore result = store.SelectQuadruples(null, new RDFResource("ex:subj"), null, new RDFResource("ex:obj"), null);

            Assert.IsNotNull(result);
            Assert.IsTrue(result.ContainsQuadruple(quadruple));
        }

        [TestMethod]
        public void ShouldNotSelectQuadruplesBySubjectObject()
        {
            DropDatabase("ShouldNotSelectQuadruplesBySubjectObject");
            CreateDatabase("ShouldNotSelectQuadruplesBySubjectObject");

            RDFQuadruple quadruple = new RDFQuadruple(new RDFContext(new Uri("ex:ctx")), new RDFResource("ex:subj"), new RDFResource("ex:pred"), new RDFResource("ex:obj"));

            RDFMySQLStore store = new RDFMySQLStore(GetConnectionString("ShouldNotSelectQuadruplesBySubjectObject"));
            store.AddQuadruple(quadruple);
            RDFMemoryStore result = store.SelectQuadruples(null, new RDFResource("ex:subj2"), null, new RDFResource("ex:obj"), null);

            Assert.IsNotNull(result);
            Assert.IsFalse(result.ContainsQuadruple(quadruple));
        }

        [TestMethod]
        public void ShouldSelectQuadruplesBySubjectLiteral()
        {
            DropDatabase("ShouldSelectQuadruplesBySubjectLiteral");
            CreateDatabase("ShouldSelectQuadruplesBySubjectLiteral");

            RDFQuadruple quadruple = new RDFQuadruple(new RDFContext(new Uri("ex:ctx")), new RDFResource("ex:subj"), new RDFResource("ex:pred"), new RDFPlainLiteral("hello"));

            RDFMySQLStore store = new RDFMySQLStore(GetConnectionString("ShouldSelectQuadruplesBySubjectLiteral"));
            store.AddQuadruple(quadruple);
            RDFMemoryStore result = store.SelectQuadruples(null, new RDFResource("ex:subj"), null, null, new RDFPlainLiteral("hello"));

            Assert.IsNotNull(result);
            Assert.IsTrue(result.ContainsQuadruple(quadruple));
        }

        [TestMethod]
        public void ShouldNotSelectQuadruplesBySubjectLiteral()
        {
            DropDatabase("ShouldNotSelectQuadruplesBySubjectLiteral");
            CreateDatabase("ShouldNotSelectQuadruplesBySubjectLiteral");

            RDFQuadruple quadruple = new RDFQuadruple(new RDFContext(new Uri("ex:ctx")), new RDFResource("ex:subj"), new RDFResource("ex:pred"), new RDFPlainLiteral("hello"));

            RDFMySQLStore store = new RDFMySQLStore(GetConnectionString("ShouldNotSelectQuadruplesBySubjectLiteral"));
            store.AddQuadruple(quadruple);
            RDFMemoryStore result = store.SelectQuadruples(null, new RDFResource("ex:subj2"), null, null, new RDFPlainLiteral("hello"));

            Assert.IsNotNull(result);
            Assert.IsFalse(result.ContainsQuadruple(quadruple));
        }

        [TestMethod]
        public void ShouldSelectQuadruplesByPredicateObject()
        {
            DropDatabase("ShouldSelectQuadruplesByPredicateObject");
            CreateDatabase("ShouldSelectQuadruplesByPredicateObject");

            RDFQuadruple quadruple = new RDFQuadruple(new RDFContext(new Uri("ex:ctx")), new RDFResource("ex:subj"), new RDFResource("ex:pred"), new RDFResource("ex:obj"));

            RDFMySQLStore store = new RDFMySQLStore(GetConnectionString("ShouldSelectQuadruplesByPredicateObject"));
            store.AddQuadruple(quadruple);
            RDFMemoryStore result = store.SelectQuadruples(null, null, new RDFResource("ex:pred"), new RDFResource("ex:obj"), null);

            Assert.IsNotNull(result);
            Assert.IsTrue(result.ContainsQuadruple(quadruple));
        }

        [TestMethod]
        public void ShouldNotSelectQuadruplesByPredicateObject()
        {
            DropDatabase("ShouldNotSelectQuadruplesByPredicateObject");
            CreateDatabase("ShouldNotSelectQuadruplesByPredicateObject");

            RDFQuadruple quadruple = new RDFQuadruple(new RDFContext(new Uri("ex:ctx")), new RDFResource("ex:subj"), new RDFResource("ex:pred"), new RDFResource("ex:obj"));

            RDFMySQLStore store = new RDFMySQLStore(GetConnectionString("ShouldNotSelectQuadruplesByPredicateObject"));
            store.AddQuadruple(quadruple);
            RDFMemoryStore result = store.SelectQuadruples(null, null, new RDFResource("ex:pred2"), new RDFResource("ex:obj"), null);

            Assert.IsNotNull(result);
            Assert.IsFalse(result.ContainsQuadruple(quadruple));
        }

        [TestMethod]
        public void ShouldSelectQuadruplesByPredicateLiteral()
        {
            DropDatabase("ShouldSelectQuadruplesByPredicateLiteral");
            CreateDatabase("ShouldSelectQuadruplesByPredicateLiteral");

            RDFQuadruple quadruple = new RDFQuadruple(new RDFContext(new Uri("ex:ctx")), new RDFResource("ex:subj"), new RDFResource("ex:pred"), new RDFPlainLiteral("hello"));

            RDFMySQLStore store = new RDFMySQLStore(GetConnectionString("ShouldSelectQuadruplesByPredicateLiteral"));
            store.AddQuadruple(quadruple);
            RDFMemoryStore result = store.SelectQuadruples(null, null, new RDFResource("ex:pred"), null, new RDFPlainLiteral("hello"));

            Assert.IsNotNull(result);
            Assert.IsTrue(result.ContainsQuadruple(quadruple));
        }

        [TestMethod]
        public void ShouldNotSelectQuadruplesByPredicateLiteral()
        {
            DropDatabase("ShouldNotSelectQuadruplesByPredicateLiteral");
            CreateDatabase("ShouldNotSelectQuadruplesByPredicateLiteral");

            RDFQuadruple quadruple = new RDFQuadruple(new RDFContext(new Uri("ex:ctx")), new RDFResource("ex:subj"), new RDFResource("ex:pred"), new RDFPlainLiteral("hello"));

            RDFMySQLStore store = new RDFMySQLStore(GetConnectionString("ShouldNotSelectQuadruplesByPredicateLiteral"));
            store.AddQuadruple(quadruple);
            RDFMemoryStore result = store.SelectQuadruples(null, null, new RDFResource("ex:pred2"), null, new RDFPlainLiteral("hello"));

            Assert.IsNotNull(result);
            Assert.IsFalse(result.ContainsQuadruple(quadruple));
        }

        [TestMethod]
        public void ShouldSelectQuadruplesBySubjectPredicateObject()
        {
            DropDatabase("ShouldSelectQuadruplesBySubjectPredicateObject");
            CreateDatabase("ShouldSelectQuadruplesBySubjectPredicateObject");

            RDFQuadruple quadruple = new RDFQuadruple(new RDFContext(new Uri("ex:ctx")), new RDFResource("ex:subj"), new RDFResource("ex:pred"), new RDFResource("ex:obj"));

            RDFMySQLStore store = new RDFMySQLStore(GetConnectionString("ShouldSelectQuadruplesBySubjectPredicateObject"));
            store.AddQuadruple(quadruple);
            RDFMemoryStore result = store.SelectQuadruples(null, new RDFResource("ex:subj"), new RDFResource("ex:pred"), new RDFResource("ex:obj"), null);

            Assert.IsNotNull(result);
            Assert.IsTrue(result.ContainsQuadruple(quadruple));
        }

        [TestMethod]
        public void ShouldNotSelectQuadruplesBySubjectPredicateObject()
        {
            DropDatabase("ShouldNotSelectQuadruplesBySubjectPredicateObject");
            CreateDatabase("ShouldNotSelectQuadruplesBySubjectPredicateObject");

            RDFQuadruple quadruple = new RDFQuadruple(new RDFContext(new Uri("ex:ctx")), new RDFResource("ex:subj"), new RDFResource("ex:pred"), new RDFResource("ex:obj"));

            RDFMySQLStore store = new RDFMySQLStore(GetConnectionString("ShouldNotSelectQuadruplesBySubjectPredicateObject"));
            store.AddQuadruple(quadruple);
            RDFMemoryStore result = store.SelectQuadruples(null, new RDFResource("ex:subj2"), new RDFResource("ex:pred"), new RDFResource("ex:obj"), null);

            Assert.IsNotNull(result);
            Assert.IsFalse(result.ContainsQuadruple(quadruple));
        }

        [TestMethod]
        public void ShouldSelectQuadruplesBySubjectPredicateLiteral()
        {
            DropDatabase("ShouldSelectQuadruplesBySubjectPredicateLiteral");
            CreateDatabase("ShouldSelectQuadruplesBySubjectPredicateLiteral");

            RDFQuadruple quadruple = new RDFQuadruple(new RDFContext(new Uri("ex:ctx")), new RDFResource("ex:subj"), new RDFResource("ex:pred"), new RDFPlainLiteral("hello"));

            RDFMySQLStore store = new RDFMySQLStore(GetConnectionString("ShouldSelectQuadruplesBySubjectPredicateLiteral"));
            store.AddQuadruple(quadruple);
            RDFMemoryStore result = store.SelectQuadruples(null, new RDFResource("ex:subj"), new RDFResource("ex:pred"), null, new RDFPlainLiteral("hello"));

            Assert.IsNotNull(result);
            Assert.IsTrue(result.ContainsQuadruple(quadruple));
        }

        [TestMethod]
        public void ShouldNotSelectQuadruplesBySubjectPredicateLiteral()
        {
            DropDatabase("ShouldNotSelectQuadruplesBySubjectPredicateLiteral");
            CreateDatabase("ShouldNotSelectQuadruplesBySubjectPredicateLiteral");

            RDFQuadruple quadruple = new RDFQuadruple(new RDFContext(new Uri("ex:ctx")), new RDFResource("ex:subj"), new RDFResource("ex:pred"), new RDFPlainLiteral("hello"));

            RDFMySQLStore store = new RDFMySQLStore(GetConnectionString("ShouldNotSelectQuadruplesBySubjectPredicateLiteral"));
            store.AddQuadruple(quadruple);
            RDFMemoryStore result = store.SelectQuadruples(null, new RDFResource("ex:subj2"), new RDFResource("ex:pred"), null, new RDFPlainLiteral("hello"));

            Assert.IsNotNull(result);
            Assert.IsFalse(result.ContainsQuadruple(quadruple));
        }

        [TestMethod]
        public void ShouldOptimize()
        {
            DropDatabase("ShouldOptimize");
            CreateDatabase("ShouldOptimize");

            RDFQuadruple quadruple1 = new RDFQuadruple(new RDFContext(new Uri("ex:ctx1")), new RDFResource("ex:subj"), new RDFResource("ex:pred"), new RDFResource("ex:obj"));
            RDFQuadruple quadruple2 = new RDFQuadruple(new RDFContext(new Uri("ex:ctx1")), new RDFResource("ex:subj"), new RDFResource("ex:pred"), new RDFPlainLiteral("hello"));
            RDFQuadruple quadruple3 = new RDFQuadruple(new RDFContext(new Uri("ex:ctx2")), new RDFResource("ex:subj"), new RDFResource("ex:pred"), new RDFPlainLiteral("hello"));
            RDFQuadruple quadruple4 = new RDFQuadruple(new RDFContext(new Uri("ex:ctx3")), new RDFResource("ex:subj"), new RDFResource("ex:pred"), new RDFPlainLiteral("hello"));
            RDFQuadruple quadruple5 = new RDFQuadruple(new RDFContext(new Uri("ex:ctx4")), new RDFResource("ex:subj"), new RDFResource("ex:pred"), new RDFResource("ex:obj"));
            RDFQuadruple quadruple6 = new RDFQuadruple(new RDFContext(new Uri("ex:ctx4")), new RDFResource("ex:subj4"), new RDFResource("ex:pred4"), new RDFResource("ex:obj"));

            RDFMySQLStore store = new RDFMySQLStore(GetConnectionString("ShouldOptimize"));
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