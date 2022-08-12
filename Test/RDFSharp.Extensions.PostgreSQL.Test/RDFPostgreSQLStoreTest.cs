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
using Npgsql;
using System;
using System.Collections.Generic;
using System.Linq;
using RDFSharp.Model;
using RDFSharp.Store;

namespace RDFSharp.Extensions.PostgreSQL.Test
{
    [TestClass]
    public class RDFPostgreSQLStoreTest
    {
        //This test suite is based on a local installation of PostgreSQL having postgres/postgres credentials
        private string User { get; set; } = "postgres";
        private string Password { get; set; } = "postgres";
        private string Host { get; set; } = "localhost";
        private int Port { get; set; } = 5432;

        private string GetConnectionString(string database)
            => $"Host={Host};Port={Port};Database={database.ToLower()};Userid={User};Password={Password};";

        private void CreateDatabase(string database)
        {
            NpgsqlConnection connection = default;
            try
            {
                //Create connection
                connection = new NpgsqlConnection(GetConnectionString("postgres"));

                //Open connection
                connection.Open();

                //Create command
                NpgsqlCommand command = new NpgsqlCommand($"CREATE DATABASE {database.ToLower()};", connection);

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
            NpgsqlConnection connection = default;
            try
            {
                //Create connection
                connection = new NpgsqlConnection(GetConnectionString("postgres"));

                //Open connection
                connection.Open();

                //Create command
                NpgsqlCommand command = new NpgsqlCommand($"DROP DATABASE IF EXISTS {database.ToLower()};", connection);

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
            DropDatabase("RDFPostgreSQLStoreTest_ShouldCreateStore");
            CreateDatabase("RDFPostgreSQLStoreTest_ShouldCreateStore");

            RDFPostgreSQLStore store = new RDFPostgreSQLStore(GetConnectionString("RDFPostgreSQLStoreTest_ShouldCreateStore"));

            Assert.IsNotNull(store);
            Assert.IsTrue(string.Equals(store.StoreType, "POSTGRESQL"));
            Assert.IsTrue(store.StoreID.Equals(RDFModelUtilities.CreateHash(store.ToString())));
            Assert.IsTrue(string.Equals(store.ToString(), string.Concat("POSTGRESQL|SERVER=", store.Connection.DataSource, ";DATABASE=", store.Connection.Database)));
        }

        [TestMethod]
        public void ShouldThrowExceptionOnCreatingStoreBecauseNullOrEmptyPath()
            => Assert.ThrowsException<RDFStoreException>(() => new RDFPostgreSQLStore(null));

        [TestMethod]
        public void ShouldThrowExceptionOnCreatingStoreBecauseUnaccessiblePath()
            => Assert.ThrowsException<RDFStoreException>(() => new RDFPostgreSQLStore(GetConnectionString("http://example.org/file")));

        [TestMethod]
        public void ShouldCreateStoreUsingDispose()
        {
            DropDatabase("RDFPostgreSQLStoreTest_ShouldCreateStoreUsingDispose");
            CreateDatabase("RDFPostgreSQLStoreTest_ShouldCreateStoreUsingDispose");

            RDFPostgreSQLStore store = default;
            using(store = new RDFPostgreSQLStore(GetConnectionString("RDFPostgreSQLStoreTest_ShouldCreateStoreUsingDispose")))
            {
                Assert.IsNotNull(store);
                Assert.IsTrue(string.Equals(store.StoreType, "POSTGRESQL"));
                Assert.IsTrue(store.StoreID.Equals(RDFModelUtilities.CreateHash(store.ToString())));
                Assert.IsTrue(string.Equals(store.ToString(), string.Concat("POSTGRESQL|SERVER=", store.Connection.DataSource, ";DATABASE=", store.Connection.Database)));
                Assert.IsFalse(store.Disposed);
            }

            Assert.IsTrue(store.Disposed);
            Assert.IsNull(store.Connection);
        }

        [TestMethod]
        public void ShouldCreateStoreInvokingDispose()
        {
            DropDatabase("RDFPostgreSQLStoreTest_ShouldCreateStoreInvokingDispose");
            CreateDatabase("RDFPostgreSQLStoreTest_ShouldCreateStoreInvokingDispose");

            RDFPostgreSQLStore store = new RDFPostgreSQLStore(GetConnectionString("RDFPostgreSQLStoreTest_ShouldCreateStoreInvokingDispose"));

            Assert.IsNotNull(store);
            Assert.IsTrue(string.Equals(store.StoreType, "POSTGRESQL"));
            Assert.IsTrue(store.StoreID.Equals(RDFModelUtilities.CreateHash(store.ToString())));
            Assert.IsTrue(string.Equals(store.ToString(), string.Concat("POSTGRESQL|SERVER=", store.Connection.DataSource, ";DATABASE=", store.Connection.Database)));
            Assert.IsFalse(store.Disposed);

            store.Dispose();
            Assert.IsTrue(store.Disposed);
            Assert.IsNull(store.Connection);
        }

        [TestMethod]
        public void ShouldAddQuadruple()
        {
            DropDatabase("RDFPostgreSQLStoreTest_ShouldAddQuadruple");
            CreateDatabase("RDFPostgreSQLStoreTest_ShouldAddQuadruple");

            RDFQuadruple quadruple = new RDFQuadruple(new RDFContext(new Uri("ex:ctx")), new RDFResource("ex:subj"), new RDFResource("ex:pred"), new RDFResource("ex:obj"));

            RDFPostgreSQLStore store = new RDFPostgreSQLStore(GetConnectionString("RDFPostgreSQLStoreTest_ShouldAddQuadruple"));
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
            DropDatabase("RDFPostgreSQLStoreTest_ShouldMergeGraph");
            CreateDatabase("RDFPostgreSQLStoreTest_ShouldMergeGraph");

            RDFGraph graph = new RDFGraph(new List<RDFTriple>() {
                new RDFTriple(new RDFResource("ex:subj"), new RDFResource("ex:pred"), new RDFResource("ex:obj"))
            }).SetContext(new Uri("ex:ctx"));

            RDFPostgreSQLStore store = new RDFPostgreSQLStore(GetConnectionString("RDFPostgreSQLStoreTest_ShouldMergeGraph"));
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
            DropDatabase("RDFPostgreSQLStoreTest_ShouldRemoveQuadruple");
            CreateDatabase("RDFPostgreSQLStoreTest_ShouldRemoveQuadruple");

            RDFQuadruple quadruple = new RDFQuadruple(new RDFContext(new Uri("ex:ctx")), new RDFResource("ex:subj"), new RDFResource("ex:pred"), new RDFResource("ex:obj"));

            RDFPostgreSQLStore store = new RDFPostgreSQLStore(GetConnectionString("RDFPostgreSQLStoreTest_ShouldRemoveQuadruple"));
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
            DropDatabase("RDFPostgreSQLStoreTest_ShouldNotRemoveQuadruple");
            CreateDatabase("RDFPostgreSQLStoreTest_ShouldNotRemoveQuadruple");

            RDFQuadruple quadruple = new RDFQuadruple(new RDFContext(new Uri("ex:ctx")), new RDFResource("ex:subj"), new RDFResource("ex:pred"), new RDFResource("ex:obj"));
            RDFQuadruple quadruple2 = new RDFQuadruple(new RDFContext(new Uri("ex:ctx2")), new RDFResource("ex:subj"), new RDFResource("ex:pred"), new RDFResource("ex:obj"));

            RDFPostgreSQLStore store = new RDFPostgreSQLStore(GetConnectionString("RDFPostgreSQLStoreTest_ShouldNotRemoveQuadruple"));
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
            DropDatabase("RDFPostgreSQLStoreTest_ShouldRemoveQuadruplesByContext");
            CreateDatabase("RDFPostgreSQLStoreTest_ShouldRemoveQuadruplesByContext");

            RDFQuadruple quadruple = new RDFQuadruple(new RDFContext(new Uri("ex:ctx")), new RDFResource("ex:subj"), new RDFResource("ex:pred"), new RDFResource("ex:obj"));

            RDFPostgreSQLStore store = new RDFPostgreSQLStore(GetConnectionString("RDFPostgreSQLStoreTest_ShouldRemoveQuadruplesByContext"));
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
            DropDatabase("RDFPostgreSQLStoreTest_ShouldNotRemoveQuadruplesByContext");
            CreateDatabase("RDFPostgreSQLStoreTest_ShouldNotRemoveQuadruplesByContext");

            RDFQuadruple quadruple = new RDFQuadruple(new RDFContext(new Uri("ex:ctx")), new RDFResource("ex:subj"), new RDFResource("ex:pred"), new RDFResource("ex:obj"));

            RDFPostgreSQLStore store = new RDFPostgreSQLStore(GetConnectionString("RDFPostgreSQLStoreTest_ShouldNotRemoveQuadruplesByContext"));
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
            DropDatabase("RDFPostgreSQLStoreTest_ShouldRemoveQuadruplesBySubject");
            CreateDatabase("RDFPostgreSQLStoreTest_ShouldRemoveQuadruplesBySubject");

            RDFQuadruple quadruple = new RDFQuadruple(new RDFContext(new Uri("ex:ctx")), new RDFResource("ex:subj"), new RDFResource("ex:pred"), new RDFResource("ex:obj"));

            RDFPostgreSQLStore store = new RDFPostgreSQLStore(GetConnectionString("RDFPostgreSQLStoreTest_ShouldRemoveQuadruplesBySubject"));
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
            DropDatabase("RDFPostgreSQLStoreTest_ShouldNotRemoveQuadruplesBySubject");
            CreateDatabase("RDFPostgreSQLStoreTest_ShouldNotRemoveQuadruplesBySubject");

            RDFQuadruple quadruple = new RDFQuadruple(new RDFContext(new Uri("ex:ctx")), new RDFResource("ex:subj"), new RDFResource("ex:pred"), new RDFResource("ex:obj"));

            RDFPostgreSQLStore store = new RDFPostgreSQLStore(GetConnectionString("RDFPostgreSQLStoreTest_ShouldNotRemoveQuadruplesBySubject"));
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
            DropDatabase("RDFPostgreSQLStoreTest_ShouldRemoveQuadruplesByPredicate");
            CreateDatabase("RDFPostgreSQLStoreTest_ShouldRemoveQuadruplesByPredicate");

            RDFQuadruple quadruple = new RDFQuadruple(new RDFContext(new Uri("ex:ctx")), new RDFResource("ex:subj"), new RDFResource("ex:pred"), new RDFResource("ex:obj"));

            RDFPostgreSQLStore store = new RDFPostgreSQLStore(GetConnectionString("RDFPostgreSQLStoreTest_ShouldRemoveQuadruplesByPredicate"));
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
            DropDatabase("RDFPostgreSQLStoreTest_ShouldNotRemoveQuadruplesByPredicate");
            CreateDatabase("RDFPostgreSQLStoreTest_ShouldNotRemoveQuadruplesByPredicate");

            RDFQuadruple quadruple = new RDFQuadruple(new RDFContext(new Uri("ex:ctx")), new RDFResource("ex:subj"), new RDFResource("ex:pred"), new RDFResource("ex:obj"));

            RDFPostgreSQLStore store = new RDFPostgreSQLStore(GetConnectionString("RDFPostgreSQLStoreTest_ShouldNotRemoveQuadruplesByPredicate"));
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
            DropDatabase("RDFPostgreSQLStoreTest_ShouldRemoveQuadruplesByObject");
            CreateDatabase("RDFPostgreSQLStoreTest_ShouldRemoveQuadruplesByObject");

            RDFQuadruple quadruple = new RDFQuadruple(new RDFContext(new Uri("ex:ctx")), new RDFResource("ex:subj"), new RDFResource("ex:pred"), new RDFResource("ex:obj"));

            RDFPostgreSQLStore store = new RDFPostgreSQLStore(GetConnectionString("RDFPostgreSQLStoreTest_ShouldRemoveQuadruplesByObject"));
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
            DropDatabase("RDFPostgreSQLStoreTest_ShouldNotRemoveQuadruplesByObject");
            CreateDatabase("RDFPostgreSQLStoreTest_ShouldNotRemoveQuadruplesByObject");

            RDFQuadruple quadruple = new RDFQuadruple(new RDFContext(new Uri("ex:ctx")), new RDFResource("ex:subj"), new RDFResource("ex:pred"), new RDFResource("ex:obj"));

            RDFPostgreSQLStore store = new RDFPostgreSQLStore(GetConnectionString("RDFPostgreSQLStoreTest_ShouldNotRemoveQuadruplesByObject"));
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
            DropDatabase("RDFPostgreSQLStoreTest_ShouldRemoveQuadruplesByLiteral");
            CreateDatabase("RDFPostgreSQLStoreTest_ShouldRemoveQuadruplesByLiteral");

            RDFQuadruple quadruple = new RDFQuadruple(new RDFContext(new Uri("ex:ctx")), new RDFResource("ex:subj"), new RDFResource("ex:pred"), new RDFPlainLiteral("hello"));

            RDFPostgreSQLStore store = new RDFPostgreSQLStore(GetConnectionString("RDFPostgreSQLStoreTest_ShouldRemoveQuadruplesByLiteral"));
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
            DropDatabase("RDFPostgreSQLStoreTest_ShouldNotRemoveQuadruplesByLiteral");
            CreateDatabase("RDFPostgreSQLStoreTest_ShouldNotRemoveQuadruplesByLiteral");

            RDFQuadruple quadruple = new RDFQuadruple(new RDFContext(new Uri("ex:ctx")), new RDFResource("ex:subj"), new RDFResource("ex:pred"), new RDFPlainLiteral("hello"));

            RDFPostgreSQLStore store = new RDFPostgreSQLStore(GetConnectionString("RDFPostgreSQLStoreTest_ShouldNotRemoveQuadruplesByLiteral"));
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
            DropDatabase("RDFPostgreSQLStoreTest_ShouldRemoveQuadruplesByContextSubject");
            CreateDatabase("RDFPostgreSQLStoreTest_ShouldRemoveQuadruplesByContextSubject");

            RDFQuadruple quadruple = new RDFQuadruple(new RDFContext(new Uri("ex:ctx")), new RDFResource("ex:subj"), new RDFResource("ex:pred"), new RDFResource("ex:obj"));

            RDFPostgreSQLStore store = new RDFPostgreSQLStore(GetConnectionString("RDFPostgreSQLStoreTest_ShouldRemoveQuadruplesByContextSubject"));
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
            DropDatabase("RDFPostgreSQLStoreTest_ShouldNotRemoveQuadruplesByContextSubject");
            CreateDatabase("RDFPostgreSQLStoreTest_ShouldNotRemoveQuadruplesByContextSubject");

            RDFQuadruple quadruple = new RDFQuadruple(new RDFContext(new Uri("ex:ctx")), new RDFResource("ex:subj"), new RDFResource("ex:pred"), new RDFResource("ex:obj"));

            RDFPostgreSQLStore store = new RDFPostgreSQLStore(GetConnectionString("RDFPostgreSQLStoreTest_ShouldNotRemoveQuadruplesByContextSubject"));
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
            DropDatabase("RDFPostgreSQLStoreTest_ShouldRemoveQuadruplesByContextPredicate");
            CreateDatabase("RDFPostgreSQLStoreTest_ShouldRemoveQuadruplesByContextPredicate");

            RDFQuadruple quadruple = new RDFQuadruple(new RDFContext(new Uri("ex:ctx")), new RDFResource("ex:subj"), new RDFResource("ex:pred"), new RDFResource("ex:obj"));

            RDFPostgreSQLStore store = new RDFPostgreSQLStore(GetConnectionString("RDFPostgreSQLStoreTest_ShouldRemoveQuadruplesByContextPredicate"));
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
            DropDatabase("RDFPostgreSQLStoreTest_ShouldNotRemoveQuadruplesByContextPredicate");
            CreateDatabase("RDFPostgreSQLStoreTest_ShouldNotRemoveQuadruplesByContextPredicate");

            RDFQuadruple quadruple = new RDFQuadruple(new RDFContext(new Uri("ex:ctx")), new RDFResource("ex:subj"), new RDFResource("ex:pred"), new RDFResource("ex:obj"));

            RDFPostgreSQLStore store = new RDFPostgreSQLStore(GetConnectionString("RDFPostgreSQLStoreTest_ShouldNotRemoveQuadruplesByContextPredicate"));
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
            DropDatabase("RDFPostgreSQLStoreTest_ShouldRemoveQuadruplesByContextObject");
            CreateDatabase("RDFPostgreSQLStoreTest_ShouldRemoveQuadruplesByContextObject");

            RDFQuadruple quadruple = new RDFQuadruple(new RDFContext(new Uri("ex:ctx")), new RDFResource("ex:subj"), new RDFResource("ex:pred"), new RDFResource("ex:obj"));

            RDFPostgreSQLStore store = new RDFPostgreSQLStore(GetConnectionString("RDFPostgreSQLStoreTest_ShouldRemoveQuadruplesByContextObject"));
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
            DropDatabase("RDFPostgreSQLStoreTest_ShouldNotRemoveQuadruplesByContextObject");
            CreateDatabase("RDFPostgreSQLStoreTest_ShouldNotRemoveQuadruplesByContextObject");

            RDFQuadruple quadruple = new RDFQuadruple(new RDFContext(new Uri("ex:ctx")), new RDFResource("ex:subj"), new RDFResource("ex:pred"), new RDFResource("ex:obj"));

            RDFPostgreSQLStore store = new RDFPostgreSQLStore(GetConnectionString("RDFPostgreSQLStoreTest_ShouldNotRemoveQuadruplesByContextObject"));
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
            DropDatabase("RDFPostgreSQLStoreTest_ShouldRemoveQuadruplesByContextLiteral");
            CreateDatabase("RDFPostgreSQLStoreTest_ShouldRemoveQuadruplesByContextLiteral");

            RDFQuadruple quadruple = new RDFQuadruple(new RDFContext(new Uri("ex:ctx")), new RDFResource("ex:subj"), new RDFResource("ex:pred"), new RDFPlainLiteral("hello"));

            RDFPostgreSQLStore store = new RDFPostgreSQLStore(GetConnectionString("RDFPostgreSQLStoreTest_ShouldRemoveQuadruplesByContextLiteral"));
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
            DropDatabase("RDFPostgreSQLStoreTest_ShouldNotRemoveQuadruplesByContextLiteral");
            CreateDatabase("RDFPostgreSQLStoreTest_ShouldNotRemoveQuadruplesByContextLiteral");

            RDFQuadruple quadruple = new RDFQuadruple(new RDFContext(new Uri("ex:ctx")), new RDFResource("ex:subj"), new RDFResource("ex:pred"), new RDFPlainLiteral("hello"));

            RDFPostgreSQLStore store = new RDFPostgreSQLStore(GetConnectionString("RDFPostgreSQLStoreTest_ShouldNotRemoveQuadruplesByContextLiteral"));
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
            DropDatabase("RDFPostgreSQLStoreTest_ShouldRemoveQuadruplesByContextSubjectPredicate");
            CreateDatabase("RDFPostgreSQLStoreTest_ShouldRemoveQuadruplesByContextSubjectPredicate");

            RDFQuadruple quadruple = new RDFQuadruple(new RDFContext(new Uri("ex:ctx")), new RDFResource("ex:subj"), new RDFResource("ex:pred"), new RDFResource("ex:obj"));

            RDFPostgreSQLStore store = new RDFPostgreSQLStore(GetConnectionString("RDFPostgreSQLStoreTest_ShouldRemoveQuadruplesByContextSubjectPredicate"));
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
            DropDatabase("RDFPostgreSQLStoreTest_ShouldNotRemoveQuadruplesByContextSubjectPredicate");
            CreateDatabase("RDFPostgreSQLStoreTest_ShouldNotRemoveQuadruplesByContextSubjectPredicate");

            RDFQuadruple quadruple = new RDFQuadruple(new RDFContext(new Uri("ex:ctx")), new RDFResource("ex:subj"), new RDFResource("ex:pred"), new RDFResource("ex:obj"));

            RDFPostgreSQLStore store = new RDFPostgreSQLStore(GetConnectionString("RDFPostgreSQLStoreTest_ShouldNotRemoveQuadruplesByContextSubjectPredicate"));
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
            DropDatabase("RDFPostgreSQLStoreTest_ShouldRemoveQuadruplesByContextSubjectObject");
            CreateDatabase("RDFPostgreSQLStoreTest_ShouldRemoveQuadruplesByContextSubjectObject");

            RDFQuadruple quadruple = new RDFQuadruple(new RDFContext(new Uri("ex:ctx")), new RDFResource("ex:subj"), new RDFResource("ex:pred"), new RDFResource("ex:obj"));

            RDFPostgreSQLStore store = new RDFPostgreSQLStore(GetConnectionString("RDFPostgreSQLStoreTest_ShouldRemoveQuadruplesByContextSubjectObject"));
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
            DropDatabase("RDFPostgreSQLStoreTest_ShouldNotRemoveQuadruplesByContextSubjectObject");
            CreateDatabase("RDFPostgreSQLStoreTest_ShouldNotRemoveQuadruplesByContextSubjectObject");

            RDFQuadruple quadruple = new RDFQuadruple(new RDFContext(new Uri("ex:ctx")), new RDFResource("ex:subj"), new RDFResource("ex:pred"), new RDFResource("ex:obj"));

            RDFPostgreSQLStore store = new RDFPostgreSQLStore(GetConnectionString("RDFPostgreSQLStoreTest_ShouldNotRemoveQuadruplesByContextSubjectObject"));
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
            DropDatabase("RDFPostgreSQLStoreTest_ShouldRemoveQuadruplesByContextSubjectLiteral");
            CreateDatabase("RDFPostgreSQLStoreTest_ShouldRemoveQuadruplesByContextSubjectLiteral");

            RDFQuadruple quadruple = new RDFQuadruple(new RDFContext(new Uri("ex:ctx")), new RDFResource("ex:subj"), new RDFResource("ex:pred"), new RDFPlainLiteral("hello"));

            RDFPostgreSQLStore store = new RDFPostgreSQLStore(GetConnectionString("RDFPostgreSQLStoreTest_ShouldRemoveQuadruplesByContextSubjectLiteral"));
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
            DropDatabase("RDFPostgreSQLStoreTest_ShouldNotRemoveQuadruplesByContextSubjectLiteral");
            CreateDatabase("RDFPostgreSQLStoreTest_ShouldNotRemoveQuadruplesByContextSubjectLiteral");

            RDFQuadruple quadruple = new RDFQuadruple(new RDFContext(new Uri("ex:ctx")), new RDFResource("ex:subj"), new RDFResource("ex:pred"), new RDFPlainLiteral("hello"));

            RDFPostgreSQLStore store = new RDFPostgreSQLStore(GetConnectionString("RDFPostgreSQLStoreTest_ShouldNotRemoveQuadruplesByContextSubjectLiteral"));
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
            DropDatabase("RDFPostgreSQLStoreTest_ShouldRemoveQuadruplesByContextPredicateObject");
            CreateDatabase("RDFPostgreSQLStoreTest_ShouldRemoveQuadruplesByContextPredicateObject");

            RDFQuadruple quadruple = new RDFQuadruple(new RDFContext(new Uri("ex:ctx")), new RDFResource("ex:subj"), new RDFResource("ex:pred"), new RDFResource("ex:obj"));

            RDFPostgreSQLStore store = new RDFPostgreSQLStore(GetConnectionString("RDFPostgreSQLStoreTest_ShouldRemoveQuadruplesByContextPredicateObject"));
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
            DropDatabase("RDFPostgreSQLStoreTest_ShouldNotRemoveQuadruplesByContextPredicateObject");
            CreateDatabase("RDFPostgreSQLStoreTest_ShouldNotRemoveQuadruplesByContextPredicateObject");

            RDFQuadruple quadruple = new RDFQuadruple(new RDFContext(new Uri("ex:ctx")), new RDFResource("ex:subj"), new RDFResource("ex:pred"), new RDFResource("ex:obj"));

            RDFPostgreSQLStore store = new RDFPostgreSQLStore(GetConnectionString("RDFPostgreSQLStoreTest_ShouldNotRemoveQuadruplesByContextPredicateObject"));
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
            DropDatabase("RDFPostgreSQLStoreTest_ShouldRemoveQuadruplesByContextPredicateLiteral");
            CreateDatabase("RDFPostgreSQLStoreTest_ShouldRemoveQuadruplesByContextPredicateLiteral");

            RDFQuadruple quadruple = new RDFQuadruple(new RDFContext(new Uri("ex:ctx")), new RDFResource("ex:subj"), new RDFResource("ex:pred"), new RDFPlainLiteral("hello"));

            RDFPostgreSQLStore store = new RDFPostgreSQLStore(GetConnectionString("RDFPostgreSQLStoreTest_ShouldRemoveQuadruplesByContextPredicateLiteral"));
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
            DropDatabase("RDFPostgreSQLStoreTest_ShouldNotRemoveQuadruplesByContextPredicateLiteral");
            CreateDatabase("RDFPostgreSQLStoreTest_ShouldNotRemoveQuadruplesByContextPredicateLiteral");

            RDFQuadruple quadruple = new RDFQuadruple(new RDFContext(new Uri("ex:ctx")), new RDFResource("ex:subj"), new RDFResource("ex:pred"), new RDFPlainLiteral("hello"));

            RDFPostgreSQLStore store = new RDFPostgreSQLStore(GetConnectionString("RDFPostgreSQLStoreTest_ShouldNotRemoveQuadruplesByContextPredicateLiteral"));
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
            DropDatabase("RDFPostgreSQLStoreTest_ShouldRemoveQuadruplesBySubjectPredicate");
            CreateDatabase("RDFPostgreSQLStoreTest_ShouldRemoveQuadruplesBySubjectPredicate");

            RDFQuadruple quadruple = new RDFQuadruple(new RDFContext(new Uri("ex:ctx")), new RDFResource("ex:subj"), new RDFResource("ex:pred"), new RDFResource("ex:obj"));

            RDFPostgreSQLStore store = new RDFPostgreSQLStore(GetConnectionString("RDFPostgreSQLStoreTest_ShouldRemoveQuadruplesBySubjectPredicate"));
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
            DropDatabase("RDFPostgreSQLStoreTest_ShouldNotRemoveQuadruplesBySubjectPredicate");
            CreateDatabase("RDFPostgreSQLStoreTest_ShouldNotRemoveQuadruplesBySubjectPredicate");

            RDFQuadruple quadruple = new RDFQuadruple(new RDFContext(new Uri("ex:ctx")), new RDFResource("ex:subj"), new RDFResource("ex:pred"), new RDFResource("ex:obj"));

            RDFPostgreSQLStore store = new RDFPostgreSQLStore(GetConnectionString("RDFPostgreSQLStoreTest_ShouldNotRemoveQuadruplesBySubjectPredicate"));
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
            DropDatabase("RDFPostgreSQLStoreTest_ShouldRemoveQuadruplesBySubjectObject");
            CreateDatabase("RDFPostgreSQLStoreTest_ShouldRemoveQuadruplesBySubjectObject");

            RDFQuadruple quadruple = new RDFQuadruple(new RDFContext(new Uri("ex:ctx")), new RDFResource("ex:subj"), new RDFResource("ex:pred"), new RDFResource("ex:obj"));

            RDFPostgreSQLStore store = new RDFPostgreSQLStore(GetConnectionString("RDFPostgreSQLStoreTest_ShouldRemoveQuadruplesBySubjectObject"));
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
            DropDatabase("RDFPostgreSQLStoreTest_ShouldNotRemoveQuadruplesBySubjectObject");
            CreateDatabase("RDFPostgreSQLStoreTest_ShouldNotRemoveQuadruplesBySubjectObject");

            RDFQuadruple quadruple = new RDFQuadruple(new RDFContext(new Uri("ex:ctx")), new RDFResource("ex:subj"), new RDFResource("ex:pred"), new RDFResource("ex:obj"));

            RDFPostgreSQLStore store = new RDFPostgreSQLStore(GetConnectionString("RDFPostgreSQLStoreTest_ShouldNotRemoveQuadruplesBySubjectObject"));
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
            DropDatabase("RDFPostgreSQLStoreTest_ShouldRemoveQuadruplesBySubjectLiteral");
            CreateDatabase("RDFPostgreSQLStoreTest_ShouldRemoveQuadruplesBySubjectLiteral");

            RDFQuadruple quadruple = new RDFQuadruple(new RDFContext(new Uri("ex:ctx")), new RDFResource("ex:subj"), new RDFResource("ex:pred"), new RDFPlainLiteral("hello"));

            RDFPostgreSQLStore store = new RDFPostgreSQLStore(GetConnectionString("RDFPostgreSQLStoreTest_ShouldRemoveQuadruplesBySubjectLiteral"));
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
            DropDatabase("RDFPostgreSQLStoreTest_ShouldNotRemoveQuadruplesBySubjectLiteral");
            CreateDatabase("RDFPostgreSQLStoreTest_ShouldNotRemoveQuadruplesBySubjectLiteral");

            RDFQuadruple quadruple = new RDFQuadruple(new RDFContext(new Uri("ex:ctx")), new RDFResource("ex:subj"), new RDFResource("ex:pred"), new RDFPlainLiteral("hello"));

            RDFPostgreSQLStore store = new RDFPostgreSQLStore(GetConnectionString("RDFPostgreSQLStoreTest_ShouldNotRemoveQuadruplesBySubjectLiteral"));
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
            DropDatabase("RDFPostgreSQLStoreTest_ShouldRemoveQuadruplesByPredicateObject");
            CreateDatabase("RDFPostgreSQLStoreTest_ShouldRemoveQuadruplesByPredicateObject");

            RDFQuadruple quadruple = new RDFQuadruple(new RDFContext(new Uri("ex:ctx")), new RDFResource("ex:subj"), new RDFResource("ex:pred"), new RDFResource("ex:obj"));

            RDFPostgreSQLStore store = new RDFPostgreSQLStore(GetConnectionString("RDFPostgreSQLStoreTest_ShouldRemoveQuadruplesByPredicateObject"));
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
            DropDatabase("RDFPostgreSQLStoreTest_ShouldNotRemoveQuadruplesByPredicateObject");
            CreateDatabase("RDFPostgreSQLStoreTest_ShouldNotRemoveQuadruplesByPredicateObject");

            RDFQuadruple quadruple = new RDFQuadruple(new RDFContext(new Uri("ex:ctx")), new RDFResource("ex:subj"), new RDFResource("ex:pred"), new RDFResource("ex:obj"));

            RDFPostgreSQLStore store = new RDFPostgreSQLStore(GetConnectionString("RDFPostgreSQLStoreTest_ShouldNotRemoveQuadruplesByPredicateObject"));
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
            DropDatabase("RDFPostgreSQLStoreTest_ShouldRemoveQuadruplesByPredicateLiteral");
            CreateDatabase("RDFPostgreSQLStoreTest_ShouldRemoveQuadruplesByPredicateLiteral");

            RDFQuadruple quadruple = new RDFQuadruple(new RDFContext(new Uri("ex:ctx")), new RDFResource("ex:subj"), new RDFResource("ex:pred"), new RDFPlainLiteral("hello"));

            RDFPostgreSQLStore store = new RDFPostgreSQLStore(GetConnectionString("RDFPostgreSQLStoreTest_ShouldRemoveQuadruplesByPredicateLiteral"));
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
            DropDatabase("RDFPostgreSQLStoreTest_ShouldNotRemoveQuadruplesByPredicateLiteral");
            CreateDatabase("RDFPostgreSQLStoreTest_ShouldNotRemoveQuadruplesByPredicateLiteral");

            RDFQuadruple quadruple = new RDFQuadruple(new RDFContext(new Uri("ex:ctx")), new RDFResource("ex:subj"), new RDFResource("ex:pred"), new RDFPlainLiteral("hello"));

            RDFPostgreSQLStore store = new RDFPostgreSQLStore(GetConnectionString("RDFPostgreSQLStoreTest_ShouldNotRemoveQuadruplesByPredicateLiteral"));
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
            DropDatabase("RDFPostgreSQLStoreTest_ShouldClearQuadruples");
            CreateDatabase("RDFPostgreSQLStoreTest_ShouldClearQuadruples");

            RDFQuadruple quadruple = new RDFQuadruple(new RDFContext(new Uri("ex:ctx")), new RDFResource("ex:subj"), new RDFResource("ex:pred"), new RDFPlainLiteral("hello"));

            RDFPostgreSQLStore store = new RDFPostgreSQLStore(GetConnectionString("RDFPostgreSQLStoreTest_ShouldClearQuadruples"));
            store.ClearQuadruples();

            RDFMemoryStore memStore = store.SelectAllQuadruples();

            Assert.IsNotNull(memStore);
            Assert.IsTrue(memStore.QuadruplesCount == 0);
        }

        [TestMethod]
        public void ShouldContainQuadruple()
        {
            DropDatabase("RDFPostgreSQLStoreTest_ShouldContainQuadruple");
            CreateDatabase("RDFPostgreSQLStoreTest_ShouldContainQuadruple");

            RDFQuadruple quadruple1 = new RDFQuadruple(new RDFContext(new Uri("ex:ctx")), new RDFResource("ex:subj"), new RDFResource("ex:pred"), new RDFPlainLiteral("hello"));
            RDFQuadruple quadruple2 = new RDFQuadruple(new RDFContext(new Uri("ex:ctx2")), new RDFResource("ex:subj"), new RDFResource("ex:pred"), new RDFPlainLiteral("hello"));

            RDFPostgreSQLStore store = new RDFPostgreSQLStore(GetConnectionString("RDFPostgreSQLStoreTest_ShouldContainQuadruple"));
            store.AddQuadruple(quadruple1);
            store.AddQuadruple(quadruple2);

            Assert.IsTrue(store.ContainsQuadruple(quadruple1));
            Assert.IsTrue(store.ContainsQuadruple(quadruple2));
        }

        [TestMethod]
        public void ShouldNotContainQuadruple()
        {
            DropDatabase("RDFPostgreSQLStoreTest_ShouldNotContainQuadruple");
            CreateDatabase("RDFPostgreSQLStoreTest_ShouldNotContainQuadruple");

            RDFQuadruple quadruple = new RDFQuadruple(new RDFContext(new Uri("ex:ctx")), new RDFResource("ex:subj"), new RDFResource("ex:pred"), new RDFPlainLiteral("hello"));
            RDFQuadruple quadruple2 = new RDFQuadruple(new RDFContext(new Uri("ex:ctx2")), new RDFResource("ex:subj"), new RDFResource("ex:pred"), new RDFPlainLiteral("hello"));

            RDFPostgreSQLStore store = new RDFPostgreSQLStore(GetConnectionString("RDFPostgreSQLStoreTest_ShouldNotContainQuadruple"));
            store.AddQuadruple(quadruple);

            Assert.IsFalse(store.ContainsQuadruple(quadruple2));
            Assert.IsFalse(store.ContainsQuadruple(null));
        }

        [TestMethod]
        public void ShouldSelectQuadruplesByContext()
        {
            DropDatabase("RDFPostgreSQLStoreTest_ShouldSelectQuadruplesByContext");
            CreateDatabase("RDFPostgreSQLStoreTest_ShouldSelectQuadruplesByContext");

            RDFQuadruple quadruple = new RDFQuadruple(new RDFContext(new Uri("ex:ctx")), new RDFResource("ex:subj"), new RDFResource("ex:pred"), new RDFResource("ex:obj"));

            RDFPostgreSQLStore store = new RDFPostgreSQLStore(GetConnectionString("RDFPostgreSQLStoreTest_ShouldSelectQuadruplesByContext"));
            store.AddQuadruple(quadruple);
            RDFMemoryStore result = store.SelectQuadruplesByContext(new RDFContext(new Uri("ex:ctx")));

            Assert.IsNotNull(result);
            Assert.IsTrue(result.ContainsQuadruple(quadruple));
        }

        [TestMethod]
        public void ShouldNotSelectQuadruplesByContext()
        {
            DropDatabase("RDFPostgreSQLStoreTest_ShouldNotSelectQuadruplesByContext");
            CreateDatabase("RDFPostgreSQLStoreTest_ShouldNotSelectQuadruplesByContext");

            RDFQuadruple quadruple = new RDFQuadruple(new RDFContext(new Uri("ex:ctx")), new RDFResource("ex:subj"), new RDFResource("ex:pred"), new RDFResource("ex:obj"));

            RDFPostgreSQLStore store = new RDFPostgreSQLStore(GetConnectionString("RDFPostgreSQLStoreTest_ShouldNotSelectQuadruplesByContext"));
            store.AddQuadruple(quadruple);
            RDFMemoryStore result = store.SelectQuadruplesByContext(new RDFContext(new Uri("ex:ctx2")));

            Assert.IsNotNull(result);
            Assert.IsFalse(result.ContainsQuadruple(quadruple));
        }

        [TestMethod]
        public void ShouldSelectQuadruplesBySubject()
        {
            DropDatabase("RDFPostgreSQLStoreTest_ShouldSelectQuadruplesBySubject");
            CreateDatabase("RDFPostgreSQLStoreTest_ShouldSelectQuadruplesBySubject");

            RDFQuadruple quadruple = new RDFQuadruple(new RDFContext(new Uri("ex:ctx")), new RDFResource("ex:subj"), new RDFResource("ex:pred"), new RDFResource("ex:obj"));

            RDFPostgreSQLStore store = new RDFPostgreSQLStore(GetConnectionString("RDFPostgreSQLStoreTest_ShouldSelectQuadruplesBySubject"));
            store.AddQuadruple(quadruple);
            RDFMemoryStore result = store.SelectQuadruplesBySubject(new RDFResource("ex:subj"));

            Assert.IsNotNull(result);
            Assert.IsTrue(result.ContainsQuadruple(quadruple));
        }

        [TestMethod]
        public void ShouldNotSelectQuadruplesBySubject()
        {
            DropDatabase("RDFPostgreSQLStoreTest_ShouldNotSelectQuadruplesBySubject");
            CreateDatabase("RDFPostgreSQLStoreTest_ShouldNotSelectQuadruplesBySubject");

            RDFQuadruple quadruple = new RDFQuadruple(new RDFContext(new Uri("ex:ctx")), new RDFResource("ex:subj"), new RDFResource("ex:pred"), new RDFResource("ex:obj"));

            RDFPostgreSQLStore store = new RDFPostgreSQLStore(GetConnectionString("RDFPostgreSQLStoreTest_ShouldNotSelectQuadruplesBySubject"));
            store.AddQuadruple(quadruple);
            RDFMemoryStore result = store.SelectQuadruplesBySubject(new RDFResource("ex:subj2"));

            Assert.IsNotNull(result);
            Assert.IsFalse(result.ContainsQuadruple(quadruple));
        }

        [TestMethod]
        public void ShouldSelectQuadruplesByPredicate()
        {
            DropDatabase("RDFPostgreSQLStoreTest_ShouldSelectQuadruplesByPredicate");
            CreateDatabase("RDFPostgreSQLStoreTest_ShouldSelectQuadruplesByPredicate");

            RDFQuadruple quadruple = new RDFQuadruple(new RDFContext(new Uri("ex:ctx")), new RDFResource("ex:subj"), new RDFResource("ex:pred"), new RDFResource("ex:obj"));

            RDFPostgreSQLStore store = new RDFPostgreSQLStore(GetConnectionString("RDFPostgreSQLStoreTest_ShouldSelectQuadruplesByPredicate"));
            store.AddQuadruple(quadruple);
            RDFMemoryStore result = store.SelectQuadruplesByPredicate(new RDFResource("ex:pred"));

            Assert.IsNotNull(result);
            Assert.IsTrue(result.ContainsQuadruple(quadruple));
        }

        [TestMethod]
        public void ShouldNotSelectQuadruplesByPredicate()
        {
            DropDatabase("RDFPostgreSQLStoreTest_ShouldNotSelectQuadruplesByPredicate");
            CreateDatabase("RDFPostgreSQLStoreTest_ShouldNotSelectQuadruplesByPredicate");

            RDFQuadruple quadruple = new RDFQuadruple(new RDFContext(new Uri("ex:ctx")), new RDFResource("ex:subj"), new RDFResource("ex:pred"), new RDFResource("ex:obj"));

            RDFPostgreSQLStore store = new RDFPostgreSQLStore(GetConnectionString("RDFPostgreSQLStoreTest_ShouldNotSelectQuadruplesByPredicate"));
            store.AddQuadruple(quadruple);
            RDFMemoryStore result = store.SelectQuadruplesByPredicate(new RDFResource("ex:pred2"));

            Assert.IsNotNull(result);
            Assert.IsFalse(result.ContainsQuadruple(quadruple));
        }

        [TestMethod]
        public void ShouldSelectQuadruplesByObject()
        {
            DropDatabase("RDFPostgreSQLStoreTest_ShouldSelectQuadruplesByObject");
            CreateDatabase("RDFPostgreSQLStoreTest_ShouldSelectQuadruplesByObject");

            RDFQuadruple quadruple = new RDFQuadruple(new RDFContext(new Uri("ex:ctx")), new RDFResource("ex:subj"), new RDFResource("ex:pred"), new RDFResource("ex:obj"));

            RDFPostgreSQLStore store = new RDFPostgreSQLStore(GetConnectionString("RDFPostgreSQLStoreTest_ShouldSelectQuadruplesByObject"));
            store.AddQuadruple(quadruple);
            RDFMemoryStore result = store.SelectQuadruplesByObject(new RDFResource("ex:obj"));

            Assert.IsNotNull(result);
            Assert.IsTrue(result.ContainsQuadruple(quadruple));
        }

        [TestMethod]
        public void ShouldNotSelectQuadruplesByObject()
        {
            DropDatabase("RDFPostgreSQLStoreTest_ShouldNotSelectQuadruplesByObject");
            CreateDatabase("RDFPostgreSQLStoreTest_ShouldNotSelectQuadruplesByObject");

            RDFQuadruple quadruple = new RDFQuadruple(new RDFContext(new Uri("ex:ctx")), new RDFResource("ex:subj"), new RDFResource("ex:pred"), new RDFResource("ex:obj"));

            RDFPostgreSQLStore store = new RDFPostgreSQLStore(GetConnectionString("RDFPostgreSQLStoreTest_ShouldNotSelectQuadruplesByObject"));
            store.AddQuadruple(quadruple);
            RDFMemoryStore result = store.SelectQuadruplesByObject(new RDFResource("ex:obj2"));

            Assert.IsNotNull(result);
            Assert.IsFalse(result.ContainsQuadruple(quadruple));
        }

        [TestMethod]
        public void ShouldSelectQuadruplesByLiteral()
        {
            DropDatabase("RDFPostgreSQLStoreTest_ShouldSelectQuadruplesByLiteral");
            CreateDatabase("RDFPostgreSQLStoreTest_ShouldSelectQuadruplesByLiteral");

            RDFQuadruple quadruple = new RDFQuadruple(new RDFContext(new Uri("ex:ctx")), new RDFResource("ex:subj"), new RDFResource("ex:pred"), new RDFPlainLiteral("hello"));

            RDFPostgreSQLStore store = new RDFPostgreSQLStore(GetConnectionString("RDFPostgreSQLStoreTest_ShouldSelectQuadruplesByLiteral"));
            store.AddQuadruple(quadruple);
            RDFMemoryStore result = store.SelectQuadruplesByLiteral(new RDFPlainLiteral("hello"));

            Assert.IsNotNull(result);
            Assert.IsTrue(result.ContainsQuadruple(quadruple));
        }

        [TestMethod]
        public void ShouldNotSelectQuadruplesByLiteral()
        {
            DropDatabase("RDFPostgreSQLStoreTest_ShouldNotSelectQuadruplesByLiteral");
            CreateDatabase("RDFPostgreSQLStoreTest_ShouldNotSelectQuadruplesByLiteral");

            RDFQuadruple quadruple = new RDFQuadruple(new RDFContext(new Uri("ex:ctx")), new RDFResource("ex:subj"), new RDFResource("ex:pred"), new RDFPlainLiteral("hello"));

            RDFPostgreSQLStore store = new RDFPostgreSQLStore(GetConnectionString("RDFPostgreSQLStoreTest_ShouldNotSelectQuadruplesByLiteral"));
            store.AddQuadruple(quadruple);
            RDFMemoryStore result = store.SelectQuadruplesByLiteral(new RDFPlainLiteral("hello","en"));

            Assert.IsNotNull(result);
            Assert.IsFalse(result.ContainsQuadruple(quadruple));
        }

        [TestMethod]
        public void ShouldSelectQuadruplesByContextSubject()
        {
            DropDatabase("RDFPostgreSQLStoreTest_ShouldSelectQuadruplesByContextSubject");
            CreateDatabase("RDFPostgreSQLStoreTest_ShouldSelectQuadruplesByContextSubject");

            RDFQuadruple quadruple = new RDFQuadruple(new RDFContext(new Uri("ex:ctx")), new RDFResource("ex:subj"), new RDFResource("ex:pred"), new RDFResource("ex:obj"));

            RDFPostgreSQLStore store = new RDFPostgreSQLStore(GetConnectionString("RDFPostgreSQLStoreTest_ShouldSelectQuadruplesByContextSubject"));
            store.AddQuadruple(quadruple);
            RDFMemoryStore result = store.SelectQuadruples(new RDFContext(new Uri("ex:ctx")), new RDFResource("ex:subj"), null, null, null);

            Assert.IsNotNull(result);
            Assert.IsTrue(result.ContainsQuadruple(quadruple));
        }

        [TestMethod]
        public void ShouldNotSelectQuadruplesByContextSubject()
        {
            DropDatabase("RDFPostgreSQLStoreTest_ShouldNotSelectQuadruplesByContextSubject");
            CreateDatabase("RDFPostgreSQLStoreTest_ShouldNotSelectQuadruplesByContextSubject");

            RDFQuadruple quadruple = new RDFQuadruple(new RDFContext(new Uri("ex:ctx")), new RDFResource("ex:subj"), new RDFResource("ex:pred"), new RDFResource("ex:obj"));

            RDFPostgreSQLStore store = new RDFPostgreSQLStore(GetConnectionString("RDFPostgreSQLStoreTest_ShouldNotSelectQuadruplesByContextSubject"));
            store.AddQuadruple(quadruple);
            RDFMemoryStore result = store.SelectQuadruples(new RDFContext(new Uri("ex:ctx2")), new RDFResource("ex:subj"), null, null, null);

            Assert.IsNotNull(result);
            Assert.IsFalse(result.ContainsQuadruple(quadruple));
        }

        [TestMethod]
        public void ShouldSelectQuadruplesByContextPredicate()
        {
            DropDatabase("RDFPostgreSQLStoreTest_ShouldSelectQuadruplesByContextPredicate");
            CreateDatabase("RDFPostgreSQLStoreTest_ShouldSelectQuadruplesByContextPredicate");

            RDFQuadruple quadruple = new RDFQuadruple(new RDFContext(new Uri("ex:ctx")), new RDFResource("ex:subj"), new RDFResource("ex:pred"), new RDFResource("ex:obj"));

            RDFPostgreSQLStore store = new RDFPostgreSQLStore(GetConnectionString("RDFPostgreSQLStoreTest_ShouldSelectQuadruplesByContextPredicate"));
            store.AddQuadruple(quadruple);
            RDFMemoryStore result = store.SelectQuadruples(new RDFContext(new Uri("ex:ctx")), null, new RDFResource("ex:pred"), null, null);

            Assert.IsNotNull(result);
            Assert.IsTrue(result.ContainsQuadruple(quadruple));
        }

        [TestMethod]
        public void ShouldNotSelectQuadruplesByContextPredicate()
        {
            DropDatabase("RDFPostgreSQLStoreTest_ShouldNotSelectQuadruplesByContextPredicate");
            CreateDatabase("RDFPostgreSQLStoreTest_ShouldNotSelectQuadruplesByContextPredicate");

            RDFQuadruple quadruple = new RDFQuadruple(new RDFContext(new Uri("ex:ctx")), new RDFResource("ex:subj"), new RDFResource("ex:pred"), new RDFResource("ex:obj"));

            RDFPostgreSQLStore store = new RDFPostgreSQLStore(GetConnectionString("RDFPostgreSQLStoreTest_ShouldNotSelectQuadruplesByContextPredicate"));
            store.AddQuadruple(quadruple);
            RDFMemoryStore result = store.SelectQuadruples(new RDFContext(new Uri("ex:ctx2")), null, new RDFResource("ex:pred"), null, null);

            Assert.IsNotNull(result);
            Assert.IsFalse(result.ContainsQuadruple(quadruple));
        }

        [TestMethod]
        public void ShouldSelectQuadruplesByContextObject()
        {
            DropDatabase("RDFPostgreSQLStoreTest_ShouldSelectQuadruplesByContextObject");
            CreateDatabase("RDFPostgreSQLStoreTest_ShouldSelectQuadruplesByContextObject");

            RDFQuadruple quadruple = new RDFQuadruple(new RDFContext(new Uri("ex:ctx")), new RDFResource("ex:subj"), new RDFResource("ex:pred"), new RDFResource("ex:obj"));

            RDFPostgreSQLStore store = new RDFPostgreSQLStore(GetConnectionString("RDFPostgreSQLStoreTest_ShouldSelectQuadruplesByContextObject"));
            store.AddQuadruple(quadruple);
            RDFMemoryStore result = store.SelectQuadruples(new RDFContext(new Uri("ex:ctx")), null, null, new RDFResource("ex:obj"), null);

            Assert.IsNotNull(result);
            Assert.IsTrue(result.ContainsQuadruple(quadruple));
        }

        [TestMethod]
        public void ShouldNotSelectQuadruplesByContextObject()
        {
            DropDatabase("RDFPostgreSQLStoreTest_ShouldNotSelectQuadruplesByContextObject");
            CreateDatabase("RDFPostgreSQLStoreTest_ShouldNotSelectQuadruplesByContextObject");

            RDFQuadruple quadruple = new RDFQuadruple(new RDFContext(new Uri("ex:ctx")), new RDFResource("ex:subj"), new RDFResource("ex:pred"), new RDFResource("ex:obj"));

            RDFPostgreSQLStore store = new RDFPostgreSQLStore(GetConnectionString("RDFPostgreSQLStoreTest_ShouldNotSelectQuadruplesByContextObject"));
            store.AddQuadruple(quadruple);
            RDFMemoryStore result = store.SelectQuadruples(new RDFContext(new Uri("ex:ctx2")), null, null, new RDFResource("ex:obj"), null);

            Assert.IsNotNull(result);
            Assert.IsFalse(result.ContainsQuadruple(quadruple));
        }

        [TestMethod]
        public void ShouldSelectQuadruplesByContextLiteral()
        {
            DropDatabase("RDFPostgreSQLStoreTest_ShouldSelectQuadruplesByContextLiteral");
            CreateDatabase("RDFPostgreSQLStoreTest_ShouldSelectQuadruplesByContextLiteral");

            RDFQuadruple quadruple = new RDFQuadruple(new RDFContext(new Uri("ex:ctx")), new RDFResource("ex:subj"), new RDFResource("ex:pred"), new RDFPlainLiteral("hello"));

            RDFPostgreSQLStore store = new RDFPostgreSQLStore(GetConnectionString("RDFPostgreSQLStoreTest_ShouldSelectQuadruplesByContextLiteral"));
            store.AddQuadruple(quadruple);
            RDFMemoryStore result = store.SelectQuadruples(new RDFContext(new Uri("ex:ctx")), null, null, null, new RDFPlainLiteral("hello"));

            Assert.IsNotNull(result);
            Assert.IsTrue(result.ContainsQuadruple(quadruple));
        }

        [TestMethod]
        public void ShouldNotSelectQuadruplesByContextLiteral()
        {
            DropDatabase("RDFPostgreSQLStoreTest_ShouldNotSelectQuadruplesByContextLiteral");
            CreateDatabase("RDFPostgreSQLStoreTest_ShouldNotSelectQuadruplesByContextLiteral");

            RDFQuadruple quadruple = new RDFQuadruple(new RDFContext(new Uri("ex:ctx")), new RDFResource("ex:subj"), new RDFResource("ex:pred"), new RDFPlainLiteral("hello"));

            RDFPostgreSQLStore store = new RDFPostgreSQLStore(GetConnectionString("RDFPostgreSQLStoreTest_ShouldNotSelectQuadruplesByContextLiteral"));
            store.AddQuadruple(quadruple);
            RDFMemoryStore result = store.SelectQuadruples(new RDFContext(new Uri("ex:ctx2")), null, null, null, new RDFPlainLiteral("hello"));

            Assert.IsNotNull(result);
            Assert.IsFalse(result.ContainsQuadruple(quadruple));
        }

        [TestMethod]
        public void ShouldSelectQuadruplesByContextSubjectPredicate()
        {
            DropDatabase("RDFPostgreSQLStoreTest_ShouldSelectQuadruplesByContextSubjectPredicate");
            CreateDatabase("RDFPostgreSQLStoreTest_ShouldSelectQuadruplesByContextSubjectPredicate");

            RDFQuadruple quadruple = new RDFQuadruple(new RDFContext(new Uri("ex:ctx")), new RDFResource("ex:subj"), new RDFResource("ex:pred"), new RDFResource("ex:obj"));

            RDFPostgreSQLStore store = new RDFPostgreSQLStore(GetConnectionString("RDFPostgreSQLStoreTest_ShouldSelectQuadruplesByContextSubjectPredicate"));
            store.AddQuadruple(quadruple);
            RDFMemoryStore result = store.SelectQuadruples(new RDFContext(new Uri("ex:ctx")), new RDFResource("ex:subj"), new RDFResource("ex:pred"), null, null);

            Assert.IsNotNull(result);
            Assert.IsTrue(result.ContainsQuadruple(quadruple));
        }

        [TestMethod]
        public void ShouldNotSelectQuadruplesByContextSubjectPredicate()
        {
            DropDatabase("RDFPostgreSQLStoreTest_ShouldNotSelectQuadruplesByContextSubjectPredicate");
            CreateDatabase("RDFPostgreSQLStoreTest_ShouldNotSelectQuadruplesByContextSubjectPredicate");

            RDFQuadruple quadruple = new RDFQuadruple(new RDFContext(new Uri("ex:ctx")), new RDFResource("ex:subj"), new RDFResource("ex:pred"), new RDFResource("ex:obj"));

            RDFPostgreSQLStore store = new RDFPostgreSQLStore(GetConnectionString("RDFPostgreSQLStoreTest_ShouldNotSelectQuadruplesByContextSubjectPredicate"));
            store.AddQuadruple(quadruple);
            RDFMemoryStore result = store.SelectQuadruples(new RDFContext(new Uri("ex:ctx2")), new RDFResource("ex:subj"), new RDFResource("ex:pred"), null, null);

            Assert.IsNotNull(result);
            Assert.IsFalse(result.ContainsQuadruple(quadruple));
        }

        [TestMethod]
        public void ShouldSelectQuadruplesByContextSubjectObject()
        {
            DropDatabase("RDFPostgreSQLStoreTest_ShouldSelectQuadruplesByContextSubjectObject");
            CreateDatabase("RDFPostgreSQLStoreTest_ShouldSelectQuadruplesByContextSubjectObject");

            RDFQuadruple quadruple = new RDFQuadruple(new RDFContext(new Uri("ex:ctx")), new RDFResource("ex:subj"), new RDFResource("ex:pred"), new RDFResource("ex:obj"));

            RDFPostgreSQLStore store = new RDFPostgreSQLStore(GetConnectionString("RDFPostgreSQLStoreTest_ShouldSelectQuadruplesByContextSubjectObject"));
            store.AddQuadruple(quadruple);
            RDFMemoryStore result = store.SelectQuadruples(new RDFContext(new Uri("ex:ctx")), new RDFResource("ex:subj"), null, new RDFResource("ex:obj"), null);

            Assert.IsNotNull(result);
            Assert.IsTrue(result.ContainsQuadruple(quadruple));
        }

        [TestMethod]
        public void ShouldNotSelectQuadruplesByContextSubjectObject()
        {
            DropDatabase("RDFPostgreSQLStoreTest_ShouldNotSelectQuadruplesByContextSubjectObject");
            CreateDatabase("RDFPostgreSQLStoreTest_ShouldNotSelectQuadruplesByContextSubjectObject");

            RDFQuadruple quadruple = new RDFQuadruple(new RDFContext(new Uri("ex:ctx")), new RDFResource("ex:subj"), new RDFResource("ex:pred"), new RDFResource("ex:obj"));

            RDFPostgreSQLStore store = new RDFPostgreSQLStore(GetConnectionString("RDFPostgreSQLStoreTest_ShouldNotSelectQuadruplesByContextSubjectObject"));
            store.AddQuadruple(quadruple);
            RDFMemoryStore result = store.SelectQuadruples(new RDFContext(new Uri("ex:ctx2")), new RDFResource("ex:subj"), null, new RDFResource("ex:obj"), null);

            Assert.IsNotNull(result);
            Assert.IsFalse(result.ContainsQuadruple(quadruple));
        }

        [TestMethod]
        public void ShouldSelectQuadruplesByContextSubjectLiteral()
        {
            DropDatabase("RDFPostgreSQLStoreTest_ShouldSelectQuadruplesByContextSubjectLiteral");
            CreateDatabase("RDFPostgreSQLStoreTest_ShouldSelectQuadruplesByContextSubjectLiteral");

            RDFQuadruple quadruple = new RDFQuadruple(new RDFContext(new Uri("ex:ctx")), new RDFResource("ex:subj"), new RDFResource("ex:pred"), new RDFPlainLiteral("hello"));

            RDFPostgreSQLStore store = new RDFPostgreSQLStore(GetConnectionString("RDFPostgreSQLStoreTest_ShouldSelectQuadruplesByContextSubjectLiteral"));
            store.AddQuadruple(quadruple);
            RDFMemoryStore result = store.SelectQuadruples(new RDFContext(new Uri("ex:ctx")), new RDFResource("ex:subj"), null, null, new RDFPlainLiteral("hello"));

            Assert.IsNotNull(result);
            Assert.IsTrue(result.ContainsQuadruple(quadruple));
        }

        [TestMethod]
        public void ShouldNotSelectQuadruplesByContextSubjectLiteral()
        {
            DropDatabase("RDFPostgreSQLStoreTest_ShouldNotSelectQuadruplesByContextSubjectLiteral");
            CreateDatabase("RDFPostgreSQLStoreTest_ShouldNotSelectQuadruplesByContextSubjectLiteral");

            RDFQuadruple quadruple = new RDFQuadruple(new RDFContext(new Uri("ex:ctx")), new RDFResource("ex:subj"), new RDFResource("ex:pred"), new RDFPlainLiteral("hello"));

            RDFPostgreSQLStore store = new RDFPostgreSQLStore(GetConnectionString("RDFPostgreSQLStoreTest_ShouldNotSelectQuadruplesByContextSubjectLiteral"));
            store.AddQuadruple(quadruple);
            RDFMemoryStore result = store.SelectQuadruples(new RDFContext(new Uri("ex:ctx2")), new RDFResource("ex:subj"), null, null, new RDFPlainLiteral("hello"));

            Assert.IsNotNull(result);
            Assert.IsFalse(result.ContainsQuadruple(quadruple));
        }

        [TestMethod]
        public void ShouldSelectQuadruplesByContextPredicateObject()
        {
            DropDatabase("RDFPostgreSQLStoreTest_ShouldSelectQuadruplesByContextPredicateObject");
            CreateDatabase("RDFPostgreSQLStoreTest_ShouldSelectQuadruplesByContextPredicateObject");

            RDFQuadruple quadruple = new RDFQuadruple(new RDFContext(new Uri("ex:ctx")), new RDFResource("ex:subj"), new RDFResource("ex:pred"), new RDFResource("ex:obj"));

            RDFPostgreSQLStore store = new RDFPostgreSQLStore(GetConnectionString("RDFPostgreSQLStoreTest_ShouldSelectQuadruplesByContextPredicateObject"));
            store.AddQuadruple(quadruple);
            RDFMemoryStore result = store.SelectQuadruples(new RDFContext(new Uri("ex:ctx")), null, new RDFResource("ex:pred"), new RDFResource("ex:obj"), null);

            Assert.IsNotNull(result);
            Assert.IsTrue(result.ContainsQuadruple(quadruple));
        }

        [TestMethod]
        public void ShouldNotSelectQuadruplesByContextPredicateObject()
        {
            DropDatabase("RDFPostgreSQLStoreTest_ShouldNotSelectQuadruplesByContextPredicateObject");
            CreateDatabase("RDFPostgreSQLStoreTest_ShouldNotSelectQuadruplesByContextPredicateObject");

            RDFQuadruple quadruple = new RDFQuadruple(new RDFContext(new Uri("ex:ctx")), new RDFResource("ex:subj"), new RDFResource("ex:pred"), new RDFResource("ex:obj"));

            RDFPostgreSQLStore store = new RDFPostgreSQLStore(GetConnectionString("RDFPostgreSQLStoreTest_ShouldNotSelectQuadruplesByContextPredicateObject"));
            store.AddQuadruple(quadruple);
            RDFMemoryStore result = store.SelectQuadruples(new RDFContext(new Uri("ex:ctx2")), null, new RDFResource("ex:pred"), new RDFResource("ex:obj"), null);

            Assert.IsNotNull(result);
            Assert.IsFalse(result.ContainsQuadruple(quadruple));
        }

        [TestMethod]
        public void ShouldSelectQuadruplesByContextPredicateLiteral()
        {
            DropDatabase("RDFPostgreSQLStoreTest_ShouldSelectQuadruplesByContextPredicateLiteral");
            CreateDatabase("RDFPostgreSQLStoreTest_ShouldSelectQuadruplesByContextPredicateLiteral");

            RDFQuadruple quadruple = new RDFQuadruple(new RDFContext(new Uri("ex:ctx")), new RDFResource("ex:subj"), new RDFResource("ex:pred"), new RDFPlainLiteral("hello"));

            RDFPostgreSQLStore store = new RDFPostgreSQLStore(GetConnectionString("RDFPostgreSQLStoreTest_ShouldSelectQuadruplesByContextPredicateLiteral"));
            store.AddQuadruple(quadruple);
            RDFMemoryStore result = store.SelectQuadruples(new RDFContext(new Uri("ex:ctx")), null, new RDFResource("ex:pred"), null, new RDFPlainLiteral("hello"));

            Assert.IsNotNull(result);
            Assert.IsTrue(result.ContainsQuadruple(quadruple));
        }

        [TestMethod]
        public void ShouldNotSelectQuadruplesByContextPredicateLiteral()
        {
            DropDatabase("RDFPostgreSQLStoreTest_ShouldNotSelectQuadruplesByContextPredicateLiteral");
            CreateDatabase("RDFPostgreSQLStoreTest_ShouldNotSelectQuadruplesByContextPredicateLiteral");

            RDFQuadruple quadruple = new RDFQuadruple(new RDFContext(new Uri("ex:ctx")), new RDFResource("ex:subj"), new RDFResource("ex:pred"), new RDFPlainLiteral("hello"));

            RDFPostgreSQLStore store = new RDFPostgreSQLStore(GetConnectionString("RDFPostgreSQLStoreTest_ShouldNotSelectQuadruplesByContextPredicateLiteral"));
            store.AddQuadruple(quadruple);
            RDFMemoryStore result = store.SelectQuadruples(new RDFContext(new Uri("ex:ctx2")), null, new RDFResource("ex:pred"), null, new RDFPlainLiteral("hello"));

            Assert.IsNotNull(result);
            Assert.IsFalse(result.ContainsQuadruple(quadruple));
        }

        [TestMethod]
        public void ShouldSelectQuadruplesByContextSubjectPredicateObject()
        {
            DropDatabase("RDFPostgreSQLStoreTest_ShouldSelectQuadruplesByContextSubjectPredicateObject");
            CreateDatabase("RDFPostgreSQLStoreTest_ShouldSelectQuadruplesByContextSubjectPredicateObject");

            RDFQuadruple quadruple = new RDFQuadruple(new RDFContext(new Uri("ex:ctx")), new RDFResource("ex:subj"), new RDFResource("ex:pred"), new RDFResource("ex:obj"));

            RDFPostgreSQLStore store = new RDFPostgreSQLStore(GetConnectionString("RDFPostgreSQLStoreTest_ShouldSelectQuadruplesByContextSubjectPredicateObject"));
            store.AddQuadruple(quadruple);
            RDFMemoryStore result = store.SelectQuadruples(new RDFContext(new Uri("ex:ctx")), new RDFResource("ex:subj"), new RDFResource("ex:pred"), new RDFResource("ex:obj"), null);

            Assert.IsNotNull(result);
            Assert.IsTrue(result.ContainsQuadruple(quadruple));
        }

        [TestMethod]
        public void ShouldNotSelectQuadruplesByContextSubjectPredicateObject()
        {
            DropDatabase("RDFPostgreSQLStoreTest_ShouldNotSelectQuadruplesByContextSubjectPredicateObject");
            CreateDatabase("RDFPostgreSQLStoreTest_ShouldNotSelectQuadruplesByContextSubjectPredicateObject");

            RDFQuadruple quadruple = new RDFQuadruple(new RDFContext(new Uri("ex:ctx")), new RDFResource("ex:subj"), new RDFResource("ex:pred"), new RDFResource("ex:obj"));

            RDFPostgreSQLStore store = new RDFPostgreSQLStore(GetConnectionString("RDFPostgreSQLStoreTest_ShouldNotSelectQuadruplesByContextSubjectPredicateObject"));
            store.AddQuadruple(quadruple);
            RDFMemoryStore result = store.SelectQuadruples(new RDFContext(new Uri("ex:ctx2")), new RDFResource("ex:subj"), new RDFResource("ex:pred"), new RDFResource("ex:obj"), null);

            Assert.IsNotNull(result);
            Assert.IsFalse(result.ContainsQuadruple(quadruple));
        }

        [TestMethod]
        public void ShouldSelectQuadruplesByContextSubjectPredicateLiteral()
        {
            DropDatabase("RDFPostgreSQLStoreTest_ShouldSelectQuadruplesByContextSubjectPredicateLiteral");
            CreateDatabase("RDFPostgreSQLStoreTest_ShouldSelectQuadruplesByContextSubjectPredicateLiteral");

            RDFQuadruple quadruple = new RDFQuadruple(new RDFContext(new Uri("ex:ctx")), new RDFResource("ex:subj"), new RDFResource("ex:pred"), new RDFPlainLiteral("hello"));

            RDFPostgreSQLStore store = new RDFPostgreSQLStore(GetConnectionString("RDFPostgreSQLStoreTest_ShouldSelectQuadruplesByContextSubjectPredicateLiteral"));
            store.AddQuadruple(quadruple);
            RDFMemoryStore result = store.SelectQuadruples(new RDFContext(new Uri("ex:ctx")), new RDFResource("ex:subj"), new RDFResource("ex:pred"), null, new RDFPlainLiteral("hello"));

            Assert.IsNotNull(result);
            Assert.IsTrue(result.ContainsQuadruple(quadruple));
        }

        [TestMethod]
        public void ShouldNotSelectQuadruplesByContextSubjectPredicateLiteral()
        {
            DropDatabase("RDFPostgreSQLStoreTest_ShouldNotSelectQuadruplesByContextSubjectPredicateLiteral");
            CreateDatabase("RDFPostgreSQLStoreTest_ShouldNotSelectQuadruplesByContextSubjectPredicateLiteral");

            RDFQuadruple quadruple = new RDFQuadruple(new RDFContext(new Uri("ex:ctx")), new RDFResource("ex:subj"), new RDFResource("ex:pred"), new RDFPlainLiteral("hello"));

            RDFPostgreSQLStore store = new RDFPostgreSQLStore(GetConnectionString("RDFPostgreSQLStoreTest_ShouldNotSelectQuadruplesByContextSubjectPredicateLiteral"));
            store.AddQuadruple(quadruple);
            RDFMemoryStore result = store.SelectQuadruples(new RDFContext(new Uri("ex:ctx2")), new RDFResource("ex:subj"), new RDFResource("ex:pred"), null, new RDFPlainLiteral("hello"));

            Assert.IsNotNull(result);
            Assert.IsFalse(result.ContainsQuadruple(quadruple));
        }

        [TestMethod]
        public void ShouldSelectQuadruplesBySubjectPredicate()
        {
            DropDatabase("RDFPostgreSQLStoreTest_ShouldSelectQuadruplesBySubjectPredicate");
            CreateDatabase("RDFPostgreSQLStoreTest_ShouldSelectQuadruplesBySubjectPredicate");

            RDFQuadruple quadruple = new RDFQuadruple(new RDFContext(new Uri("ex:ctx")), new RDFResource("ex:subj"), new RDFResource("ex:pred"), new RDFResource("ex:obj"));

            RDFPostgreSQLStore store = new RDFPostgreSQLStore(GetConnectionString("RDFPostgreSQLStoreTest_ShouldSelectQuadruplesBySubjectPredicate"));
            store.AddQuadruple(quadruple);
            RDFMemoryStore result = store.SelectQuadruples(null, new RDFResource("ex:subj"), new RDFResource("ex:pred"), null, null);

            Assert.IsNotNull(result);
            Assert.IsTrue(result.ContainsQuadruple(quadruple));
        }

        [TestMethod]
        public void ShouldNotSelectQuadruplesBySubjectPredicate()
        {
            DropDatabase("RDFPostgreSQLStoreTest_ShouldNotSelectQuadruplesBySubjectPredicate");
            CreateDatabase("RDFPostgreSQLStoreTest_ShouldNotSelectQuadruplesBySubjectPredicate");

            RDFQuadruple quadruple = new RDFQuadruple(new RDFContext(new Uri("ex:ctx")), new RDFResource("ex:subj"), new RDFResource("ex:pred"), new RDFResource("ex:obj"));

            RDFPostgreSQLStore store = new RDFPostgreSQLStore(GetConnectionString("RDFPostgreSQLStoreTest_ShouldNotSelectQuadruplesBySubjectPredicate"));
            store.AddQuadruple(quadruple);
            RDFMemoryStore result = store.SelectQuadruples(null, new RDFResource("ex:subj2"), new RDFResource("ex:pred"), null, null);

            Assert.IsNotNull(result);
            Assert.IsFalse(result.ContainsQuadruple(quadruple));
        }

        [TestMethod]
        public void ShouldSelectQuadruplesBySubjectObject()
        {
            DropDatabase("RDFPostgreSQLStoreTest_ShouldSelectQuadruplesBySubjectObject");
            CreateDatabase("RDFPostgreSQLStoreTest_ShouldSelectQuadruplesBySubjectObject");

            RDFQuadruple quadruple = new RDFQuadruple(new RDFContext(new Uri("ex:ctx")), new RDFResource("ex:subj"), new RDFResource("ex:pred"), new RDFResource("ex:obj"));

            RDFPostgreSQLStore store = new RDFPostgreSQLStore(GetConnectionString("RDFPostgreSQLStoreTest_ShouldSelectQuadruplesBySubjectObject"));
            store.AddQuadruple(quadruple);
            RDFMemoryStore result = store.SelectQuadruples(null, new RDFResource("ex:subj"), null, new RDFResource("ex:obj"), null);

            Assert.IsNotNull(result);
            Assert.IsTrue(result.ContainsQuadruple(quadruple));
        }

        [TestMethod]
        public void ShouldNotSelectQuadruplesBySubjectObject()
        {
            DropDatabase("RDFPostgreSQLStoreTest_ShouldNotSelectQuadruplesBySubjectObject");
            CreateDatabase("RDFPostgreSQLStoreTest_ShouldNotSelectQuadruplesBySubjectObject");

            RDFQuadruple quadruple = new RDFQuadruple(new RDFContext(new Uri("ex:ctx")), new RDFResource("ex:subj"), new RDFResource("ex:pred"), new RDFResource("ex:obj"));

            RDFPostgreSQLStore store = new RDFPostgreSQLStore(GetConnectionString("RDFPostgreSQLStoreTest_ShouldNotSelectQuadruplesBySubjectObject"));
            store.AddQuadruple(quadruple);
            RDFMemoryStore result = store.SelectQuadruples(null, new RDFResource("ex:subj2"), null, new RDFResource("ex:obj"), null);

            Assert.IsNotNull(result);
            Assert.IsFalse(result.ContainsQuadruple(quadruple));
        }

        [TestMethod]
        public void ShouldSelectQuadruplesBySubjectLiteral()
        {
            DropDatabase("RDFPostgreSQLStoreTest_ShouldSelectQuadruplesBySubjectLiteral");
            CreateDatabase("RDFPostgreSQLStoreTest_ShouldSelectQuadruplesBySubjectLiteral");

            RDFQuadruple quadruple = new RDFQuadruple(new RDFContext(new Uri("ex:ctx")), new RDFResource("ex:subj"), new RDFResource("ex:pred"), new RDFPlainLiteral("hello"));

            RDFPostgreSQLStore store = new RDFPostgreSQLStore(GetConnectionString("RDFPostgreSQLStoreTest_ShouldSelectQuadruplesBySubjectLiteral"));
            store.AddQuadruple(quadruple);
            RDFMemoryStore result = store.SelectQuadruples(null, new RDFResource("ex:subj"), null, null, new RDFPlainLiteral("hello"));

            Assert.IsNotNull(result);
            Assert.IsTrue(result.ContainsQuadruple(quadruple));
        }

        [TestMethod]
        public void ShouldNotSelectQuadruplesBySubjectLiteral()
        {
            DropDatabase("RDFPostgreSQLStoreTest_ShouldNotSelectQuadruplesBySubjectLiteral");
            CreateDatabase("RDFPostgreSQLStoreTest_ShouldNotSelectQuadruplesBySubjectLiteral");

            RDFQuadruple quadruple = new RDFQuadruple(new RDFContext(new Uri("ex:ctx")), new RDFResource("ex:subj"), new RDFResource("ex:pred"), new RDFPlainLiteral("hello"));

            RDFPostgreSQLStore store = new RDFPostgreSQLStore(GetConnectionString("RDFPostgreSQLStoreTest_ShouldNotSelectQuadruplesBySubjectLiteral"));
            store.AddQuadruple(quadruple);
            RDFMemoryStore result = store.SelectQuadruples(null, new RDFResource("ex:subj2"), null, null, new RDFPlainLiteral("hello"));

            Assert.IsNotNull(result);
            Assert.IsFalse(result.ContainsQuadruple(quadruple));
        }

        [TestMethod]
        public void ShouldSelectQuadruplesByPredicateObject()
        {
            DropDatabase("RDFPostgreSQLStoreTest_ShouldSelectQuadruplesByPredicateObject");
            CreateDatabase("RDFPostgreSQLStoreTest_ShouldSelectQuadruplesByPredicateObject");

            RDFQuadruple quadruple = new RDFQuadruple(new RDFContext(new Uri("ex:ctx")), new RDFResource("ex:subj"), new RDFResource("ex:pred"), new RDFResource("ex:obj"));

            RDFPostgreSQLStore store = new RDFPostgreSQLStore(GetConnectionString("RDFPostgreSQLStoreTest_ShouldSelectQuadruplesByPredicateObject"));
            store.AddQuadruple(quadruple);
            RDFMemoryStore result = store.SelectQuadruples(null, null, new RDFResource("ex:pred"), new RDFResource("ex:obj"), null);

            Assert.IsNotNull(result);
            Assert.IsTrue(result.ContainsQuadruple(quadruple));
        }

        [TestMethod]
        public void ShouldNotSelectQuadruplesByPredicateObject()
        {
            DropDatabase("RDFPostgreSQLStoreTest_ShouldNotSelectQuadruplesByPredicateObject");
            CreateDatabase("RDFPostgreSQLStoreTest_ShouldNotSelectQuadruplesByPredicateObject");

            RDFQuadruple quadruple = new RDFQuadruple(new RDFContext(new Uri("ex:ctx")), new RDFResource("ex:subj"), new RDFResource("ex:pred"), new RDFResource("ex:obj"));

            RDFPostgreSQLStore store = new RDFPostgreSQLStore(GetConnectionString("RDFPostgreSQLStoreTest_ShouldNotSelectQuadruplesByPredicateObject"));
            store.AddQuadruple(quadruple);
            RDFMemoryStore result = store.SelectQuadruples(null, null, new RDFResource("ex:pred2"), new RDFResource("ex:obj"), null);

            Assert.IsNotNull(result);
            Assert.IsFalse(result.ContainsQuadruple(quadruple));
        }

        [TestMethod]
        public void ShouldSelectQuadruplesByPredicateLiteral()
        {
            DropDatabase("RDFPostgreSQLStoreTest_ShouldSelectQuadruplesByPredicateLiteral");
            CreateDatabase("RDFPostgreSQLStoreTest_ShouldSelectQuadruplesByPredicateLiteral");

            RDFQuadruple quadruple = new RDFQuadruple(new RDFContext(new Uri("ex:ctx")), new RDFResource("ex:subj"), new RDFResource("ex:pred"), new RDFPlainLiteral("hello"));

            RDFPostgreSQLStore store = new RDFPostgreSQLStore(GetConnectionString("RDFPostgreSQLStoreTest_ShouldSelectQuadruplesByPredicateLiteral"));
            store.AddQuadruple(quadruple);
            RDFMemoryStore result = store.SelectQuadruples(null, null, new RDFResource("ex:pred"), null, new RDFPlainLiteral("hello"));

            Assert.IsNotNull(result);
            Assert.IsTrue(result.ContainsQuadruple(quadruple));
        }

        [TestMethod]
        public void ShouldNotSelectQuadruplesByPredicateLiteral()
        {
            DropDatabase("RDFPostgreSQLStoreTest_ShouldNotSelectQuadruplesByPredicateLiteral");
            CreateDatabase("RDFPostgreSQLStoreTest_ShouldNotSelectQuadruplesByPredicateLiteral");

            RDFQuadruple quadruple = new RDFQuadruple(new RDFContext(new Uri("ex:ctx")), new RDFResource("ex:subj"), new RDFResource("ex:pred"), new RDFPlainLiteral("hello"));

            RDFPostgreSQLStore store = new RDFPostgreSQLStore(GetConnectionString("RDFPostgreSQLStoreTest_ShouldNotSelectQuadruplesByPredicateLiteral"));
            store.AddQuadruple(quadruple);
            RDFMemoryStore result = store.SelectQuadruples(null, null, new RDFResource("ex:pred2"), null, new RDFPlainLiteral("hello"));

            Assert.IsNotNull(result);
            Assert.IsFalse(result.ContainsQuadruple(quadruple));
        }

        [TestMethod]
        public void ShouldSelectQuadruplesBySubjectPredicateObject()
        {
            DropDatabase("RDFPostgreSQLStoreTest_ShouldSelectQuadruplesBySubjectPredicateObject");
            CreateDatabase("RDFPostgreSQLStoreTest_ShouldSelectQuadruplesBySubjectPredicateObject");

            RDFQuadruple quadruple = new RDFQuadruple(new RDFContext(new Uri("ex:ctx")), new RDFResource("ex:subj"), new RDFResource("ex:pred"), new RDFResource("ex:obj"));

            RDFPostgreSQLStore store = new RDFPostgreSQLStore(GetConnectionString("RDFPostgreSQLStoreTest_ShouldSelectQuadruplesBySubjectPredicateObject"));
            store.AddQuadruple(quadruple);
            RDFMemoryStore result = store.SelectQuadruples(null, new RDFResource("ex:subj"), new RDFResource("ex:pred"), new RDFResource("ex:obj"), null);

            Assert.IsNotNull(result);
            Assert.IsTrue(result.ContainsQuadruple(quadruple));
        }

        [TestMethod]
        public void ShouldNotSelectQuadruplesBySubjectPredicateObject()
        {
            DropDatabase("RDFPostgreSQLStoreTest_ShouldNotSelectQuadruplesBySubjectPredicateObject");
            CreateDatabase("RDFPostgreSQLStoreTest_ShouldNotSelectQuadruplesBySubjectPredicateObject");

            RDFQuadruple quadruple = new RDFQuadruple(new RDFContext(new Uri("ex:ctx")), new RDFResource("ex:subj"), new RDFResource("ex:pred"), new RDFResource("ex:obj"));

            RDFPostgreSQLStore store = new RDFPostgreSQLStore(GetConnectionString("RDFPostgreSQLStoreTest_ShouldNotSelectQuadruplesBySubjectPredicateObject"));
            store.AddQuadruple(quadruple);
            RDFMemoryStore result = store.SelectQuadruples(null, new RDFResource("ex:subj2"), new RDFResource("ex:pred"), new RDFResource("ex:obj"), null);

            Assert.IsNotNull(result);
            Assert.IsFalse(result.ContainsQuadruple(quadruple));
        }

        [TestMethod]
        public void ShouldSelectQuadruplesBySubjectPredicateLiteral()
        {
            DropDatabase("RDFPostgreSQLStoreTest_ShouldSelectQuadruplesBySubjectPredicateLiteral");
            CreateDatabase("RDFPostgreSQLStoreTest_ShouldSelectQuadruplesBySubjectPredicateLiteral");

            RDFQuadruple quadruple = new RDFQuadruple(new RDFContext(new Uri("ex:ctx")), new RDFResource("ex:subj"), new RDFResource("ex:pred"), new RDFPlainLiteral("hello"));

            RDFPostgreSQLStore store = new RDFPostgreSQLStore(GetConnectionString("RDFPostgreSQLStoreTest_ShouldSelectQuadruplesBySubjectPredicateLiteral"));
            store.AddQuadruple(quadruple);
            RDFMemoryStore result = store.SelectQuadruples(null, new RDFResource("ex:subj"), new RDFResource("ex:pred"), null, new RDFPlainLiteral("hello"));

            Assert.IsNotNull(result);
            Assert.IsTrue(result.ContainsQuadruple(quadruple));
        }

        [TestMethod]
        public void ShouldNotSelectQuadruplesBySubjectPredicateLiteral()
        {
            DropDatabase("RDFPostgreSQLStoreTest_ShouldNotSelectQuadruplesBySubjectPredicateLiteral");
            CreateDatabase("RDFPostgreSQLStoreTest_ShouldNotSelectQuadruplesBySubjectPredicateLiteral");

            RDFQuadruple quadruple = new RDFQuadruple(new RDFContext(new Uri("ex:ctx")), new RDFResource("ex:subj"), new RDFResource("ex:pred"), new RDFPlainLiteral("hello"));

            RDFPostgreSQLStore store = new RDFPostgreSQLStore(GetConnectionString("RDFPostgreSQLStoreTest_ShouldNotSelectQuadruplesBySubjectPredicateLiteral"));
            store.AddQuadruple(quadruple);
            RDFMemoryStore result = store.SelectQuadruples(null, new RDFResource("ex:subj2"), new RDFResource("ex:pred"), null, new RDFPlainLiteral("hello"));

            Assert.IsNotNull(result);
            Assert.IsFalse(result.ContainsQuadruple(quadruple));
        }

        [TestMethod]
        public void ShouldOptimize()
        {
            DropDatabase("RDFPostgreSQLStoreTest_ShouldOptimize");
            CreateDatabase("RDFPostgreSQLStoreTest_ShouldOptimize");

            RDFQuadruple quadruple1 = new RDFQuadruple(new RDFContext(new Uri("ex:ctx1")), new RDFResource("ex:subj"), new RDFResource("ex:pred"), new RDFResource("ex:obj"));
            RDFQuadruple quadruple2 = new RDFQuadruple(new RDFContext(new Uri("ex:ctx1")), new RDFResource("ex:subj"), new RDFResource("ex:pred"), new RDFPlainLiteral("hello"));
            RDFQuadruple quadruple3 = new RDFQuadruple(new RDFContext(new Uri("ex:ctx2")), new RDFResource("ex:subj"), new RDFResource("ex:pred"), new RDFPlainLiteral("hello"));
            RDFQuadruple quadruple4 = new RDFQuadruple(new RDFContext(new Uri("ex:ctx3")), new RDFResource("ex:subj"), new RDFResource("ex:pred"), new RDFPlainLiteral("hello"));
            RDFQuadruple quadruple5 = new RDFQuadruple(new RDFContext(new Uri("ex:ctx4")), new RDFResource("ex:subj"), new RDFResource("ex:pred"), new RDFResource("ex:obj"));
            RDFQuadruple quadruple6 = new RDFQuadruple(new RDFContext(new Uri("ex:ctx4")), new RDFResource("ex:subj4"), new RDFResource("ex:pred4"), new RDFResource("ex:obj"));

            RDFPostgreSQLStore store = new RDFPostgreSQLStore(GetConnectionString("RDFPostgreSQLStoreTest_ShouldOptimize"));
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