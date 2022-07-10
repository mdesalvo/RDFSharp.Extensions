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
using System.Data;
using System.IO;
using System.Linq;
using System.Net;
using System.Text;
using System.Text.RegularExpressions;
using WireMock.RequestBuilders;
using WireMock.ResponseBuilders;
using WireMock.Server;
using RDFSharp.Model;
using RDFSharp.Query;
using RDFSharp.Store;

namespace RDFSharp.Store.Test
{
    [TestClass]
    public class RDFSQLiteStoreTest
    {
        private WireMockServer server;

        [TestInitialize]
        public void Initialize() { server = WireMockServer.Start(); }

        [TestCleanup]
        public void Cleanup()
        { 
            server.Stop(); 
            server.Dispose();

            foreach (string file in Directory.EnumerateFiles(Environment.CurrentDirectory, "RDFSQLiteStoreTest_Should*"))
                File.Delete(file);
        }

        #region Tests
        [TestMethod]
        public void ShouldCreateStore()
        {
            RDFSQLiteStore store = new RDFSQLiteStore(Path.Combine(Environment.CurrentDirectory, "RDFSQLiteStoreTest_ShouldCreateStore.db"));

            Assert.IsNotNull(store);
            Assert.IsTrue(string.Equals(store.StoreType, "SQLITE"));
            Assert.IsTrue(store.StoreID.Equals(RDFModelUtilities.CreateHash(store.ToString())));
            Assert.IsTrue(string.Equals(store.ToString(), string.Concat("SQLITE|SERVER=", store.Connection.DataSource, ";DATABASE=", store.Connection.Database)));
            Assert.IsTrue(File.Exists(Path.Combine(Environment.CurrentDirectory, "RDFSQLiteStoreTest_ShouldCreateStore.db")));
        }

        [TestMethod]
        public void ShouldThrowExceptionOnCreatingStoreBecauseNullOrEmptyPath()
            => Assert.ThrowsException<RDFStoreException>(() => new RDFSQLiteStore(null));

        [TestMethod]
        public void ShouldThrowExceptionOnCreatingStoreBecauseUnaccessiblePath()
            => Assert.ThrowsException<RDFStoreException>(() => new RDFSQLiteStore("http://example.org/file.db"));
        #endregion
    }
}