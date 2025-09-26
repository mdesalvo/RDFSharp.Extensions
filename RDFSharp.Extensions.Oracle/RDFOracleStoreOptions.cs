/*
   Copyright 2012-2025 Marco De Salvo

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

namespace RDFSharp.Extensions.Oracle
{
    /// <summary>
    /// RDFOracleStoreOptions is a collector of options for customizing the default behaviour of an Oracle store
    /// </summary>
    public class RDFOracleStoreOptions
    {
        #region Properties
        /// <summary>
        /// Timeout in seconds for SELECT queries executed on the Oracle store (default: 120)
        /// </summary>
        public int SelectTimeout { get; set; } = 120;

        /// <summary>
        /// Timeout in seconds for DELETE queries executed on the Oracle store (default: 120)
        /// </summary>
        public int DeleteTimeout { get; set; } = 120;

        /// <summary>
        /// Timeout in seconds for INSERT queries executed on the Oracle store (default: 120)
        /// </summary>
        public int InsertTimeout { get; set; } = 120;
        #endregion
    }
}