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

namespace RDFSharp.Extensions.Firebird
{
    /// <summary>
    /// RDFFirebirdStoreEnums represents a collector for all the enumerations used by RDFSharp.Extensions.Firebird namespace
    /// </summary>
    public static class RDFFirebirdStoreEnums
    {
        /// <summary>
        /// RDFFirebirdVersion represents an enumeration for supported versions of new Firebird databases
        /// </summary>
        public enum RDFFirebirdVersion
        {
            /// <summary>
            /// New databases will be created by cloning internal Firebird3 template
            /// </summary>
            Firebird3 = 3,
            /// <summary>
            /// New databases will be created by cloning internal Firebird4 template
            /// </summary>
            Firebird4 = 4
        }
    }
}