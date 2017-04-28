﻿//
//  IndexType.cs
//
//  Author:
//  	Jim Borden  <jim.borden@couchbase.com>
//
//  Copyright (c) 2017 Couchbase, Inc All rights reserved.
//
//  Licensed under the Apache License, Version 2.0 (the "License");
//  you may not use this file except in compliance with the License.
//  You may obtain a copy of the License at
//
//  http://www.apache.org/licenses/LICENSE-2.0
//
//  Unless required by applicable law or agreed to in writing, software
//  distributed under the License is distributed on an "AS IS" BASIS,
//  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//  See the License for the specific language governing permissions and
//  limitations under the License.
//

using LiteCore.Interop;

namespace Couchbase.Lite
{
    /// <summary>
    /// A type setting for creating an index in a database
    /// </summary>
    public enum IndexType : uint
    {
        /// <summary>
        /// Creates an index on simple property values
        /// </summary>
        ValueIndex = C4IndexType.ValueIndex,

        /// <summary>
        /// Creates a full text index
        /// </summary>
        FullTextIndex = C4IndexType.FullTextIndex,

        // Not being included?
        //GeoIndex = C4IndexType.GeoIndex
    }
}
