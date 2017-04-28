// 
// Subdocument.cs
// 
// Author:
//     Jim Borden  <jim.borden@couchbase.com>
// 
// Copyright (c) 2017 Couchbase, Inc All rights reserved.
// 
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
// 
// http://www.apache.org/licenses/LICENSE-2.0
// 
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
// 
using System;
using System.Collections.Generic;

using Couchbase.Lite.Serialization;

namespace Couchbase.Lite
{
    public sealed class Subdocument : PropertyContainer
    {
        #region Variables

        private Action _onMutate;

        #endregion

        #region Properties

        /// <summary>
        /// Gets the parent document for this subdocument
        /// </summary>
        public Document Document
        {
            get {
                var p = Parent;
                return (p is Document) ? (Document)p : (Document)((Subdocument)p)?.Parent;
            }
        }

        /// <summary>
        /// Gets whether or not this subdocument has been saved into a document yet
        /// </summary>
        public bool Exists => _threadSafety.DoLocked(HasRoot);

        internal override bool HasChanges
        {
            get => base.HasChanges;
            set {
                if (base.HasChanges == value) {
                    return;
                }

                _threadSafety.DoLocked(() =>
                {
                    base.HasChanges = value;
                    _onMutate?.Invoke();
                });
            }
        }

        internal string Key { get; set; }

        internal PropertyContainer Parent { get; set; }

        #endregion

        #region Constructors

        /// <summary>
        /// Creates a new blank <see cref="Subdocument"/>
        /// </summary>
        /// <returns>A constructed <see cref="Subdocument"/> object</returns>
        public Subdocument()
            : base(new SharedStringCache())
        {
            
        }

        internal Subdocument(PropertyContainer parent, SharedStringCache sharedKeys)
            : base(sharedKeys)
        {
            Parent = parent;
        }

        #endregion

        #region Internal Methods

        internal unsafe void Invalidate()
        {
            Parent = null;
            SetOnMutate(null);
            SetRootDict(null);
            Properties = null;
            ResetChangesKeys();
        }

        internal void SetOnMutate(Action onMutate)
        {
            _onMutate = onMutate;
        }

        #endregion

        #region Overrides

        protected internal override Blob CreateBlob(IDictionary<string, object> properties)
        {
            var doc = Document;
            if (doc != null) {
                return new Blob(doc.Database as Database, properties);
            }

            throw new InvalidOperationException("Cannot read blob inside a subdocument not attached to a document");
        }

        #endregion
    }
}

