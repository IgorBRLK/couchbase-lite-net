// 
// Database.cs
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
using System.Collections;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

using Couchbase.Lite.Logging;
using Couchbase.Lite.Query;
using Couchbase.Lite.Serialization;
using Couchbase.Lite.Support;
using Couchbase.Lite.Sync;
using Couchbase.Lite.Util;
using LiteCore;
using LiteCore.Interop;
using LiteCore.Util;
using Newtonsoft.Json;
using ObjCRuntime;

namespace Couchbase.Lite
{
    public sealed unsafe class Database : IDisposable
    {
        #region Constants

        private static readonly C4DatabaseConfig _DBConfig = new C4DatabaseConfig {
            flags = C4DatabaseFlags.Create | C4DatabaseFlags.AutoCompact | C4DatabaseFlags.Bundled | C4DatabaseFlags.SharedKeys,
            storageEngine = "SQLite",
            versioning = C4DocumentVersioning.RevisionTrees
        };

        private static readonly DatabaseObserverCallback _DbObserverCallback;

        // ReSharper disable PrivateFieldCanBeConvertedToLocalVariable
        private static readonly C4LogCallback _LogCallback;
        // ReSharper restore PrivateFieldCanBeConvertedToLocalVariable

        private const string Tag = nameof(Database);

        #endregion

        #region Variables

        private readonly SharedStringCache _sharedStrings;
        private readonly ThreadSafety _threadSafety = new ThreadSafety();
        private readonly HashSet<Document> _unsavedDocuments = new HashSet<Document>();

        /// <summary>
        /// An event fired whenever the database changes
        /// </summary>
        public event EventHandler<DatabaseChangedEventArgs> Changed;


        private IConflictResolver _conflictResolver;
        private IJsonSerializer _jsonSerializer;
        private DatabaseObserver _obs;
        private long p_c4db;

        #endregion

        #region Properties

        /// <summary>
        /// Gets or sets the conflict resolver to use when conflicts arise
        /// </summary>
        public IConflictResolver ConflictResolver
        {
            get => _threadSafety.DoLocked(() => _conflictResolver);
            set => _threadSafety.DoLocked(() => _conflictResolver = value);
        }

        /// <summary>
        /// Bracket operator for retrieving <see cref="Document"/>s
        /// </summary>
        /// <param name="id">The ID of the <see cref="Document"/> to retrieve</param>
        /// <returns>The instantiated <see cref="Document"/></returns>
        public Document this[string id] => GetDocument(id);

        /// <summary>
        /// Gets the name of the database
        /// </summary>
        public string Name { get; }

        /// <summary>
        /// Gets the options that were used to create the database
        /// </summary>
        public DatabaseOptions Options { get; }

        /// <summary>
        /// Gets the path on disk where the database exists
        /// </summary>
        public string Path
        {
            get {
                CheckOpen();
                return Native.c4db_getPath(c4db);
            }
        }

        internal ICollection<IReplication> ActiveReplications { get; } = new HashSet<IReplication>();

        internal C4BlobStore* BlobStore
        {
            get {
                CheckOpen();
                C4BlobStore* retVal = null;
                _threadSafety.DoLocked(() => retVal = (C4BlobStore *)LiteCoreBridge.Check(err => Native.c4db_getBlobStore(c4db, err)));
                return retVal;
            }
        }

        internal C4Database* c4db
        {
            get {
                C4Database* retVal = null;
                _threadSafety.DoLocked(() => retVal = _c4db);
                return retVal;
            }
        }

        internal IJsonSerializer JsonSerializer
        {
            get => _jsonSerializer ?? (_jsonSerializer = Serializer.CreateDefaultFor(this));
            set => _jsonSerializer = value;
        }

        internal IDictionary<Uri, IReplication> Replications { get; } = new Dictionary<Uri, IReplication>();

        internal SharedStringCache SharedStrings => _threadSafety.DoLocked(() => _sharedStrings);

        private C4Database *_c4db
        {
            get => (C4Database *)p_c4db;
            set => p_c4db = (long)value;
        }

        #endregion

        #region Constructors

        static Database()
        {
            _LogCallback = LiteCoreLog;
            Native.c4log_register(C4LogLevel.Warning, _LogCallback);
            _DbObserverCallback = DbObserverCallback;
        }

        /// <summary>
        /// Creates a new Database with the given name and default options
        /// </summary>
        /// <param name="name">The name for the new database object</param>
        public Database(string name) : this(name, DatabaseOptions.Default)
        {
            
        }

        /// <summary>
        /// Creates a new database with the given name and options
        /// </summary>
        /// <param name="name">The name for the new database object</param>
        /// <param name="options">The options for the new database object</param>
        public Database(string name, DatabaseOptions options) 
        {
            Name = name ?? throw new ArgumentNullException(nameof(name));
            Options = options;
            Open();
            _sharedStrings = new SharedStringCache(_c4db);
        }

        ~Database()
        {
            Dispose(false);
        }

        #endregion

        #region Public Methods

        /// <summary>
        /// Deletes the underlying database file for a database matching
        /// the given name in the given directory
        /// </summary>
        /// <param name="name">The name of the database to delete the file for</param>
        /// <param name="directory">The directory to search for the database in</param>
        public static void Delete(string name, string directory)
        {
            if(name == null) {
                throw new ArgumentNullException(nameof(name));
            }

            var path = DatabasePath(name, directory);
            LiteCoreBridge.Check(err =>
            {
                var localConfig = _DBConfig;
                return Native.c4db_deleteAtPath(path, &localConfig, err) || err->code == 0;
            });
        }

        /// <summary>
        /// Checks if a database with the given name exists in the given
        /// directory
        /// </summary>
        /// <param name="name">The name of the database to search for</param>
        /// <param name="directory">The directory to search for the database in</param>
        /// <returns><c>true</c> if the database exists, <c>false</c> otherwise</returns>
        public static bool Exists(string name, string directory)
        {
            if(name == null) {
                throw new ArgumentNullException(nameof(name));
            }

            return File.Exists(DatabasePath(name, directory));
        }

        //public void ChangeEncryptionKey(object key)
        //{
        //    throw new NotImplementedException();
        //}

        /// <summary>
        /// Closes the database
        /// </summary>
        public void Close()
        {
            Dispose();
        }

        /// <summary>
        /// Creates a new <see cref="Document"/> with a unique ID
        /// </summary>
        /// <returns>The created document</returns>
        public Document CreateDocument()
        {
            
            return _threadSafety.DoLocked(() => GetDocument(Misc.CreateGuid(), false));
        }

        /// <summary>
        /// Creates an index of the given type on the given path with the given options
        /// </summary>
        /// <param name="expressions">The expressions to create the index on (must be either string
        /// or IExpression)</param>
        /// <param name="indexType">The type of index to create</param>
        /// <param name="options">The options to apply to the index</param>
        public void CreateIndex(IList expressions, IndexType indexType, IndexOptions options)
        {
            _threadSafety.DoLocked(() =>
            {
                CheckOpen();
                var jsonObj = QueryExpression.EncodeToJSON(expressions);
                var json = JsonConvert.SerializeObject(jsonObj);
                LiteCoreBridge.Check(err =>
                {
                    if (options == null) {
                        return Native.c4db_createIndex(c4db, json, (C4IndexType) indexType, null, err);
                    } else {
                        var localOpts = IndexOptions.Internal(options);
                        return Native.c4db_createIndex(c4db, json, (C4IndexType) indexType, &localOpts, err);
                    }
                });
            });
        }

        /// <summary>
        /// Creates an <see cref="IndexType.ValueIndex"/> index on the given path
        /// </summary>
        /// <param name="expressions">The expressions to create the index on</param>
        public void CreateIndex(IList<IExpression> expressions)
        {
            _threadSafety.DoLocked(() =>
            {
                CheckOpen();
                CreateIndex(expressions as IList, IndexType.ValueIndex, null);
            });
        }

        /// <summary>
        /// Creates an <see cref="IReplication"/> object to or from the database
        /// to a given remote endpoint
        /// </summary>
        /// <param name="remoteUrl">The remote endpoint to replicate with</param>
        /// <returns>The instantiated <see cref="IReplication"/> object</returns>
        public IReplication CreateReplication(Uri remoteUrl)
        {
            if (remoteUrl == null) {
                throw new ArgumentNullException(nameof(remoteUrl));
            }

            var repl = Replications.Get(remoteUrl);
            if (repl == null) {
                repl = new Replication(this, remoteUrl, null);
                Replications[remoteUrl] = repl;
            }

            return repl;
        }

        /// <summary>
        /// Creates an <see cref="IReplication"/> object to or from the database
        /// to another local database
        /// </summary>
        /// <param name="otherDatabase">The other database to replicate with</param>
        /// <returns>The instantiated <see cref="IReplication"/> object</returns>
        public IReplication CreateReplication(Database otherDatabase)
        {
            if (otherDatabase == null) {
                throw new ArgumentNullException(nameof(otherDatabase));
            }

            if (otherDatabase == this) {
                throw new InvalidOperationException("Source and target database are the same");
            }

            var key = new Uri(otherDatabase.Path);
            var repl = Replications.Get(key);
            if (repl == null) {
                repl = new Replication(this, null, otherDatabase);
                Replications[key] = repl;
            }

            return repl;
        }

        /// <summary>
        /// Deletes the database
        /// </summary>
        public void Delete()
        {
            _threadSafety.DoLocked(() =>
            {
                CheckOpen();
                var old = (C4Database*) Interlocked.Exchange(ref p_c4db, 0);
                if (old == null) {
                    throw new InvalidOperationException("Attempt to perform an operation on a closed database");
                }

                LiteCoreBridge.Check(err => Native.c4db_delete(old, err));
                Native.c4db_free(old);
                _obs?.Dispose();
                _obs = null;
            });
        }

        /// <summary>
        /// Deletes an index of the given <see cref="IndexType"/> on the given propertyPath
        /// </summary>
        /// <param name="propertyPath">The path of the index to delete</param>
        /// <param name="type">The type of the index to delete</param>
        public void DeleteIndex(string propertyPath, IndexType type)
        {
            _threadSafety.DoLocked(() =>
            {
                CheckOpen();
                LiteCoreBridge.Check(err => Native.c4db_deleteIndex(c4db, propertyPath, (C4IndexType) type, err));
            });
        }

        /// <summary>
        /// Checks if the <see cref="Document"/> with the given ID exists in the database
        /// </summary>
        /// <param name="documentID">The ID to check</param>
        /// <returns><c>true</c> if the document exists, <c>false</c> otherwise</returns>
        public bool DocumentExists(string documentID)
        {
            return _threadSafety.DoLocked(() =>
            {
                CheckOpen();
                if (documentID == null) {
                    throw new ArgumentNullException(nameof(documentID));
                }

                var check = (C4Document*) RetryHandler.RetryIfBusy()
                    .AllowError((int) LiteCoreError.NotFound, C4ErrorDomain.LiteCoreDomain)
                    .Execute(err => Native.c4doc_get(c4db, documentID, true, err));
                var exists = check != null;
                Native.c4doc_free(check);
                return exists;
            });
        }

        /// <summary>
        /// Gets or creates an <see cref="Document"/> with the specified ID
        /// </summary>
        /// <param name="id">The ID to use when creating or getting the document</param>
        /// <returns>The instantiated <see cref="Document" /></returns>
        public Document GetDocument(string id)
        {
            return _threadSafety.DoLocked(() => GetDocument(id, false));
        }

        /// <summary>
        /// Runs the given batch of operations as an atomic unit
        /// </summary>
        /// <param name="a">The <see cref="Action" /> containing the operations.  
        public void InBatch(Action a)
        {
            _threadSafety.DoLocked(() =>
            {
                CheckOpen();
                PerfTimer.StartEvent("InBatch_BeginTransaction");
                LiteCoreBridge.Check(err => Native.c4db_beginTransaction(_c4db, err));
                PerfTimer.StopEvent("InBatch_BeginTransaction");
                var success = true;
                try {
                    a();
                } catch (Exception e) {
                    Log.To.Database.W(Tag, "Exception during InBatch, rolling back...", e);
                    success = false;
                    throw;
                } finally {
                    PerfTimer.StartEvent("InBatch_EndTransaction");
                    LiteCoreBridge.Check(err => Native.c4db_endTransaction(_c4db, success, err));
                    PerfTimer.StopEvent("InBatch_EndTransaction");
                }

                PostDatabaseChanged();
            });
        }

        #endregion

        #region Internal Methods

        internal void SetHasUnsavedChanges(Document doc, bool hasChanges)
        {
            if (hasChanges) {
                _unsavedDocuments.Add(doc);
            } else {
                _unsavedDocuments.Remove(doc);
            }
        }

        #endregion

        #region Private Methods

        private static string DatabasePath(string name, string directory)
        {
            return System.IO.Path.Combine(Directory(directory), name);
        }

        private static void DbObserverCallback(C4DatabaseObserver* db, object context)
        {
            var dbObj = (Database)context;
            dbObj?.PostDatabaseChanged();
        }

        private static string DefaultDirectory()
        {
            return InjectableCollection.GetImplementation<IDefaultDirectoryResolver>().DefaultDirectory();
        }

        private static string Directory(string directory)
        {
            return directory ?? DefaultDirectory();
        }

        [MonoPInvokeCallback(typeof(C4LogCallback))]
        private static void LiteCoreLog(C4LogDomain* domain, C4LogLevel level, C4Slice msg)
        {
            var name = Native.c4log_getDomainName(domain);
            switch(level) {
                case C4LogLevel.Error:
                    Log.To.DomainOrLiteCore(name).E(name, msg.CreateString());
                    break;
                case C4LogLevel.Warning:
                    Log.To.DomainOrLiteCore(name).W(name, msg.CreateString());
                    break;
                case C4LogLevel.Info:
                    Log.To.DomainOrLiteCore(name).I(name, msg.CreateString());
                    break;
                case C4LogLevel.Verbose:
                    Log.To.DomainOrLiteCore(name).V(name, msg.CreateString());
                    break;
                case C4LogLevel.Debug:
                    Log.To.DomainOrLiteCore(name).D(name, msg.CreateString());
                    break;
            }
        }

        private void CheckOpen()
        {
            if(_c4db == null) {
                throw new InvalidOperationException("Attempt to perform an operation on a closed database");
            }
        }

        private void Dispose(bool disposing)
        {
            Debug.WriteLine("DISPOSE");
            if(disposing) {
                var obs = Interlocked.Exchange(ref _obs, null);
                obs?.Dispose();
                if(_unsavedDocuments.Count > 0) {
                    Log.To.Database.W(Tag,
                        $"Closing database with {_unsavedDocuments.Count} such as {_unsavedDocuments.Any()}");
                }
                _unsavedDocuments.Clear();
            }

            var old = (C4Database *)Interlocked.Exchange(ref p_c4db, 0);
            if(old != null) {
                Log.To.Database.I(Tag, $"Closing database at path {Native.c4db_getPath(old)}");
                LiteCoreBridge.Check(err => Native.c4db_close(old, err));
                Native.c4db_free(old);
            }
        }

        private Document GetDocument(string docID, bool mustExist)
        {
            CheckOpen();
            var doc = new Document(this, docID, mustExist);

            if (mustExist && !doc.Exists) {
                Log.To.Database.V(Tag, "Requested existing document {0}, but it doesn't exist", 
                    new SecureLogString(docID, LogMessageSensitivity.PotentiallyInsecure));
                return null;
            }

            return doc;
        }

        private void Open()
        {
            if(_c4db != null) {
                return;
            }

            Debug.WriteLine("OPEN!");
            System.IO.Directory.CreateDirectory(Directory(Options.Directory));
            var path = DatabasePath(Name, Options.Directory);
            var config = _DBConfig;
            if(Options.ReadOnly) {
                config.flags |= C4DatabaseFlags.ReadOnly;
            }

            var encrypted = "";
            if(Options.EncryptionKey != null) {
#if true
                throw new NotImplementedException("Encryption is not yet supported");
#else
                var key = Options.EncryptionKey;
                int i = 0;
                config.encryptionKey.algorithm = C4EncryptionAlgorithm.AES256;
                foreach(var b in key.KeyData) {
                    config.encryptionKey.bytes[i++] = b;
                }

                encrypted = "encrypted ";
#endif
            }

            Log.To.Database.I(Tag, $"Opening {encrypted}database at {path}");
            var localConfig1 = config;
            _c4db = (C4Database *)LiteCoreBridge.Check(err => {
                var localConfig2 = localConfig1;
                return Native.c4db_open(path, &localConfig2, err);
            });

            _obs = Native.c4dbobs_create(_c4db, _DbObserverCallback, this);
        }

        private void PostDatabaseChanged()
        {
            if(_obs == null || _c4db == null || Native.c4db_isInTransaction(_c4db)) {
                return;
            }

            const uint maxChanges = 100u;
            var external = false;
            uint nChanges;
            var changes = new C4DatabaseChange[maxChanges];
            var docIDs = new List<string>();
            do {
                // Read changes in batches of MaxChanges:
                bool newExternal;
                var lastSequence = 0UL;
                nChanges = Native.c4dbobs_getChanges(_obs.Observer, changes, maxChanges, &newExternal);
                if(nChanges == 0 || external != newExternal || docIDs.Count > 1000) {
                    if(docIDs.Count > 0) {
                        // Only notify if there are actually changes to send
                        var args = new DatabaseChangedEventArgs(docIDs.ToArray(), lastSequence, external);
                        Changed?.Invoke(this, args);
                        docIDs.Clear();
                    }
                }

                external = newExternal;
                for(int i = 0; i < nChanges; i++) {
                    var docID = changes[i].docID.CreateString();
                    docIDs.Add(docID);
                }

                if(nChanges > 0) {
                    lastSequence = changes.Last().sequence;
                }
            } while(nChanges > 0);
        }

        #endregion

        #region IDisposable

        public void Dispose()
        {
            Dispose(true);
            GC.SuppressFinalize(this);
        }

        #endregion
    }
}
