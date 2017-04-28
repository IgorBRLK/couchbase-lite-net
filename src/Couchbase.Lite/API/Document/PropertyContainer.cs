// 
// PropertyContainer.cs
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
using System.Globalization;
using System.Linq;
using System.Reflection;

using Couchbase.Lite.Serialization;
using Couchbase.Lite.Support;
using Couchbase.Lite.Util;
using LiteCore.Interop;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;

namespace Couchbase.Lite
{
    internal sealed class DictionaryObjectConverter : JsonConverter
    {
        public override bool CanRead => false;

        public override bool CanWrite => true;

        public override void WriteJson(JsonWriter writer, object value, JsonSerializer serializer)
        {
            var dict = (PropertyContainer)value;
            writer.WriteStartObject();
            foreach (var pair in dict.Properties) {
                writer.WritePropertyName(pair.Key);
                serializer.Serialize(writer, pair.Value);
            }
            writer.WriteEndObject();
        }

        public override object ReadJson(JsonReader reader, Type objectType, object existingValue, JsonSerializer serializer)
        {
            throw new NotImplementedException();
        }

        public override bool CanConvert(Type objectType)
        {
            return typeof(PropertyContainer).GetTypeInfo().IsAssignableFrom(objectType.GetTypeInfo());
        }
    }

    [JsonConverter(typeof(DictionaryObjectConverter))]
    public abstract unsafe class PropertyContainer
    {
        #region Constants

        private static readonly TypeInfo[] _ValidTypes = new[] {
            typeof(string).GetTypeInfo(),
            typeof(DateTime).GetTypeInfo(),
            typeof(DateTimeOffset).GetTypeInfo(),
            typeof(decimal).GetTypeInfo()
        };

        #endregion

        #region Variables

        protected Dictionary<string, object> _properties;
        internal SharedStringCache _sharedKeys;

        private HashSet<string> _changesKeys;
        private bool _hasChanges;
        private FLDict* _root;
        internal readonly ThreadSafety _threadSafety = new ThreadSafety();

        #endregion

        #region Properties

        public object this[string key]
        {
            get => Get(key);
            set => Set(key, value);
        }

        public IDictionary<string, object> Properties
        {
            get {
                return _threadSafety.DoLocked(() =>
                {
                    if (!HasChanges) {
                        if (_properties == null) {
                            var saved = SavedProperties;
                            if (saved?.Count > 0) {
                                _properties = new Dictionary<string, object>();
                                foreach (var pair in saved) {
                                    _properties[pair.Key] = pair.Value;
                                }
                            }

                        } else if (_root != null) {
                            LoadRootIntoProperties();
                        }
                    }

                    return _properties?.Keys.Where(x => _properties[x] != null).ToDictionary(x => x, x => _properties[x]);
                });
            }
            set {
                _threadSafety.DoLocked(() =>
                {
                    if (_properties == value) {
                        return;
                    }

                    // Convert each property value if needed, build up changesKeys set, and invalidate
                    // obsolete subdocuments
                    var changesKeys = new HashSet<string>();
                    var result = value != null ? new Dictionary<string, object>(value) : null;

                    if (value?.Count > 0) {
                        foreach (var pair in value) {
                            ValidateObjectType(pair.Value);
                            result[pair.Key] = ConvertValue(pair.Value, Properties?.Get(pair.Key), pair.Key);
                            changesKeys.Add(pair.Key);
                        }
                    }

                    // Invalidate obsolete subdocuments from the current _properties:
                    var oldKeys = _properties?.Keys;
                    if (oldKeys != null) {
                        var removedKeys = oldKeys.Except(changesKeys);
                        foreach (var key in removedKeys) {
                            InvalidateIfSubdocument(_properties[key]);
                        }
                    }

                    // Add keys from _root that do not exist in the changedKeys (deleting):
                    if (_root != null) {
                        FLDictIterator iter;
                        Native.FLDictIterator_Begin(_root, &iter);
                        string key;
                        while (null != (key = SharedKeys.GetDictIterKey(&iter))) {
                            changesKeys.Add(key);
                            Native.FLDictIterator_Next(&iter);
                        }
                    }

                    // Update _properties:
                    _properties = result;

                    // Mark changes:
                    _changesKeys = changesKeys;
                    HasChanges = true;
                });
            }
        }

        protected IReadOnlyDictionary<string, object> SavedProperties
        {
            get {
                return _threadSafety.DoLocked(() =>
                {
                    if (_properties != null && !HasChanges) {
                        LoadRootIntoProperties();
                        return _properties;
                    }

                    return FLValueConverter.ToObject((FLValue*)_root, SharedKeys) as IReadOnlyDictionary<string, object>;
                });
            }
        }

        internal virtual bool HasChanges
        {
            get => _threadSafety.DoLocked(() => _hasChanges);
            set => _threadSafety.DoLocked(() => _hasChanges = value);
        }

        internal SharedStringCache SharedKeys
        {
            get => _threadSafety.DoLocked(() => _sharedKeys);
            set => _threadSafety.DoLocked(() => _sharedKeys = value);
        }

        #endregion

        #region Constructors

        internal PropertyContainer(SharedStringCache sharedKeys)
        {
            _sharedKeys = sharedKeys;
        }

        #endregion

        #region Public Methods

        public bool Contains(string key)
        {
            return _threadSafety.DoLocked(() =>
            {
                if (_properties != null) {
                    return _properties.ContainsKey(key);
                }

                return FleeceValueForKey(key) != null;
            });
        }

        public object Get(string key)
        {
            return _threadSafety.DoLocked(() =>
            {
                var obj = ((IDictionary<string, object>)_properties)?.Get(key);
                if (obj != null || HasChanges) {
                    return obj;
                }

                obj = FleeceValueToObject(FleeceValueForKey(key), key);
                CacheValue(obj, key, false);
                return obj;
            });
        }

        public IList<object> GetArray(string key)
        {
            return Get(key) as IList<object>;
        }

        public Blob GetBlob(string key)
        {
            return Get(key) as Blob;
        }

        public bool GetBoolean(string key)
        {
            return _threadSafety.DoLocked(() =>
            {
                var val = Get(key);
                if (val == null || HasChanges) {
                    if (val == null) {
                        return false;
                    }

                    return val is bool ? (bool)val : true;
                }

                return Native.FLValue_AsBool(FleeceValueForKey(key));
            });
        }

        public DateTimeOffset? GetDate(string key)
        {
            return _threadSafety.DoLocked<DateTimeOffset?>(() =>
            {
                DateTimeOffset retVal;
                if (TryGet(key, out retVal)) {
                    return retVal;
                }

                var dateString = GetString(key);
                if (dateString == null) {
                    return null;
                }

                return DateTimeOffset.ParseExact(dateString, "o", CultureInfo.InvariantCulture, DateTimeStyles.None);
            });
        }

        public double GetDouble(string key)
        {
            return _threadSafety.DoLocked(() =>
            {
                double retVal;
                return TryGet(key, out retVal) ? retVal : Native.FLValue_AsDouble(FleeceValueForKey(key));
            });
        }

        public float GetFloat(string key)
        {
            return _threadSafety.DoLocked(() =>
            {
                float retVal;
                return TryGet(key, out retVal) ? retVal : Native.FLValue_AsFloat(FleeceValueForKey(key));
            });
        }

        public long GetLong(string key)
        {
            return _threadSafety.DoLocked(() =>
            {
                long retVal;
                return TryGet(key, out retVal) ? retVal : Native.FLValue_AsInt(FleeceValueForKey(key));
            });
        }

        public string GetString(string key)
        {
            return Get(key) as string;
        }

        public Subdocument GetSubdocument(string key)
        {
            return Get(key) as Subdocument;
        }

        public PropertyContainer Remove(string key)
        {
            Set(key, null);
            return this;
        }

        public void Revert()
        {
            _threadSafety.DoLocked(() =>
            {
                if (_changesKeys == null) {
                    return;
                }

                foreach (var key in _changesKeys) {
                    IDictionary<string, object> properties = _properties;
                    var value = properties.Get(key);
                    var subdoc = value as Subdocument;
                    var arr = value as IList;
                    if (subdoc != null) {
                        if (subdoc.HasRoot()) {
                            subdoc.Revert();
                            continue; // Keep the subdocument value
                        }

                        // Invalidate the subdocument set to the properties
                        subdoc.Invalidate();
                    } else if (arr != null) {
                        foreach (var v in arr) {
                            (v as Subdocument)?.Invalidate();
                        }
                    }

                    _properties.Remove(key);
                }

                _changesKeys.Clear();
                HasChanges = false;
            });
        }

        public PropertyContainer Set(string key, object value)
        {
            _threadSafety.DoLocked(() =>
            {
                ValidateObjectType(value);
                var oldValue = Properties?.Get(key);
                if (value?.Equals(oldValue) == true) {
                    return;
                }

                value = ConvertValue(value, oldValue, key);
                MutateProperties();
                CacheValue(value, key, true);
                MarkChangedKey(key);
            });

            return this;
        }

        #endregion

        #region Protected Internal Methods

        protected internal abstract Blob CreateBlob(IDictionary<string, object> properties);

        #endregion

        #region Protected Methods

        protected bool HasRoot()
        {
            return _root != null;
        }

        internal void SetRootDict(FLDict* root)
        {
            _root = root;
            _sharedKeys.UseDocumentRoot(root);
        }

        #endregion

        #region Internal Methods

        internal virtual void MarkChangedKey(string key)
        {
            if(_changesKeys?.Contains(key) == true) {
                return;
            }

            if(_changesKeys == null) {
                _changesKeys = new HashSet<string>();
            }

            _changesKeys.Add(key);
            if (!_hasChanges) {
                HasChanges = true;
            }
        }

        internal void ResetChangesKeys()
        {
            _threadSafety.DoLocked(() =>
            {
                if (_properties != null) {
                    foreach (var pair in _properties) {
                        ResetChangesKeys(pair.Value as Subdocument);
                    }
                }

                _changesKeys?.Clear();
                HasChanges = false;
            });
        }

        // Update all subdocuments in _properties with the new FLDict values and invalidate all
        // obsolete subdocuments.  Other properties besides subdocuments and array will be removed so that
        // they can be reread from the new root.  This method is called after the new root has been updated
        // to the document when saving the document or updating the document from external changes
        internal void UseNewRoot()
        {
            if (_properties == null) {
                return;
            }

            var nuProps = new Dictionary<string, object>();
            foreach (var pair in _properties) {
                var fValue = FleeceValueForKey(pair.Key);
                var newVal = UpdateRoot(pair.Value, fValue, pair.Key);
                if (newVal != null) {
                    nuProps[pair.Key] = newVal;
                }
            }

            _properties = nuProps;
        }

        #endregion

        #region Private Methods

        private static bool IsValidScalarType(Type type)
        {
            var info = type.GetTypeInfo();
            if (info.IsPrimitive) {
                return true;
            }

            return _ValidTypes.Any(x => info.IsAssignableFrom(x));
        }

        private static void ValidateObjectType(object value)
        {
            if (value == null) {
                return;
            }

            var type = value.GetType();
            if(IsValidScalarType(type)) {
                return;
            }

            if(value is Subdocument || value is Blob) {
                return;
            }

            var jType = value as JToken;
            if (jType != null) {
                if (jType.Type == JTokenType.Object || jType.Type == JTokenType.Array) {
                    return;
                }

                throw new ArgumentException($"Invalid type in document properties: {type.Name}", nameof(value));
            }

            var array = value as IList;
            if(array != null) {
                foreach (var item in array) {
                    ValidateObjectType(item);
                }

                return;
            }

            var dict = value as IDictionary<string, object>;
            if (dict == null) {
                throw new ArgumentException($"Invalid type in document properties: {type.Name}", nameof(value));
            }

            foreach(var item in dict.Values) {
                ValidateObjectType(item);
            }
        }

        private void CacheValue(object value, string key, bool changed)
        {
            if (changed || value is Subdocument || value is IList) {
                if(_properties == null) {
                    _properties = new Dictionary<string, object>();
                }

                _properties[key] = value;
            }
        }

        private object ConvertArray(IList<object> array, object oldValue, string key)
        {
            var result = new List<object>(array.Count);
            var oldArray = oldValue as IList<object>;
            HashSet<object> arraySet = null;
            if (oldArray?.Count > 0) {
                arraySet = new HashSet<object>(array);
            }

            int i;
            for (i = 0; i < array.Count; i++) {
                var nValue = array[i];
                var oValue = oldArray?.Count > i ? oldArray[i] : null;

                // FIXME: Array can be nested, using a simple arraySet is not enough.
                if (oValue is Subdocument && arraySet?.Contains(oValue) == true) {
                    // Prevent the subdocument from being invalidate so the subdocument can be reordered:
                    oValue = null;
                }

                nValue = ConvertValue(nValue, oValue, key);
                result.Add(nValue);
            }

            // Invalidate the rest of the old array values that are not included in the result:
            for (; i < oldArray?.Count; i++) {
                InvalidateIfSubdocument(oldArray[i]);
            }

            return result;
        }

        private object ConvertDict(IDictionary<string, object> dict, object oldValue, string key)
        {
            var obj = ConvertDictionary(dict);
            if (obj == null) {
                var subdoc = oldValue as Subdocument;
                if (subdoc == null) {
                    subdoc = CreateSubdocument(key);
                }

                subdoc.Properties = dict;
                return subdoc;
            }

            return obj;
        }

        private object ConvertDictionary(IDictionary<string, object> dict)
        {
            var type = dict.GetCast<string>("_cbltype");
            if (type != null) {
                if (type == "blob") {
                    return CreateBlob(dict);
                }
            }

            return null; // Invalid!
        }

        private object ConvertSubdoc(Subdocument subdoc, object oldValue, string key)
        {
            // If the subdocument has already been set to a property, copy its properties:
            var parent = subdoc?.Parent;
            if (parent != null) {
                var oldSubdoc = oldValue as Subdocument;
                if (parent == this && subdoc.Key.Equals(key)) {
                    // e.g. array reorder case
                    oldSubdoc?.Invalidate();
                    return subdoc;
                } else {
                    // Copy the properties value into the old subdocument or copy the new subdoc:
                    if (oldSubdoc != null) {
                        oldSubdoc.Properties = subdoc.Properties;
                        return oldSubdoc;
                    } else {
                        subdoc = new Subdocument {
                            Properties = subdoc.Properties
                        };
                    }
                }
            }

            if (subdoc != null) {
                // Install subdocument:
                subdoc.Parent = this;
                subdoc.Key = key;
                subdoc.SetOnMutate(GetOnMutateBlock(key));
            }

            InvalidateIfSubdocument(oldValue);

            return subdoc;
        }

        private object ConvertValue(object value, object oldValue, string key)
        {
            if (value?.Equals(oldValue) == true) {
                return value; // nothing to convert
            }

            var subdoc = value as Subdocument;
            if (subdoc != null) {
                return ConvertSubdoc(subdoc, oldValue, key);
            }

            var jToken = value as JToken;
            if (jToken != null) {
                value = jToken.Type == JTokenType.Object
                    ? (object)jToken.ToObject<Dictionary<string, object>>()
                    : jToken.ToObject<List<object>>();
            }

            var dict = value as IDictionary<string, object>;
            if (dict != null) {
                return ConvertDict(dict, oldValue, key);
            }

            var arr = value as IList<object>;
            if (arr != null) {
                return ConvertArray(arr, oldValue, key);
            }

            InvalidateIfSubdocument(oldValue);

            return value;
        }

        private Subdocument CreateSubdocument(string key)
        {
            var sk = SharedKeys;
            var subDoc = new Subdocument(this, sk) {
                Key = key
            };

            subDoc.SetOnMutate(GetOnMutateBlock(key));
            return subDoc;
        }

        private FLValue* FleeceValueForKey(string key)
        {
            return _sharedKeys.GetDictValue(_root, key);
        }

        private object FleeceValueToObject(FLValue* value, string key)
        {
            switch (Native.FLValue_GetType(value)) {
                case FLValueType.Array:
                {
                    var array = Native.FLValue_AsArray(value);
                    FLArrayIterator iter;
                    Native.FLArrayIterator_Begin(array, &iter);
                    var result = new List<object>((int) Native.FLArray_Count(array));
                    FLValue* item;
                    while (null != (item = Native.FLArrayIterator_GetValue(&iter))) {
                        result.Add(FleeceValueToObject(item, key));
                        Native.FLArrayIterator_Next(&iter);
                    }

                    return result;
                }
                case FLValueType.Dict:
                {
                    var dict = Native.FLValue_AsDict(value);
                    var type = TypeForDict(dict);
                    if (type.buf == null) {
                        var subdoc = CreateSubdocument(key);
                        subdoc.SetRootDict(dict);
                        return subdoc;
                    }

                    var result = FLValueConverter.ToObject(value, SharedKeys) as IDictionary<string, object>;
                    return ConvertDictionary(result);
                }
                default:
                    return FLValueConverter.ToObject(value, SharedKeys);
            }
        }

        private Action GetOnMutateBlock(string key)
        {
            return () =>
            {
                MarkChangedKey(key);
            };
        }

        private void InvalidateIfSubdocument(object value)
        {
            var subdoc = value as Subdocument;
            var arr = value as IList;
            if (subdoc != null) {
                subdoc.Invalidate();
            } else if (arr != null) {
                foreach (var v in arr) {
                    InvalidateIfSubdocument(v);
                }
            }
        }

        private void LoadRootIntoProperties()
        {
            if (HasChanges) {
                throw new InvalidOperationException("Cannot load root into properties if there are changes in memory");
            }

            if (_root == null || _properties == null) {
                throw new InvalidOperationException("Cannot load root into properties if _root or _properties are null");
            }

            if (_properties.Count == Native.FLDict_Count(_root)) {
                return; // already loaded
            }

            FLDictIterator iter;
            Native.FLDictIterator_Begin(_root, &iter);
            string key;
            while (null != (key = SharedKeys.GetDictIterKey(&iter))) {
                if (!_properties.ContainsKey(key)) {
                    _properties[key] = FleeceValueToObject(Native.FLDictIterator_GetValue(&iter), key);
                }

                Native.FLDictIterator_Next(&iter);
            }
        }

        private void MutateProperties()
        {
            if(_properties == null) {
                _properties = FLValueConverter.ToObject((FLValue*)_root, SharedKeys) as Dictionary<string, object>
                    ?? new Dictionary<string, object>();
            } else if(_root != null && !HasChanges) {
                LoadRootIntoProperties();
            }
        }

        private void ResetChangesKeys(object value)
        {
            var subdoc = value as Subdocument;
            var arr = value as IList;
            if (subdoc != null) {
                subdoc.ResetChangesKeys();
            } else if(arr != null) {
                foreach (var v in arr) {
                    ResetChangesKeys(v);
                }
            }
        }

        private bool TryGet<T>(string key, out T value)
        {
            if(_properties != null) {
                if(Properties.TryGetValue(key, out value)) {
                    return true;
                }
            } 

            value = default(T);
            return HasChanges;
        }

        private FLSlice TypeForDict(FLDict* dict)
        {
            var typeKey = FLSlice.Constant("_cbltype");
            var type = SharedKeys.GetDictValue(dict, typeKey);
            return NativeRaw.FLValue_AsString(type);
        }

        private object UpdateRoot(object value, FLValue* fValue, string key)
        {
            var subdoc = value as Subdocument;
            var array = value as IList;
            if (subdoc != null) {
                var dict = Native.FLValue_AsDict(fValue);
                if (dict == null) {
                    InvalidateIfSubdocument(subdoc);
                    return null;
                }

                subdoc.SharedKeys = SharedKeys;
                subdoc.SetRootDict(dict);
                subdoc.UseNewRoot();
                return subdoc;
            } else if (array != null) {
                var fArray = Native.FLValue_AsArray(fValue);
                if (fArray == null) {
                    InvalidateIfSubdocument(value);
                    return null;
                }

                var result = new List<object>((int)Native.FLArray_Count(fArray));
                var i = 0;
                FLArrayIterator iter;
                Native.FLArrayIterator_Begin(fArray, &iter);
                FLValue* item;
                while (null != (item = Native.FLArrayIterator_GetValue(&iter))) {
                    var obj = i < array.Count ? array[i++] : null;
                    if (obj != null) {
                        obj = UpdateRoot(obj, item, key);
                    }

                    obj = obj ?? FleeceValueToObject(item, key);
                    result.Add(obj);
                    Native.FLArrayIterator_Next(&iter);
                }

                // Invalidate the rest of the subdocuments not included in the new array:
                for (; i < array.Count; i++) {
                    InvalidateIfSubdocument(array[i]);
                }

                return result;
            }

            return null;
        }

        #endregion
    }
}
