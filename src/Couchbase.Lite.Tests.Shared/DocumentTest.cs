﻿//
//  DocumentTest.cs
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

using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

using Couchbase.Lite;
using FluentAssertions;
using LiteCore;
using LiteCore.Interop;
#if !WINDOWS_UWP
using Xunit;
using Xunit.Abstractions;
#else
using Fact = Microsoft.VisualStudio.TestTools.UnitTesting.TestMethodAttribute;
#endif

namespace Test
{
#if WINDOWS_UWP
    [Microsoft.VisualStudio.TestTools.UnitTesting.TestClass]
#endif
    public class DocumentTest : TestCase
    {
        private Document _doc;

#if !WINDOWS_UWP
        public DocumentTest(ITestOutputHelper output) : base(output)
#else
        public DocumentTest()
#endif
        {
            Db.ConflictResolver = new DoNotResolve();
            _doc = Db["doc1"];
        }

        [Fact]
        public void TestNewDoc()
        {
            var doc = Db.CreateDocument();
            doc.Id.Should().NotBeNullOrEmpty("because a document should always have an ID");
            doc.Database.Should().BeSameAs(Db, "because a doc should have a reference to its owner");
            doc.Exists.Should().BeFalse("because the document has not been saved yet");
            doc.IsDeleted.Should().BeFalse("because the document is not deleted");
            doc.Properties.Should().BeNull("because no properties have been added");
            doc["prop"].Should().BeNull("because this property does not exist");
            doc.GetBoolean("prop").Should().BeFalse("because this bool does not exist");
            doc.GetLong("prop").Should().Be(0L, "because this int does not exist");
            doc.GetFloat("prop").Should().BeApproximately(0.0f, Single.Epsilon, "because this float does not exist");
            doc.GetDouble("prop").Should().BeApproximately(0.0, Double.Epsilon, "because this double does not exist");
            doc.GetDate("prop").Should().BeNull("because this date does not exist");
            doc.GetString("prop").Should().BeNull("because this string does not exist");

            doc.Save();
            doc.Exists.Should().BeTrue("because the document was saved");
            doc.IsDeleted.Should().BeFalse("because the document is not deleted");
            doc.Properties.Should().BeNull("because no properties were added");
        }

        [Fact]
        public void TestNewDocWithID()
        {
            var doc = Db.GetDocument("doc1");
            doc.Id.Should().Be("doc1", "because that is the ID that was passed");
            doc.Database.Should().BeSameAs(Db, "because a doc should have a reference to its owner");
            doc.Exists.Should().BeFalse("because the document has not been saved yet");
            doc.IsDeleted.Should().BeFalse("because the document is not deleted");
            doc.Properties.Should().BeNull("because no properties have been added");
            doc["prop"].Should().BeNull("because this property does not exist");
            doc.GetBoolean("prop").Should().BeFalse("because this bool does not exist");
            doc.GetLong("prop").Should().Be(0L, "because this int does not exist");
            doc.GetFloat("prop")
                .Should()
                .BeApproximately(0.0f, Single.Epsilon, "because this float does not exist");
            doc.GetDouble("prop")
                .Should()
                .BeApproximately(0.0, Double.Epsilon, "because this double does not exist");
            doc.GetDate("prop").Should().BeNull("because this date does not exist");
            doc.GetString("prop").Should().BeNull("because this string does not exist");

            doc.Save();
            doc.Exists.Should().BeTrue("because the document was saved");
            doc.IsDeleted.Should().BeFalse("because the document is not deleted");
            doc.Properties.Should().BeNull("because no properties were added");
        }

        [Fact]
        public void TestPropertyAccessors()
        {
            var date = DateTimeOffset.Now;
            var doc = Db.GetDocument("doc1");
            doc.Set("bool", true)
                .Set("double", 1.1)
                .Set("float", 1.2f)
                .Set("integer", 2L)
                .Set("string", "str")
                .Set("array", new[] { "1", "2" })
                .Set("date", date)
                .Save();

            doc.GetBoolean("bool").Should().BeTrue("because that is the bool that was saved");
            doc.GetDouble("double").Should().BeApproximately(1.1, Double.Epsilon, "because that is the double that was saved");
            doc.GetFloat("float").Should().BeApproximately(1.2f, Single.Epsilon, "because that is the float that was saved");
            doc.GetLong("integer").Should().Be(2L, "because that is the integer that was saved");

            doc.GetString("string").Should().Be("str", "because that is the string that was saved");
            doc.Get("array").ShouldBeEquivalentTo(new[] { "1", "2" }, "because that is the array that was saved");

            doc.GetDate("date").Should().Be(date, "because that is the date that was saved");

            // Get the doc from another database
            using(var otherDB = new Database(Db.Name, Db.Options)) {
                var doc1 = otherDB.GetDocument("doc1");
                doc1.GetBoolean("bool").Should().BeTrue("because that is the bool that was saved");
                doc1.GetDouble("double").Should().BeApproximately(1.1, Double.Epsilon, "because that is the double that was saved");
                doc1.GetFloat("float").Should().BeApproximately(1.2f, Single.Epsilon, "because that is the float that was saved");
                doc1.GetLong("integer").Should().Be(2L, "because that is the integer that was saved");

                doc1.GetString("string").Should().Be("str", "because that is the string that was saved");
                //doc1.Get("dict").ShouldBeEquivalentTo(new Dictionary<string, object> { ["foo"] = "bar" }, "because that is the dict that was saved");
                doc1.Get("array").ShouldBeEquivalentTo(new[] { "1", "2" }, "because that is the array that was saved");

                doc1.GetDate("date").Should().Be(date, "because that is the date that was saved");
            }
        }

        [Fact]
        public void TestProperties()
        {
            var doc = Db["doc1"];
            doc["type"] = "demo";
            doc["weight"] = 12.5;
            doc["tags"] = new[] { "useless", "emergency" };

            doc["type"].Should().Be("demo", "because that is the type that was entered");
            doc["weight"].As<double>().Should().BeApproximately(12.5, Double.Epsilon, "beacuse that is the weight that was entered");
            doc.Properties.Should().Contain("type", "demo").And.Contain("weight", 12.5, "because those simple values were added");
            doc.Properties["tags"].As<IEnumerable<object>>().Should()
                .ContainInOrder(new[] { "useless", "emergency" }, "because those were the tags that were added");
        }

        [Fact]
        public void TestRemoveProperties()
        {
            _doc.Properties = new Dictionary<string, object> {
                ["type"] = "profile",
                ["name"] = "Jason",
                ["weight"] = 130.5,
                ["address"] = new Dictionary<string, object> {
                    ["street"] = "1 milky way.",
                    ["city"] = "galaxy city",
                    ["zip"] = 12345
                }
            };

            _doc.Save();
            _doc["name"] = null;
            _doc["weight"] = null;
            _doc["age"] = null;
            _doc["active"] = null;
            _doc.GetSubdocument("address")["city"] = null;
            _doc.GetString("name").Should().BeNull("because it was removed");
            _doc.GetFloat("weight").Should().Be(0.0f, "because it was removed");
            _doc.GetDouble("weight").Should().Be(0.0, "because it was removed");
            _doc.GetLong("age").Should().Be(0L, "because it was removed");
            _doc.GetBoolean("active").Should().BeFalse("because it was removed");

            _doc["name"].Should().BeNull("because it was removed");
            _doc["weight"].Should().BeNull("because it was removed");
            _doc["age"].Should().BeNull("because it was removed");
            _doc["active"].Should().BeNull("because it was removed");
            _doc.GetSubdocument("address")["city"].Should().BeNull("because it was removed");

            var address = _doc.GetSubdocument("address");
            _doc.Properties.ShouldBeEquivalentTo(new Dictionary<string, object> {
                ["type"] = "profile",
                ["address"] = address
            });
            address.Properties.ShouldBeEquivalentTo(new Dictionary<string, object> {
                ["street"] = "1 milky way.",
                ["zip"] = 12345L
            });
        }

        [Fact]
        public void TestContainsKey()
        {
            var doc = Db["doc1"];
            doc.Properties = new Dictionary<string, object> {
                ["type"] = "profile",
                ["name"] = "Jaon",
                ["address"] = new Dictionary<string, object> {
                    ["street"] = "1 milky way."
                }
            };

            doc.Contains("type").Should().BeTrue("because 'type' exists in the document");
            doc.Contains("name").Should().BeTrue("because 'name' exists in the document");
            doc.Contains("address").Should().BeTrue("because 'address' exists in the document");
            doc.Contains("weight").Should().BeFalse("because 'weight' does not exist in the document");
        }

        [Fact]
        public void TestDelete()
        {
            var doc = Db["doc1"];
            doc["type"] = "Profile";
            doc["name"] = "Scott";
            doc.Exists.Should().BeFalse("because the document has not been saved yet");
            doc.IsDeleted.Should().BeFalse("beacuse the document is not deleted");

            // Delete before save:
            doc.Invoking(d => d.Delete()).ShouldThrow<LiteCoreException>().Which.Error.Should()
                .Be(new C4Error(LiteCoreError.NotFound), "because an attempt to delete a non-existent document was made");

            doc["type"].Should().Be("Profile", "because the doc should still exist");
            doc["name"].Should().Be("Scott", "because the doc should still exist");

            // Save:
            doc.Save();
            doc.Exists.Should().BeTrue("because the document was saved");
            doc.IsDeleted.Should().BeFalse("beacuse the document is still not deleted");

            // Delete:
            doc.Delete();
            doc.Exists.Should().BeTrue("because the document still exists in terms of the DB");
            doc.IsDeleted.Should().BeTrue("because now the document is deleted");
            doc.Properties.Should().BeNull("because a deleted document has no properties");
        }

        [Fact]
        public void TestPurge()
        {
            var doc = Db["doc1"];
            doc["type"] = "Profile";
            doc["name"] = "Scott";
            doc.Exists.Should().BeFalse("because the document has not been saved yet");
            doc.IsDeleted.Should().BeFalse("beacuse the document is not deleted");

            // Purge before save:
            doc.Purge().Should().BeFalse("because deleting a non-existent document is invalid");
            doc["type"].Should().Be("Profile", "because the doc should still exist");
            doc["name"].Should().Be("Scott", "because the doc should still exist");

            // Save:
            doc.Save();
            doc.Exists.Should().BeTrue("because the document was saved");
            doc.IsDeleted.Should().BeFalse("beacuse the document is still not deleted");

            // Purge:
            doc.Purge().Should().BeTrue("because the purge should succeed now");
            doc.Exists.Should().BeFalse("because the document was blown away");
            doc.IsDeleted.Should().BeFalse("because the document does not exist");
            doc.Properties.Should().BeEmpty("because a purged document has no properties");
        }

        [Fact]
        public void TestRevert()
        {
            var doc = Db["doc1"];
            doc["type"] = "Profile";
            doc["name"] = "Scott";

            // Reset before save:
            doc.Revert();
            doc["type"].Should().BeNull("because the document was reset");
            doc["name"].Should().BeNull("because the document was reset");

            // Save:
            doc["type"] = "Profile";
            doc["name"] = "Scott";
            doc.Save();
            doc["type"].Should().Be("Profile", "because the save completed");
            doc["name"].Should().Be("Scott", "because the save completed");

            // Make some changes:
            doc["type"] = "user";
            doc["name"] = "Scottie";

            // Reset:
            doc.Revert();
            doc["type"].Should().Be("Profile", "because the document was reset");
            doc["name"].Should().Be("Scott", "because the document was reset");
        }

        [Fact]
        public void TestReopenDB()
        {
            var doc = Db["doc1"];
            doc["string"] = "str";
            doc.Properties.Should().Equal(new Dictionary<string, object> { ["string"] = "str" }, "because otherwise the property didn't get inserted");
            doc.Save();

            ReopenDB();

            doc = Db["doc1"];
            doc.Properties.Should().Equal(new Dictionary<string, object> { ["string"] = "str" }, "because otherwise the property didn't get saved");
            doc["string"].Should().Be("str", "because otherwise the property didn't get saved");
        }

        [Fact]
        public void TestConflict()
        {
            Db.ConflictResolver = new TheirsWins();
            var doc = SetupConflict();
            doc.Save();
            doc["name"].Should().Be("Scotty", "because the 'theirs' version should win");

            doc = Db["doc2"];
            doc.ConflictResolver = new MergeThenTheirsWins();
            doc["type"] = "profile";
            doc["name"] = "Scott";
            doc.Save();

            var properties = doc.Properties;
            properties["type"] = "bio";
            properties["gender"] = "male";
            SaveProperties(properties, doc.Id);
            doc["type"] = "biography";
            doc["age"] = 31;
            doc.Save();
            doc["age"].Should().Be(31L, "because 'age' was changed by 'mine' and not 'theirs'");
            doc["type"].Should().Be("bio", "because 'type' was changed by 'mine' and 'theirs' so 'theirs' should win");
            doc["gender"].Should().Be("male", "because 'gender' was changed by 'theirs' but not 'mine'");
            doc["name"].Should().Be("Scott", "because 'name' was unchanged");
        }

        [Fact]
        public void TestConflictResolverGivesUp()
        {
            Db.ConflictResolver = new GiveUp();
            var doc = SetupConflict();
            var ex = doc.Invoking(d => d.Save()).ShouldThrow<LiteCoreException>().Which.Error.Should().Be(new C4Error(LiteCoreError.Conflict), "because the conflict resolver gave up");
            doc.HasChanges.Should().BeTrue("because the document wasn't saved");
        }

        [Fact]
        public void TestDeletionConflict()
        {
            Db.ConflictResolver = new DoNotResolve();
            var doc = SetupConflict();
            doc.Delete();
            doc.Exists.Should().BeTrue("because there was a conflict in place of the deletion");
            doc.IsDeleted.Should().BeFalse("because there was a conflict in place of the deletion");
            doc["name"].Should().Be("Scotty", "because that was the pre-deletion value");
        }

        [Fact]
        public void TestConflictMineIsDeeper()
        {
            Db.ConflictResolver = null;
            var doc = SetupConflict();
            doc.Save();
            doc["name"].Should().Be("Scott Pilgrim", "because the current in memory document has a longer history");
        }

        [Fact]
        public void TestConflictTheirsIsDeeper()
        {
            Db.ConflictResolver = null;
            var doc = SetupConflict();

            // Add another revision to the conflict, so it'll have a higher generation
            var properties = doc.Properties;
            properties["name"] = "Scott of the Sahara";
            SaveProperties(properties, doc.Id);
            doc.Save();

            doc["name"].Should().Be("Scott of the Sahara", "because the conflict has a longer history");
        }

        [Fact]
        public void TestBlob()
        {
            var content = Encoding.UTF8.GetBytes("12345");
            var data = new Blob("text/plain", content);
            _doc["data"] = data;
            _doc["name"] = "Jim";
            _doc.Save();

            using(var otherDb = new Database(Db.Name, Db.Options)) {
                var doc1 = otherDb["doc1"];
                doc1["name"].Should().Be("Jim", "because the document should be persistent after save");
                doc1["data"].Should().BeAssignableTo<Blob>("because otherwise the data did not save correctly");
                data = doc1.GetBlob("data");

                data.Length.Should().Be(5, "because the data is 5 bytes long");
                data.Content.Should().Equal(content, "because the data should have been retrieved correctly");
                var contentStream = data.ContentStream;
                var buffer = new byte[10];
                var bytesRead = contentStream.Read(buffer, 0, 10);
                contentStream.Dispose();
                bytesRead.Should().Be(5, "because the data is 5 bytes long");
            }
        }

        [Fact]
        public void TestEmptyBlob()
        {
            var content = new byte[0];
            var data = new Blob("text/plain", content);
            _doc["data"] = data;
            _doc.Save();

            using(var otherDb = new Database(Db.Name, Db.Options)) {
                var doc1 = otherDb["doc1"];
                doc1["data"].Should().BeAssignableTo<Blob>("because otherwise the data did not save correctly");
                data = doc1.GetBlob("data");

                data.Length.Should().Be(0, "because the data is 5 bytes long");
                data.Content.Should().Equal(content, "because the data should have been retrieved correctly");
                var contentStream = data.ContentStream;
                var buffer = new byte[10];
                var bytesRead = contentStream.Read(buffer, 0, 10);
                contentStream.Dispose();
                bytesRead.Should().Be(0, "because the data is 5 bytes long");
            }
        }

        [Fact]
        public void TestBlobWithStream()
        {
            var content = new byte[0];
            Stream contentStream = new MemoryStream(content);
            var data = new Blob("text/plain", contentStream);
            _doc["data"] = data;
            _doc.Save();

            using(var otherDb = new Database(Db.Name, Db.Options)) {
                var doc1 = otherDb["doc1"];
                doc1["data"].Should().BeAssignableTo<Blob>("because otherwise the data did not save correctly");
                data = doc1.GetBlob("data");

                data.Length.Should().Be(0, "because the data is 5 bytes long");
                data.Content.Should().Equal(content, "because the data should have been retrieved correctly");
                contentStream = data.ContentStream;
                var buffer = new byte[10];
                var bytesRead = contentStream.Read(buffer, 0, 10);
                contentStream.Dispose();
                bytesRead.Should().Be(0, "because the data is 5 bytes long");
            }
        }

        [Fact]
        public void TestMultipleBlobRead()
        {
            var content = Encoding.UTF8.GetBytes("12345");
            var data = new Blob("text/plain", content);
            _doc["data"] = data;
            data = _doc.GetBlob("data");
            for (int i = 0; i < 5; i++) {
                data.Content.Should().Equal(content, "because otherwise incorrect data was read");
                using (var contentStream = data.ContentStream) {
                    var buffer = new byte[10];
                    var bytesRead = contentStream.Read(buffer, 0, 10);
                    bytesRead.Should().Be(5, "because the data has 5 bytes");
                }
            }

            _doc.Save();
            
            using(var otherDb = new Database(Db.Name, Db.Options)) {
                var doc1 = otherDb["doc1"];
                doc1["data"].Should().BeAssignableTo<Blob>("because otherwise the data did not save correctly");
                data = doc1.GetBlob("data");

                data.Length.Should().Be(5, "because the data is 5 bytes long");
                data.Content.Should().Equal(content, "because the data should have been retrieved correctly");
                var contentStream = data.ContentStream;
                var buffer = new byte[10];
                var bytesRead = contentStream.Read(buffer, 0, 10);
                contentStream.Dispose();
                bytesRead.Should().Be(5, "because the data is 5 bytes long");
            }
        }

        [Fact]
        public void TestReadExistingBlob()
        {
            var content = Encoding.UTF8.GetBytes("12345");
            var data = new Blob("text/plain", content);
            _doc["data"] = data;
            _doc["name"] = "Jim";
            _doc.Save();

            ReopenDB();

            _doc = Db["doc1"];
            _doc["data"].As<Blob>().Content.Should().Equal(content, "because the data should have been retrieved correctly");

            ReopenDB();

            _doc = Db["doc1"];
            _doc["foo"] = "bar";
            _doc.Save();
            _doc["data"].As<Blob>().Content.Should().Equal(content, "because the data should have been retrieved correctly");
        }

        private Document SetupConflict()
        {
            var doc =  Db["doc1"];
            doc["type"] = "profile";
            doc["name"] = "Scott";
            doc.Save();
            var properties = doc.Properties;

            properties["name"] = "Scotty";
            SaveProperties(properties, doc.Id);

            doc["name"] = "Scott Pilgrim";
            return doc;
        }

        private unsafe void SaveProperties(IDictionary<string, object> props, string docID)
        {
            Db.InBatch(() =>
            {
                var tricky =
                    (C4Document*) LiteCoreBridge.Check(err => Native.c4doc_get(Db.c4db, docID, true, err));
                var put = new C4DocPutRequest {
                    docID = tricky->docID,
                    history = &tricky->revID,
                    historyCount = 1,
                    save = true
                };

                var body = Db.JsonSerializer.Serialize(props);
                put.body = body;

                var newDoc = (C4Document*) LiteCoreBridge.Check(err =>
                {
                    var localPut = put;
                    var retVal = Native.c4doc_put(Db.c4db, &localPut, null, err);
                    Native.FLSliceResult_Free(body);
                    return retVal;
                });
            });
        }

        protected override void Dispose(bool disposing)
        {
            if(disposing) {
                _doc.Revert();
            }

            base.Dispose(disposing);
        }
    }

    internal class TheirsWins : IConflictResolver
    {
        public IDictionary<string, object> Resolve(IReadOnlyDictionary<string, object> mine, IReadOnlyDictionary<string, object> theirs, IReadOnlyDictionary<string, object> baseProps)
        {
            return theirs.ToDictionary(k => k.Key, v => v.Value);
        }
    }

    internal class MergeThenTheirsWins : IConflictResolver
    {
        public IDictionary<string, object> Resolve(IReadOnlyDictionary<string, object> mine, IReadOnlyDictionary<string, object> theirs, IReadOnlyDictionary<string, object> baseProps)
        {
            var resolved = baseProps.ToDictionary(k => k.Key, v => v.Value);
            var changed = new HashSet<string>();
            foreach(var pair in theirs) {
                resolved[pair.Key] = pair.Value;
                changed.Add(pair.Key);
            }

            foreach(var pair in mine) {
                if(!changed.Contains(pair.Key)) {
                    resolved[pair.Key] = pair.Value;
                }
            }

            return resolved;
        }
    }

    internal class GiveUp : IConflictResolver
    {
        public IDictionary<string, object> Resolve(IReadOnlyDictionary<string, object> mine, IReadOnlyDictionary<string, object> theirs, IReadOnlyDictionary<string, object> baseProps)
        {
            return null;
        }
    }

    internal class DoNotResolve : IConflictResolver
    {
        public IDictionary<string, object> Resolve(IReadOnlyDictionary<string, object> mine, IReadOnlyDictionary<string, object> theirs, IReadOnlyDictionary<string, object> baseProps)
        {
            throw new NotImplementedException();
        }
    }
}
