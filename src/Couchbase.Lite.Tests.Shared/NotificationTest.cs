﻿//
//  NotificationTest.cs
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
using System.Threading;
using System.Threading.Tasks;

using Couchbase.Lite;
using FluentAssertions;
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
    public class NotificationTest : TestCase
    {
#if !WINDOWS_UWP
        public NotificationTest(ITestOutputHelper output) : base(output)
        {

        }
#endif

        [Fact]
        public void TestDatabaseNotification()
        {
            var gotCount = 0;
            var mre = new ManualResetEventSlim();
            Db.Changed += (sender, args) =>
            {
                gotCount = args.DocIDs.Count;
                mre.Set();
            };

            Db.InBatch(() =>
            {
                for (uint i = 0; i < 10; i++) {
                    var doc = Db[$"doc-{i}"];
                    doc["type"] = "demo";
                    doc.Save();
                }
            });

            mre.Wait(5000).Should().BeTrue("because otherwise the event never fired");
            gotCount.Should().Be(10, "because 10 documents were added");
        }

        //[Fact]
        public void TestDocumentNotification()
        {
            var docA = Db["A"];
            var docB = Db["B"];
            var callbackCount = 0;
            bool external = false;
            var are = new AutoResetEvent(false);
            docA.Saved += (sender, args) =>
            {
                external = args.IsExternal;
                callbackCount++;
                are.Set();
            };

            docB.Set("thewronganswer", 18);
            docB.Save();

            are.WaitOne(TimeSpan.FromSeconds(2)).Should().BeFalse("because otherwise the document changed fired when it shouldn't have");
            callbackCount.Should().Be(0, "because docA has not been changed yet");

            docA.Set("therightanswer", 42);
            docA.Save();

            are.WaitOne(TimeSpan.FromSeconds(2)).Should().BeTrue("because otherwise the document changed event didn't fire");
            callbackCount.Should().Be(1, "because docA was saved once");
            external.Should().BeFalse("because the event was fired from the same object that was saved");

            docA.Set("thewronganswer", 18);
            docA.Save();

            are.WaitOne(TimeSpan.FromSeconds(2)).Should().BeTrue("because otherwise the document changed event didn't fire");
            callbackCount.Should().Be(2, "because docA was saved once again");
            external.Should().BeFalse("because the event was fired from the same object that was saved");
        }

        //[Fact]
        public void TestExternalChanges()
        {
            using(var db2 = new Database(Db.Name, Db.Options)) {
                var gotCount = 0;
                var dbExternal = false;
                var mre1 = new ManualResetEventSlim();
                var mre2 = new ManualResetEventSlim();
                db2.Changed += (sender, args) =>
                {
                    gotCount = args.DocIDs.Count;
                    dbExternal = args.External;
                    mre1.Set();
                };

                var db2doc6 = db2["doc-6"];
                var type = default(string);
                var docExternal = false;
                db2doc6.Saved += (sender, args) =>
                {
                    docExternal = args.IsExternal;
                    type = db2doc6.GetString("type");
                    mre2.Set();
                };

                Db.InBatch(() =>
                {
                    for (uint i = 0; i < 10; i++) {
                        var doc = Db[$"doc-{i}"];
                        doc["type"] = "demo";
                        doc.Save();
                    }
                });

                mre1.Wait(TimeSpan.FromSeconds(5)).Should().BeTrue("because otherwise the database event didn't fire");
                mre2.Wait(TimeSpan.FromSeconds(5)).Should().BeTrue("because otherwise the document event didn't fire");
                dbExternal.Should().BeTrue("because a different db instance triggered the event");
                docExternal.Should().BeTrue("because the document being monitored belongs to another db instance");
                gotCount.Should().Be(10, "because 10 documents were created");
                type.Should().Be("demo", "because that was the type that was set externally");
            }
        }
    }
}
