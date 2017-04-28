﻿//
//  PerfTest.cs
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
using System.Diagnostics;
using System.IO;
using System.Text;
using Couchbase.Lite;
using Couchbase.Lite.Logging;
using Couchbase.Lite.Support;
using FluentAssertions;
using Test.Util;
#if !WINDOWS_UWP
using Xunit;
using Xunit.Abstractions;
#else
using Microsoft.VisualStudio.TestTools.UnitTesting;
using Fact = Microsoft.VisualStudio.TestTools.UnitTesting.TestMethodAttribute;
#endif

namespace Test
{
#if WINDOWS_UWP
    [TestClass]
#endif
    public abstract class PerfTest
    {
#if !WINDOWS_UWP
        private ITestOutputHelper _output;
#else 
        private TestContext _output;
        public TestContext TestContext
        {
            get => _output;
            set {
                _output = value;
                Log.AddLogger(new MSTestLogger(_output));
            }
        }
#endif
        public static string ResourceDir;
        private DatabaseOptions _dbOptions;
        private string _dbName;

        public Database Db { get; private set; }

#if !WINDOWS_UWP
        protected PerfTest(ITestOutputHelper output)
        {
            _output = output;
            Log.SetLogger(new XunitLogger(output));
        }
#endif

        protected void SetOptions(DatabaseOptions dbOptions)
        {
            _dbOptions = dbOptions;
            _dbName = "perfdb";
            Database.Delete(_dbName, _dbOptions.Directory);
        }

        protected string ReadData(string resourceName)
        {
            var path = Path.Combine(ResourceDir, resourceName);
            return File.ReadAllText(path);
        }

        protected void OpenDB()
        {
            _dbName.Should().NotBeNull("because otherwise we cannot open the database");
            Db.Should().BeNull("because otherwise we are trying to reopen the database incorrectly");
            Db = new Database(_dbName, _dbOptions);
        }

        protected void ReopenDB()
        {
            Db?.Dispose();
            Db = null;
            OpenDB();
        }

        protected void EraseDB()
        {
            Db?.Dispose();
            Db = null;
            Database.Delete(_dbName, _dbOptions.Directory);
            OpenDB();
        }

        protected virtual void SetUp()
        {
            Log.Disabled = true;
        }

        protected abstract void Test();

        protected virtual void TearDown()
        {
            Log.Disabled = false;
            Db?.Dispose();
            Db = null;
        }

        protected void Run()
        {
            WriteLine($"===== {GetType().Name} =====");
            SetUp();
            try {
                Test();
            } finally {
                TearDown();
            }
        }

        protected void WriteLine(string line)
        {
#if !WINDOWS_UWP
            _output.WriteLine(line);
#else
            Console.WriteLine(line);
#endif
        }

        protected void Measure(uint count, string unit, Action a)
        {
            var b = new Benchmark(_output);
            const int reps = 10;
            for(int i = 0; i < reps; i++) {
                EraseDB();
                b.Start();
                a();
                var t = b.Stop();
                WriteLine($"{t}");
            }

            b.PrintReport();
            if(count > 1) {
                b.PrintReport(1.0 / count, unit);
            }
        }
    }
}
