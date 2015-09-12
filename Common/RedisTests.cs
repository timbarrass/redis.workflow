using Microsoft.VisualStudio.TestTools.UnitTesting;
using StackExchange.Redis;
using System;
using System.Text;

namespace Redis.Workflow.Common
{
    [TestClass]
    public class RedisTests 
    {
        [TestMethod]
        public void CreateJSon()
        {
            var mux = ConnectionMultiplexer.Connect("localhost");

            var db = mux.GetDatabase();

            db.HashSet("task", new HashEntry[] { new HashEntry("key1", "value1"), new HashEntry("key2", "value2") });

            var script =
                  "local allResults = { }\r\n"
                + "local task = \"task\"\r\n"
                + "local taskDetails = redis.call(\"hgetall\", task)\r\n"
                + "local result = { }\r\n"
                + "for idx = 1, #taskDetails, 2 do\r\n"
                + "result[taskDetails[idx]] = taskDetails[idx + 1]\r\n"
                + "end\r\n"
                + "allResults[task] = result\r\n"
                + "return cjson.encode(allResults)"
                ;

            var result = db.ScriptEvaluate(script);

            db.ScriptEvaluate("redis.call(\"flushdb\")");
        }

        [TestMethod, Ignore]
        public void ScriptReload()
        {
            var script = "print(\"Hello World!\")";

            var mux = ConnectionMultiplexer.Connect("localhost");

            var db = mux.GetDatabase();

            var srv = mux.GetServer("localhost:6379");

            var prepped = LuaScript.Prepare(script);

            var loaded = prepped.Load(srv);

            var prepped2 = LuaScript.Prepare(script);

            var loaded2 = prepped2.Load(srv);

            Assert.AreEqual(Encoding.Default.GetString(loaded.Hash), Encoding.Default.GetString(loaded2.Hash));

            Assert.AreEqual(loaded, loaded2);
        }

        [TestMethod, Ignore]
        // To monitor I'll want to query list lengths -- I want to be happy that getting 
        // list length is O(1) rather than O(N), or I'll have to keep track of the list
        // size myself.
        public void ListLengthComplexity()
        {
            var mux = ConnectionMultiplexer.Connect("localhost");

            var db = mux.GetDatabase();

            var maxListLength = 100000;
            var listLength = 1;

            while (listLength < maxListLength)
            {
                for (var i = 0; i < listLength; i++)
                {
                    db.ListLeftPush("theList", new RedisValue[] { i.ToString() });
                }

                var iterationsRemaining = 100000;

                DateTime start = DateTime.Now;

                while (iterationsRemaining-- > 0)
                {
                    var len = db.ListLength("theList");
                }

                var duration = new TimeSpan(DateTime.Now.Ticks - start.Ticks);

                var average = duration.Ticks / 100000;

                Console.WriteLine("listLength " + listLength + " duration " + duration + " average " + average);

                listLength *= 10;
            }

            Assert.IsTrue(false);
        }
    }
}
