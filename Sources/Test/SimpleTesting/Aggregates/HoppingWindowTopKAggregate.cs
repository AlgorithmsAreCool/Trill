// *********************************************************************
// Copyright (c) Microsoft Corporation.  All rights reserved.
// Licensed under the MIT License
// *********************************************************************
using System;
using System.Collections.Generic;
using System.Linq;
using Microsoft.StreamProcessing;
using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace SimpleTesting
{
    [TestClass]
    public class HoppingWindowTopKAggregate : TestWithConfigSettingsAndMemoryLeakDetection
    {
        private const long HopSize = 10;
        private const long WindowSize = 4 * HopSize;
        private const int K = 3;

        private StreamEvent<int> EndEvent = StreamEvent.CreatePunctuation<int>(StreamEvent.InfinitySyncTime);
        private Random random = new Random(Seed: (int)DateTime.UtcNow.Ticks);

        [TestMethod, TestCategory("Gated")]
        public void TestHoppingWindowTopKAggregateSimple()
        {
            var input = new[]
            {
                StreamEvent.CreateStart(1, 10),
                StreamEvent.CreateStart(3, 20),
                StreamEvent.CreateStart(6, 10),
                StreamEvent.CreateStart(8, 30),
                StreamEvent.CreateStart(12, 20),
                StreamEvent.CreateStart(18, 10),
                EndEvent
            };

            var output = input.ToStreamable().HoppingWindowLifetime(10, 5).TopK(K);

            var correctValues = new[] { 20, 30, 30, 20, 10 };
            var correctEvents = new List<StreamEvent<int>>();
            for (int i = 0; i < correctValues.Length; i++)
            {
                correctEvents.Add(StreamEvent.CreateStart(5 * (i + 1), correctValues[i]));
                correctEvents.Add(StreamEvent.CreateEnd(5 * (i + 2), 5 * (i + 1), correctValues[i]));
            }
            correctEvents.Add(EndEvent);

            CollectionAssert.AreEqual(correctEvents, output.Select(e => e.First().Payload).ToStreamEventArray());
        }

        [TestMethod, TestCategory("Gated")]
        public void TestHoppingWindowTopKAggregateRandomDistribution()
        {
            // Distribution: [1,2,3,4,5] hops : 10% each, Closely-Spaced: 50%
            GenerateDataAndTestInput(
                numValues: 1000,
                valueGenerator: v => random.Next(100, 110),
                distanceGenerator: () =>
                {
                    var hopType = random.Next(1, 11);
                    return (hopType < 6) ? (hopType * HopSize) : (hopType - 5);
                });
        }

        [TestMethod, TestCategory("Gated")]
        public void TestHoppingWindowTopKAggregateUnaligned()
        {
            GenerateDataAndTestInput(
                numValues: 1000,
                valueGenerator: v => random.Next(100, 110),
                distanceGenerator: () => 10,
                windowSize: 27);

            GenerateDataAndTestInput(
                numValues: 1000,
                valueGenerator: v => random.Next(100, 110),
                distanceGenerator: () => 10,
                windowSize: 17);
        }

        [TestMethod, TestCategory("Gated")]
        public void TestHoppingWindowTopKAggregateAllIncreasing()
        {
            GenerateDataAndTestInput(
                numValues: 1000,
                valueGenerator: v => v + random.Next(1, 11),
                distanceGenerator: () => HopSize);
        }

        [TestMethod, TestCategory("Gated")]
        public void TestHoppingWindowTopKAggregateAllDecreasing()
        {
            GenerateDataAndTestInput(
                numValues: 1000,
                valueGenerator: v => (v == 0) ? 10000 : v - random.Next(1, 11),
                distanceGenerator: () => HopSize);
        }

        [TestMethod, TestCategory("Gated")]
        public void TestHoppingWindowTopKAggregateCyclingValues()
        {
            // Test values 1 -> 2 -> 3 -> 1, and run for 1/2, 1, 2, 4, 8 intervals of hops
            for (long distance = HopSize / 2; distance <= HopSize * 8; distance *= 2)
            {
                GenerateDataAndTestInput(
                    numValues: 10,
                    valueGenerator: v => (v % 3) + 1,
                    distanceGenerator: () => distance);
            }
        }

        private void GenerateDataAndTestInput(
            int numValues,
            Func<int, int> valueGenerator,
            Func<long> distanceGenerator,
            long windowSize = WindowSize)
        {
            var input = new List<StreamEvent<int>>();
            long maxStartTime = 0;

            long startTime = 0;
            int value = 100;
            for (int i = 0; i < numValues; i++)
            {
                startTime += distanceGenerator();
                value = valueGenerator(value);
                input.Add(StreamEvent.CreateStart(startTime, value));
                maxStartTime = Math.Max(maxStartTime, startTime);
            }
            input.Add(EndEvent);

            TestHoppingWindowTopKAggregateInternal(input, maxStartTime, windowSize);
        }

        private void TestHoppingWindowTopKAggregateInternal(IEnumerable<StreamEvent<int>> streamEvents, long maxStartTime, long windowSize)
        {
            var output = streamEvents.ToStreamable().HoppingWindowLifetime(windowSize, HopSize).TopK(K);

            var correct = new List<StreamEvent<List<int>>>();

            maxStartTime += windowSize;
            for (long startTime = 0; startTime < maxStartTime; startTime += HopSize)
            {
                var eventsInWindow = streamEvents.Where(x => x.StartTime > (startTime - windowSize) && x.StartTime <= startTime);

                if (!eventsInWindow.Any())
                    continue;

                var dataInWindow = eventsInWindow.Select(se => se.Payload).ToList();
                dataInWindow.Sort((x, y) => y.CompareTo(x));

                var last = dataInWindow[Math.Min(K, dataInWindow.Count) - 1];
                var topKInWindow = dataInWindow.TakeWhile(e => e >= last).ToList();

                correct.Add(StreamEvent.CreateStart(startTime, topKInWindow));
                correct.Add(StreamEvent.CreateEnd(startTime + HopSize, startTime, topKInWindow));
            }

            var output2 = output.Select(e => e.Select(re => re.Payload).ToList());

            var expected = NormalizeToInterval(correct).ToList();
            var actual = NormalizeToInterval(output2.ToStreamEventArray()).ToList();

            Assert.AreEqual(expected.Count, actual.Count);

            for (int i = 0; i < expected.Count; i++)
            {
                Assert.AreEqual(expected[i].Kind, actual[i].Kind);
                Assert.AreEqual(expected[i].StartTime, actual[i].StartTime);
                Assert.AreEqual(expected[i].EndTime, actual[i].EndTime);
                CollectionAssert.AreEquivalent(expected[i].Payload, actual[i].Payload);
            }
        }

        private IEnumerable<StreamEvent<List<int>>> NormalizeToInterval(IEnumerable<StreamEvent<List<int>>> streamEvents)
        {
            var result = new List<StreamEvent<List<int>>>();

            var endEvents = streamEvents.Where(se => se.Kind == StreamEventKind.End || se.Kind == StreamEventKind.Interval);

            if (!endEvents.Any())
                return result;

            var firstEvent = endEvents.First();
            StreamEvent<List<int>> curInterval = StreamEvent.CreateInterval(firstEvent.StartTime, firstEvent.EndTime, firstEvent.Payload);

            foreach (var streamEvent in endEvents.Skip(1))
            {
                if ((streamEvent.StartTime == curInterval.EndTime || streamEvent.Kind == StreamEventKind.Interval) && // Merge into current interval if payload is same
                    Enumerable.SequenceEqual(streamEvent.Payload, curInterval.Payload))
                {
                    curInterval.OtherTime = streamEvent.EndTime;
                }
                else
                {
                    result.Add(curInterval);
                    curInterval = StreamEvent.CreateInterval(streamEvent.StartTime, streamEvent.EndTime, streamEvent.Payload); ;
                }
            }
            result.Add(curInterval);
            return result;
        }
    }
}
