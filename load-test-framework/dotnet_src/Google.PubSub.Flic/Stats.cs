// Copyright 2019 Google LLC
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     https://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

using Google.Protobuf.WellKnownTypes;
using Google.Pubsub.Loadtest;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Security.Cryptography;

namespace Google.PubSub.Flic
{
    internal class Stats
    {
        private readonly object _lock = new object();
        private readonly long _clientId = BitConverter.ToInt64(MD5.Create().ComputeHash(Guid.NewGuid().ToByteArray()));
        private DateTime? _startTime;
        private bool _includeIds;
        private DateTime? _endTime = null;

        private long _errorCount = 0;
        private long[] _histogram = new long[50]; // 50 buckets allows for ~24 hours.
        private List<int> _msgIds = new List<int>();

        public void Init(DateTime startTime, bool includeIds) => (_startTime, _includeIds) = (startTime, includeIds);

        public void Record(TimeSpan duration, bool isError, int msgId)
        {
            lock (_lock)
            {
                if (isError)
                {
                    _errorCount += 1;
                }
                else
                {
                    var ms = duration.TotalMilliseconds;
                    var bucket = ms < 1.0 ? 0 : Math.Min((int)Math.Floor(Math.Log(ms, 1.5)) + 1, _histogram.Length - 1);
                    _histogram[bucket] += 1;
                    if (_includeIds)
                    {
                        _msgIds.Add(msgId);
                    }
                }
            }
        }

        public void End(DateTime endTime)
        {
            lock (_lock)
            {
                _endTime = endTime;
            }
        }

        public CheckResponse BuildCheckResponse()
        {
            lock (_lock)
            {
                var ret = new CheckResponse
                {
                    BucketValues = { _histogram.Reverse().SkipWhile(x => x == 0).Reverse() },
                    Failed = _errorCount,
                    IsFinished = _endTime != null,
                    RunningDuration = Duration.FromTimeSpan(_startTime is DateTime dt ? (_endTime ?? DateTime.UtcNow) - dt : TimeSpan.Zero),
                    ReceivedMessages = { _msgIds.Select(x => new MessageIdentifier { PublisherClientId = _clientId, SequenceNumber = x }) },
                };
                _errorCount = 0;
                Array.Clear(_histogram, 0, _histogram.Length);
                _msgIds.Clear();
                return ret;
            }
        }
    }
}
