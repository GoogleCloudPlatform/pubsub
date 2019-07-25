using CommandLine;
using Google.Api.Gax;
using Google.Cloud.PubSub.V1;
using Google.Protobuf.WellKnownTypes;
using Google.Pubsub.Loadtest;
using Grpc.Core;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Security.Cryptography;
using System.Threading;
using System.Threading.Tasks;

namespace Google.PubSub.Flic
{
    class Program
    {
        private class Options
        {
            [Option("port", Required = false)]
            public int Port { get; private set; } = 5000;
        }

        private class LoadtestServer : Pubsub.Loadtest.LoadtestWorker.LoadtestWorkerBase
        {
            public LoadtestServer(TaskCompletionSource<StartRequest> startTcs, Func<CheckResponse> buildCheckResponseFn) =>
                (_startTcs, _buildCheckResponseFn) = (startTcs, buildCheckResponseFn);

            private TaskCompletionSource<StartRequest> _startTcs;
            private Func<CheckResponse> _buildCheckResponseFn;

            public override Task<StartResponse> Start(StartRequest request, ServerCallContext context)
            {
                _startTcs.SetResult(request);
                return Task.FromResult(new StartResponse { });
            }

            public override Task<CheckResponse> Check(CheckRequest request, ServerCallContext context)
            {
                return Task.FromResult(_buildCheckResponseFn());
            }
        }

        private class Stats
        {
            private readonly object _lock = new object();
            private readonly long _clientId = BitConverter.ToInt64(MD5.Create().ComputeHash(Guid.NewGuid().ToByteArray()));
            private DateTime? _startTime;
            private bool _includeIds;
            private DateTime? _endTime = null;

            private long _errorCount = 0;
            private long[] _histogram = new long[50];
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
                        ReceivedMessages = { _includeIds ?
                            _msgIds.Select(x => new MessageIdentifier { PublisherClientId = _clientId, SequenceNumber = x }) :
                            Enumerable.Empty<MessageIdentifier>() },
                    };
                    _errorCount = 0;
                    Array.Clear(_histogram, 0, _histogram.Length);
                    _msgIds.Clear();
                    return ret;
                }
            }
        }

        static async Task<int> Main(string[] args)
        {
            var parsed = Parser.Default.ParseArguments<Options>(args);
            switch (parsed)
            {
                case Parsed<Options> success:
                    var options = success.Value;
                    var startTcs = new TaskCompletionSource<StartRequest>();
                    var stats = new Stats();
                    var server = new Server
                    {
                        Services = { LoadtestWorker.BindService(new LoadtestServer(startTcs, stats.BuildCheckResponse)) },
                        Ports = { new ServerPort("0.0.0.0", options.Port, ServerCredentials.Insecure) },
                    };
                    server.Start();
                    var startRequest = await startTcs.Task;
                    if (startRequest.OptionsCase != StartRequest.OptionsOneofCase.PubsubOptions)
                    {
                        throw new InvalidOperationException($"Invalid pubsub options: '{startRequest.OptionsCase}'");
                    }
                    var now = DateTime.UtcNow;
                    stats.Init(now, startRequest.IncludeIds);
                    var waitDuration = startRequest.StartTime.ToDateTime() - now;
                    if (waitDuration > TimeSpan.Zero)
                    {
                        await Task.Delay(waitDuration);
                    }
                    switch (startRequest.ClientOptionsCase)
                    {
                        case StartRequest.ClientOptionsOneofCase.PublisherOptions:
                            await Publish(startRequest, stats);
                            break;
                        case StartRequest.ClientOptionsOneofCase.SubscriberOptions:
                            await Subscribe(startRequest, stats);
                            break;
                        default:
                            throw new InvalidOperationException($"Invalid client options case: '{startRequest.ClientOptionsCase}'");
                    }
                    await server.ShutdownAsync();
                    return 0;
                case NotParsed<Options> failure:
                    return 1;
                default:
                    return 2;
            }
        }

        static async Task Publish(StartRequest startRequest, Stats stats)
        {
            var topicName = new TopicName(startRequest.Project, startRequest.Topic);
            var settings = new PublisherClient.Settings
            {
                BatchingSettings = new BatchingSettings(
                    elementCountThreshold: startRequest.PublisherOptions.BatchSize,
                    byteCountThreshold: null,
                    delayThreshold: startRequest.PublisherOptions.BatchDuration.ToTimeSpan())
            };
            var client = await PublisherClient.CreateAsync(topicName, settings: settings);
            int msgsSent = 0;
            double rate = startRequest.PublisherOptions.Rate;
            var startTime = DateTime.UtcNow;
            var endTime = startTime + startRequest.TestDuration.ToTimeSpan();
            var msg = new byte[Math.Max(12, startRequest.PublisherOptions.MessageSize)];
            var rnd = new Random(1);
            var msgsInFlight = 0;
            const int MaxQueued = 1_000;
            while (true)
            {
                while (true)
                {
                    var now = DateTime.UtcNow;
                    if (now >= endTime)
                    {
                        stats.End(now);
                        return;
                    }
                    var expectedMsgsSent = (now - startTime).TotalSeconds * rate;
                    if ((rate == 0.0 || msgsSent < expectedMsgsSent) && Interlocked.Add(ref msgsInFlight, 0) < MaxQueued)
                    {
                        break;
                    }
                    await Task.Delay(TimeSpan.FromMilliseconds(2));
                }
                rnd.NextBytes(msg);
                var msgId = msgsSent;
                var publishTime = DateTime.UtcNow;
                Array.Copy(BitConverter.GetBytes(msgId), 0, msg, 0, 4);
                Array.Copy(BitConverter.GetBytes(publishTime.Ticks), 0, msg, 4, 8);
                Interlocked.Increment(ref msgsInFlight);
                Task pubTask = client.PublishAsync(msg);
                msgsSent += 1;
                pubTask = pubTask.ContinueWith(task =>
                {
                    stats.Record(DateTime.UtcNow - publishTime, task.IsFaulted, msgId);
                    Interlocked.Decrement(ref msgsInFlight);
                }, TaskContinuationOptions.ExecuteSynchronously);
            }
        }

        static async Task Subscribe(StartRequest startRequest, Stats stats)
        {
            var subscriptionName = new SubscriptionName(startRequest.Project, startRequest.PubsubOptions.Subscription);
            var client = await SubscriberClient.CreateAsync(subscriptionName);
            var recvTask = client.StartAsync((msg, ct) =>
            {
                var now = DateTime.UtcNow;
                var msgBytes = msg.Data.Take(12).ToArray();
                var msgId = BitConverter.ToInt32(msgBytes, 0);
                var publishTime = new DateTime(BitConverter.ToInt64(msgBytes, 4), DateTimeKind.Utc);
                stats.Record(now - publishTime, false, msgId);
                return Task.FromResult(SubscriberClient.Reply.Ack);
            });
            await Task.Delay(startRequest.TestDuration.ToTimeSpan());
            await client.StopAsync(TimeSpan.FromSeconds(2));
        }
    }
}
