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

using CommandLine;
using Google.Api.Gax;
using Google.Cloud.PubSub.V1;
using Google.Protobuf;
using Google.Protobuf.WellKnownTypes;
using Google.Pubsub.Loadtest;
using Grpc.Core;
using System;
using System.Threading;
using System.Threading.Tasks;

namespace Google.PubSub.Flic
{
    public class Program
    {
        private class Options
        {
            [Option("port", Required = false)]
            public int Port { get; private set; } = 5000;
        }

        public static async Task<int> Main(string[] args)
        {
            var parsed = Parser.Default.ParseArguments<Options>(args);
            switch (parsed)
            {
                case Parsed<Options> success:
                    // Run the performance test.
                    await Run(success.Value);
                    // Do not exit the process after test has completed.
                    // The client still requires access to complete the perf-test.
                    // There is no method on the gRPC server instance to wait, so simply
                    // delay here for a long time.
                    await Task.Delay(TimeSpan.FromHours(6));
                    return 0;
                case NotParsed<Options> failure:
                    return 1;
                default:
                    return 2;
            }
        }

        private static async Task Run(Options options)
        {
            var startTcs = new TaskCompletionSource<StartRequest>();
            var stats = new Stats();
            Console.WriteLine($"Starting dotnet server on port:{options.Port}");
            var server = new Server
            {
                Services = { LoadtestWorker.BindService(new LoadtestServer(startTcs, stats.BuildCheckResponse)) },
                Ports = { new ServerPort("0.0.0.0", options.Port, ServerCredentials.Insecure) },
            };
            server.Start();
            var startRequest = await startTcs.Task;
            Console.WriteLine("Start perf test: " +
                $"Test duration:{(int)startRequest.TestDuration.ToTimeSpan().TotalSeconds}s; " +
                $"include-ids:{startRequest.IncludeIds}; " +
                $"CPU scaling:{startRequest.CpuScaling}");
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
                default: // no option is sent for subscribe
                    await Subscribe(startRequest, stats);
                    break;
            }
        }

        private static async Task Publish(StartRequest startRequest, Stats stats)
        {
            var topicName = new TopicName(startRequest.Project, startRequest.Topic);
            var clientCreationSettings = new PublisherClient.ClientCreationSettings(
                clientCount: (int)Math.Max(Environment.ProcessorCount * startRequest.CpuScaling, 1));
            var settings = new PublisherClient.Settings
            {
                BatchingSettings = new BatchingSettings(
                    elementCountThreshold: startRequest.PublisherOptions.BatchSize,
                    byteCountThreshold: null,
                    delayThreshold: startRequest.PublisherOptions.BatchDuration.ToTimeSpan())
            };
            var client = await PublisherClient.CreateAsync(topicName, clientCreationSettings,  settings);
            int msgsSent = 0;
            double rate = startRequest.PublisherOptions.Rate;
            var startTime = DateTime.UtcNow;
            var endTime = startTime + startRequest.TestDuration.ToTimeSpan();
            var dataBytes = new byte[startRequest.PublisherOptions.MessageSize];
            var rnd = new Random(1);
            rnd.NextBytes(dataBytes);
            var data = ByteString.CopyFrom(dataBytes);
            var msgsInFlight = 0;
            const int MaxQueued = 1_000_000;
            while (true)
            {
                while (true)
                {
                    // Inner loop to throttle publish rate on both requested publish rate, and local queue.
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
                    await Task.Delay(TimeSpan.FromMilliseconds(1));
                }
                var msgId = msgsSent;
                var publishTime = DateTime.UtcNow;
                Interlocked.Increment(ref msgsInFlight);
                var pubsubMsg = new PubsubMessage
                {
                    Data = data,
                    Attributes =
                    {
                        { "msgId", msgId.ToString() },
                        { "pubTime", publishTime.Ticks.ToString() },
                    }
                };
                Task pubTask = client.PublishAsync(pubsubMsg);
                msgsSent += 1;
                pubTask = pubTask.ContinueWith(task =>
                {
                    stats.Record(DateTime.UtcNow - publishTime, task.IsFaulted, msgId);
                    Interlocked.Decrement(ref msgsInFlight);
                }, TaskContinuationOptions.ExecuteSynchronously);
            }
        }

        private static async Task Subscribe(StartRequest startRequest, Stats stats)
        {
            var subscriptionName = new SubscriptionName(startRequest.Project, startRequest.PubsubOptions.Subscription);
            var clientCreationSettings = new SubscriberClient.ClientCreationSettings(
                clientCount: (int)Math.Max(Environment.ProcessorCount * startRequest.CpuScaling, 1));
            var settings = new SubscriberClient.Settings { FlowControlSettings = new FlowControlSettings(100_000, 100_000_000) };
            var client = await SubscriberClient.CreateAsync(subscriptionName, clientCreationSettings, settings);
            var recvTask = client.StartAsync((msg, ct) =>
            {
                var now = DateTime.UtcNow;
                var msgId = int.Parse(msg.Attributes["msgId"]);
                var publishTime = new DateTime(long.Parse(msg.Attributes["pubTime"]), DateTimeKind.Utc);
                stats.Record(now - publishTime, false, msgId);
                return Task.FromResult(SubscriberClient.Reply.Ack);
            });
            await Task.Delay(startRequest.TestDuration.ToTimeSpan());
            stats.End(DateTime.UtcNow);
        }
    }
}
