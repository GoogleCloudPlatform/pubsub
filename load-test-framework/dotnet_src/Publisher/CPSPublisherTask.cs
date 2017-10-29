// Copyright 2017 Google Inc. All Rights Reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

using Google.Api.Gax;
using Google.Protobuf;
using Grpc.Core;
using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;


namespace Google.Pubsub.Loadtest
{
    class LoadtestWorkerImpl : LoadtestWorker.LoadtestWorkerBase
    {
        private static readonly DateTime Jan1st1970 = new DateTime(1970, 1, 1, 0, 0, 0, DateTimeKind.Utc);
        private Mutex mutex;
        private Int64 sequenceNumber;
        private string clientId;
        private int batchSize;
        private Google.Cloud.PubSub.V1.SimplePublisher client;
        private ByteString messageData;
        private List<long> latencies;

        public override Task<StartResponse> Start(StartRequest request, ServerCallContext context)
        {
            Console.WriteLine("Start called.");
            mutex = new Mutex();
            sequenceNumber = 0;
            latencies = new List<long>();
            clientId = (new Random()).Next().ToString();
            messageData = ByteString.CopyFromUtf8(new string('A', request.MessageSize));
            batchSize = request.PublishBatchSize;
            Console.WriteLine("Initializing client.");
            try {
                client = Google.Cloud.PubSub.V1.SimplePublisher.CreateAsync(new Google.Cloud.PubSub.V1.TopicName(request.Project, request.Topic), null,
                    new Google.Cloud.PubSub.V1.SimplePublisher.Settings { BatchingSettings = new Google.Api.Gax.BatchingSettings(batchSize, 9500000 /* 9.5 MB */, TimeSpan.FromMilliseconds(request.PublishBatchDuration.Seconds * 1000 + request.PublishBatchDuration.Nanos / 1000000.0)) }).Result;
            } catch (Exception e) {
                Console.WriteLine("Error initializing client: " + e.ToString());
            }
            Console.WriteLine("Start returning.");
            return Task.FromResult(new StartResponse());
        }

        public static long CurrentTimeMillis()
        {
            return (long)(DateTime.UtcNow - Jan1st1970).TotalMilliseconds;
        }

        public override Task<ExecuteResponse> Execute(ExecuteRequest request, ServerCallContext context)
        {
            Console.WriteLine("Execute called.");
            Int64 batchSequenceNumber;
            ExecuteResponse executeResponse = new ExecuteResponse();
            mutex.WaitOne();
            batchSequenceNumber = sequenceNumber;
            executeResponse.Latencies.Add(latencies);
            latencies.Clear();
            sequenceNumber += 1;
            mutex.ReleaseMutex();
            long currentTimeMillis = CurrentTimeMillis();
            for (int i = 0; i < batchSize; ++i)
            {
                Google.Cloud.PubSub.V1.PubsubMessage message = new Google.Cloud.PubSub.V1.PubsubMessage
                {
                    Data = messageData,
                    Attributes = {
                        { "sendTime", currentTimeMillis.ToString () },
                        { "clientId", clientId },
                        { "sequenceNumber", (batchSequenceNumber * batchSize + i).ToString () }
                    }
                };
                client.PublishAsync(message).ContinueWith(
                    task => {
                        Console.WriteLine("Publish handler invoked.");
                        if (!task.IsFaulted)
                        {
                            long duration = CurrentTimeMillis() - currentTimeMillis;
                            mutex.WaitOne();
                            latencies.Add(duration);
                            mutex.ReleaseMutex();
                        }
                        Console.WriteLine("Publish handler done.");
                    }
                );
            }
            Console.WriteLine("Execute returned.");
            return Task.FromResult(executeResponse);
        }

        public static void Main(string[] args)
        {
            // Build server
            var server = new Server
            {
                Services = { LoadtestWorker.BindService(new LoadtestWorkerImpl()) },
                Ports = { new ServerPort("localhost", 6000, ServerCredentials.Insecure) }
            };

            // Start server
            server.Start();
            Console.WriteLine("Server listening on port 6000.");
            var waiter = new ManualResetEvent(false);
            waiter.WaitOne();
            Console.WriteLine("Server shutting down.");
        }
    }
}
