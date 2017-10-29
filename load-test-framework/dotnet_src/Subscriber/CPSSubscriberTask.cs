ing Google.Cloud.PubSub.V1;
using Grpc.Core;
using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;


namespace Google.Pubsub.Loadtest
{
    class LoadtestWorkerImpl : LoadtestWorker.LoadtestWorkerBase
    {
        private static readonly DateTime Jan1st1970 = new DateTime(
            1970, 1, 1, 0, 0, 0, DateTimeKind.Utc);
        private Mutex mutex;
        private SimpleSubscriber client;
        private List<long> latencies;
        private List<MessageIdentifier> received_messages;

        public override Task<StartResponse> Start(StartRequest request,
                                                  ServerCallContext context)
        {
            mutex = new Mutex();
            latencies = new List<long>();
            received_messages = new List<MessageIdentifier>();
            client = SimpleSubscriber.CreateAsync(
                new SubscriptionName(request.Project,
                                     request.PubsubOptions.Subscription)
            ).Result;
            client.StartAsync((msg, ct) =>
            {
                long now = CurrentTimeMillis();
                var identifier = new MessageIdentifier();
                mutex.WaitOne();
                latencies.Add(now - long.Parse(msg.Attributes["sendTime"]));
                identifier.PublisherClientId =
                              long.Parse(msg.Attributes["clientId"]);
                identifier.SequenceNumber =
                              int.Parse(msg.Attributes["sequenceNumber"]);
                received_messages.Add(identifier);
                mutex.ReleaseMutex();
                return Task.FromResult(SimpleSubscriber.Reply.Ack);
            });
            return Task.FromResult(new StartResponse());
        }

        public static long CurrentTimeMillis()
        {
            return (long)(DateTime.UtcNow - Jan1st1970).TotalMilliseconds;
        }

        public override Task<ExecuteResponse> Execute(ExecuteRequest request,
                                                      ServerCallContext context)
        {
            ExecuteResponse executeResponse = new ExecuteResponse();
            mutex.WaitOne();
            executeResponse.Latencies.Add(latencies);
            executeResponse.ReceivedMessages.Add(received_messages);
            latencies.Clear();
            received_messages.Clear();
            mutex.ReleaseMutex();
            return Task.FromResult(executeResponse);
        }

        public static void Main(string[] args)
        {
            // Build server
            var server = new Server
            {
                Services =
                {
                    LoadtestWorker.BindService(new LoadtestWorkerImpl())
                },
                Ports =
                {
                    new ServerPort("localhost", 6000,
                                   ServerCredentials.Insecure)
                }
            };

            // Start server
            server.Start();
            Console.WriteLine("Server listening on port 6000.");
            var waiter = new ManualResetEvent(false);
            waiter.WaitOne();
        }
    }
}
