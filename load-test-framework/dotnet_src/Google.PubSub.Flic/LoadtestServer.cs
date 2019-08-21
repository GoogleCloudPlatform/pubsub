namespace Google.PubSub.Flic
{
    class LoadtestServer : Pubsub.Loadtest.LoadtestWorker.LoadtestWorkerBase
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
}
