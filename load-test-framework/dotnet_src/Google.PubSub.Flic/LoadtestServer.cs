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

using Google.Pubsub.Loadtest;
using Grpc.Core;
using System;
using System.Threading.Tasks;

namespace Google.PubSub.Flic
{
    internal class LoadtestServer : LoadtestWorker.LoadtestWorkerBase
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
