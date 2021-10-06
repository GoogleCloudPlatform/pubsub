/*
 * Copyright 2021 Google LLC
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.google.cloud.pubsub.republisher.mq;

import com.google.common.flogger.GoogleLogger;
import com.hivemq.extension.sdk.api.interceptor.subscribe.SubscribeInboundInterceptor;
import com.hivemq.extension.sdk.api.interceptor.subscribe.parameter.SubscribeInboundInput;
import com.hivemq.extension.sdk.api.interceptor.subscribe.parameter.SubscribeInboundOutput;

public class StopSubscribeInterceptor implements SubscribeInboundInterceptor {
  private final GoogleLogger logger = GoogleLogger.forEnclosingClass();

  @Override
  public void onInboundSubscribe(
      SubscribeInboundInput subscribeInboundInput,
      SubscribeInboundOutput subscribeInboundOutput) {
    logger.atSevere().log("Subscribe sent with {} to pubsub republisher- this is not allowed.", subscribeInboundInput.getSubscribePacket());
    subscribeInboundOutput.getSubscribePacket().getSubscriptions().clear();
  }
}
