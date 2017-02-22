/**
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license
 * agreements. See the NOTICE file distributed with this work for additional information regarding
 * copyright ownership. The ASF licenses this file to You under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */

package com.google.pubsub.common;

import com.google.auth.oauth2.GoogleCredentials;
import io.grpc.Channel;
import io.grpc.ClientInterceptors;
import io.grpc.auth.ClientAuthInterceptor;
import io.grpc.internal.ManagedChannelImpl;
import io.grpc.netty.NegotiationType;
import io.grpc.netty.NettyChannelBuilder;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.Executors;

public class PubsubUtils {

  private static final String ENDPOINT = "pubsub.googleapis.com";
  private static final List<String> CPS_SCOPE = Arrays.asList("https://www.googleapis.com/auth/pubsub");

  public static final String CPS_TOPIC_FORMAT = "projects/%s/topics/%s";
  public static final String KEY_ATTRIBUTE = "key";
  public static final String NO_PROJECT_ERROR = "No project specified. Use AdapterClient to specify project.";

  public static Channel createChannel() throws Exception {
    final ManagedChannelImpl channelImpl = NettyChannelBuilder.forAddress(ENDPOINT, 443).negotiationType(NegotiationType.TLS).build();
    final ClientAuthInterceptor interceptor =
        new ClientAuthInterceptor(
            GoogleCredentials.getApplicationDefault().createScoped(CPS_SCOPE),
            Executors.newCachedThreadPool());
    return ClientInterceptors.intercept(channelImpl, interceptor);
  }
}