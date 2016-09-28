// Copyright 2016 Google Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
//
////////////////////////////////////////////////////////////////////////////////
package com.google.pubsub.flic.common;

import com.beust.jcommander.IParameterValidator;
import com.beust.jcommander.ParameterException;
import com.google.auth.oauth2.GoogleCredentials;
import io.grpc.Channel;
import io.grpc.ClientInterceptors;
import io.grpc.auth.ClientAuthInterceptor;
import io.grpc.internal.ManagedChannelImpl;
import io.grpc.netty.NegotiationType;
import io.grpc.netty.NettyChannelBuilder;
import java.io.File;
import java.io.FileOutputStream;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Random;
import java.util.concurrent.Executors;
import org.apache.commons.io.FileUtils;

/** A collection of common methods/enums/constants. */
public class Utils {

  private static final String ENDPOINT = "pubsub.googleapis.com";
  private static final List<String> CPS_SCOPE =
      Collections.singletonList("https://www.googleapis.com/auth/pubsub");

  public static final String BASE_FILEDUMP_PATH = "data/";
  public static final String CPS_FILEDUMP_PATH = BASE_FILEDUMP_PATH + "CPS";
  public static final String KAFKA_FILEDUMP_PATH = BASE_FILEDUMP_PATH + "KAFKA";
  public static final String KEY_ATTRIBUTE = "key";
  public static final String TIMESTAMP_ATTRIBUTE = "timestamp";
  public static final String CPS_TOPIC_FORMAT = "projects/%s/topics/%s";
  public static final String CPS_SUBSCRIPTION_FORMAT = "projects/%s/subscriptions/%s";

  /** A validator that makes sure the parameter is an integer that is greater than 0. */
  public static class GreaterThanZeroValidator implements IParameterValidator {

    @Override
    public void validate(String name, String value) throws ParameterException {
      try {
        int n = Integer.parseInt(value);
        if (n > 0) return;
      } catch (NumberFormatException e) { } // Parameter was not a number
      throw new ParameterException(
            "Parameter " + name + " should be an int greater than 0 (found " + value + ")");
    }
  }

  /** Creates a random string message of a certain size. */
  public static String createMessage(int msgSize, long seed) {
    StringBuilder sb = new StringBuilder(msgSize);
    Random r = new Random();
    r.setSeed(seed);
    for (int i = 0; i < msgSize; ++i) {
      sb.append((char) (r.nextInt(26) + 'a'));
    }
    sb.append('_');
    return sb.toString();
  }

  /**
   * Writes the given buffer of {@link MessagePacketProto.MessagePacket}'s to the specified file.
   */
  public static synchronized void writeToFile(
      List<MessagePacketProto.MessagePacket> buffer, File filedump) throws Exception {
    FileOutputStream os = FileUtils.openOutputStream(filedump, true);
    for (MessagePacketProto.MessagePacket mp : buffer) {
      mp.writeDelimitedTo(os);
    }
    os.close();
    os.flush();
  }

  /** Return a {@link io.grpc.Channel} for use with Cloud Pub/Sub gRPC API. */
  public static Channel createChannel() throws Exception {
    final ManagedChannelImpl channelImpl =
        NettyChannelBuilder.forAddress(ENDPOINT, 443).negotiationType(NegotiationType.TLS).build();
    final ClientAuthInterceptor interceptor =
        new ClientAuthInterceptor(
            GoogleCredentials.getApplicationDefault().createScoped(CPS_SCOPE),
            Executors.newCachedThreadPool());
    return ClientInterceptors.intercept(channelImpl, interceptor);
  }

  /** Creates a full Cloud Pub/Sub topic name based on a single word topic and the project name. */
  public static String getCPSTopic(String topic, String cpsProject) {
    return String.format(Utils.CPS_TOPIC_FORMAT, cpsProject, topic);
  }
  
  public static String getCPSSubscription(String subscription, String cpsProject) {
    return String.format(Utils.CPS_SUBSCRIPTION_FORMAT, cpsProject, subscription);
  }
}
