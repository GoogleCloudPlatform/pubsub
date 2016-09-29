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
package com.google.pubsub.clients.grpc;

import com.google.auth.oauth2.AccessToken;
import com.google.auth.oauth2.GoogleCredentials;
import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.Uninterruptibles;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

/**
 * An access token provider, that pre-fetches tokens before they expire.
 */
class AccessTokenProvider {

  private static final Logger log = LoggerFactory.getLogger(AccessTokenProvider.class.getName());
  private final BlockingQueue<AccessToken> accessTokens = new LinkedBlockingQueue<>();
  private final GoogleCredentials creds;

  public AccessTokenProvider() throws IOException {
    creds =
        GoogleCredentials.getApplicationDefault()
            .createScoped(ImmutableList.of("https://www.googleapis.com/auth/cloud-platform"));
    generateAccessToken();
  }

  public AccessToken getAccessToken() throws IOException {
    AccessToken at = Uninterruptibles.takeUninterruptibly(accessTokens);

    long accessTokenExpirationTime = at.getExpirationTime().getTime();
    if (accessTokenExpirationTime - 5000 < System.currentTimeMillis()) {
      generateAccessToken();
    }

    while (accessTokenExpirationTime - 1000 < System.currentTimeMillis()) {
      at = Uninterruptibles.takeUninterruptibly(accessTokens);
    }
    accessTokens.offer(at);
    return at;
  }

  public GoogleCredentials getCredentials() {
    return creds;
  }

  private void generateAccessToken() throws IOException {
    creds.refresh();
    AccessToken at = creds.getAccessToken();
    accessTokens.offer(at);
    log.info("Generated access token: " + at.getTokenValue());
  }
}
