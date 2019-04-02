// Copyright 2018 Google Inc.
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
package com.google.pubsub.kafka.common;

import com.google.api.gax.core.CredentialsProvider;
import com.google.auth.Credentials;
import com.google.auth.oauth2.GoogleCredentials;
import java.io.ByteArrayInputStream;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;

public class ConnectorCredentialsProvider implements CredentialsProvider {

  private static final List<String> CPS_SCOPE =
    Arrays.asList("https://www.googleapis.com/auth/pubsub");

  GoogleCredentials credentials;

  public void loadFromFile(String credentialPath) throws IOException {
    this.credentials = GoogleCredentials.fromStream(new FileInputStream(credentialPath));
  }

  public void loadJson(String credentialsJson) throws IOException {
    ByteArrayInputStream bs = new ByteArrayInputStream(credentialsJson.getBytes());
    this.credentials = credentials = GoogleCredentials.fromStream(bs);
  }

  @Override
  public Credentials getCredentials() throws IOException {
    if (this.credentials == null) {
      return GoogleCredentials.getApplicationDefault().createScoped(this.CPS_SCOPE);
    } else {
      return this.credentials.createScoped(this.CPS_SCOPE);
    }
  }

}
