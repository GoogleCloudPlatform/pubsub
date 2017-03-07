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

package com.google.pubsub.flic.output;

import com.google.api.client.auth.oauth2.Credential;
import com.google.api.client.extensions.java6.auth.oauth2.AuthorizationCodeInstalledApp;
import com.google.api.client.extensions.jetty.auth.oauth2.LocalServerReceiver;
import com.google.api.client.googleapis.auth.oauth2.GoogleAuthorizationCodeFlow;
import com.google.api.client.googleapis.auth.oauth2.GoogleClientSecrets;
import com.google.api.client.googleapis.javanet.GoogleNetHttpTransport;
import com.google.api.client.http.HttpTransport;
import com.google.api.client.json.JsonFactory;
import com.google.api.client.json.jackson2.JacksonFactory;
import com.google.api.client.util.store.FileDataStoreFactory;
import com.google.api.services.sheets.v4.Sheets;
import com.google.api.services.sheets.v4.SheetsScopes;
import com.google.api.services.sheets.v4.model.ValueRange;
import com.google.pubsub.flic.common.LatencyDistribution;
import com.google.pubsub.flic.controllers.Client;
import com.google.pubsub.flic.controllers.Client.ClientType;
import com.google.pubsub.flic.controllers.ClientParams;
import com.google.pubsub.flic.controllers.Controller;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.Charset;
import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.LongStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Outputs load test results to Google Sheets.
 */
public class SheetsService {
  private static final Logger log = LoggerFactory.getLogger(SheetsService.class);
  private static final String APPLICATION_NAME = "loadtest-framework";
  private final Sheets service;
  private int cpsPublisherCount = 0;
  private int cpsSubscriberCount = 0;
  private int kafkaPublisherCount = 0;
  private int kafkaSubscriberCount = 0;
  private String dataStoreDirectory;

  public SheetsService(String dataStoreDirectory, Map<String, Map<ClientParams, Integer>> types) {
    this.dataStoreDirectory = dataStoreDirectory;
    this.service = authorize();
    fillClientCounts(types);
  }

  private void fillClientCounts(Map<String, Map<ClientParams, Integer>> types) {
    types.values().forEach(paramsMap -> {
      Map<ClientType, Integer> countMap = paramsMap.keySet().stream().
          collect(Collectors.groupingBy(
              ClientParams::getClientType, Collectors.summingInt(t -> 1)));
      cpsPublisherCount += (countMap.get(ClientType.CPS_GCLOUD_JAVA_PUBLISHER) != null) ? countMap.get(ClientType.CPS_GCLOUD_JAVA_PUBLISHER) : 0;
      cpsPublisherCount += (countMap.get(ClientType.CPS_GCLOUD_PYTHON_PUBLISHER) != null) ? countMap.get(ClientType.CPS_GCLOUD_PYTHON_PUBLISHER) : 0;
      cpsPublisherCount += (countMap.get(ClientType.CPS_EXPERIMENTAL_JAVA_PUBLISHER) != null) ? countMap.get(ClientType.CPS_EXPERIMENTAL_JAVA_PUBLISHER): 0;
      cpsPublisherCount += (countMap.get(ClientType.CPS_VTK_JAVA_PUBLISHER) != null) ? countMap.get(ClientType.CPS_VTK_JAVA_PUBLISHER) : 0;
      cpsPublisherCount += (countMap.get(ClientType.CPS_MAPPED_JAVA_PUBLISHER) != null) ? countMap.get(ClientType.CPS_MAPPED_JAVA_PUBLISHER) : 0;
      cpsSubscriberCount += (countMap.get(ClientType.CPS_GCLOUD_JAVA_SUBSCRIBER) != null) ? countMap.get(ClientType.CPS_GCLOUD_JAVA_SUBSCRIBER): 0;
      cpsSubscriberCount += (countMap.get(ClientType.CPS_EXPERIMENTAL_JAVA_SUBSCRIBER) != null) ? countMap.get(ClientType.CPS_EXPERIMENTAL_JAVA_SUBSCRIBER): 0;
      kafkaPublisherCount += (countMap.get(ClientType.KAFKA_PUBLISHER) != null) ? countMap.get(ClientType.KAFKA_PUBLISHER) : 0;
      kafkaSubscriberCount += (countMap.get(ClientType.KAFKA_SUBSCRIBER) != null) ? countMap.get(ClientType.KAFKA_SUBSCRIBER) : 0;
    });
  }

  private Sheets authorize() {
    try {
      InputStream in = new FileInputStream(new File(System.getenv("GOOGLE_OATH2_CREDENTIALS")));
      JsonFactory factory = new JacksonFactory();
      GoogleClientSecrets clientSecrets =
          GoogleClientSecrets.load(factory, new InputStreamReader(in, Charset.defaultCharset()));
      HttpTransport transport = GoogleNetHttpTransport.newTrustedTransport();
      FileDataStoreFactory dataStoreFactory =
          new FileDataStoreFactory(new File(dataStoreDirectory));
      List<String> scopes = Collections.singletonList(SheetsScopes.SPREADSHEETS);
      GoogleAuthorizationCodeFlow flow =
          new GoogleAuthorizationCodeFlow.Builder(transport, factory, clientSecrets, scopes)
              .setAccessType("offline")
              .setDataStoreFactory(dataStoreFactory)
              .build();
      Credential credential =
          new AuthorizationCodeInstalledApp(flow, new LocalServerReceiver()).authorize("user");
      return new Sheets.Builder(transport, factory, credential)
          .setApplicationName(APPLICATION_NAME)
          .build();
    } catch (Exception e) {
      return null;
    }
  }

  /* Publishes stats information to Google Sheets document. Format for sheet assumes the following
   * column order: Publisher #; Subscriber #; Message size (B); Test length (s); # messages;
   * Publish batch size; Subscribe pull size; Request rate; Max outstanding requests;
   * Throughput (MB/s); 50% (ms); 90% (ms); 99% (ms)
   */
  public void sendToSheets(String sheetId, Map<ClientType, Controller.LoadtestStats> results) {
    List<List<List<Object>>> values = getValuesList(results);
    try {
      service.spreadsheets().values().append(sheetId, "CPS",
          new ValueRange().setValues(values.get(0))).setValueInputOption("USER_ENTERED").execute();
      service.spreadsheets().values().append(sheetId, "Kafka",
          new ValueRange().setValues(values.get(1))).setValueInputOption("USER_ENTERED").execute();
    } catch (IOException e) {
      log.error("Error publishing to spreadsheet " + sheetId + ": " + e);
    }
  }

  public List<List<List<Object>>> getValuesList(Map<ClientType, Controller.LoadtestStats> results) {
    List<List<Object>> cpsValues = new ArrayList<>(results.size());
    List<List<Object>> kafkaValues = new ArrayList<>(results.size());

    results.forEach((type, stats) -> {
      List<Object> valueRow = new ArrayList<>(13);
      switch (type) {
        case CPS_EXPERIMENTAL_JAVA_PUBLISHER:
        case CPS_GCLOUD_JAVA_PUBLISHER:
        case CPS_GCLOUD_PYTHON_PUBLISHER:
        case CPS_VTK_JAVA_PUBLISHER:
          if (cpsPublisherCount == 0) {
            return;
          }
          valueRow.add(cpsPublisherCount);
          valueRow.add(0);
          cpsValues.add(0, valueRow);
          break;
        case CPS_EXPERIMENTAL_JAVA_SUBSCRIBER:
        case CPS_GCLOUD_JAVA_SUBSCRIBER:
          if (cpsSubscriberCount == 0) {
            return;
          }
          valueRow.add(0);
          valueRow.add(cpsSubscriberCount);
          cpsValues.add(valueRow);
          break;
        case KAFKA_PUBLISHER:
          if (kafkaPublisherCount == 0) {
            return;
          }
          valueRow.add(kafkaPublisherCount);
          valueRow.add(0);
          kafkaValues.add(0, valueRow);
          break;
        case KAFKA_SUBSCRIBER:
          if (kafkaSubscriberCount == 0) {
            return;
          }
          valueRow.add(0);
          valueRow.add(kafkaSubscriberCount);
          kafkaValues.add(valueRow);
          break;
        case CPS_MAPPED_JAVA_PUBLISHER:
          return;
        default:
          throw new IllegalArgumentException("Type " + type + " in results map was not expected.");
      }
      valueRow.add(Client.messageSize);
      if (Client.numberOfMessages <= 0) {
        valueRow.add(Client.loadtestDuration.getSeconds());
        valueRow.add("N/A");
      } else {
        valueRow.add("N/A");
        valueRow.add(Client.numberOfMessages);
      }
      valueRow.add(Client.publishBatchSize);
      valueRow.add(Client.maxMessagesPerPull);
      valueRow.add(Client.requestRate);
      valueRow.add(Client.maxOutstandingRequests);
      valueRow.add(new DecimalFormat("#.##").format(
          (double) LongStream.of(
              stats.bucketValues).sum() / stats.runningSeconds * Client.messageSize / 1000000.0));
      valueRow.add(LatencyDistribution.getNthPercentileMidpoint(stats.bucketValues, 50.0));
      valueRow.add(LatencyDistribution.getNthPercentileMidpoint(stats.bucketValues, 99.0));
      valueRow.add(LatencyDistribution.getNthPercentileMidpoint(stats.bucketValues, 99.9));
    });
    List<List<List<Object>>> out = new ArrayList<>();
    out.add(cpsValues);
    out.add(kafkaValues);
    return out;
  }

  /**
   * @return the cpsPublisherCount
   */
  int getCpsPublisherCount() {
    return cpsPublisherCount;
  }

  /**
   * @return the cpsSubscriberCount
   */
  int getCpsSubscriberCount() {
    return cpsSubscriberCount;
  }

  /**
   * @return the kafkaPublisherCount
   */
  int getKafkaPublisherCount() {
    return kafkaPublisherCount;
  }

  /**
   * @return the kafkaSubscriberCount
   */
  int getKafkaSubscriberCount() {
    return kafkaSubscriberCount;
  }
}
