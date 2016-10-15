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
import com.google.pubsub.flic.controllers.Controller;
import com.google.pubsub.flic.controllers.Client;
import com.google.pubsub.flic.controllers.Client.ClientType;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.LongStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SheetsService {
  private final static Logger log = LoggerFactory.getLogger(SheetsService.class);
  private Sheets service;
  
  private final String APPLICATION_NAME = "loadtest-framework";
  
  public SheetsService(String dataStoreDirectory) throws Exception {
    InputStream in = new FileInputStream(new File(System.getenv("GOOGLE_OATH2_CREDENTIALS")));
    JsonFactory factory = new JacksonFactory();
    GoogleClientSecrets clientSecrets = GoogleClientSecrets.load(factory, new InputStreamReader(in));
    HttpTransport transport = GoogleNetHttpTransport.newTrustedTransport(); 
    FileDataStoreFactory dataStoreFactory = new FileDataStoreFactory(new File(dataStoreDirectory));
    List<String> scopes = Arrays.asList(SheetsScopes.SPREADSHEETS);
    GoogleAuthorizationCodeFlow flow =
        new GoogleAuthorizationCodeFlow.Builder(
                transport, factory, clientSecrets, scopes)
        .setAccessType("offline")
        .setDataStoreFactory(dataStoreFactory)
        .build();
    Credential credential = new AuthorizationCodeInstalledApp(
        flow, new LocalServerReceiver()).authorize("user");
    service = new Sheets.Builder(transport, factory, credential)
        .setApplicationName(APPLICATION_NAME).build();
  }
  
  /* Publishes stats information to Google Sheets document. Format for sheet assumes the following
   * column order: Publisher #; Subscriber #; Message size (B); Test length (s); # messages;
   * Publish batch size; Subscribe pull size; Request rate; Max outstanding requests; 
   * Throughput (MB/s); 50% (ms); 90% (ms); 99% (ms)
   */
  public void sendToSheets(String sheetId, Map<ClientType, Controller.LoadtestStats> results) {
    List<List<Object>> cpsValues = new ArrayList<List<Object>>(results.size());
    List<List<Object>> kafkaValues = new ArrayList<List<Object>>(results.size());
    results.forEach((type, stats) -> {
      List<Object> valueRow = new ArrayList<Object>(13);
      switch (type) {
        case CPS_GCLOUD_PUBLISHER:
          if (Client.cpsPublisherCount == 0) return;
          valueRow.add(Client.cpsPublisherCount);
          valueRow.add(0);
          cpsValues.add(valueRow);
          break;
        case CPS_GCLOUD_SUBSCRIBER:
          if (Client.cpsSubscriberCount == 0) return;
          valueRow.add(0);
          valueRow.add(Client.cpsSubscriberCount);
          cpsValues.add(valueRow);
          break;
        case KAFKA_PUBLISHER:
          if (Client.kafkaPublisherCount == 0) return;
          valueRow.add(Client.kafkaPublisherCount);
          valueRow.add(0);
          kafkaValues.add(valueRow);
          break;
        case KAFKA_SUBSCRIBER:
          if (Client.kafkaSubscriberCount == 0) return;
          valueRow.add(0);
          valueRow.add(Client.kafkaSubscriberCount);
          kafkaValues.add(valueRow);
          break;
      }
      valueRow.add(Client.messageSize);
      if (Client.numberOfMessages <= 0) {
        valueRow.add(Client.loadtestLengthSeconds);
        valueRow.add("N/A");
      }
      else {
        valueRow.add("N/A");
        valueRow.add(Client.numberOfMessages);
      }
      valueRow.add(Client.cpsPublishBatchSize);
      valueRow.add(Client.maxMessagesPerPull);
      valueRow.add(Client.requestRate);
      valueRow.add(Client.maxOutstandingRequests);
      valueRow.add(new DecimalFormat("#.##").format(
          (double) LongStream.of(
              stats.bucketValues).sum() / stats.runningSeconds * Client.messageSize / 1000000.0
              * (type.isCpsPublisher() ? Client.cpsPublishBatchSize : 1)));
      valueRow.add(LatencyDistribution.getNthPercentile(stats.bucketValues, 50.0));
      valueRow.add(LatencyDistribution.getNthPercentile(stats.bucketValues, 95.0));
      valueRow.add(LatencyDistribution.getNthPercentile(stats.bucketValues, 99.0));
    });
    try {
      service.spreadsheets().values().append(sheetId, "CPS", 
          new ValueRange().setValues(cpsValues)).setValueInputOption("USER_ENTERED").execute();
      service.spreadsheets().values().append(sheetId, "Kafka", 
          new ValueRange().setValues(kafkaValues)).setValueInputOption("USER_ENTERED").execute();
    } catch (IOException e) {
      log.error("Error publishing to spreadsheet: " + sheetId);
    }
  }
}