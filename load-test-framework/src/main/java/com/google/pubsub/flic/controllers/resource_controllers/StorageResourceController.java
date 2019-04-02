/*
 * Copyright 2019 Google Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * You may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions
 * and limitations under the License.
 */

package com.google.pubsub.flic.controllers.resource_controllers;

import com.google.api.client.googleapis.json.GoogleJsonResponseException;
import com.google.api.client.http.FileContent;
import com.google.api.client.util.Base64;
import com.google.api.services.storage.Storage;
import com.google.api.services.storage.model.Bucket;
import org.apache.commons.codec.digest.DigestUtils;
import org.apache.http.HttpStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ScheduledExecutorService;
import java.util.stream.Collectors;

public class StorageResourceController extends ResourceController {
  protected static final Logger log = LoggerFactory.getLogger(StorageResourceController.class);
  private final String project;
  private final String resourceDirectory;
  private final boolean cleanUpOnShutdown;
  private final boolean uploadJava;
  private final Storage storage;

  public StorageResourceController(
      String project,
      String resourceDirectory,
      boolean cleanUpOnShutdown,
      boolean uploadJava,
      ScheduledExecutorService executor,
      Storage storage) {
    super(executor);
    this.project = project;
    this.resourceDirectory = resourceDirectory;
    this.cleanUpOnShutdown = cleanUpOnShutdown;
    this.uploadJava = uploadJava;
    this.storage = storage;
  }

  public static String bucketName(String projectName) {
    return projectName + "-cloud-pubsub-loadtest";
  }

  @Override
  protected void startAction() throws Exception {
    createStorageBucket();
    List<Path> paths =
        Files.walk(Paths.get(resourceDirectory))
            .filter(Files::isRegularFile)
            .collect(Collectors.toList());
    for (Path path : paths) {
      if (path.toString().endsWith(".sh")) {
        uploadFile(path);
      }
    }
    uploadFile(Paths.get(resourceDirectory, "cps.zip"));
    if (uploadJava) {
      // The jar file is large, only upload if necessary.
      uploadFile(Paths.get(resourceDirectory, "driver.jar"));
    }
  }

  private void createStorageBucket() throws IOException {
    try {
      storage.buckets().insert(project, new Bucket().setName(bucketName(project))).execute();
    } catch (GoogleJsonResponseException e) {
      if (e.getStatusCode() != HttpStatus.SC_CONFLICT) {
        throw e;
      }
    }
  }

  /** Uploads a given file to Google Storage. */
  private void uploadFile(Path filePath) throws IOException {
    try {
      byte[] md5hash =
          Base64.decodeBase64(
              storage
                  .objects()
                  .get(bucketName(project), filePath.getFileName().toString())
                  .execute()
                  .getMd5Hash());
      try (InputStream inputStream = Files.newInputStream(filePath, StandardOpenOption.READ)) {
        if (Arrays.equals(md5hash, DigestUtils.md5(inputStream))) {
          log.info("File " + filePath.getFileName() + " is current, reusing.");
          return;
        }
      }
      log.info("File " + filePath.getFileName() + " is out of date, uploading new version.");
      storage.objects().delete(bucketName(project), filePath.getFileName().toString()).execute();
    } catch (GoogleJsonResponseException e) {
      if (e.getStatusCode() != HttpStatus.SC_NOT_FOUND) {
        throw e;
      }
    }

    storage
        .objects()
        .insert(
            bucketName(project),
            null,
            new FileContent("application/octet-stream", filePath.toFile()))
        .setName(filePath.getFileName().toString())
        .execute();
    log.info("File " + filePath.getFileName() + " created.");
  }

  @Override
  protected void stopAction() throws Exception {
    if (cleanUpOnShutdown) {
      log.info("Cleaning up storage buckets.");
      storage.buckets().delete(bucketName(project));
    }
  }
}
