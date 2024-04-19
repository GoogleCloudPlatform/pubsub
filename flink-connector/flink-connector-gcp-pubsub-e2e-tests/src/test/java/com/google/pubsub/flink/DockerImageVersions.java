/*
 * Copyright 2023 Google LLC
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

package com.google.pubsub.flink;

/**
 * Utility class for defining the image names and versions of Docker containers used during the Java
 * tests. The names/versions are centralised here in order to make testing version updates easier,
 * as well as to provide a central file to use as a key when caching testing Docker files.
 */
public class DockerImageVersions {
  public static final String GOOGLE_CLOUD_PUBSUB_EMULATOR =
      "gcr.io/google.com/cloudsdktool/cloud-sdk:379.0.0";
}
