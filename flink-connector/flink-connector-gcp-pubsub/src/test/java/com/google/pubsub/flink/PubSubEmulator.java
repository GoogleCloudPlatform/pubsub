/*
 * Copyright 2026 Google LLC
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

import org.testcontainers.containers.PubSubEmulatorContainer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.utility.DockerImageName;

/** One Pub/Sub emulator container shared by all integration tests in the JVM. Requires Docker. */
final class PubSubEmulator {
  private static final PubSubEmulatorContainer CONTAINER =
      new PubSubEmulatorContainer(
              DockerImageName.parse("gcr.io/google.com/cloudsdktool/google-cloud-cli:emulators"))
          // The port opens before the emulator answers HTTP; wait for a real response.
          .waitingFor(Wait.forHttp("/v1/projects/probe/topics").forStatusCode(200));

  static {
    // Testcontainers removes the container when the JVM exits.
    CONTAINER.start();
  }

  private PubSubEmulator() {}

  /** Returns the emulator's {@code host:port}. */
  static String endpoint() {
    return CONTAINER.getEmulatorEndpoint();
  }
}
