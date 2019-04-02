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

package com.google.pubsub.clients.flow_control;

public interface FlowController {
  /**
   * Request starting a flow controlled action, block until allowed. Return the number of requests
   * allowed.
   */
  int requestStart();

  /** Inform the FlowController that an action has finished. */
  void informFinished(boolean wasSuccessful);
}
