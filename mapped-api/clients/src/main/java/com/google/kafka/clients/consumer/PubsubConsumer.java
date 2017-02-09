/**
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements. See the NOTICE
 * file distributed with this work for additional information regarding copyright ownership. The ASF licenses this file
 * to You under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the
 * License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 */
package com.google.kafka.clients.consumer;

public class PubsubConsumer<K, V> implements Consumer<K, V> {

  private static final Logger log = LoggerFactory.getLogger(PubsubConsumer.class);
  private static final long NO_CURRENT_THREAD = -1L;
  private static final AtomicInteger CONSUMER_CLIENT_ID_SEQUENCE = new AtomicInteger(1);
  private static final String JMX_PREFIX = "cps.consumer";
  static final long DEFAULT_CLOSE_TIMEOUT_MS = 30 * 1000;

  private final String clientId;
  private final ConsumerCoordinator coordinator;  // this might need to be an /internal class
  private final Deserializer<K> keyDeserializer;
  private final Deserializer<V> valueDeserializer;
  private final Fetcher<K, V> fetcher;


}