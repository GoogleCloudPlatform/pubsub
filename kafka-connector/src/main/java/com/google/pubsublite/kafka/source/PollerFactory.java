package com.google.pubsublite.kafka.source;

import java.util.Map;

interface PollerFactory {

  Poller newPoller(Map<String, String> params);
}
