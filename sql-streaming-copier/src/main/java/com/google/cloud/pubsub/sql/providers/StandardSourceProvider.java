package com.google.cloud.pubsub.sql.providers;

import org.apache.beam.sdk.schemas.io.Providers.Identifyable;

public interface StandardSourceProvider extends Identifyable {
  StandardSqlSource getSource();
}
