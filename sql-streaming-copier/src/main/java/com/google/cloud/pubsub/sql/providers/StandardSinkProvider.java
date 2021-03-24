package com.google.cloud.pubsub.sql.providers;

import org.apache.beam.sdk.schemas.io.Providers.Identifyable;

/**
 * Additional SinkProviders can be added by implementing this interface and annotating it with
 * AutoService.
 */
public interface StandardSinkProvider extends Identifyable {

  StandardSink getSink();
}
