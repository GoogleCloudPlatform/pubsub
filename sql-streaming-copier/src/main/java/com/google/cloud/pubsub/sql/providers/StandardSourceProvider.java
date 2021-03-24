package com.google.cloud.pubsub.sql.providers;

import org.apache.beam.sdk.schemas.io.Providers.Identifyable;

/**
 * Additional SourceProviders can be added by implementing this interface and annotating it with
 * AutoService.
 */
public interface StandardSourceProvider extends Identifyable {

  StandardSource getSource();
}
