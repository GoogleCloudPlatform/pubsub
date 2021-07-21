package com.google.cloud.pubsub.sql;

import org.apache.beam.sdk.extensions.sql.impl.BeamSqlPipelineOptions;
import org.apache.beam.sdk.options.StreamingOptions;

public interface SqlStreamingOptions extends StreamingOptions, BeamSqlPipelineOptions {}
