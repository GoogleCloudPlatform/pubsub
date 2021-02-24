package com.google.cloud.pubsub.sql;

import java.util.Map;
import javax.annotation.Nullable;
import org.apache.beam.sdk.extensions.sql.impl.BeamSqlPipelineOptions;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.StreamingOptions;
import org.apache.beam.sdk.options.Validation.Required;

public interface Options extends StreamingOptions, BeamSqlPipelineOptions {
  @Description("Type of the source. Valid types are: [pubsub, pubsublite, kafka]")
  @Required()
  String getSourceType();

  void setSourceType(String type);

  @Description("Location within the source to read from.")
  @Required()
  String getSourceLocation();

  void setSourceLocation(String location);

  @Description("Additional options to pass to the source.")
  @Nullable
  Map<String, String> getSourceOptions();

  void setSourceOptions(Map<String, String> options);

  @Description("Type of the sink. Valid types are: [pubsub, pubsublite, kafka, bigquery]")
  @Required()
  String getSinkType();

  void setSinkType(String type);

  @Description("Location within the sink to read from.")
  @Required()
  String getSinkLocation();

  void setSinkLocation(String location);

  @Description("Additional options to pass to the sink.")
  @Nullable
  Map<String, String> getSinkOptions();

  void setSinkOptions(Map<String, String> options);
}
