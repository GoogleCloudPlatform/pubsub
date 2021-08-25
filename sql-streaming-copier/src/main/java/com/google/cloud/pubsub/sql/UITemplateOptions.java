package com.google.cloud.pubsub.sql;

import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.Validation.Required;

public interface UITemplateOptions extends SqlStreamingOptions {
  @Description("Source spec JSON. Structured as {\"id\":\"<id>\",\"location\":\"<location>\",\"properties\":{...}}")
  @Required()
  String getSourceSpec();
  void setSourceSpec(String type);

  @Description("Sink spec JSON. Structured as {\"id\":\"<id>\",\"location\":\"<location>\",\"properties\":{...}}")
  @Required()
  String getSinkSpec();
  void setSinkSpec(String type);
}
