package com.google.cloud.pubsub.sql;

import java.util.Map;
import javax.annotation.Nullable;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.extensions.sql.zetasql.ZetaSQLQueryPlanner;
import org.apache.beam.sdk.options.PipelineOptionsFactory;

public class Main {
  private static Map<String, String> getNonNull(@Nullable Map<String, String> map) {
    if (map == null) return Map.of();
    return map;
  }

  public static void main(final String[] args) {
    Options options = PipelineOptionsFactory.fromArgs(args).withValidation().as(Options.class);
    options.setStreaming(true);
    options.setPlannerName(ZetaSQLQueryPlanner.class.getName());
    TableSpec sourceSpec =
        TableSpec.builder()
            .setId(options.getSourceType())
            .setLocation(options.getSourceLocation())
            .setProperties(getNonNull(options.getSourceOptions()))
            .build();
    TableSpec sinkSpec =
        TableSpec.builder()
            .setId(options.getSinkType())
            .setLocation(options.getSinkLocation())
            .setProperties(getNonNull(options.getSinkOptions()))
            .build();
    Pipeline pipeline = Pipeline.create(options);
    Compiler.compile(pipeline, sourceSpec, sinkSpec);
    // For a Dataflow Flex Template, do NOT waitUntilFinish().
    pipeline.run();
  }
}
