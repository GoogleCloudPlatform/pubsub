package com.google.cloud.pubsub.sql;

import java.util.Map;
import javax.annotation.Nullable;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.extensions.sql.zetasql.ZetaSQLQueryPlanner;
import org.apache.beam.sdk.options.PipelineOptionsFactory;

/**
 * A main class for running the pipeline locally.
 */
public class RunPipeline {

  private static Map<String, Object> getNonNull(@Nullable Map<String, Object> map) {
    if (map == null) {
      return Map.of();
    }
    return map;
  }

  public static void run(TemplateOptions options) {
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
    run(options, sourceSpec, sinkSpec);
  }

  public static void run(SqlStreamingOptions options, TableSpec sourceSpec, TableSpec sinkSpec) {
    options.setStreaming(true);
    options.setPlannerName(ZetaSQLQueryPlanner.class.getName());
    Pipeline pipeline = Pipeline.create(options);
    Compiler.compile(pipeline, sourceSpec, sinkSpec);
    // For a Dataflow Flex Template, do NOT waitUntilFinish().
    pipeline.run();
  }

  public static void main(final String[] args) {
    run(PipelineOptionsFactory.fromArgs(args).withValidation().as(TemplateOptions.class));
  }
}
