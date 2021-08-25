package com.google.cloud.pubsub.sql;

import org.apache.beam.runners.dataflow.DataflowRunner;
import org.apache.beam.runners.dataflow.options.DataflowPipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;

/**
 * The main class used in the dataflow template. Cannot be run locally.
 */
public class TemplateMain {
  public interface Options extends DataflowPipelineOptions, TemplateOptions {}

  public static void main(final String[] args) {
    Options options = PipelineOptionsFactory.fromArgs(args).withValidation().as(Options.class);
    options.setEnableStreamingEngine(true);
    options.setRunner(DataflowRunner.class);
    RunPipeline.run(options);
  }
}
