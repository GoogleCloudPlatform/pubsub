package com.google.cloud.pubsub.sql;

import org.apache.beam.runners.dataflow.DataflowRunner;
import org.apache.beam.runners.dataflow.options.DataflowPipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;

public class UITemplateMain {

  public interface Options extends DataflowPipelineOptions, UITemplateOptions {}

  public static void main(final String[] args) {
    Options options = PipelineOptionsFactory.fromArgs(args).withValidation().as(Options.class);
    options.setEnableStreamingEngine(true);
    options.setRunner(DataflowRunner.class);
    RunPipeline.run(options, TableSpec.parse(options.getSourceSpec()),
        TableSpec.parse(options.getSinkSpec()));
  }
}
