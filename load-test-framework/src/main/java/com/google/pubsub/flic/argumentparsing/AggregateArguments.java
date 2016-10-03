package com.google.pubsub.flic.argumentparsing;

import com.beust.jcommander.Parameter;
import com.beust.jcommander.Parameters;
import java.util.ArrayList;
import java.util.List;

@Parameters(separators = "=")
public class AggregateArguments {
  
  public static final String COMMAND = "agg";
  
  @Parameter(
      names = {"--files", "-f"},
      required = true,
      description = "Files to read data from for aggregation."
    )
  private List<String> files = new ArrayList<>();

  public List<String> getFiles() {
    return files;
  }
}