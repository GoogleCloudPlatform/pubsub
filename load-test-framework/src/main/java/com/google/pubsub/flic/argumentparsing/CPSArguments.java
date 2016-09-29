// Copyright 2016 Google Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
//
////////////////////////////////////////////////////////////////////////////////
package com.google.pubsub.flic.argumentparsing;

import com.beust.jcommander.Parameter;
import com.beust.jcommander.Parameters;
import com.google.pubsub.flic.common.Utils.GreaterThanZeroValidator;

/** Arguments needed for the "cps" command. */
@Parameters(separators = "=")
public class CPSArguments {

  public static final String COMMAND = "cps";

  @Parameter(
    names = {"--batch_size", "-b"},
    description = "Number of messages to batch per publish request.",
    validateWith = GreaterThanZeroValidator.class
  )
  private int batchSize = 1000;

  @Parameter(
    names = {"--response_threads", "-r"},
    description = "Number of threads to use to handle response callbacks.",
    validateWith = GreaterThanZeroValidator.class
  )
  private int numResponseThreads = 1;

  @Parameter(
    names = {"--rate_limit", "-l"},
    description = "Max number of requests per second.",
    validateWith = GreaterThanZeroValidator.class
  )
  private int rateLimit = 1000;

  public int getBatchSize() {
    return batchSize;
  }

  public int getNumResponseThreads() {
    return numResponseThreads;
  }

  public int getRateLimit() {
    return rateLimit;
  }
}
