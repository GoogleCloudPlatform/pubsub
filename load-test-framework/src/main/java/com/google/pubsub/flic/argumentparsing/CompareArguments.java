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

/** Arguments needed for the "compare" command. */
@Parameters(separators = "=")
public class CompareArguments {

  public static final String COMMAND = "compare";

  @Parameter(
    names = {"--file1", "-f1"},
    required = true
  )
  private String file1;

  @Parameter(
    names = {"--file2", "-f2"},
    required = true
  )
  private String file2;

  public String getFile1() {
    return file1;
  }

  public String getFile2() {
    return file2;
  }
}
