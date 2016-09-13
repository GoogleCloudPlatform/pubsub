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

import com.beust.jcommander.IParameterValidator;
import com.beust.jcommander.Parameter;
import com.beust.jcommander.ParameterException;
import com.beust.jcommander.Parameters;
import com.google.common.primitives.Ints;

/** Arguments needs for the "kafka" command. */
@Parameters(separators = "=")
public class KafkaArguments {

  public static final String COMMAND = "kafka";

  @Parameter(
    names = {"--broker", "-b"},
    required = true,
    description = "Hostname and port of one kafka broker.",
    validateWith = HostPortValidator.class
  )
  private String broker;

  public String getBroker() {
    return broker;
  }

  /**
   * A validator that makes sure the parameter is in hostname:port format and that the port is a
   * valid integer. The validity of the hostname is not checked. Note: Adapted from Guava
   * HostAndPort.java
   */
  public static class HostPortValidator implements IParameterValidator {

    @Override
    public void validate(String name, String value) throws ParameterException {
      int colonPos = value.indexOf(':');
      if (!(colonPos >= 0 && value.indexOf(':', colonPos + 1) == -1)
          || (Ints.tryParse(value.substring(colonPos + 1)) == null)) {
        throw new ParameterException("Parameter " + name + " does not follow hostname:port format");
      }
    }
  }
}
