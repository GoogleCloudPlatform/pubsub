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
package com.google.pubsub.flic;

import static org.junit.Assert.assertTrue;

import com.beust.jcommander.ParameterException;
import org.junit.Test;

/**
 * Tests for {@link Driver}.
 */
public class DriverTest {
  @Test
  public void testGreaterThanZeroValidator() {
    Driver.GreaterThanZeroValidator validator = new Driver.GreaterThanZeroValidator();
    validator.validate("test", "1");
    try {
      validator.validate("test", "-1");
      assertTrue(false);
    } catch (ParameterException e) {
      // we expect this to throw since -1 < 1
    }
    try {
      validator.validate("test", "0");
      assertTrue(false);
    } catch (ParameterException e) {
      // we expect this to throw since 0 < 1
    }
    try {
      validator.validate("test", "abc");
      assertTrue(false);
    } catch (ParameterException e) {
      // we expect this to throw since abc is not a number
    }
  }
}
