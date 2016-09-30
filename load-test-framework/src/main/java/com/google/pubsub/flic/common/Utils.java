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
package com.google.pubsub.flic.common;

import com.beust.jcommander.IParameterValidator;
import com.beust.jcommander.ParameterException;
import org.apache.commons.io.FileUtils;

import java.io.File;
import java.io.FileOutputStream;
import java.util.List;
import java.util.Random;

/** A collection of common methods/enums/constants. */
public class Utils {

  /** Creates a random string message of a certain size. */
  public static String createMessage(int msgSize) {
    StringBuilder sb = new StringBuilder(msgSize);
    Random r = new Random();
    for (int i = 0; i < msgSize; ++i) {
      sb.append((char) (r.nextInt(26) + 'a'));
    }
    sb.append('_');
    return sb.toString();
  }

  /**
   * Writes the given buffer of {@link MessagePacketProto.MessagePacket}'s to the specified file.
   */
  public static synchronized void writeToFile(
      List<MessagePacketProto.MessagePacket> buffer, File file) throws Exception {
    FileOutputStream os = FileUtils.openOutputStream(file, true);
    for (MessagePacketProto.MessagePacket mp : buffer) {
      mp.writeDelimitedTo(os);
    }
    os.close();
    os.flush();
  }

  /**
   * A validator that makes sure the parameter is an integer that is greater than 0.
   */
  public static class GreaterThanZeroValidator implements IParameterValidator {

    @Override
    public void validate(String name, String value) throws ParameterException {
      try {
        int n = Integer.parseInt(value);
        if (n > 0) return;
        throw new NumberFormatException();
      } catch (NumberFormatException e) {
        throw new ParameterException(
            "Parameter " + name + " should be an int greater than 0 (found " + value + ")");
      }
    }
  }
}
