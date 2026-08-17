// Copyright 2024 Google Inc.
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
package com.google.cloud.pubsub.archetype;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.logging.Logger;

/**
 * Reference implementation of an <b>archetype validation gate</b> in front of Cloud Pub/Sub.
 *
 * <p>It demonstrates the principle that only what minimally fits the process should ever be
 * enqueued. Every payload is validated and canonicalized against a single, versioned archetype
 * (JSON Schema) at the gate:
 *
 * <ul>
 *   <li><b>Accepted</b> &rarr; would be published to the topic (safe to process downstream).
 *   <li><b>Rejected</b> &rarr; refused synchronously with machine-readable reason codes and
 *       <em>never enqueued</em> &mdash; retrying it would fail identically forever.
 * </ul>
 *
 * <p>Provided as-is for demonstration purposes only, with no SLA. Not meant to run as part of a
 * production or critical workload. Run without arguments for a self-contained, offline demo of the
 * gate over a set of representative payloads.
 */
public final class ArchetypeValidationGateway {

  private static final Logger logger = Logger.getLogger(ArchetypeValidationGateway.class.getName());

  public static final class Args {
    @Parameter(
        names = "--archetype",
        description =
            "Path to the JSON Schema archetype. Defaults to the bundled archetype.schema.json.")
    private String archetypePath = null;

    @Parameter(
        names = "--charset",
        description = "Declared charset of incoming payloads used for canonicalization.")
    private String charset = "UTF-8";

    @Parameter(names = "--help", help = true, description = "Print usage and exit.")
    private boolean help = false;
  }

  public static void main(String[] argv) throws IOException {
    Args args = new Args();
    JCommander jc = JCommander.newBuilder().addObject(args).build();
    jc.parse(argv);
    if (args.help) {
      jc.usage();
      return;
    }

    Charset charset = Charset.forName(args.charset);
    Archetype archetype = new Archetype(openArchetype(args.archetypePath));

    logger.info("Archetype validation gate ready. Running offline demo over sample payloads.\n");
    for (Map.Entry<String, byte[]> sample : samplePayloads().entrySet()) {
      ValidationResult result = archetype.validate(sample.getValue(), charset);
      if (result.isAccepted()) {
        // In a live run this is where Publisher.publish(...) would be called.
        logger.info(String.format("[PUBLISH ] %-24s -> ACCEPTED (enqueued)", sample.getKey()));
      } else {
        // Rejected at the gate: reported to the emitter, never enqueued.
        logger.info(
            String.format(
                "[REJECT  ] %-24s -> %s", sample.getKey(), result.reasons()));
      }
    }
  }

  private static InputStream openArchetype(String path) throws IOException {
    if (path != null) {
      return new FileInputStream(path);
    }
    InputStream bundled =
        ArchetypeValidationGateway.class.getResourceAsStream("/archetype.schema.json");
    if (bundled == null) {
      throw new IOException("Bundled archetype.schema.json not found on classpath");
    }
    return bundled;
  }

  /**
   * Representative payloads: a valid one, and the deterministic failure classes the gate is meant
   * to stop before they ever reach the queue.
   */
  private static Map<String, byte[]> samplePayloads() {
    Map<String, byte[]> samples = new LinkedHashMap<>();

    samples.put(
        "valid",
        utf8("{\"policyNumber\":\"POL-000123\",\"amount\":150.5,\"channel\":\"WEB\"}"));

    // Structural: wrong type (amount as string) and missing required field are contract violations.
    samples.put(
        "wrong-type",
        utf8("{\"policyNumber\":\"POL-000123\",\"amount\":\"150.5\",\"channel\":\"WEB\"}"));
    samples.put(
        "missing-required",
        utf8("{\"amount\":10.0,\"channel\":\"WEB\"}"));

    // Structural: value outside the declared enum.
    samples.put(
        "bad-enum",
        utf8("{\"policyNumber\":\"POL-000999\",\"amount\":1.0,\"channel\":\"CARRIER_PIGEON\"}"));

    // Structural: pattern violation on the identifier.
    samples.put(
        "bad-pattern",
        utf8("{\"policyNumber\":\"nope\",\"amount\":1.0,\"channel\":\"WEB\"}"));

    // Syntactic: not JSON at all.
    samples.put("not-json", utf8("<xml>not a json payload</xml>"));

    // Canonicalization: 'ñ' expressed as NFD (n + U+0303). Semantically equal to NFC; the gate
    // normalizes it so it does NOT become a false reject.
    samples.put(
        "nfd-normalization",
        utf8("{\"policyNumber\":\"POL-000123\",\"amount\":1.0,\"channel\":\"WEB\",\"name\":\"Nun\u0303ez\"}"));

    return samples;
  }

  private static byte[] utf8(String s) {
    return s.getBytes(StandardCharsets.UTF_8);
  }

  private ArchetypeValidationGateway() {}
}
