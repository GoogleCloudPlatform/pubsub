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

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.networknt.schema.JsonSchema;
import com.networknt.schema.JsonSchemaFactory;
import com.networknt.schema.SpecVersion;
import com.networknt.schema.ValidationMessage;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.charset.CharacterCodingException;
import java.nio.charset.Charset;
import java.nio.charset.CharsetDecoder;
import java.nio.charset.CodingErrorAction;
import java.text.Normalizer;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;

/**
 * The ingestion gate.
 *
 * <p>An {@code Archetype} is the single, canonical, versioned contract that every incoming payload
 * must satisfy <em>before</em> it is ever published to a topic. It performs, cheapest check first:
 *
 * <ol>
 *   <li><b>Canonicalization</b> &mdash; decode with the declared charset and normalize to Unicode
 *       NFC, so byte-different-but-semantically-equal payloads (e.g. {@code ñ} as one code point vs
 *       {@code n} + combining tilde) do not produce false rejects.
 *   <li><b>Syntactic</b> &mdash; is it parseable JSON at all?
 *   <li><b>Structural</b> &mdash; does it match the archetype schema (required fields, types,
 *       enums, patterns, cardinality)?
 * </ol>
 *
 * <p>Whatever passes is guaranteed to <em>at minimum fit the process</em>: the downstream engine
 * will not blow up on a missing field or a type mismatch. Whatever fails is rejected
 * deterministically with reason codes and is never enqueued. This deliberately keeps the error
 * store (dead-letter / quarantine) small: it only ever holds the failures that genuinely cannot be
 * predicted at the gate (downstream outages, state-dependent business rejections).
 */
public final class Archetype {

  private final JsonSchema schema;
  private final ObjectMapper mapper = new ObjectMapper();

  /** Loads the archetype from a JSON Schema (Draft 7) stream. */
  public Archetype(InputStream schemaStream) {
    JsonSchemaFactory factory = JsonSchemaFactory.getInstance(SpecVersion.VersionFlag.V7);
    this.schema = factory.getSchema(schemaStream);
  }

  /**
   * Loads the archetype from a classpath resource (e.g. {@code "/archetype.schema.json"}).
   * Convenience factory for tests and the offline demo.
   */
  public static Archetype fromResource(String resourcePath) {
    InputStream stream = Archetype.class.getResourceAsStream(resourcePath);
    if (stream == null) {
      throw new IllegalArgumentException("Classpath resource not found: " + resourcePath);
    }
    return new Archetype(stream);
  }

  /**
   * Validates and canonicalizes a raw payload at the gate.
   *
   * @param rawBytes the payload exactly as received on the wire
   * @param declaredCharset the charset the emitter claims to have used
   * @return {@link ValidationResult#accepted} with the canonical form, or
   *     {@link ValidationResult#rejected} with reason codes; never throws for invalid input.
   */
  public ValidationResult validate(byte[] rawBytes, Charset declaredCharset) {
    // 1. Canonicalization: decode + normalize (Unicode NFC). Kills false rejects up front.
    String canonical;
    try {
      CharsetDecoder decoder = declaredCharset.newDecoder();
      decoder.onMalformedInput(CodingErrorAction.REPORT);
      decoder.onUnmappableCharacter(CodingErrorAction.REPORT);
      String decoded = decoder.decode(ByteBuffer.wrap(rawBytes)).toString();
      canonical = Normalizer.normalize(decoded, Normalizer.Form.NFC);
    } catch (CharacterCodingException | RuntimeException e) {
      return ValidationResult.rejected(
          single("ENCODING_UNDECODABLE: payload is not valid " + declaredCharset.name()));
    }

    // 2. Syntactic: is it parseable JSON?
    JsonNode node;
    try {
      node = mapper.readTree(canonical);
    } catch (IOException e) {
      return ValidationResult.rejected(single("SYNTAX_NOT_JSON: " + e.getMessage()));
    }
    if (node == null || node.isMissingNode()) {
      return ValidationResult.rejected(single("SYNTAX_EMPTY: payload contained no JSON value"));
    }

    // 3. Structural: does it satisfy the archetype schema?
    Set<ValidationMessage> violations = schema.validate(node);
    if (!violations.isEmpty()) {
      List<String> reasons = new ArrayList<>(violations.size());
      for (ValidationMessage v : violations) {
        // e.g. "CONTRACT_VIOLATION[$.policyNumber]: string found, integer expected"
        reasons.add("CONTRACT_VIOLATION[" + v.getPath() + "]: " + v.getMessage());
      }
      return ValidationResult.rejected(reasons);
    }

    return ValidationResult.accepted(canonical);
  }

  private static List<String> single(String reason) {
    List<String> list = new ArrayList<>(1);
    list.add(reason);
    return list;
  }
}
