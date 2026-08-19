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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import org.junit.Before;
import org.junit.Test;

/** Unit tests for the {@link Archetype} ingestion gate. */
public class ArchetypeTest {

  private Archetype archetype;

  @Before
  public void setUp() {
    InputStream schema = getClass().getResourceAsStream("/archetype.schema.json");
    archetype = new Archetype(schema);
  }

  private ValidationResult validate(String json) {
    return archetype.validate(json.getBytes(StandardCharsets.UTF_8), StandardCharsets.UTF_8);
  }

  @Test
  public void acceptsConformantPayload() {
    ValidationResult r = validate("{\"policyNumber\":\"POL-000123\",\"amount\":150.5,\"channel\":\"WEB\"}");
    assertTrue(r.reasons().toString(), r.isAccepted());
  }

  @Test
  public void rejectsWrongType() {
    ValidationResult r = validate("{\"policyNumber\":\"POL-000123\",\"amount\":\"150.5\",\"channel\":\"WEB\"}");
    assertFalse(r.isAccepted());
    assertTrue(r.reasons().toString().contains("amount"));
  }

  @Test
  public void rejectsMissingRequired() {
    ValidationResult r = validate("{\"amount\":10.0,\"channel\":\"WEB\"}");
    assertFalse(r.isAccepted());
    assertTrue(r.reasons().toString().contains("policyNumber"));
  }

  @Test
  public void rejectsValueOutsideEnum() {
    ValidationResult r = validate("{\"policyNumber\":\"POL-000999\",\"amount\":1.0,\"channel\":\"CARRIER_PIGEON\"}");
    assertFalse(r.isAccepted());
  }

  @Test
  public void rejectsPatternViolation() {
    ValidationResult r = validate("{\"policyNumber\":\"nope\",\"amount\":1.0,\"channel\":\"WEB\"}");
    assertFalse(r.isAccepted());
  }

  @Test
  public void rejectsNonJson() {
    ValidationResult r = validate("<xml>not json</xml>");
    assertFalse(r.isAccepted());
    assertTrue(r.reasons().toString().contains("SYNTAX"));
  }

  @Test
  public void rejectsMalformedUtf8Bytes() {
    byte[] malformed = new byte[] {(byte) 0xC3, (byte) 0x28};
    ValidationResult r = archetype.validate(malformed, StandardCharsets.UTF_8);
    assertFalse(r.isAccepted());
    assertTrue(r.reasons().toString(), r.reasons().get(0).startsWith("ENCODING_UNDECODABLE"));
  }

  @Test
  public void canonicalizesNfdToNfcSoItIsNotAFalseReject() {
    // "Nuñez" with 'ñ' as NFD (n + U+0303). Must be accepted and stored as NFC.
    String nfd = "{\"policyNumber\":\"POL-000123\",\"amount\":1.0,\"channel\":\"WEB\",\"name\":\"Nun\u0303ez\"}";
    ValidationResult r = validate(nfd);
    assertTrue(r.reasons().toString(), r.isAccepted());
    // NFC form of the name ("Nuñez") must be present; the NFD combining sequence must be gone.
    assertTrue(r.canonicalPayload().contains("Nu\u00F1ez"));
    assertFalse(r.canonicalPayload().contains("n\u0303"));
  }

  @Test
  public void reportsMultipleReasonsAtOnce() {
    ValidationResult r = validate("{\"policyNumber\":\"nope\",\"channel\":\"CARRIER_PIGEON\"}");
    assertFalse(r.isAccepted());
    // At least the missing 'amount' and the bad enum/pattern should be reported.
    assertTrue(r.reasons().size() >= 2);
  }
}
