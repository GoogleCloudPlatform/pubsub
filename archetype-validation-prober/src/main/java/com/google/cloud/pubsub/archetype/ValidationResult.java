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

import java.util.Collections;
import java.util.List;

/**
 * Outcome of validating an incoming payload against the archetype at the ingestion gate.
 *
 * <p>A payload is either {@link Status#ACCEPTED} (structurally conformant and therefore safe to
 * publish, carrying its canonicalized form) or {@link Status#REJECTED} (deterministically invalid,
 * carrying machine-readable reason codes). A rejected payload is never enqueued: retrying it would
 * fail identically forever, so it is reported synchronously to the emitter instead.
 */
public final class ValidationResult {

  /** Terminal classification of a payload at the gate. */
  public enum Status {
    ACCEPTED,
    REJECTED
  }

  private final Status status;
  private final String canonicalPayload;
  private final List<String> reasons;

  private ValidationResult(Status status, String canonicalPayload, List<String> reasons) {
    this.status = status;
    this.canonicalPayload = canonicalPayload;
    this.reasons = reasons;
  }

  /** Builds an accepted result carrying the canonicalized (normalized) payload. */
  public static ValidationResult accepted(String canonicalPayload) {
    return new ValidationResult(Status.ACCEPTED, canonicalPayload, Collections.emptyList());
  }

  /** Builds a rejected result carrying one or more reason codes describing why it was refused. */
  public static ValidationResult rejected(List<String> reasons) {
    return new ValidationResult(Status.REJECTED, null, Collections.unmodifiableList(reasons));
  }

  public Status status() {
    return status;
  }

  public boolean isAccepted() {
    return status == Status.ACCEPTED;
  }

  /** The canonicalized payload, only present when {@link #isAccepted()} is true. */
  public String canonicalPayload() {
    return canonicalPayload;
  }

  /** Machine-readable reason codes, only populated when the payload was rejected. */
  public List<String> reasons() {
    return reasons;
  }

  @Override
  public String toString() {
    return status == Status.ACCEPTED
        ? "ACCEPTED"
        : "REJECTED" + reasons;
  }
}
