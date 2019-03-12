/*
 * Copyright 2019 Google Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * You may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions
 * and limitations under the License.
 */

class MessageAndDuration {
    constructor(publisher_id, sequence_number, latency_ms) {
        this.publisher_id = publisher_id;
        this.sequence_number = sequence_number;
        this.latency_ms = latency_ms;
    }
}

class MetricsTracker {
    constructor(include_ids) {
        this.include_ids = include_ids;
        this.identifiers = [];
        this.latencies = [];
        this.failed = 0
    }

    static logBase(value, base) {
        return Math.log(value) / Math.log(base);
    }

    static bucketFor(latency_ms) {
        let raw_bucket = Math.floor(MetricsTracker.logBase(latency_ms, 1.5));
        return Math.max(0, raw_bucket);
    }

    put(messageAndDuration) {
        let bucket = MetricsTracker.bucketFor(messageAndDuration.latency_ms);
        while (this.latencies.length <= bucket) {
            this.latencies.push(0);
        }
        this.latencies[bucket] += 1;
        if (this.include_ids) {
            let identifier = {
                publisher_client_id: messageAndDuration.publisher_id,
                sequence_number: messageAndDuration.sequence_number
            };
            this.identifiers.push(identifier);
        }
    }

    putError() {
        this.failed++;
    }

    check() {
        let checkResponse = {
            bucket_values: this.latencies,
            received_messages: this.identifiers,
            failed: this.failed
        };
        this.identifiers = [];
        this.latencies = [];
        this.failed = 0;
        return checkResponse;
    }

    static combineResponses(responses) {
        let combinedIdentifiers = [];
        let combinedLatencies = [];
        let failed = 0;
        responses.forEach(response => {
            while (combinedLatencies.length <= response.bucket_values.length) {
                combinedLatencies.push(0);
            }
            response.bucket_values.forEach((value, index) => {
                combinedLatencies[index] += Number(value);
            });
            response.received_messages.forEach(received => {
                combinedIdentifiers.push(received);
            });
            failed += response.failed
        });
        return {
            bucket_values: combinedLatencies,
            received_messages: combinedIdentifiers,
            failed: failed
        };
    }
}

module.exports = {
    MessageAndDuration: MessageAndDuration,
    MetricsTracker: MetricsTracker
};
