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

    check() {
        let checkResponse = {
            bucket_values: this.latencies,
            received_messages: this.identifiers
        };
        this.identifiers = [];
        this.latencies = [];
        return checkResponse;
    }

    static combineResponses(responses) {
        let combinedIdentifiers = [];
        let combinedLatencies = [];
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
        });
        return {
          bucket_values: combinedLatencies,
          received_messages: combinedIdentifiers
        };
    }
}

module.exports = {
    MessageAndDuration: MessageAndDuration,
    MetricsTracker: MetricsTracker
};
