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

let task = require('./task.js');
let metrics_tracker = require('./metrics_tracker.js');
let {PubSub} = require('@google-cloud/pubsub');

let BYTES_PER_PROCESS = 500000000;  // 500MB per process (1 per core)

class SubscriberSubtaskWorker extends task.SubtaskWorker {
    constructor() {
        super();
    }

    childStart(startRequest) {
        let client = new PubSub({
            projectId: startRequest.project
        });
        let topic = client.topic(startRequest.topic);
        let options = {
            flowControl: {
                maxBytes: BYTES_PER_PROCESS,
                maxMessages: Number.MAX_SAFE_INTEGER,
            },
        };
        let subscription = topic.subscription(startRequest.pubsub_options.subscription, options);
        subscription.on('message', this.onMessage.bind(this));
        subscription.on(`error`, error => {
            console.error(`ERROR: ${error}`);
        });
    }

    onMessage(message) {
        let latency = (new Date).getTime() - parseInt(message.attributes['sendTime']);
        let pubId = parseInt(message.attributes['clientId']);
        let sequenceNumber = parseInt(message.attributes['sequenceNumber']);
        let messageAndDuration = new metrics_tracker.MessageAndDuration(
            pubId, sequenceNumber, latency);
        this.metricsTracker.put(messageAndDuration);
        message.ack();
    }
}

class SubscriberWorker extends task.TaskWorker {
    constructor() {
        super(__dirname + '/subscriber_task_main.js');
    }
}

class SubscriberTask extends task.Task {
    getWorker() {
        return new SubscriberWorker();
    }
}

module.exports = {
    SubscriberTask: SubscriberTask,
    SubscriberSubtaskWorker: SubscriberSubtaskWorker
};
