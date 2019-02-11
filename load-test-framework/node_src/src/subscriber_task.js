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
