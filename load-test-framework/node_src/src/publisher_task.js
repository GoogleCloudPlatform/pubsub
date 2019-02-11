let task = require('./task.js');
let metrics_tracker = require('./metrics_tracker.js');
let {PubSub} = require('@google-cloud/pubsub');
let RateLimiterFlowController = require('./flow_control/rate_limiter_flow_controller.js');
let OutstandingCountFlowController = require('./flow_control/outstanding_count_flow_controller.js');

// Start at 100 kB/s/worker
let kStartingPerWorkerBytesPerSecond = Math.pow(10, 5);

class PublisherSubtaskWorker extends task.SubtaskWorker {
    constructor() {
        super();
    }

    childStart(startRequest) {
        let client = new PubSub({
            projectId: startRequest.project,
        });
        let options = startRequest.publisher_options;
        let duration = options.batch_duration;
        this.publisher = client.topic(
            startRequest.topic,
            {
                batching: {
                    maxMilliseconds: task.Task.toMillis(duration),
                    maxBytes: 9500000,
                    maxMessages: options.batch_size
                }
            });
        this.data = Buffer.from('A'.repeat(options.message_size));
        this.pubId = Number.MIN_SAFE_INTEGER + Math.floor(
            Math.random() * (Number.MAX_SAFE_INTEGER - Number.MIN_SAFE_INTEGER));
        this.sequenceNumber = 0;

        if (options.rate > 0) {
            console.log("Rate limited at", options.rate, "per second.");
            this.flowController = new RateLimiterFlowController(options.rate)
        } else {
            let startingRate = kStartingPerWorkerBytesPerSecond / options.message_size;
            console.log("Dynamic flow controlled starting at", startingRate, "per second.");
            this.flowController = new OutstandingCountFlowController(startingRate);
        }
        this.publishNext();
    }

    publishNext() {
        this.flowController.requestStart().then(count => {
            Array(count).forEach(() => {
                let publishTime = (new Date).getTime();
                let sequenceNumber = this.sequenceNumber;
                ++this.sequenceNumber;
                let attributes = {
                    'sendTime': publishTime.toString(),
                    'clientId': this.pubId.toString(),
                    'sequenceNumber': sequenceNumber.toString()
                };
                this.publisher.publish(this.data, attributes, (err, {}) => {
                    if (err) {
                        this.metricsTracker.putError();
                        this.flowController.informFinished(false);
                        return;
                    }
                    this.flowController.informFinished(true);
                    let recvTime = (new Date).getTime();
                    let latencyMs = recvTime - publishTime;
                    let out = new metrics_tracker.MessageAndDuration(this.pubId, sequenceNumber, latencyMs);
                    this.metricsTracker.put(out);
                });
                this.publishNext();
            });
        })
    }
}

class PublisherWorker extends task.TaskWorker {
    constructor() {
        super(__dirname + '/publisher_task_main.js');
    }
}

class PublisherTask extends task.Task {
    getWorker() {
        return new PublisherWorker();
    }
}

module.exports = {
    PublisherTask: PublisherTask,
    PublisherSubtaskWorker: PublisherSubtaskWorker
};
