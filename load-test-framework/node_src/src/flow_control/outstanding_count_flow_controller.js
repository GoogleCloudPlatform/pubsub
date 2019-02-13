let FlowController = require('./flow_controller.js');
let SettablePromise = require('../settable_promise.js');

let kUpdateDelayMilliseconds = 100;
let kBuckets = 150;


class CyclicBucketer {
    constructor() {
        // The array of buckets.
        this.buckets = new Array(kBuckets);
        // The current bucket's index.
        this.bucketIndex = 0;
        // Whether this has cycled one full rotation.
        this.hasCycled = false;
    }

    // Cycle the bucketer, returning the current sum of all buckets and zeroing out the next one
    cycle() {
        let sum = 0;
        this.buckets.forEach(value => {
            sum += value;
        });
        this.bucketIndex = (this.bucketIndex + 1) % this.buckets.length;
        this.hasCycled = this.hasCycled || (this.bucketIndex === 0);
        this.buckets[this.bucketIndex] = 0;
        return sum;
    }

    add() {
        this.buckets[this.bucketIndex] += 1;
    }
}


class OutstandingCountFlowController extends FlowController {
    // A FlowController that tries to ensure that the outstanding count is roughly equivalent to
    // the completion rate in the next two seconds.
    constructor(initalPerSecondRate) {
        super();
        this.ratePerSecond = initalPerSecondRate;
        this.bucketer = new CyclicBucketer();
        this.outstanding = 0;

        this.waiters = [];

        setInterval(() => {
            this.resetRate();
        }, kUpdateDelayMilliseconds);
    }

    resetRate() {
        let sum = this.bucketer.cycle();
        if (this.bucketer.hasCycled === false) return;
        let ratePerMillisecond = sum / (kUpdateDelayMilliseconds * kBuckets);
        this.ratePerSecond = ratePerMillisecond * 1000;
        let waiters = this.waiters;
        this.waiters = [];
        waiters.forEach(waiter => {
            waiter.set();
        });
    }

    async requestStart() {
        while (true) {
            let availableTokens = this.tokensAvailable();
            if (availableTokens >= 1) {
                this.outstanding += availableTokens;
                return availableTokens;
            }
            let waiter = new SettablePromise();
            this.waiters.push(waiter);
            await waiter.promise;
        }
    }

    // Return the number of tokens currently available.
    tokensAvailable() {
        return Math.floor((this.ratePerSecond * 2) - this.outstanding);
    }

    triggerNext() {
        let waiter = this.waiters.pop();
        if (undefined === waiter) return;
        waiter.set();
    }

    informFinished(wasSuccessful) {
        if (wasSuccessful) { this.bucketer.add(); }
        --this.outstanding;
        this.triggerNext();
    }
}

module.exports = OutstandingCountFlowController;
