let FlowController = require('./flow_controller.js');
let Keyv = require('keyv');
let SettablePromise = require('../settable_promise.js');

let kExpiryLatencySeconds = 15;
let kUpdateDelaySeconds = .1;


class OutstandingCountFlowController extends FlowController {
    // A FlowController that tries to ensure that the outstanding count is roughly equivalent to
    // the completion rate in the next two seconds.
    constructor(initalPerSecondRate) {
        super();
        this.ratePerSecond = initalPerSecondRate;
        this.underlyingMap = new Map();
        this.expiryCache = new Keyv({
            store: this.underlyingMap,
            ttl: kExpiryLatencySeconds * 1000
        });
        this.nextIndex = 0;
        this.outstanding = 0;

        this.waiters = [];

        setTimeout(() => {
            this.resetRate();
            setInterval(() => {
                this.resetRate();
            }, kUpdateDelaySeconds * 1000);
        }, kExpiryLatencySeconds * 1000);
    }

    resetRate() {
        this.ratePerSecond = this.underlyingMap.size / kExpiryLatencySeconds;
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
        if (wasSuccessful) {
            this.expiryCache.set(this.nextIndex, null);
            ++this.nextIndex;
        }
        --this.outstanding;
        this.triggerNext();
    }
}

module.exports = OutstandingCountFlowController;
