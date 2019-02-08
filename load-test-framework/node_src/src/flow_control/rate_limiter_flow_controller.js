let FlowController = require('./flow_controller.js');


class RateLimiterFlowController extends FlowController {
    // A FlowController that allows actions at a given per second rate.
    constructor(perSecondRate) {
        super();
        let seconds_per_run = 1 / perSecondRate;
        this.callbacks = [];
        setInterval(() => {
            let cb = this.callbacks.pop();
            if (undefined === cb) return;
            cb();
        }, seconds_per_run * 1000);
    }

    async requestStart() {
        await new Promise(resolve => {
            this.callbacks.push(() => {
                resolve(null);
            });
        });
    }

    informFinished(wasSuccessful) {
    }
}

module.exports = RateLimiterFlowController;
