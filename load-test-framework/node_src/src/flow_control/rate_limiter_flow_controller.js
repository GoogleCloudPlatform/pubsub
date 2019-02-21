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
                resolve(1);
            });
        });
    }

    informFinished(wasSuccessful) {
    }
}

module.exports = RateLimiterFlowController;
