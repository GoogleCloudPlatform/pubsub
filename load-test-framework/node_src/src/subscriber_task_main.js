let TaskWorker = require("./task.js").TaskWorker;
let SubscriberSubtaskWorker = require("./subscriber_task.js").SubscriberSubtaskWorker;

TaskWorker.runWorker(new SubscriberSubtaskWorker());