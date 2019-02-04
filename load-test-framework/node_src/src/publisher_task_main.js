let TaskWorker = require("./task.js").TaskWorker;
let PublisherSubtaskWorker = require("./publisher_task.js").PublisherSubtaskWorker;

TaskWorker.runWorker(new PublisherSubtaskWorker());
