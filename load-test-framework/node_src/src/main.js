let parseArgs = require('minimist');
let PublisherTask = require('./publisher_task.js').PublisherTask;
let SubscriberTask = require('./subscriber_task.js').SubscriberTask;
let loadtestService = require('./loadtest_service.js');
let grpc = require('grpc');

if (require.main === module) {
    var argv = parseArgs(process.argv.slice(2), {
        boolean: 'publisher',
        integer: 'port',
        default: {
            port: 5000
        }
    });
    if (undefined === argv.publisher) {
        process.exit(1);
    }

    process.on('unhandledRejection', (reason, p) => {
        console.log('Unhandled Rejection at: Promise', p, 'reason:', reason, 'stack trace:', reason.stack);
        process.exit(1);
    });

    let task;
    if (argv.publisher) {
        task = new PublisherTask();
    } else {
        task = new SubscriberTask();
    }
    task.init().then(() => {
        let server = new grpc.Server();
        server.addService(loadtestService.LoadtestWorker.service, {
            Start: task.startHandler.bind(task),
            Check: task.checkHandler.bind(task)
        });
        server.bind('0.0.0.0:' + argv.port, grpc.ServerCredentials.createInsecure());
        server.start();
        console.log("starting " + (argv.publisher ? "publisher" : "subscriber") + " at port " + argv.port);
    });
}