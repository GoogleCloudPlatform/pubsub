/*
 *
 * Copyright 2017 Google.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

var PROTO_PATH = __dirname + '/loadtest.proto';

var parseArgs = require('minimist');
var grpc = require('grpc');
var pubsub = require('@google-cloud/pubsub')
var loadtest = grpc.load(PROTO_PATH).google.pubsub.loadtest;
var publisher;
var message;
var batch_size;
var client_id;
var sequence_number;
var latencies;

function start(call, callback) {
  var pubsubClient = pubsub({
    projectId: call.request.project,
  });

  // Reference a topic that has been previously created.
  var topic = pubsubClient.topic(call.request.topic);

  // Publish a message to the topic.
  publisher = topic.publisher({
    batching: {
      maxMilliseconds: Math.round(call.request.publish_batch_duration.seconds * 1000 +
                                  call.request.publish_batch_duration.nanos / 1000000.0)
    }
  });
  message = new Buffer('A' * call.request.message_size);
  batch_size = call.request.publish_batch_size;
  client_id = (Math.random() * Number.MAX_SAFE_INTEGER).toString();
  sequence_number = 0;
  latencies = [];
  callback(null, {});
}

function execute(call, callback) {
  var publish_time = (new Date).getTime();
  for (var i = 0; i < batch_size; ++i) {
    var attributes = {
      'sendTime': publish_time.toString(),
      'clientId': client_id,
      'sequenceNumber': (sequence_number++).toString()
    };
    publisher.publish(message, attributes, function(publish_time) {
      return function(err, messageId) {
        if (!err) {
          latencies.push((new Date).getTime() - publish_time);
        } else {
          console.log("Error publishing message: " + err);
        }
      };
    }(publish_time));
  }
  callback(null, { 'latencies': latencies});
  latencies = [];
}

if (require.main === module) {
  var argv = parseArgs(process.argv, {
      string: 'worker_port'
  });
  worker_port = "6000";
  if (argv.worker_port) {
    worker_port = argv.worker_port;
  }
  var server = new grpc.Server();
  server.addService(loadtest.LoadtestWorker.service, {
    start: start,
    execute: execute
  });
  server.bind('0.0.0.0:' + worker_port, grpc.ServerCredentials.createInsecure());
  server.start();
}