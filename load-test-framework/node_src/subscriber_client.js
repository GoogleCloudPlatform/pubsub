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
var subscription;
var latencies = [];
var received_messages = [];

function onMessage(message) {
  latencies.push((new Date).getTime() - message.attributes['sendTime']);
  received_messages.push({
    publisher_client_id: message.attributes['clientId'],
    sequence_number: message.attributes['sequenceNumber']
  });
  message.ack();
}

function start(call, callback) {
  var pubsubClient = pubsub({
    projectId: call.request.project,
  });
  var topic = pubsubClient.topic(call.request.topic);
  subscription = topic.subscription(call.request.pubsub_options.subscription);
  subscription.on('message', onMessage)
  callback(null, {});
}

function execute(call, callback) {
  callback(null, { 'latencies': latencies, 'received_messages': received_messages});
  latencies = [];
  received_messages = [];
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
