let PROTO_PATH = __dirname + '/../../proto/loadtest.proto';
let grpc = require('grpc');
let protoLoader = require('@grpc/proto-loader');
var packageDefinition = protoLoader.loadSync(
    PROTO_PATH,
    {
        keepCase: true,
        longs: String,
        enums: String,
        defaults: true,
        oneofs: true
    });

let serviceDescriptor = grpc.loadPackageDefinition(packageDefinition);

module.exports = serviceDescriptor.google.pubsub.loadtest;
