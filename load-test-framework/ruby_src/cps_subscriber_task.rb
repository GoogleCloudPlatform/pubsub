#!/usr/bin/ruby

require 'grpc'
require 'thread'
require 'google/cloud/pubsub'
require './loadtest_services_pb'

include Google::Pubsub::Loadtest

class ServerImpl < Google::Pubsub::Loadtest::LoadtestWorker::Service

  def start(request, _call)
    puts "Received Start"
    @latencies = []
    @recv_msgs = []
    @semaphore = Mutex.new
    pubsub = Google::Cloud::Pubsub.new(project: request.project)
    sub = pubsub.subscription request.pubsub_options.subscription
    @subscriber = sub.listen do |msg|
      puts "Received message " + msg.to_s
      @semaphore.synchronize do
        @latencies.push (Time.now.to_f * 1000.0 - msg.attributes['sendTime'].to_f).to_i
	@recv_msgs.push MessageIdentifier.new(publisher_client_id: msg.attributes['clientId'].to_i, sequence_number: msg.attributes['sequenceNumber'].to_i)
      end
      msg.ack!
    end
    @subscriber.start
    StartResponse.new
  end

  def execute(request, _call)
    latencies = []
    recv_msgs = []
    @semaphore.synchronize do
      latencies = @latencies
      recv_msgs = @recv_msgs
      @latencies = []
      @recv_msgs = []
    end
    puts "Returning " + latencies.to_s + ", " + recv_msgs.to_s
    ExecuteResponse.new(latencies: latencies, received_messages: recv_msgs)
  end
end

def main
  s = GRPC::RpcServer.new
  s.add_http2_port("0.0.0.0:6000", :this_port_is_insecure)
  s.handle(ServerImpl.new)
  puts "Started server on port 6000."
  s.run_till_terminated
end

main
