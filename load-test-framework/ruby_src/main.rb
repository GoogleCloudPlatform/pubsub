#!/usr/bin/ruby

require 'grpc'
require 'thread'
require 'google/cloud/pubsub'
require './loadtest_services_pb'

include Google::Pubsub::Loadtest

class ServerImpl < Google::Pubsub::Loadtest::LoadtestWorker::Service

  def start(request, _call)
    puts "Received Start"
    @message_size = request.message_size
    @batch_size = request.publish_batch_size
    pubsub = Google::Cloud::Pubsub.new(project: request.project)
    @topic = pubsub.topic request.topic
    @client_id = (Random.rand(2 ** 31)).to_s
    @num_msgs_published = 0
    @latencies = []
    @semaphore = Mutex.new
    StartResponse.new
  end

  def execute(request, _call)
    sequence_number = @num_msgs_published
    @num_msgs_published += @batch_size
    now = Time.now
    latencies = []
    puts "Received execute, going to publish " + @batch_size.to_s + " messages."
    0.upto(@batch_size) do |i|
      @topic.publish_async ("A" * @message_size),
        :sendTime => (now.to_f * 1000).to_i.to_s,
        :clientId => @client_id,
        :sequenceNumber => (sequence_number + i).to_s do |result|
          puts "Message success? " + result.succeeded?.to_s
          if result.succeeded?
            @semaphore.synchronize do
              @latencies.push ((Time.now - now).to_f * 1000).to_i
            end
          end
        end
    end
    latencies = []
    @semaphore.synchronize do
      latencies = @latencies
      @latencies = []
    end
    puts "Returning " + latencies.to_s
    ExecuteResponse.new(latencies: latencies)
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
