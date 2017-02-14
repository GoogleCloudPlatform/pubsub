/**
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license agreements. See the NOTICE
 * file distributed with this work for additional information regarding copyright ownership. The ASF licenses this file
 * to You under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with the
 * License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 */
package com.google.kafka.clients.producer.internals;

import com.google.pubsub.v1.PublishRequest;
import com.google.pubsub.v1.PublishResponse;
import com.google.pubsub.v1.PublisherGrpc;
import io.grpc.stub.StreamObserver;
import java.util.LinkedList;
import java.util.Queue;

public class MockPubsubServer extends PublisherGrpc.PublisherImplBase {
    private Queue<StreamObserver<PublishResponse>> responseList;

    public MockPubsubServer() {
        responseList = new LinkedList<>();
    }

    @Override
    public void publish(PublishRequest request, StreamObserver<PublishResponse> responseObserver) {
        responseList.add(responseObserver);
    }

    public int inFlightCount() {
        return responseList.size();
    }

    public void respond(PublishResponse response) {
        StreamObserver<PublishResponse> stream = responseList.poll();
        stream.onNext(response);
        stream.onCompleted();
    }

    public void disconnect() {
        for (int i = 0; i < 100; i++) {
            if (responseList.isEmpty()) {
                try {
                    Thread.sleep(50);
                } catch (InterruptedException e) { } // not an issue, ignore
            }
        }
        StreamObserver<PublishResponse> stream = responseList.poll();
        stream.onCompleted();
    }

    public boolean listen(int messagesExpected, long waitInMillis) {
        for (int i = 0; i < waitInMillis / 50; i++) {
            if (responseList.size() == messagesExpected) {
                return true;
            }
            try {
                Thread.sleep(50);
            } catch (InterruptedException e) { }
        }
        return false;
    }
}
