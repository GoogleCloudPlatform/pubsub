package com.google.pubsub.kafka.source;

import static com.google.common.truth.Truth.assertThat;
import static org.junit.Assert.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.api.core.ApiService;
import com.google.api.gax.rpc.ApiException;
import com.google.api.gax.rpc.StatusCode;
import com.google.api.gax.rpc.StatusCode.Code;
import com.google.cloud.pubsub.v1.AckReplyConsumer;
import com.google.cloud.pubsub.v1.MessageReceiver;
import com.google.cloud.pubsub.v1.SubscriberInterface;
import com.google.cloud.pubsublite.internal.CheckedApiException;
import com.google.common.collect.ImmutableList;
import com.google.protobuf.ByteString;
import com.google.pubsub.v1.PubsubMessage;
import com.google.pubsub.v1.ReceivedMessage;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.stream.Collectors;
import org.junit.Before;
import org.junit.Test;
import org.mockito.InOrder;
import org.mockito.stubbing.Answer;

public class StreamingPullSubscriberTest {
  private final StreamingPullSubscriberFactory underlyingFactory = mock(StreamingPullSubscriberFactory.class);
  private final SubscriberInterface underlying = mock(SubscriberInterface.class);
  // Initialized in setUp.
  private StreamingPullSubscriber subscriber;
  private MessageReceiver messageReceiver;
  private ApiService.Listener errorListener;
  private final ExecutorService executorService = Executors.newCachedThreadPool();

  private static List<PubsubMessage> messagesFor(List<ReceivedMessage> received) {
    return received.stream().map(ReceivedMessage::getMessage).collect(Collectors.toList());
  }

  @Before
  public void setUp() throws Exception {
    when(underlying.startAsync()).thenReturn(underlying);
    when(underlyingFactory.newSubscriber(any()))
        .thenAnswer(
            args -> {
              messageReceiver = args.getArgument(0);
              return underlying;
            });
    doAnswer(
            (Answer<Void>)
                args -> {
                  errorListener = args.getArgument(0);
                  return null;
                })
        .when(underlying)
        .addListener(any(), any());

    subscriber = new StreamingPullSubscriber(underlyingFactory);

    InOrder inOrder = inOrder(underlyingFactory, underlying);
    inOrder.verify(underlyingFactory).newSubscriber(any());
    inOrder.verify(underlying).addListener(any(), any());
    inOrder.verify(underlying).startAsync();
    inOrder.verify(underlying).awaitRunning();

    assertThat(messageReceiver).isNotNull();
    assertThat(errorListener).isNotNull();
  }

  @Test
  public void closeStops() {
    when(underlying.stopAsync()).thenReturn(underlying);
    subscriber.close();
    verify(underlying).stopAsync();
    verify(underlying).awaitTerminated();
  }

  @Test
  public void pullAfterErrorThrows() {
    ApiException expected = new CheckedApiException(Code.INTERNAL).underlying;
    errorListener.failed(null, expected);
    ExecutionException e = assertThrows(ExecutionException.class, () -> subscriber.pull().get());
    assertThat(expected).isEqualTo(e.getCause());
  }

  @Test
  public void pullBeforeErrorThrows() throws Exception {
    ApiException expected = new CheckedApiException(Code.INTERNAL).underlying;
    Future<List<ReceivedMessage>> future = subscriber.pull();
    Thread.sleep(1000);
    assertThat(future.isDone()).isFalse();

    errorListener.failed(null, expected);
    ExecutionException e = assertThrows(ExecutionException.class, future::get);
    assertThat(expected).isEqualTo(e.getCause());
  }

  @Test
  public void pullSuccess() throws Exception {
    PubsubMessage message = PubsubMessage.newBuilder().setData(ByteString.copyFromUtf8("abc")).build();
    Future<List<ReceivedMessage>> future = executorService.submit(() -> subscriber.pull().get());
    messageReceiver.receiveMessage(message, mock(AckReplyConsumer.class));
    assertThat(messagesFor(future.get())).isEqualTo(ImmutableList.of(message));
  }

  @Test
  public void pullMultiple() throws Exception {
    PubsubMessage message1 = PubsubMessage.newBuilder().setData(ByteString.copyFromUtf8("abc")).build();
    PubsubMessage message2 = PubsubMessage.newBuilder().setData(ByteString.copyFromUtf8("abc")).build();
    messageReceiver.receiveMessage(message1, mock(AckReplyConsumer.class));
    messageReceiver.receiveMessage(message2, mock(AckReplyConsumer.class));
    assertThat(messagesFor(subscriber.pull().get())).isEqualTo(ImmutableList.of(message1, message2));
  }

  @Test
  public void pullMessageWhenError() {
    ApiException expected = new CheckedApiException(Code.INTERNAL).underlying;
    errorListener.failed(null, expected);
    ExecutionException e = assertThrows(ExecutionException.class, () -> subscriber.pull().get());
    assertThat(e.getCause()).isEqualTo(expected);
  }

  @Test
  public void pullMessagePrioritizeErrorOverExistingMessage() {
    ApiException expected = new CheckedApiException(Code.INTERNAL).underlying;
    errorListener.failed(null, expected);
    PubsubMessage message = PubsubMessage.newBuilder().setData(ByteString.copyFromUtf8("abc")).build();
    messageReceiver.receiveMessage(message, mock(AckReplyConsumer.class));

    ExecutionException e = assertThrows(ExecutionException.class, () -> subscriber.pull().get());
    assertThat(e.getCause()).isEqualTo(expected);
  }

  @Test
  public void pullThenAck() throws Exception {
    PubsubMessage message = PubsubMessage.newBuilder().setData(ByteString.copyFromUtf8("abc")).build();
    Future<List<ReceivedMessage>> future = executorService.submit(() -> subscriber.pull().get());
    AckReplyConsumer ackReplyConsumer = mock(AckReplyConsumer.class);
    messageReceiver.receiveMessage(message, ackReplyConsumer);
    List<ReceivedMessage> batch = future.get();
    assertThat(batch.size()).isEqualTo(1);
    ReceivedMessage received = batch.get(0);
    assertThat(received.getMessage()).isEqualTo(message);
    verify(ackReplyConsumer, times(0)).ack();
    // Invalid ack id ignored.
    subscriber.ackMessages(ImmutableList.of("not a real ack id", received.getAckId())).get();
    verify(ackReplyConsumer, times(1)).ack();
  }

  @Test
  public void multiAck() throws Exception {
    PubsubMessage message1 = PubsubMessage.newBuilder().setData(ByteString.copyFromUtf8("abc")).build();
    PubsubMessage message2 = PubsubMessage.newBuilder().setData(ByteString.copyFromUtf8("def")).build();
    AckReplyConsumer ackReplyConsumer1 = mock(AckReplyConsumer.class);
    AckReplyConsumer ackReplyConsumer2 = mock(AckReplyConsumer.class);
    messageReceiver.receiveMessage(message1, ackReplyConsumer1);
    messageReceiver.receiveMessage(message2, ackReplyConsumer2);
    List<ReceivedMessage> batch = subscriber.pull().get();
    assertThat(batch.size()).isEqualTo(2);
    ReceivedMessage received1 = batch.get(0);
    ReceivedMessage received2 = batch.get(1);
    assertThat(received1.getMessage()).isEqualTo(message1);
    assertThat(received2.getMessage()).isEqualTo(message2);
    verify(ackReplyConsumer1, times(0)).ack();
    verify(ackReplyConsumer2, times(0)).ack();
    subscriber.ackMessages(ImmutableList.of(received2.getAckId(), received1.getAckId())).get();
    verify(ackReplyConsumer1, times(1)).ack();
    verify(ackReplyConsumer2, times(1)).ack();
    // Duplicate ack ignored.
    subscriber.ackMessages(ImmutableList.of(received2.getAckId(), received1.getAckId())).get();
    verify(ackReplyConsumer1, times(1)).ack();
    verify(ackReplyConsumer2, times(1)).ack();
  }
}
