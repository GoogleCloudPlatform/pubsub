package com.google.pubsub.kafka.source;

import com.google.api.core.ApiFuture;
import com.google.api.core.ApiFutureCallback;
import com.google.api.core.ApiFutures;
import com.google.api.core.SettableApiFuture;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.errorprone.annotations.concurrent.GuardedBy;
import com.google.protobuf.Empty;
import com.google.pubsub.v1.ReceivedMessage;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Deque;
import java.util.List;
import java.util.concurrent.Future;

public class AckBatchingSubscriber implements CloudPubSubSubscriber {
  interface AlarmFactory {
    Future<?> newAlarm(Runnable runnable);
  }

  private static class IdsAndFuture {
    Collection<String> ids;
    SettableApiFuture<Empty> future;
  };

  private final CloudPubSubSubscriber underlying;
  @GuardedBy("this")
  private final Deque<IdsAndFuture> toSend = new ArrayDeque<>();
  private final Future<?> alarm;

  public AckBatchingSubscriber(
      CloudPubSubSubscriber underlying,
      AlarmFactory alarmFactory) {
    this.underlying = underlying;
    this.alarm = alarmFactory.newAlarm(this::flush);
  }

  @Override
  public ApiFuture<List<ReceivedMessage>> pull() {
    return underlying.pull();
  }

  @Override
  public synchronized ApiFuture<Empty> ackMessages(Collection<String> ackIds) {
    IdsAndFuture idsAndFuture = new IdsAndFuture();
    idsAndFuture.ids = ackIds;
    idsAndFuture.future = SettableApiFuture.create();
    toSend.add(idsAndFuture);
    return idsAndFuture.future;
  }

  private void flush() {
    List<String> ackIds = new ArrayList<>();
    List<SettableApiFuture<Empty>> futures = new ArrayList<>();
    synchronized (this) {
      if (toSend.isEmpty()) {
        return;
      }
      toSend.forEach(pair -> {
        ackIds.addAll(pair.ids);
        futures.add(pair.future);
      });
      toSend.clear();
    }
    ApiFuture<Empty> response = underlying.ackMessages(ackIds);
    ApiFutures.addCallback(response, new ApiFutureCallback<Empty>() {
      @Override
      public void onFailure(Throwable t) {
        futures.forEach(future -> future.setException(t));
      }

      @Override
      public void onSuccess(Empty result) {
        futures.forEach(future -> future.set(result));
      }
    }, MoreExecutors.directExecutor());
  }

  @Override
  public void close() {
    alarm.cancel(false);
    try {
      alarm.get();
    } catch (Throwable ignored) {}
    flush();
    underlying.close();
  }
}
