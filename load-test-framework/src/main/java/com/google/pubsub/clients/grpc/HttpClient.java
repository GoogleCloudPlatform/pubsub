// Copyright 2016 Google Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
//
////////////////////////////////////////////////////////////////////////////////
package com.google.pubsub.clients.grpc;

import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.apache.http.client.ResponseHandler;
import org.apache.http.client.methods.HttpUriRequest;
import org.apache.http.client.protocol.RequestAcceptEncoding;
import org.apache.http.client.protocol.ResponseContentEncoding;
import org.apache.http.conn.scheme.PlainSocketFactory;
import org.apache.http.conn.scheme.Scheme;
import org.apache.http.conn.scheme.SchemeRegistry;
import org.apache.http.conn.ssl.SSLSocketFactory;
import org.apache.http.impl.client.DefaultHttpClient;
import org.apache.http.impl.conn.PoolingClientConnectionManager;
import org.apache.http.params.BasicHttpParams;
import org.apache.http.params.HttpConnectionParams;
import org.apache.http.protocol.BasicHttpContext;
import org.apache.http.protocol.HttpContext;

import java.io.Closeable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * An extension of {@link DefaultHttpClient}, that offers an asynchronous
 * (backed by a thread pool) execute method based on {@link ListenableFuture}s.
 * <p>
 * It also manages the shutdown of the underlying connection manager by
 * implementing the {@link Closeable} interface.
 */
public class HttpClient extends DefaultHttpClient implements Closeable {
  private final int maxConnections;
  private final int connectionKeepAliveSeconds;
  private final ExecutorService requestsExecutor;
  private final ScheduledExecutorService cleanupExecutor;
  private PoolingClientConnectionManager connectionManager;

  private HttpClient(Builder builder) {
    super(new PoolingClientConnectionManager(new SchemeRegistry()), new BasicHttpParams());

    addRequestInterceptor(new RequestAcceptEncoding());
    addResponseInterceptor(new ResponseContentEncoding());

    HttpConnectionParams.setConnectionTimeout(getParams(), builder.timeoutMillis);
    HttpConnectionParams.setSoTimeout(getParams(), builder.timeoutMillis);
    HttpConnectionParams.setSoKeepalive(getParams(), true);
    HttpConnectionParams.setStaleCheckingEnabled(getParams(), false);
    HttpConnectionParams.setTcpNoDelay(getParams(), true);

    maxConnections = builder.maxConnections;
    connectionKeepAliveSeconds = builder.connectionKeepAliveSeconds;

    connectionManager = (PoolingClientConnectionManager) getConnectionManager();
    connectionManager.setMaxTotal(maxConnections);
    connectionManager.setDefaultMaxPerRoute(maxConnections);

    SchemeRegistry schemeRegistry = connectionManager.getSchemeRegistry();
    schemeRegistry.register(new Scheme("http", PlainSocketFactory.getSocketFactory(), 80));
    schemeRegistry.register(new Scheme("https", SSLSocketFactory.getSocketFactory(), 443));
    setKeepAliveStrategy((response, ctx) -> connectionKeepAliveSeconds);

    requestsExecutor =
        Executors.newCachedThreadPool(
            new ThreadFactoryBuilder().setDaemon(true).setNameFormat("http-thread").build());
    cleanupExecutor =
        Executors.newScheduledThreadPool(
            1, new ThreadFactoryBuilder().setDaemon(true).setNameFormat("http-cleanup").build());
    cleanupExecutor.scheduleWithFixedDelay(() -> getConnectionManager().closeIdleConnections(120, TimeUnit.SECONDS),
        10, 10, TimeUnit.SECONDS);
  }

  public static Builder builder() {
    return new Builder();
  }

  public <T> ListenableFuture<T> executeFuture(
      final HttpUriRequest request,
      final ResponseHandler<? extends T> responseHandler) {
    return this.executeFuture(request, new BasicHttpContext(), responseHandler);
  }

  public <T> ListenableFuture<T> executeFuture(
      final HttpUriRequest request,
      final HttpContext context,
      final ResponseHandler<? extends T> responseHandler) {
    final SettableFuture<T> result = SettableFuture.create();
    requestsExecutor.execute(() -> {
      try {
        result.set(execute(request, responseHandler, context));
      } catch (Exception e) {
        result.setException(e);
      }
    });

    return result;
  }

  @Override
  public void close() {
    requestsExecutor.shutdownNow();
    cleanupExecutor.shutdownNow();
    getConnectionManager().shutdown();
  }

  /**
   * Builder for {@link HttpClient}.
   */
  public static class Builder {
    private int maxConnections = 100;
    private int timeoutMillis = 90000;
    private int connectionKeepAliveSeconds = 60 * 50;  // 50 min

    private Builder() {
    }

    public Builder maxConnections(int connections) {
      maxConnections = connections;
      return this;
    }

    public Builder timeoutMillis(int timeoutMillis) {
      this.timeoutMillis = timeoutMillis;
      return this;
    }

    public Builder connectionKeepAliveSeconds(int connectionKeepAliveSeconds) {
      this.connectionKeepAliveSeconds = connectionKeepAliveSeconds;
      return this;
    }

    public HttpClient build() {
      return new HttpClient(this);
    }
  }
}