import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.protobuf.ByteString;
import com.google.pubsub.flink.PubSubSerializationSchema;
import com.google.pubsub.flink.internal.sink.PubSubSinkWriter;
import com.google.pubsub.flink.internal.sink.FlushablePublisher;
import com.google.pubsub.v1.PubsubMessage;
import org.apache.flink.api.connector.sink2.SinkWriter.Context;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

@RunWith(MockitoJUnitRunner.class)
public class PubSubSinkWriterTest {
  @Mock FlushablePublisher mockPublisher;
  @Mock PubSubSerializationSchema<String> mockSchema;
  PubSubSinkWriter<String> sinkWriter;

  @Before
  public void doBeforeEachTest() {
    sinkWriter = new PubSubSinkWriter<>(mockPublisher, mockSchema);
  }

  @Test
  public void flush_flushesPublisher() throws Exception {
    sinkWriter.flush(false);
    verify(mockPublisher).flush();
  }

  @Test
  public void close_flushesPublisher() throws Exception {
    sinkWriter.close();
    verify(mockPublisher).flush();
  }

  @Test
  public void publish_serializesMessage() throws Exception {
    PubsubMessage message =
        PubsubMessage.newBuilder().setData(ByteString.copyFromUtf8("data")).build();
    when(mockSchema.serialize("message")).thenReturn(message);
    sinkWriter.write(
        "message",
        new Context() {
          @Override
          public long currentWatermark() {
            return System.currentTimeMillis();
          }

          @Override
          public Long timestamp() {
            return System.currentTimeMillis();
          }
        });
    verify(mockPublisher).publish(message);
  }
}
