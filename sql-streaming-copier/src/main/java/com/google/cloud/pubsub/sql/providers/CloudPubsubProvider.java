package com.google.cloud.pubsub.sql.providers;

import static java.nio.charset.StandardCharsets.UTF_8;

import com.google.auto.service.AutoService;
import com.google.cloud.pubsub.sql.MakePtransform;
import com.google.cloud.pubsub.sql.Rows;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.schemas.Schema.FieldType;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.sdk.values.TypeDescriptor;

@AutoService({StandardSourceProvider.class, StandardSinkProvider.class})
public class CloudPubsubProvider implements StandardSourceProvider, StandardSinkProvider {
  static final String ROW_MESSAGE_KEY_ATTRIBUTE = "beam-sql-copier-message-key";
  static final Schema.FieldType ATTRIBUTE_MAP_FIELD_TYPE =
      Schema.FieldType.map(FieldType.STRING, FieldType.STRING);
  static final Schema SCHEMA =
      Schema.builder()
          .addByteArrayField("payload")
          .addField("attributes", ATTRIBUTE_MAP_FIELD_TYPE)
          .addField("event_timestamp", FieldType.DATETIME.withNullable(true))
          .build();

  private static Row toPubsubRow(Row input) {
    ImmutableMap.Builder<String, String> attributesBuilder = ImmutableMap.builder();
    for (Object standardAttribute : input.getArray("attributes")) {
      Row entryRow = (Row) standardAttribute;
      String key = entryRow.getString("key");
      ImmutableList.Builder<String> values = ImmutableList.builder();
      for (Object value : entryRow.getArray("values")) {
        byte[] bytes = (byte[]) value;
        values.add(new String(bytes, UTF_8));
      }
      attributesBuilder.put(key, String.join("|", values.build()));
    }
    byte[] key = input.getBytes(Rows.MESSAGE_KEY_FIELD);
    if (key.length != 0) {
      attributesBuilder.put(ROW_MESSAGE_KEY_ATTRIBUTE, new String(key, UTF_8));
    }
    return Row.withSchema(SCHEMA)
        .withFieldValue(Rows.PAYLOAD_FIELD, input.getBytes("payload"))
        .withFieldValue(Rows.ATTRIBUTES_FIELD, attributesBuilder.build())
        .withFieldValue(Rows.EVENT_TIMESTAMP_FIELD, input.getDateTime("event_timestamp"))
        .build();
  }

  private static Row fromPubsubRow(Row input) {
    ImmutableList.Builder<Row> entries = ImmutableList.builder();
    input.getMap(Rows.ATTRIBUTES_FIELD)
        .forEach((key, value) -> entries.add(Row.withSchema(Rows.ATTRIBUTES_ENTRY_SCHEMA)
            .attachValues(key, ImmutableList.of(value.toString().getBytes(UTF_8)))));
    return Row.withSchema(Rows.STANDARD_SCHEMA)
        .withFieldValue(Rows.PAYLOAD_FIELD, input.getBytes("payload"))
        .withFieldValue(Rows.ATTRIBUTES_FIELD, entries.build())
        .withFieldValue(Rows.EVENT_TIMESTAMP_FIELD, input.getDateTime("event_timestamp"))
        .withFieldValue(Rows.MESSAGE_KEY_FIELD, new byte[]{})
        .build();
  }

  @Override
  public StandardSink getSink() {
    return new StandardSink() {
      @Override
      public Schema nativeSchema() {
        return SCHEMA;
      }

      @Override
      public PTransform<PCollection<Row>, PCollection<Row>> transform() {
        return MakePtransform.from(rows -> rows.apply(
            MapElements.into(TypeDescriptor.of(Row.class)).via(CloudPubsubProvider::toPubsubRow))
            .setRowSchema(SCHEMA), "CloudPubsub Sink Transform");
      }
    };
  }

  @Override
  public StandardSource getSource() {
    return new StandardSource() {
      @Override
      public Schema nativeSchema() {
        return SCHEMA;
      }

      @Override
      public PTransform<PCollection<Row>, PCollection<Row>> transform() {
        return MakePtransform.from(rows -> rows.apply(
            MapElements.into(TypeDescriptor.of(Row.class)).via(CloudPubsubProvider::fromPubsubRow))
            .setRowSchema(Rows.STANDARD_SCHEMA), "CloudPubsub Source Transform");
      }
    };
  }

  @Override
  public String identifier() {
    return "pubsub";
  }
}
