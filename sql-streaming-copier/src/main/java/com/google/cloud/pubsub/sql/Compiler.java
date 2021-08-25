package com.google.cloud.pubsub.sql;

import com.alibaba.fastjson.JSONObject;
import com.google.cloud.pubsub.sql.providers.StandardSinkProvider;
import com.google.cloud.pubsub.sql.providers.StandardSourceProvider;
import com.google.cloud.pubsub.sql.providers.StandardSink;
import com.google.cloud.pubsub.sql.providers.StandardSource;
import java.util.Map;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.extensions.sql.meta.Table;
import org.apache.beam.sdk.schemas.io.Providers;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.values.PBegin;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PDone;
import org.apache.beam.sdk.values.Row;

public class Compiler {

  private Compiler() {
  }

  private static JSONObject toProperties(Map<String, Object> map) {
    JSONObject properties = new JSONObject();
    map.forEach(properties::put);
    return properties;
  }

  private static PTransform<PBegin, PCollection<Row>> getSource(TableSpec spec) {
    StandardSource source =
        Providers.loadProviders(StandardSourceProvider.class).get(spec.id()).getSource();
    String name = spec.id() + "_sql_source";
    Table table =
        Table.builder()
            .name(name)
            .schema(source.nativeSchema())
            .location(spec.location())
            .type(spec.id())
            .properties(toProperties(spec.properties()))
            .build();
    PTransform<PCollection<Row>, PCollection<Row>> toStandard = source.transform();
    return MakePtransform.from(input -> {
      PCollection<Row> rows = TableLoader.buildBeamSqlTable(table).buildIOReader(input)
          .setRowSchema(source.nativeSchema());
      return rows.apply(toStandard);
    }, name);
  }

  private static PTransform<PCollection<Row>, PDone> getSink(TableSpec spec) {
    StandardSink sink =
        Providers.loadProviders(StandardSinkProvider.class).get(spec.id()).getSink();
    String name = spec.id() + "_sql_sink";
    Table table =
        Table.builder()
            .name(name)
            .schema(sink.nativeSchema())
            .location(spec.location())
            .type(spec.id())
            .properties(toProperties(spec.properties()))
            .build();
    PTransform<PCollection<Row>, PCollection<Row>> fromStandard = sink.transform();
    return MakePtransform.from(input -> {
      PCollection<Row> output = input.apply("Map to sink format", fromStandard);
      TableLoader.buildBeamSqlTable(table).buildIOWriter(output);
      return PDone.in(output.getPipeline());
    }, name);
  }

  public static void compile(Pipeline pipeline, TableSpec sourceSpec, TableSpec sinkSpec) {
    PCollection<Row> rows = pipeline.apply("Load from source", getSource(sourceSpec));
    rows.apply("Write to sink", getSink(sinkSpec));
  }
}
