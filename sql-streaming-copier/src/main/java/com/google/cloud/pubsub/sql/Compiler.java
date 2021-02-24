package com.google.cloud.pubsub.sql;

import com.alibaba.fastjson.JSONObject;
import com.google.cloud.pubsub.sql.providers.StandardSinkProvider;
import com.google.cloud.pubsub.sql.providers.StandardSourceProvider;
import com.google.cloud.pubsub.sql.providers.StandardSqlSink;
import com.google.cloud.pubsub.sql.providers.StandardSqlSource;
import java.util.Map;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.extensions.sql.SqlTransform;
import org.apache.beam.sdk.extensions.sql.meta.Table;
import org.apache.beam.sdk.schemas.io.Providers;
import org.apache.beam.sdk.schemas.transforms.Cast;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.values.PBegin;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PDone;
import org.apache.beam.sdk.values.Row;

public class Compiler {
  private Compiler() {}

  private static JSONObject toProperties(Map<String, String> map) {
    JSONObject properties = new JSONObject();
    map.forEach(properties::put);
    return properties;
  }

  private static PTransform<PBegin, PCollection<Row>> getSource(TableSpec spec) {
    StandardSqlSource source =
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
    String select = source.selectStatement();
    return new PTransform<>(name) {
      @Override
      public PCollection<Row> expand(PBegin input) {
        PCollection<Row> rows = TableLoader.buildBeamSqlTable(table).buildIOReader(input);
        if (!select.isEmpty()) {
          rows = rows.apply(SqlTransform.query(select));
        }
        return rows;
      }
    };
  }

  private static PTransform<PCollection<Row>, PDone> getSink(TableSpec spec) {
    StandardSqlSink sink =
        Providers.loadProviders(StandardSinkProvider.class).get(spec.id()).getSink();
    String name = spec.id() + "_sql_source";
    Table table =
        Table.builder()
            .name(name)
            .schema(sink.nativeSchema())
            .location(spec.location())
            .type(spec.id())
            .properties(toProperties(spec.properties()))
            .build();
    String select = sink.selectStatement();
    return new PTransform<>(name) {
      @Override
      public PDone expand(PCollection<Row> input) {
        if (!select.isEmpty()) {
          input = input.apply(SqlTransform.query(select));
        }
        TableLoader.buildBeamSqlTable(table).buildIOWriter(input);
        return PDone.in(input.getPipeline());
      }
    };
  }

  public static void compile(Pipeline pipeline, TableSpec sourceSpec, TableSpec sinkSpec) {
    PCollection<Row> rows = pipeline.apply("Load from source", getSource(sourceSpec));
    PCollection<Row> standardized =
        rows.apply("Cast to standard", Cast.widening(Rows.STANDARD_SCHEMA));
    standardized.apply(getSink(sinkSpec));
  }
}
