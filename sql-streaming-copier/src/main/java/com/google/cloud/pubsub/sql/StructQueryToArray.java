package com.google.cloud.pubsub.sql;

import com.google.common.collect.ImmutableList;
import java.util.List;
import org.apache.beam.sdk.transforms.Combine.CombineFn;
import org.apache.beam.sdk.values.Row;

public class StructQueryToArray extends CombineFn<Row, List<Row>, List<Row>> {

  @Override
  public List<Row> createAccumulator() {
    return ImmutableList.of();
  }

  @Override
  public List<Row> addInput(List<Row> mutableAccumulator, Row input) {
    return ImmutableList.<Row>builder().addAll(mutableAccumulator).add(input).build();
  }

  @Override
  public List<Row> mergeAccumulators(Iterable<List<Row>> accumulators) {
    ImmutableList.Builder<Row> rows = ImmutableList.builder();
    for (List<Row> sublist : accumulators) {
      rows.addAll(sublist);
    }
    return rows.build();
  }

  @Override
  public List<Row> extractOutput(List<Row> accumulator) {
    return accumulator;
  }
}
