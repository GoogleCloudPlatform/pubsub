package com.google.cloud.pubsub.sql;

import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.values.PInput;
import org.apache.beam.sdk.values.POutput;

public class MakePtransform {

  private MakePtransform() {
  }

  /**
   * Creates a PTransform from a SerializableFunction from PInput to POutput.
   */
  public static <InputT extends PInput, OutputT extends POutput> PTransform<InputT, OutputT> from(
      SerializableFunction<InputT, OutputT> transform, String name) {
    return new PTransform<>(name) {
      @Override
      public OutputT expand(InputT input) {
        return transform.apply(input);
      }
    };
  }
}
