package com.google.cloud.pubsub.sql;

import com.google.auto.value.AutoValue;
import java.util.Map;

@AutoValue
public abstract class TableSpec {
  public abstract String id();

  public abstract String location();

  public abstract Map<String, String> properties();

  public static Builder builder() {
    return new AutoValue_TableSpec.Builder();
  }

  @AutoValue.Builder
  public static abstract class Builder {
    public abstract Builder setId(String id);

    public abstract Builder setLocation(String location);

    public abstract Builder setProperties(Map<String, String> properties);

    public abstract TableSpec build();
  }
}
