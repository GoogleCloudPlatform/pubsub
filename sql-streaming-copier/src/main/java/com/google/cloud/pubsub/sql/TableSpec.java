package com.google.cloud.pubsub.sql;

import com.google.auto.value.AutoValue;
import java.util.Map;

@AutoValue
public abstract class TableSpec {
  // The id for the table type in beam sql.
  public abstract String id();

  // The location string for the resource in beam sql.
  public abstract String location();

  // Additional properties to be passed to the beam sql table.
  public abstract Map<String, Object> properties();

  public static Builder builder() {
    return new AutoValue_TableSpec.Builder();
  }

  @AutoValue.Builder
  public static abstract class Builder {
    public abstract Builder setId(String id);

    public abstract Builder setLocation(String location);

    public abstract Builder setProperties(Map<String, Object> properties);

    public abstract TableSpec build();
  }
}
