package com.google.cloud.pubsub.sql;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.google.auto.value.AutoValue;
import java.util.Map;

@AutoValue
public abstract class TableSpec {
  // The id for the table type in beam sql.
  public abstract String id();

  // The location string for the resource in beam sql.
  public abstract String location();

  // Additional properties to be passed to the beam sql table.
  public abstract JSONObject properties();

  public static Builder builder() {
    return new AutoValue_TableSpec.Builder();
  }

  public static TableSpec parse(String specJson) {
    JSONObject parsed = JSON.parseObject(specJson);
    Builder toReturn = builder();
    toReturn.setId(parsed.get("id").toString());
    toReturn.setLocation(parsed.get("location").toString());
    if (parsed.containsKey("properties")) {
      toReturn.setProperties(parsed.getJSONObject("properties"));
    } else {
      toReturn.setProperties(new JSONObject());
    }
    return toReturn.build();
  }

  @AutoValue.Builder
  public static abstract class Builder {
    public abstract Builder setId(String id);

    public abstract Builder setLocation(String location);

    public abstract Builder setProperties(JSONObject properties);
    public Builder setProperties(Map<String, Object> properties) {
      JSONObject object = new JSONObject();
      properties.forEach(object::put);
      return setProperties(object);
    }

    public abstract TableSpec build();
  }
}
