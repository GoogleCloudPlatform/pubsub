package com.google.cloud.pubsub.sql;

import java.util.ServiceLoader;
import org.apache.beam.sdk.extensions.sql.meta.BeamSqlTable;
import org.apache.beam.sdk.extensions.sql.meta.Table;
import org.apache.beam.sdk.extensions.sql.meta.provider.TableProvider;
import org.apache.beam.sdk.extensions.sql.meta.store.InMemoryMetaStore;

/**
 * Loads all TableProviders on the classpath, then looks up the correct one based on the table id.
 */
public class TableLoader {

  private TableLoader() {
  }

  private static final InMemoryMetaStore META_STORE = new InMemoryMetaStore();

  static {
    ServiceLoader.load(TableProvider.class).forEach(META_STORE::registerProvider);
  }

  public static BeamSqlTable buildBeamSqlTable(Table table) {
    return META_STORE.buildBeamSqlTable(table);
  }
}
