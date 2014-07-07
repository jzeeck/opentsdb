package net.opentsdb.storage;

import com.stumbleupon.async.Deferred;
import net.opentsdb.utils.Config;
import org.hbase.async.AtomicIncrementRequest;
import org.hbase.async.PutRequest;
import org.hbase.async.DeleteRequest;
import org.hbase.async.GetRequest;
import org.hbase.async.KeyValue;
import org.hbase.async.Scanner;
import org.hbase.async.ClientStats;

import java.util.ArrayList;

/**
 * The HBaseClient that implements the client interface required by TSDB.
 */
public final class HBaseClient implements Client{
    final org.hbase.async.HBaseClient client;

  private final boolean ENABLE_REALTIME_TS;
  private final boolean ENABLE_REALTIME_UID;
  private final boolean ENABLE_TSUID_INCREMENTING;
  private final boolean ENABLE_TREE_PROCESSING;

  private final String DATA_TABLE_NAME;
  private final String UID_TABLE_NAME;
  private final String TREE_TABLE_NAME;
  private final String META_TABLE_NAME;

  public HBaseClient(final Config config) {
        this.client = new org.hbase.async.HBaseClient(
                config.getString("tsd.storage.hbase.zk_quorum"),
                config.getString("tsd.storage.hbase.zk_basedir"));

    ENABLE_TREE_PROCESSING = config.enable_tree_processing();

    ENABLE_REALTIME_TS = config.enable_realtime_ts();
    ENABLE_REALTIME_UID = config.enable_realtime_uid();
    ENABLE_TSUID_INCREMENTING = config.enable_tsuid_incrementing();

    DATA_TABLE_NAME = config.getString("tsd.storage.hbase.data_table");
    UID_TABLE_NAME = config.getString("tsd.storage.hbase.uid_table");
    TREE_TABLE_NAME = config.getString("tsd.storage.hbase.tree_table");
    META_TABLE_NAME = config.getString("tsd.storage.hbase.meta_table");
    }

    @Override
    public Deferred<Long> bufferAtomicIncrement(AtomicIncrementRequest request) {
        return this.client.bufferAtomicIncrement(request);
    }

    @Override
    public Deferred<Boolean> compareAndSet(PutRequest edit, byte[] expected) {
        return this.client.compareAndSet(edit ,expected);
    }

    @Override
    public Deferred<Object> delete(DeleteRequest request) {
        return this.client.delete(request);
    }

    @Override
    public Deferred<ArrayList<Object>> ensureTableExists() {
      final ArrayList<Deferred<Object>> checks =
          new ArrayList<Deferred<Object>>(2);

      checks.add(client.ensureTableExists(DATA_TABLE_NAME));
      checks.add(client.ensureTableExists(UID_TABLE_NAME));

      if (ENABLE_TREE_PROCESSING) {
        checks.add(client.ensureTableExists(TREE_TABLE_NAME));
      }
      if (ENABLE_REALTIME_TS || ENABLE_REALTIME_UID ||
          ENABLE_TSUID_INCREMENTING) {
        checks.add(client.ensureTableExists(META_TABLE_NAME));
      }

      return Deferred.group(checks);
    }

    @Override
    public Deferred<Object> flush() {
        return this.client.flush();
    }

    @Override
    public Deferred<ArrayList<KeyValue>> get(GetRequest request) {
        return this.client.get(request);
    }

    @Override
    public Scanner newScanner(byte[] table) {
        return this.client.newScanner(table);
    }

    @Override
    public Deferred<Object> put(PutRequest request) {
        return this.client.put(request);
    }

    @Override
    public Deferred<Object> close() {
        return this.client.shutdown();
    }

    @Override
    public ClientStats stats() {
        return this.client.stats();
    }

    @Override
    public void atomicIncrement(byte[] table, byte[] maxidRow, byte[] idFamily, byte[] bytes, long diff) {
        final AtomicIncrementRequest air = new AtomicIncrementRequest(table,
                maxidRow, idFamily, bytes, diff);
        this.client.atomicIncrement(air);
    }

    @Override
    public void setFlushInterval(short aShort) {
        this.client.setFlushInterval(aShort);
    }

    @Override
    public long getFlushInterval() {
        return this.client.getFlushInterval();
    }

    @Override
    public Deferred<Long> atomicIncrement(AtomicIncrementRequest air) {
        return this.client.atomicIncrement(air);
    }

}
