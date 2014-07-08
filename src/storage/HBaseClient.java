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
public final class HBaseClient implements Client {
  private final org.hbase.async.HBaseClient client;

  private final boolean enable_realtime_ts;
  private final boolean enable_realtime_uid;
  private final boolean enable_tsuid_incrementing;
  private final boolean enable_tree_processing;

  private final String data_table_name;
  private final String uid_table_name;
  private final String tree_table_name;
  private final String meta_table_name;


  public HBaseClient(final Config config) {
    this.client = new org.hbase.async.HBaseClient(
        config.getString("tsd.storage.hbase.zk_quorum"),
        config.getString("tsd.storage.hbase.zk_basedir"));

    enable_tree_processing = config.enable_tree_processing();

    enable_realtime_ts = config.enable_realtime_ts();
    enable_realtime_uid = config.enable_realtime_uid();
    enable_tsuid_incrementing = config.enable_tsuid_incrementing();

    data_table_name = config.getString("tsd.storage.hbase.data_table");
    uid_table_name = config.getString("tsd.storage.hbase.uid_table");
    tree_table_name = config.getString("tsd.storage.hbase.tree_table");
    meta_table_name = config.getString("tsd.storage.hbase.meta_table");
  }

  @Override
  public Deferred<Long> bufferAtomicIncrement(AtomicIncrementRequest request) {
    return this.client.bufferAtomicIncrement(request);
  }

  @Override
  public Deferred<Boolean> compareAndSet(PutRequest edit, byte[] expected) {
    return this.client.compareAndSet(edit, expected);
  }

  @Override
  public Deferred<Object> delete(DeleteRequest request) {
    return this.client.delete(request);
  }

  @Override
  public Deferred<ArrayList<Object>> ensureTableExists() {
    final ArrayList<Deferred<Object>> checks =
        new ArrayList<Deferred<Object>>(2);

    checks.add(client.ensureTableExists(data_table_name));
    checks.add(client.ensureTableExists(uid_table_name));

    if (enable_tree_processing) {
      checks.add(client.ensureTableExists(tree_table_name));
    }
    if (enable_realtime_ts || enable_realtime_uid ||
        enable_tsuid_incrementing) {
      checks.add(client.ensureTableExists(meta_table_name));
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
