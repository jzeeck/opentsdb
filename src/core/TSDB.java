// This file is part of OpenTSDB.
// Copyright (C) 2010-2012  The OpenTSDB Authors.
//
// This program is free software: you can redistribute it and/or modify it
// under the terms of the GNU Lesser General Public License as published by
// the Free Software Foundation, either version 2.1 of the License, or (at your
// option) any later version.  This program is distributed in the hope that it
// will be useful, but WITHOUT ANY WARRANTY; without even the implied warranty
// of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU Lesser
// General Public License for more details.  You should have received a copy
// of the GNU Lesser General Public License along with this program.  If not,
// see <http://www.gnu.org/licenses/>.
package net.opentsdb.core;

import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.google.common.base.Strings;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSortedSet;
import com.google.common.collect.Sets;
import com.google.common.collect.TreeMultimap;

import com.stumbleupon.async.Callback;
import com.stumbleupon.async.Deferred;
import com.stumbleupon.async.DeferredGroupException;

import net.opentsdb.tree.TreeRule;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.hbase.async.Bytes.ByteMap;
import org.hbase.async.HBaseException;
import org.hbase.async.TableNotFoundException;
import org.hbase.async.Bytes;
import org.hbase.async.HBaseClient;
import org.hbase.async.PutRequest;

import net.opentsdb.storage.hbase.HBaseStore;
import net.opentsdb.storage.TsdbStore;
import net.opentsdb.tsd.RTPublisher;
import net.opentsdb.tsd.RpcPlugin;
import net.opentsdb.tree.Branch;
import net.opentsdb.tree.Leaf;
import net.opentsdb.tree.Tree;
import net.opentsdb.tree.TreeBuilder;
import net.opentsdb.uid.NoSuchUniqueName;
import net.opentsdb.uid.UniqueId;
import net.opentsdb.uid.UniqueIdType;
import net.opentsdb.utils.Config;
import net.opentsdb.utils.DateTime;
import net.opentsdb.utils.PluginLoader;
import net.opentsdb.utils.JSONException;
import net.opentsdb.meta.Annotation;
import net.opentsdb.meta.TSMeta;
import net.opentsdb.meta.UIDMeta;
import net.opentsdb.search.SearchPlugin;
import net.opentsdb.search.SearchQuery;
import net.opentsdb.stats.Histogram;
import net.opentsdb.stats.StatsCollector;
import net.opentsdb.uid.NoSuchUniqueId;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

/**
 * Thread-safe implementation of the TSDB client.
 * <p>
 * This class is the central class of OpenTSDB.  You use it to add new data
 * points or query the database.
 */
public class TSDB {
  private static final Logger LOG = LoggerFactory.getLogger(TSDB.class);
  
  static final byte[] FAMILY = { 't' };

  /** Charset used to convert Strings to byte arrays and back. */
  private static final Charset CHARSET = Charset.forName("ISO-8859-1");

  /** TsdbStore, the database cluster to use for storage.  */
  final TsdbStore tsdb_store;

  /** Name of the table in which timeseries are stored.  */
  final byte[] table;
  /** Name of the table in which UID information is stored. */
  final byte[] uidtable;
  /** Name of the table where tree data is stored. */
  final byte[] treetable;
  /** Name of the table where meta data is stored. */
  final byte[] meta_table;

  /** Unique IDs for the metric names. */
  final UniqueId metrics;
  /** Unique IDs for the tag names. */
  final UniqueId tag_names;
  /** Unique IDs for the tag values. */
  final UniqueId tag_values;

  /** Configuration object for all TSDB components */
  final Config config;

  /** Search indexer to use if configure */
  private SearchPlugin search = null;
  
  /** Optional real time pulblisher plugin to use if configured */
  private RTPublisher rt_publisher = null;
  
  /** List of activated RPC plugins */
  private List<RpcPlugin> rpc_plugins = null;

  /**
   * Constructor
   * @param client An initialized TsdbStore object
   * @param config An initialized configuration object
   * @since 2.1
   */
  public TSDB(final TsdbStore client, final Config config) {
    this.config = checkNotNull(config);
    this.tsdb_store = checkNotNull(client);

    table = config.getString("tsd.storage.hbase.data_table").getBytes(CHARSET);
    uidtable = config.getString("tsd.storage.hbase.uid_table").getBytes(CHARSET);
    treetable = config.getString("tsd.storage.hbase.tree_table").getBytes(CHARSET);
    meta_table = config.getString("tsd.storage.hbase.meta_table").getBytes(CHARSET);

    metrics = new UniqueId(client, uidtable, UniqueIdType.METRIC);
    tag_names = new UniqueId(client, uidtable, UniqueIdType.TAGK);
    tag_values = new UniqueId(client, uidtable, UniqueIdType.TAGV);

    if (config.hasProperty("tsd.core.timezone")) {
      DateTime.setDefaultTimezone(config.getString("tsd.core.timezone"));
    }
    if (config.enable_realtime_ts() || config.enable_realtime_uid()) {
      // this is cleaner than another constructor and defaults to null. UIDs 
      // will be refactored with DAL code anyways
      metrics.setTSDB(this);
      tag_names.setTSDB(this);
      tag_values.setTSDB(this);
    }
    
    if (config.getBoolean("tsd.core.preload_uid_cache")) {
      final ByteMap<UniqueId> uid_cache_map = new ByteMap<UniqueId>();
      uid_cache_map.put(Const.METRICS_QUAL.getBytes(CHARSET), metrics);
      uid_cache_map.put(Const.TAG_NAME_QUAL.getBytes(CHARSET), tag_names);
      uid_cache_map.put(Const.TAG_VALUE_QUAL.getBytes(CHARSET), tag_values);
      UniqueId.preloadUidCache(this, uid_cache_map);
    }
    LOG.debug(config.dumpConfiguration());
  }

  /**
   * Constructor
   * @param config An initialized configuration object
   * @since 2.0
   */
  public TSDB(final Config config) {
    this(new HBaseStore(
      new HBaseClient(
        config.getString("tsd.storage.hbase.zk_quorum"),
        config.getString("tsd.storage.hbase.zk_basedir")), config),
         config);
  }
  
  /** @return The data point column family name */
  public static byte[] FAMILY() {
    return FAMILY;
  }

  /**
   * Deletes global or TSUID associated annotiations for the given time range.
   * @param tsuid An optional TSUID. If set to null, then global annotations for
   * the given range will be deleted
   * @param start_time A start timestamp in milliseconds
   * @param end_time An end timestamp in millseconds
   * @return The number of annotations deleted
   * @throws IllegalArgumentException if the timestamps are invalid
   * @since 2.1
   */
  public Deferred<Integer> deleteRange(final byte[] tsuid, final long start_time, final long end_time) {
    if (end_time < 1) {
      throw new IllegalArgumentException("The end timestamp has not been set");
    }
    if (end_time < start_time) {
      throw new IllegalArgumentException(
          "The end timestamp cannot be less than the start timestamp");
    }

    return tsdb_store.deleteAnnotationRange(tsuid, start_time, end_time);
  }

  /**
   * Scans through the global annotation storage rows and returns a list of
   * parsed annotation objects. If no annotations were found for the given
   * timespan, the resulting list will be empty.
   * @param start_time Start time to scan from. May be 0
   * @param end_time End time to scan to. Must be greater than 0
   * @return A list with detected annotations. May be empty.
   * @throws IllegalArgumentException if the end timestamp has not been set or
   * the end time is less than the start time
   */
  public Deferred<List<Annotation>> getGlobalAnnotations(final long start_time, final long end_time) {
    if (end_time < 1) {
      throw new IllegalArgumentException("The end timestamp has not been set");
    }
    if (end_time < start_time) {
      throw new IllegalArgumentException(
          "The end timestamp cannot be less than the start timestamp");
    }

    return tsdb_store.getGlobalAnnotations(start_time, end_time);
  }

  /**
   * Attempts to fetch a global or local annotation from storage
   * @param tsuid The TSUID as a string. May be empty if retrieving a global
   * annotation
   * @param start_time The start time as a Unix epoch timestamp
   * @return A valid annotation object if found, null if not
   */
  public Deferred<Annotation> getAnnotation(final String tsuid, final long start_time) {
    if (Strings.isNullOrEmpty(tsuid)) {
      return tsdb_store.getAnnotation(null, start_time);
    }

    return tsdb_store.getAnnotation(UniqueId.stringToUid(tsuid), start_time);
  }

  /**
   * Returns a partially initialized row key for this metric and these tags.
   * The only thing left to fill in is the base timestamp.
   * @since 2.0
   */
  Deferred<byte[]> rowKeyTemplateAsync(final String metric,
                                       final Map<String, String> tags) {
    final short metric_width = metrics.width();
    final short tag_name_width = tag_names.width();
    final short tag_value_width = tag_values.width();
    final short num_tags = (short) tags.size();

    int row_size = (metric_width + Const.TIMESTAMP_BYTES
                    + tag_name_width * num_tags
                    + tag_value_width * num_tags);
    final byte[] row = new byte[row_size];

    final boolean auto_create_metrics =
            config.getBoolean("tsd.core.auto_create_metrics");

    // Lookup or create the metric ID.
    final Deferred<byte[]> metric_id = metrics.getIdAsync(metric);

    // Copy the metric ID at the beginning of the row key.
    class CopyMetricInRowKeyCB implements Callback<byte[], byte[]> {
      public byte[] call(final byte[] metricid) {
        copyInRowKey(row, (short) 0, metricid);
        return row;
      }
    }

    class HandleNoSuchUniqueNameCB implements Callback<Object, Exception> {
      public Object call(final Exception e) {
        if (e instanceof NoSuchUniqueName && auto_create_metrics) {
          return metrics.createId(metric);
        }

        return e; // Other unexpected exception, let it bubble up.
      }
    }

    // Copy the tag IDs in the row key.
    class CopyTagsInRowKeyCB
      implements Callback<Deferred<byte[]>, ArrayList<byte[]>> {
      public Deferred<byte[]> call(final ArrayList<byte[]> tags) {
        short pos = metric_width;
        pos += Const.TIMESTAMP_BYTES;
        for (final byte[] tag : tags) {
          copyInRowKey(row, pos, tag);
          pos += tag.length;
        }
        // Once we've resolved all the tags, schedule the copy of the metric
        // ID and return the row key we produced.
        return metric_id
                .addErrback(new HandleNoSuchUniqueNameCB())
                .addCallback(new CopyMetricInRowKeyCB());
      }
    }

    // Kick off the resolution of all tags.
    return Tags.resolveOrCreateAllAsync(this, tags)
      .addCallbackDeferring(new CopyTagsInRowKeyCB());
  }

  /**
   * Copies the specified byte array at the specified offset in the row key.
   * @param row The row key into which to copy the bytes.
   * @param offset The offset in the row key to start writing at.
   * @param bytes The bytes to copy.
   */
  private void copyInRowKey(final byte[] row, final short offset, final byte[] bytes) {
    System.arraycopy(bytes, 0, row, offset, bytes.length);
  }

  /**
   * Should be called immediately after construction to initialize plugins and
   * objects that rely on such. It also moves most of the potential exception
   * throwing code out of the constructor so TSDMain can shutdown clients and
   * such properly.
   * @param init_rpcs Whether or not to initialize RPC plugins as well
   * @throws RuntimeException if the plugin path could not be processed
   * @throws IllegalArgumentException if a plugin could not be initialized
   * @since 2.0
   */
  public void initializePlugins(final boolean init_rpcs) {
    final String plugin_path = config.getString("tsd.core.plugin_path");
    if (plugin_path != null && !plugin_path.isEmpty()) {
      try {
        PluginLoader.loadJARs(plugin_path);
      } catch (Exception e) {
        LOG.error("Error loading plugins from plugin path: {}", plugin_path, e);
        throw new RuntimeException("Error loading plugins from plugin path: " + 
            plugin_path, e);
      }
    }

    // load the search plugin if enabled
    if (config.getBoolean("tsd.search.enable")) {
      search = PluginLoader.loadSpecificPlugin(
          config.getString("tsd.search.plugin"), SearchPlugin.class);
      if (search == null) {
        throw new IllegalArgumentException("Unable to locate search plugin: " + 
            config.getString("tsd.search.plugin"));
      }
      try {
        search.initialize(this);
      } catch (Exception e) {
        throw new RuntimeException("Failed to initialize search plugin", e);
      }
      LOG.info("Successfully initialized search plugin [{}] version: {}", search.getClass().getCanonicalName(), search.version());
    } else {
      search = null;
    }
    
    // load the real time publisher plugin if enabled
    if (config.getBoolean("tsd.rtpublisher.enable")) {
      rt_publisher = PluginLoader.loadSpecificPlugin(
          config.getString("tsd.rtpublisher.plugin"), RTPublisher.class);
      if (rt_publisher == null) {
        throw new IllegalArgumentException(
            "Unable to locate real time publisher plugin: " + 
            config.getString("tsd.rtpublisher.plugin"));
      }
      try {
        rt_publisher.initialize(this);
      } catch (Exception e) {
        throw new RuntimeException(
            "Failed to initialize real time publisher plugin", e);
      }
      LOG.info("Successfully initialized real time publisher plugin [{}] version: {}", rt_publisher.getClass().getCanonicalName(), rt_publisher.version());
    } else {
      rt_publisher = null;
    }
    
    if (init_rpcs && config.hasProperty("tsd.rpc.plugins")) {
      final String[] plugins = config.getString("tsd.rpc.plugins").split(",");
      for (final String plugin : plugins) {
        final RpcPlugin rpc = PluginLoader.loadSpecificPlugin(plugin.trim(), 
            RpcPlugin.class);
        if (rpc == null) {
          throw new IllegalArgumentException(
              "Unable to locate RPC plugin: " + plugin.trim());
        }
        try {
          rpc.initialize(this);
        } catch (Exception e) {
          throw new RuntimeException(
              "Failed to initialize RPC plugin", e);
        }
        
        if (rpc_plugins == null) {
          rpc_plugins = new ArrayList<RpcPlugin>(1);
        }
        rpc_plugins.add(rpc);
        LOG.info("Successfully initialized RPC plugin [{}] version: {}", rpc.getClass().getCanonicalName(), rpc.version());
      }
    }
  }
  
  /** 
   * Returns the configured TsdbStore
   * @return The TsdbStore
   * @since 2.0 
   */
  public final TsdbStore getTsdbStore() {
    return this.tsdb_store;
  }
  
  /** 
   * Getter that returns the configuration object
   * @return The configuration object
   * @since 2.0 
   */
  public final Config getConfig() {
    return this.config;
  }

  /**
   * Attempts to find the name for a unique identifier given a type
   * @param type The type of UID
   * @param uid The UID to search for
   * @return The name of the UID object if found
   * @throws IllegalArgumentException if the type is not valid
   * @throws NoSuchUniqueId if the UID was not found
   * @since 2.0
   */
  public Deferred<String> getUidName(final UniqueIdType type, final byte[] uid) {
    if (uid == null) {
      throw new IllegalArgumentException("Missing UID");
    }

    UniqueId uniqueId = uniqueIdInstanceForType(type);
    return uniqueId.getNameAsync(uid);
  }
  
  /**
   * Attempts to find the UID matching a given name
   * @param type The type of UID
   * @param name The name to search for
   * @throws IllegalArgumentException if the type is not valid
   * @since 2.0
   */
  public Deferred<byte[]> getUID(final UniqueIdType type, final String name) {
    if (name == null || name.isEmpty()) {
      throw new IllegalArgumentException("Missing UID name");
    }

    UniqueId uniqueId = uniqueIdInstanceForType(type);
    return uniqueId.getIdAsync(name);
  }
  
  /**
   * Verifies that the data and UID tables exist in TsdbStore and optionally the
   * tree and meta data tables if the user has enabled meta tracking or tree
   * building
   * @return An ArrayList of objects to wait for
   * @throws TableNotFoundException
   * @since 2.0
   */
  public Deferred<ArrayList<Object>> checkNecessaryTablesExist() {
    return tsdb_store.checkNecessaryTablesExist();
  }
  
  /** Number of cache hits during lookups involving UIDs. */
  public int uidCacheHits() {
    return (metrics.cacheHits() + tag_names.cacheHits()
            + tag_values.cacheHits());
  }

  /** Number of cache misses during lookups involving UIDs. */
  public int uidCacheMisses() {
    return (metrics.cacheMisses() + tag_names.cacheMisses()
            + tag_values.cacheMisses());
  }

  /** Number of cache entries currently in RAM for lookups involving UIDs. */
  public int uidCacheSize() {
    return (metrics.cacheSize() + tag_names.cacheSize()
            + tag_values.cacheSize());
  }

  /**
   * Collects the stats and metrics tracked by this instance.
   * @param collector The collector to use.
   */
  public void collectStats(final StatsCollector collector) {
    final byte[][] kinds = {
            Const.METRICS_QUAL.getBytes(CHARSET),
            Const.TAG_NAME_QUAL.getBytes(CHARSET),
            Const.TAG_VALUE_QUAL.getBytes(CHARSET)
    };
    try {
      final Map<String, Long> used_uids = UniqueId.getUsedUIDs(this, kinds)
              .joinUninterruptibly();

      collectUidStats(metrics, collector);
      collector.record("uid.ids-used", used_uids.get(Const.METRICS_QUAL),
              "kind=" + Const.METRICS_QUAL);
      collector.record("uid.ids-available",
              (metrics.maxPossibleId() - used_uids.get(Const.METRICS_QUAL)),
              "kind=" + Const.METRICS_QUAL);

      collectUidStats(tag_names, collector);
      collector.record("uid.ids-used", used_uids.get(Const.TAG_NAME_QUAL),
              "kind=" + Const.TAG_NAME_QUAL);
      collector.record("uid.ids-available",
              (tag_names.maxPossibleId() - used_uids.get(Const.TAG_NAME_QUAL)),
              "kind=" + Const.TAG_NAME_QUAL);

      collectUidStats(tag_values, collector);
      collector.record("uid.ids-used", used_uids.get(Const.TAG_VALUE_QUAL),
              "kind=" + Const.TAG_VALUE_QUAL);
      collector.record("uid.ids-available",
              (tag_values.maxPossibleId() - used_uids.get(Const.TAG_VALUE_QUAL)),
              "kind=" + Const.TAG_VALUE_QUAL);
    } catch (Exception e) {
      throw new RuntimeException("Shouldn't be here", e);
    }

    {
      final Runtime runtime = Runtime.getRuntime();
      collector.record("jvm.ramfree", runtime.freeMemory());
      collector.record("jvm.ramused", runtime.totalMemory());
    }

    collector.addExtraTag("class", "IncomingDataPoints");
    try {
      collector.record("hbase.latency", IncomingDataPoints.putlatency, "method=put");
    } finally {
      collector.clearExtraTag("class");
    }

    collector.addExtraTag("class", "TsdbQuery");
    try {
      collector.record("hbase.latency", Query.scanlatency, "method=scan");
    } finally {
      collector.clearExtraTag("class");
    }

    tsdb_store.recordStats(collector);

    // Collect Stats from Plugins
    if (rt_publisher != null) {
      try {
        collector.addExtraTag("plugin", "publish");
        rt_publisher.collectStats(collector);
      } finally {
        collector.clearExtraTag("plugin");
      }
    }
    if (search != null) {
      try {
        collector.addExtraTag("plugin", "search");
        search.collectStats(collector);
      } finally {
        collector.clearExtraTag("plugin");
      }
    }
    if (rpc_plugins != null) {
      try {
        collector.addExtraTag("plugin", "rpc");
        for (RpcPlugin rpc : rpc_plugins) {
          rpc.collectStats(collector);
        }
      } finally {
        collector.clearExtraTag("plugin");
      }
    }
  }

  /** Returns a latency histogram for Put RPCs used to store data points. */
  public Histogram getPutLatencyHistogram() {
    return IncomingDataPoints.putlatency;
  }

  /** Returns a latency histogram for Scan RPCs used to fetch data points.  */
  public Histogram getScanLatencyHistogram() {
    return Query.scanlatency;
  }

  /**
   * Collects the stats for a {@link UniqueId}.
   * @param uid The instance from which to collect stats.
   * @param collector The collector to use.
   */
  private static void collectUidStats(final UniqueId uid,
                                      final StatsCollector collector) {
    collector.record("uid.cache-hit", uid.cacheHits(), "kind=" + uid.kind());
    collector.record("uid.cache-miss", uid.cacheMisses(), "kind=" + uid.kind());
    collector.record("uid.cache-size", uid.cacheSize(), "kind=" + uid.kind());
  }

  /**
   * Returns a new {@link WritableDataPoints} instance suitable for this TSDB.
   * <p>
   * If you want to add a single data-point, consider using {@link #addPoint}
   * instead.
   */
  public WritableDataPoints newDataPoints() {
    return new IncomingDataPoints(this);
  }

  /**
   * Adds a single integer value data point in the TSDB.
   * @param metric A non-empty string.
   * @param timestamp The timestamp associated with the value.
   * @param value The value of the data point.
   * @param tags The tags on this series.  This map must be non-empty.
   * @return A deferred object that indicates the completion of the request.
   * The {@link Object} has not special meaning and can be {@code null} (think
   * of it as {@code Deferred<Void>}). But you probably want to attach at
   * least an errback to this {@code Deferred} to handle failures.
   * @throws IllegalArgumentException if the timestamp is less than or equal
   * to the previous timestamp added or 0 for the first timestamp, or if the
   * difference with the previous timestamp is too large.
   * @throws IllegalArgumentException if the metric name is empty or contains
   * illegal characters.
   * @throws IllegalArgumentException if the tags list is empty or one of the
   * elements contains illegal characters.
   * @throws HBaseException (deferred) if there was a problem while persisting
   * data.
   */
  public Deferred<Object> addPoint(final String metric,
                                   final long timestamp,
                                   final long value,
                                   final Map<String, String> tags) {
    final byte[] v;
    if (Byte.MIN_VALUE <= value && value <= Byte.MAX_VALUE) {
      v = new byte[] { (byte) value };
    } else if (Short.MIN_VALUE <= value && value <= Short.MAX_VALUE) {
      v = Bytes.fromShort((short) value);
    } else if (Integer.MIN_VALUE <= value && value <= Integer.MAX_VALUE) {
      v = Bytes.fromInt((int) value);
    } else {
      v = Bytes.fromLong(value);
    }
    final short flags = (short) (v.length - 1);  // Just the length.
    return addPointInternal(metric, timestamp, v, tags, flags);
  }

  /**
   * Adds a double precision floating-point value data point in the TSDB.
   * @param metric A non-empty string.
   * @param timestamp The timestamp associated with the value.
   * @param value The value of the data point.
   * @param tags The tags on this series.  This map must be non-empty.
   * @return A deferred object that indicates the completion of the request.
   * The {@link Object} has not special meaning and can be {@code null} (think
   * of it as {@code Deferred<Void>}). But you probably want to attach at
   * least an errback to this {@code Deferred} to handle failures.
   * @throws IllegalArgumentException if the timestamp is less than or equal
   * to the previous timestamp added or 0 for the first timestamp, or if the
   * difference with the previous timestamp is too large.
   * @throws IllegalArgumentException if the metric name is empty or contains
   * illegal characters.
   * @throws IllegalArgumentException if the value is NaN or infinite.
   * @throws IllegalArgumentException if the tags list is empty or one of the
   * elements contains illegal characters.
   * @throws HBaseException (deferred) if there was a problem while persisting
   * data.
   * @since 1.2
   */
  public Deferred<Object> addPoint(final String metric,
                                   final long timestamp,
                                   final double value,
                                   final Map<String, String> tags) {
    if (Double.isNaN(value) || Double.isInfinite(value)) {
      throw new IllegalArgumentException("value is NaN or Infinite: " + value
                                         + " for metric=" + metric
                                         + " timestamp=" + timestamp);
    }
    final short flags = Const.FLAG_FLOAT | 0x7;  // A float stored on 8 bytes.
    return addPointInternal(metric, timestamp,
                            Bytes.fromLong(Double.doubleToRawLongBits(value)),
                            tags, flags);
  }

  /**
   * Adds a single floating-point value data point in the TSDB.
   * @param metric A non-empty string.
   * @param timestamp The timestamp associated with the value.
   * @param value The value of the data point.
   * @param tags The tags on this series.  This map must be non-empty.
   * @return A deferred object that indicates the completion of the request.
   * The {@link Object} has not special meaning and can be {@code null} (think
   * of it as {@code Deferred<Void>}). But you probably want to attach at
   * least an errback to this {@code Deferred} to handle failures.
   * @throws IllegalArgumentException if the timestamp is less than or equal
   * to the previous timestamp added or 0 for the first timestamp, or if the
   * difference with the previous timestamp is too large.
   * @throws IllegalArgumentException if the metric name is empty or contains
   * illegal characters.
   * @throws IllegalArgumentException if the value is NaN or infinite.
   * @throws IllegalArgumentException if the tags list is empty or one of the
   * elements contains illegal characters.
   * @throws HBaseException (deferred) if there was a problem while persisting
   * data.
   */
  public Deferred<Object> addPoint(final String metric,
                                   final long timestamp,
                                   final float value,
                                   final Map<String, String> tags) {
    if (Float.isNaN(value) || Float.isInfinite(value)) {
      throw new IllegalArgumentException("value is NaN or Infinite: " + value
                                         + " for metric=" + metric
                                         + " timestamp=" + timestamp);
    }
    final short flags = Const.FLAG_FLOAT | 0x3;  // A float stored on 4 bytes.
    return addPointInternal(metric, timestamp,
            Bytes.fromInt(Float.floatToRawIntBits(value)),
            tags, flags);
  }

  private Deferred<Object> addPointInternal(final String metric,
                                            final long timestamp,
                                            final byte[] value,
                                            final Map<String, String> tags,
                                            final short flags) {
    checkTimestamp(timestamp);
    IncomingDataPoints.checkMetricAndTags(metric, tags);

    class RowKeyCB implements Callback<Deferred<Object>, byte[]> {
      @Override
      public Deferred<Object> call(byte[] row) throws Exception {
        final byte[] qualifier = Internal.buildQualifier(timestamp, flags);

        final long base_time = HBaseStore.buildBaseTime(timestamp);
        Bytes.setInt(row, (int) base_time, metrics.width());

        // TODO(tsuna): Add a callback to time the latency of HBase and store the
        // timing in a moving Histogram (once we have a class for this).
        Deferred<Object> result = tsdb_store.addPoint(row, qualifier, value);

        if (!config.enable_realtime_ts() && !config.enable_tsuid_incrementing() &&
                !config.enable_tsuid_tracking() && rt_publisher == null) {
          return result;
        }

        final byte[] tsuid = RowKey.tsuid(row);

        // for busy TSDs we may only enable TSUID tracking, storing a 1 in the
        // counter field for a TSUID with the proper timestamp. If the user would
        // rather have TSUID incrementing enabled, that will trump the PUT
        if (config.enable_tsuid_tracking() && !config.enable_tsuid_incrementing()) {
          final PutRequest tracking = new PutRequest(meta_table, tsuid,
                  TSMeta.FAMILY(), TSMeta.COUNTER_QUALIFIER(), Bytes.fromLong(1));
          tsdb_store.put(tracking);
        } else if (config.enable_tsuid_incrementing() || config.enable_realtime_ts()) {
          TSMeta.incrementAndGetCounter(TSDB.this, tsuid);
        }

        if (rt_publisher != null) {
          rt_publisher.sinkDataPoint(metric, timestamp, value, tags, tsuid, flags);
        }
        return result;
      }
    }

    return this.rowKeyTemplateAsync(metric, tags)
            .addCallbackDeferring(new RowKeyCB());
  }

  /**
   * Validates that the timestamp is within valid bounds.
   * @throws java.lang.IllegalArgumentException if the timestamp isn't within
   * bounds.
   */
  static long checkTimestamp(long timestamp) {
    checkArgument(timestamp >= 0, "The timestamp must be positive but was %s", timestamp);
    checkArgument((timestamp & Const.SECOND_MASK) == 0 || timestamp <= Const.MAX_MS_TIMESTAMP,
            "The timestamp was too large (%s)", timestamp);

    return timestamp;
  }

  /**
   * Forces a flush of any un-committed in memory data including left over 
   * compactions.
   * <p>
   * For instance, any data point not persisted will be sent to the TsdbStore.
   * @return A {@link Deferred} that will be called once all the un-committed
   * data has been successfully and durably stored.  The value of the deferred
   * object return is meaningless and unspecified, and can be {@code null}.
   * @throws HBaseException (deferred) if there was a problem sending
   * un-committed data to HBase.  Please refer to the {@link HBaseException}
   * hierarchy to handle the possible failures.  Some of them are easily
   * recoverable by retrying, some are not.
   */
  public Deferred<Object> flush() throws HBaseException {
    return tsdb_store.flush();
  }

  /**
   * Gracefully shuts down this TSD instance.
   * <p>
   * The method must call {@code shutdown()} on all plugins as well as flush the
   * compaction queue.
   * @return A {@link Deferred} that will be called once all the un-committed
   * data has been successfully and durably stored, and all resources used by
   * this instance have been released.  The value of the deferred object
   * return is meaningless and unspecified, and can be {@code null}.
   * @throws HBaseException (deferred) if there was a problem sending
   * un-committed data to HBase.  Please refer to the {@link HBaseException}
   * hierarchy to handle the possible failures.  Some of them are easily
   * recoverable by retrying, some are not.
   */
  public Deferred<Object> shutdown() {
    final ArrayList<Deferred<Object>> deferreds = 
      new ArrayList<Deferred<Object>>();
    
    final class StoreShutdown implements Callback<Object, ArrayList<Object>> {
      public Object call(final ArrayList<Object> args) {
        return tsdb_store.shutdown();
      }
      public String toString() {
        return "shutdown TsdbStore";
      }
    }
    
    final class ShutdownErrback implements Callback<Object, Exception> {
      public Object call(final Exception e) {
        final Logger LOG = LoggerFactory.getLogger(ShutdownErrback.class);
        if (e instanceof DeferredGroupException) {
          final DeferredGroupException ge = (DeferredGroupException) e;
          for (final Object r : ge.results()) {
            if (r instanceof Exception) {
              LOG.error("Failed to shutdown the TSD", (Exception) r);
            }
          }
        } else {
          LOG.error("Failed to shutdown the TSD", e);
        }
        return tsdb_store.shutdown();
      }
      public String toString() {
        return "shutdown TsdbStore after error";
      }
    }

    if (search != null) {
      LOG.info("Shutting down search plugin: {}", search.getClass().getCanonicalName());
      deferreds.add(search.shutdown());
    }
    if (rt_publisher != null) {
      LOG.info("Shutting down RT plugin: {}", rt_publisher.getClass().getCanonicalName());
      deferreds.add(rt_publisher.shutdown());
    }
    
    if (rpc_plugins != null && !rpc_plugins.isEmpty()) {
      for (final RpcPlugin rpc : rpc_plugins) {
        LOG.info("Shutting down RPC plugin: {}", rpc.getClass().getCanonicalName());
        deferreds.add(rpc.shutdown());
      }
    }
    
    // wait for plugins to shutdown before we close the TsdbStore
    return deferreds.size() > 0
      ? Deferred.group(deferreds).addCallbacks(new StoreShutdown(),
                                               new ShutdownErrback())
      : tsdb_store.shutdown();
  }

  /**
   * Given a prefix search, returns matching names from the specified id
   * type.
   * @param type The type of ids to search
   * @param search A prefix to search.
   * @since 2.0
   */
  public Deferred<List<String>> suggest(final UniqueIdType type,
                                        final String search) {
    UniqueId uniqueId = uniqueIdInstanceForType(type);
    return uniqueId.suggest(search);
  }

  /**
   * Given a prefix search, returns matching names from the specified id
   * type.
   * @param type The type of ids to search
   * @param search A prefix to search.
   * @param max_results Maximum number of results to return.
   * @since 2.0
   */
  public Deferred<List<String>> suggest(final UniqueIdType type,
                                        final String search,
                                        final int max_results) {
    UniqueId uniqueId = uniqueIdInstanceForType(type);
    return uniqueId.suggest(search, max_results);
  }

  /**
   * Discards all in-memory caches.
   * @since 1.1
   */
  public void dropCaches() {
    metrics.dropCaches();
    tag_names.dropCaches();
    tag_values.dropCaches();
  }

  /**
   * Attempts to assign a UID to a name for the given type
   * Used by the UniqueIdRpc call to generate IDs for new metrics, tagks or 
   * tagvs. The name must pass validation and if it's already assigned a UID,
   * this method will throw an error with the proper UID. Otherwise if it can
   * create the UID, it will be returned
   * @param type The type of uid to assign, metric, tagk or tagv
   * @param name The name of the uid object
   * @return A byte array with the UID if the assignment was successful
   * @throws IllegalArgumentException if the name is invalid or it already 
   * exists
   * @since 2.0
   */
  public byte[] assignUid(final UniqueIdType type, final String name) {
    Tags.validateString(type.toString(), name);
    UniqueId instance = uniqueIdInstanceForType(type);

    try {
      try {
        final byte[] uid = instance.getIdAsync(name).joinUninterruptibly();
        throw new IllegalArgumentException("Name already exists with UID: " +
                UniqueId.uidToString(uid));
      } catch (NoSuchUniqueName nsue) {
        return instance.createId(name).joinUninterruptibly();
      }
    } catch (Exception e) {
      throw Throwables.propagate(e);
    }
  }

  private UniqueId uniqueIdInstanceForType(UniqueIdType type) {
    switch (type) {
      case METRIC:
        return metrics;
      case TAGK:
        return tag_names;
      case TAGV:
        return tag_values;
      default:
        throw new IllegalArgumentException(type + " is unknown");
    }
  }
  
  /** @return the name of the UID table as a byte array for TsdbStore requests */
  public byte[] uidTable() {
    return this.uidtable;
  }
  
  /** @return the name of the data table as a byte array for TsdbStore requests */
  public byte[] dataTable() {
    return this.table;
  }
  
  /** @return the name of the tree table as a byte array for TsdbStore requests */
  public byte[] treeTable() {
    return this.treetable;
  }
  
  /** @return the name of the meta table as a byte array for TsdbStore requests */
  public byte[] metaTable() {
    return this.meta_table;
  }

  /**
   * Index the given timeseries meta object via the configured search plugin
   * @param meta The meta data object to index
   * @since 2.0
   */
  public void indexTSMeta(final TSMeta meta) {
    if (search != null) {
      search.indexTSMeta(meta).addErrback(new PluginError());
    }
  }
  
  /**
   * Delete the timeseries meta object from the search index
   * @param tsuid The TSUID to delete
   * @since 2.0
   */
  public void deleteTSMeta(final String tsuid) {
    if (search != null) {
      search.deleteTSMeta(tsuid).addErrback(new PluginError());
    }
  }
  
  /**
   * Index the given UID meta object via the configured search plugin
   * @param meta The meta data object to index
   * @since 2.0
   */
  public void indexUIDMeta(final UIDMeta meta) {
    if (search != null) {
      search.indexUIDMeta(meta).addErrback(new PluginError());
    }
  }
  
  /**
   * Delete the UID meta object from the search index
   * @param meta The UID meta object to delete
   * @since 2.0
   */
  public void deleteUIDMeta(final UIDMeta meta) {
    if (search != null) {
      search.deleteUIDMeta(meta).addErrback(new PluginError());
    }
  }
  
  /**
   * Index the given Annotation object via the configured search plugin
   * @param note The annotation object to index
   * @since 2.0
   */
  public void indexAnnotation(final Annotation note) {
    if (search != null) {
      search.indexAnnotation(note).addErrback(new PluginError());
    }
    if( rt_publisher != null ) {
    	rt_publisher.publishAnnotation(note);
    }
  }
  
  /**
   * Delete the annotation object from the search index
   * @param note The annotation object to delete
   * @since 2.0
   */
  public void deleteAnnotation(final Annotation note) {
    if (search != null) {
      search.deleteAnnotation(note).addErrback(new PluginError());
    }
  }
  
  /**
   * Processes the TSMeta through all of the trees if configured to do so
   * @param meta The meta data to process
   * @since 2.0
   */
  public Deferred<Boolean> processTSMetaThroughTrees(final TSMeta meta) {
    if (config.enable_tree_processing()) {
      return TreeBuilder.processAllTrees(this, meta);
    }
    return Deferred.fromResult(false);
  }
  
  /**
   * Executes a search query using the search plugin
   * @param query The query to execute
   * @return A deferred object to wait on for the results to be fetched
   * @throws IllegalStateException if the search plugin has not been enabled or
   * configured
   * @since 2.0
   */
  public Deferred<SearchQuery> executeSearch(final SearchQuery query) {
    if (search == null) {
      throw new IllegalStateException(
          "Searching has not been enabled on this TSD");
    }
    
    return search.executeQuery(query);
  }

  /**
   * Attempts a CompareAndSet storage call, loading the object from storage,
   * synchronizing changes, and attempting a put.
   * <b>Note:</b> If the local object didn't have any fields set by the caller
   * or there weren't any changes, then the data will not be written and an
   * exception will be thrown.
   * @param annotation The The Annotation we want to store.
   * @param overwrite When the RPC method is PUT, will overwrite all user
   * accessible fields
   * True if the storage call was successful, false if the object was
   * modified in storage during the CAS call. If false, retry the call. Other
   * failures will result in an exception being thrown.
   * @throws org.hbase.async.HBaseException if there was an issue
   * @throws IllegalArgumentException if required data was missing such as the
   * {@code #start_time}
   * @throws IllegalStateException if the data hasn't changed. This is OK!
   * @throws net.opentsdb.utils.JSONException if the object could not be serialized
   */
  public Deferred<Boolean> syncToStorage(final Annotation annotation,
                                         final boolean overwrite) {
    if (annotation.getStartTime() < 1) {
      throw new IllegalArgumentException("The start timestamp has not been set");
    }

    if (!annotation.hasChanges()) {
      LOG.debug("{} does not have changes, skipping sync to storage", annotation);
      throw new IllegalStateException("No changes detected in Annotation data");
    }

    final class StoreCB implements Callback<Deferred<Boolean>, Annotation> {
      @Override
      public Deferred<Boolean> call(final Annotation stored_note)
        throws Exception {
        if (stored_note != null) {
          annotation.syncNote(stored_note, overwrite);
        }

        return tsdb_store.updateAnnotation(stored_note, annotation);
      }
    }

    final byte[] tsuid;
    if (Strings.isNullOrEmpty(annotation.getTSUID())) {
      tsuid = null;
    } else {
      tsuid = UniqueId.stringToUid(annotation.getTSUID());
    }

    return tsdb_store.getAnnotation(tsuid, annotation.getStartTime()).addCallbackDeferring(new StoreCB());
  }

  /**
   * Attempts to mark an Annotation object for deletion. Note that if the
   * annotation does not exist in storage, this delete call will not throw an
   * error.
   *
   * @param annotation The Annotation we want to store.
   * @return A meaningless Deferred for the caller to wait on until the call is
   * complete. The value may be null.
   */
  public Deferred<Object> delete(Annotation annotation) {
    if (annotation.getStartTime() < 1) {
      throw new IllegalArgumentException("The start timestamp has not been set");
    }

    return tsdb_store.delete(annotation);
  }

  /**
   * Executes the query asynchronously
   * @return The data points matched by this query.
   * <p>
   * Each element in the non-{@code null} but possibly empty array returned
   * corresponds to one time series for which some data points have been
   * matched by the query.
   * @since 1.2
   * @param query
   */
  public Deferred<DataPoints[]> executeQuery(Query query) {
    return tsdb_store.executeQuery(query)
            .addCallback(new GroupByAndAggregateCB(query));
  }

  /**
   * Callback that should be attached the the output of
   * {@link net.opentsdb.storage.TsdbStore#executeQuery} to group and sort the results.
   */
  private class GroupByAndAggregateCB implements
          Callback<DataPoints[], ImmutableList<DataPoints>> {

    private final Query query;

    public GroupByAndAggregateCB(final Query query) {
      this.query = query;
    }

    /**
     * Creates the {@link SpanGroup}s to form the final results of this query.
     *
     * @param dps The {@link Span}s found for this query ({@link TSDB#findSpans}).
     *              Can be {@code null}, in which case the array returned will be empty.
     * @return A possibly empty array of {@link SpanGroup}s built according to
     * any 'GROUP BY' formulated in this query.
     */
    @Override
    public DataPoints[] call(final ImmutableList<DataPoints> dps) {
      if (dps.isEmpty()) {
        // Result is empty so return an empty array
        return new DataPoints[0];
      }

      TreeMultimap<String, DataPoints> spans2 = TreeMultimap.create();

      for (DataPoints dp : dps) {
        List<String> tsuids = dp.getTSUIDs();
        spans2.put(tsuids.get(0), dp);
      }

      Set<Span> spans = Sets.newTreeSet();
      for (String tsuid : spans2.keySet()) {
        spans.add(new Span(ImmutableSortedSet.copyOf(spans2.get(tsuid))));
      }

      final List<byte[]> group_bys = query.getGroupBys();
      if (group_bys == null) {
        // We haven't been asked to find groups, so let's put all the spans
        // together in the same group.
        final SpanGroup group = SpanGroup.create(query, spans);
        return new SpanGroup[]{group};
      }

      // Maps group value IDs to the SpanGroup for those values. Say we've
      // been asked to group by two things: foo=* bar=* Then the keys in this
      // map will contain all the value IDs combinations we've seen. If the
      // name IDs for `foo' and `bar' are respectively [0, 0, 7] and [0, 0, 2]
      // then we'll have group_bys=[[0, 0, 2], [0, 0, 7]] (notice it's sorted
      // by ID, so bar is first) and say we find foo=LOL bar=OMG as well as
      // foo=LOL bar=WTF and that the IDs of the tag values are:
      // LOL=[0, 0, 1] OMG=[0, 0, 4] WTF=[0, 0, 3]
      // then the map will have two keys:
      // - one for the LOL-OMG combination: [0, 0, 1, 0, 0, 4] and,
      // - one for the LOL-WTF combination: [0, 0, 1, 0, 0, 3].
      final ByteMap<SpanGroup> groups = new ByteMap<SpanGroup>();
      final byte[] group = new byte[group_bys.size() * Const.TAG_VALUE_WIDTH];

      for (final Span span : spans) {
        final Map<byte[], byte[]> dp_tags = span.tags();

        int i = 0;
        // TODO(tsuna): The following loop has a quadratic behavior. We can
        // make it much better since both the row key and group_bys are sorted.
        for (final byte[] tag_id : group_bys) {
          final byte[] value_id = dp_tags.get(tag_id);

          if (value_id == null) {
            throw new IllegalDataException("The " + span + " did not contain a " +
                    "value for the tag key " + Arrays.toString(tag_id));
          }

          System.arraycopy(value_id, 0, group, i, Const.TAG_VALUE_WIDTH);
          i += Const.TAG_VALUE_WIDTH;
        }

        //LOG.info("Span belongs to group " + Arrays.toString(group) + ": " + Arrays.toString(row));
        SpanGroup thegroup = groups.get(group);
        if (thegroup == null) {
          // Copy the array because we're going to keep `group' and overwrite
          // its contents. So we want the collection to have an immutable copy.
          final byte[] group_copy = Arrays.copyOf(group, group.length);

          thegroup = SpanGroup.create(query, null);
          groups.put(group_copy, thegroup);
        }
        thegroup.add(span);
      }

      //for (final Map.Entry<byte[], SpanGroup> entry : groups) {
      // LOG.info("group for " + Arrays.toString(entry.getKey()) + ": " + entry.getValue());
      //}
      return groups.values().toArray(new SpanGroup[groups.size()]);
    }
  }

  /**
   * Simply logs plugin errors when they're thrown by attaching as an errorback. 
   * Without this, exceptions will just disappear (unless logged by the plugin) 
   * since we don't wait for a result.
   */
  final class PluginError implements Callback<Object, Exception> {
    @Override
    public Object call(final Exception e) throws Exception {
      LOG.error("Exception from Search plugin indexer", e);
      return null;
    }
  }

  /**
   * Attempts a CompareAndSet storage call, loading the object from storage,
   * synchronizing changes, and attempting a put.
   * <b>Note:</b> If the local object didn't have any fields set by the caller
   * then the data will not be written.
   *
   * @param meta      The UIDMeta to store.
   * @param overwrite When the RPC method is PUT, will overwrite all user
   *                  accessible fields
   * @return True if the storage call was successful, false if the object
   * was
   * modified in storage during the CAS call. If false, retry the call. Other
   * failures will result in an exception being thrown.
   * @throws org.hbase.async.HBaseException           if there was an issue fetching
   * @throws IllegalArgumentException if parsing failed
   * @throws NoSuchUniqueId           If the UID does not exist
   * @throws IllegalStateException    if the data hasn't changed. This is OK!
   * @throws net.opentsdb.utils.JSONException            if the object could not be serialized
   */
  public Deferred<Boolean> syncUIDMetaToStorage(final UIDMeta meta,
                                                final boolean overwrite) {
    if (Strings.isNullOrEmpty(meta.getUID())) {
      throw new IllegalArgumentException("Missing UID");
    }
    if (meta.getType() == null) {
      throw new IllegalArgumentException("Missing type");
    }

    if (!meta.hasChanges()) {
      LOG.debug("{} does not have changes, skipping sync to storage", meta);
      throw new IllegalStateException("No changes detected in UID meta data");
    }

    return this.getUidName(meta.getType(),
      UniqueId.stringToUid(meta.getUID())).addCallbackDeferring(

      new Callback<Deferred<Boolean>, String>() {
        @Override
        public Deferred<Boolean> call(String arg) {
          return tsdb_store.updateMeta(meta, overwrite);
        }
      }
    );
  }

  /**
   * Attempts to delete the meta object from storage
   *
   * @param meta The meta object to delete
   * @return A deferred without meaning. The response may be null and should
   * only be used to track completion.
   * @throws org.hbase.async.HBaseException           if there was an issue
   * @throws IllegalArgumentException if data was missing (uid and type)
   */
  public Deferred<Object> delete(final UIDMeta meta) {
    if (Strings.isNullOrEmpty(meta.getUID())) {
      throw new IllegalArgumentException("Missing UID");
    }
    if (meta.getType() == null) {
      throw new IllegalArgumentException("Missing type");
    }

    return tsdb_store.delete(meta);
  }

  /**
   * Attempts to store a blank, new UID meta object in the proper location.
   * <b>Warning:</b> This should not be called by user accessible methods as it
   * will overwrite any data already in the column. This method does not use
   * a CAS, instead it uses a PUT to overwrite anything in the column.
   * @param meta The meta object to store
   * @return A deferred without meaning. The response may be null and should
   * only be used to track completion.
   * @throws org.hbase.async.HBaseException if there was an issue writing to storage
   * @throws IllegalArgumentException if data was missing
   * @throws net.opentsdb.utils.JSONException if the object could not be serialized
   */
  public Deferred<Object> add(final UIDMeta meta) {
    if (Strings.isNullOrEmpty(meta.getUID())) {
      throw new IllegalArgumentException("Missing UID");
    }
    if (meta.getType() == null) {
      throw new IllegalArgumentException("Missing type");
    }
    if (Strings.isNullOrEmpty(meta.getName())) {
      throw new IllegalArgumentException("Missing name");
    }

    return tsdb_store.add(meta);
  }

  /**
   * Convenience overload of {@code getUIDMeta(UniqueIdType, byte[])}
   * @param type The type of UID to fetch
   * @param uid The ID of the meta to fetch
   * @return A UIDMeta from storage or a default
   * @throws HBaseException if there was an issue fetching
   * @throws NoSuchUniqueId If the UID does not exist
   */
  public Deferred<UIDMeta> getUIDMeta(final UniqueIdType type,
                                             final String uid) {
    return getUIDMeta(type, UniqueId.stringToUid(uid));
  }

  /**
   * Verifies the UID object exists, then attempts to fetch the meta from
   * storage and if not found, returns a default object.
   * <p>
   * The reason for returning a default object (with the type, uid and name set)
   * is due to users who may have just enabled meta data or have upgraded; we
   * want to return valid data. If they modify the entry, it will write to
   * storage. You can tell it's a default if the {@code created} value is 0. If
   * the meta was generated at UID assignment or updated by the meta sync CLI
   * command, it will have a valid created timestamp.
   * @param type The type of UID to fetch
   * @param uid The ID of the meta to fetch
   * @return A UIDMeta from storage or a default
   * @throws HBaseException if there was an issue fetching
   * @throws NoSuchUniqueId If the UID does not exist
   */
  public Deferred<UIDMeta> getUIDMeta(final UniqueIdType type,
                                       final byte[] uid) {
    /**
     * Callback used to verify that the UID to name mapping exists. Uses the TSD
     * for verification so the name may be cached. If the name does not exist
     * it will throw a NoSuchUniqueId and the meta data will not be returned.
     * This helps in case the user deletes a UID but the meta data is still
     * stored. The fsck utility can be used later to cleanup orphaned objects.
     */
    class NameCB implements Callback<Deferred<UIDMeta>, String> {

      /**
       * Called after verifying that the name mapping exists
       * @return The results of {@link TsdbStore#getMeta(
       *      byte[], String, net.opentsdb.uid.UniqueIdType)}
       */
      @Override
      public Deferred<UIDMeta> call(final String name) throws Exception {
        return tsdb_store.getMeta(uid, name, type);
      }
    }

    // verify that the UID is still in the map before fetching from storage
    return getUidName(type, uid).addCallbackDeferring(new NameCB());
  }

  /**
   * Attempts to store the tree definition via a CompareAndSet call.
   *
   * @param tree The Tree to be stored.
   * @param overwrite Whether or not tree data should be overwritten
   * @return True if the write was successful, false if an error occurred
   * @throws IllegalArgumentException if the tree ID is missing or invalid
   * @throws HBaseException if a storage exception occurred
   */
  public Deferred<Boolean> storeTree(final Tree tree, final boolean overwrite) {
    Tree.validateTreeID(tree.getTreeId());
    // if there aren't any changes, save time and bandwidth by not writing to
    // storage
    if (!tree.hasChanged()) {
      LOG.debug("{} does not have changes, skipping sync to storage", this);
      throw new IllegalStateException("No changes detected in the tree");
    }
    return tsdb_store.storeTree(tree, overwrite);
  }

  /**
   * Attempts to fetch the given tree from storage, loading the rule set at
   * the same time.
   * @param tree_id The Tree to fetch
   * @return A tree object if found, null if the tree did not exist
   * @throws IllegalArgumentException if the tree ID was invalid
   * @throws HBaseException if a storage exception occurred
   * @throws JSONException if the object could not be deserialized
   */
  public Deferred<Tree> fetchTree(final int tree_id) {
    Tree.validateTreeID(tree_id);

    return tsdb_store.fetchTree(tree_id);
  }

  /**
   * Attempts to store the local tree in a new row, automatically assigning a
   * new tree ID and returning the value.
   * This method will scan the UID table for the maximum tree ID, increment it,
   * store the new tree, and return the new ID. If no trees have been created,
   * the returned ID will be "1". If we have reached the limit of trees for the
   * system, as determined by {@link Const#MAX_TREE_ID_EXCLUSIVE}, we will throw an
   * exception.
   *
   * @param tree The Tree to store
   * @return A positive ID, greater than 0 if successful, 0 if there was
   * an error
   */
  public Deferred<Integer> createNewTree(final Tree tree) {
    if (tree.getTreeId() > 0) {
      throw new IllegalArgumentException("Tree ID has already been set");
    }
    if (tree.getName() == null || tree.getName().isEmpty()) {
      throw new IllegalArgumentException("Tree was missing the name");
    }
    return tsdb_store.createNewTree(tree);
  }
  /**
   * Attempts to delete all branches, leaves, collisions and not-matched entries
   * for the given tree. Optionally can delete the tree definition and rules as
   * well.
   * <b>Warning:</b> This call can take a long time to complete so it should
   * only be done from a command line or issues once via RPC and allowed to
   * process. Multiple deletes running at the same time on the same tree
   * shouldn't be an issue but it's a waste of resources.
   * @param tree_id ID of the tree to delete
   * @param delete_definition Whether or not the tree definition and rule set
   * should be deleted as well
   * @return True if the deletion completed successfully, false if there was an
   * issue.
   * @throws HBaseException if there was an issue
   * @throws IllegalArgumentException if the tree ID was invalid
   */
  public Deferred<Boolean> deleteTree(final int tree_id, final boolean delete_definition) {

    Tree.validateTreeID(tree_id);

    return tsdb_store.deleteTree(tree_id, delete_definition);
  }
  /**
   * Returns the collision set from storage for the given tree, optionally for
   * only the list of TSUIDs provided.
   * <b>Note:</b> This can potentially be a large list if the rule set was
   * written poorly and there were many timeseries so only call this
   * without a list of TSUIDs if you feel confident the number is small.
   *
   * @param tree_id ID of the tree to fetch collisions for
   * @param tsuids An optional list of TSUIDs to fetch collisions for. This may
   * be empty or null, in which case all collisions for the tree will be
   * returned.
   * @return A list of collisions or null if nothing was found
   * @throws HBaseException if there was an issue
   * @throws IllegalArgumentException if the tree ID was invalid
   */
  public Deferred<Map<String, String>> fetchCollisions(final int tree_id,
                                                              final List<String> tsuids) {
    Tree.validateTreeID(tree_id);
    return tsdb_store.fetchCollisions(tree_id, tsuids);
  }

  /**
   * Returns the not-matched set from storage for the given tree, optionally for
   * only the list of TSUIDs provided.
   * <b>Note:</b> This can potentially be a large list if the rule set was
   * written poorly and there were many timeseries so only call this
   * without a list of TSUIDs if you feel confident the number is small.
   *
   * @param tree_id ID of the tree to fetch non matches for
   * @param tsuids An optional list of TSUIDs to fetch non-matches for. This may
   * be empty or null, in which case all non-matches for the tree will be
   * returned.
   * @return A list of not-matched mappings or null if nothing was found
   * @throws HBaseException if there was an issue
   * @throws IllegalArgumentException if the tree ID was invalid
   */
  public Deferred<Map<String, String>> fetchNotMatched(final int tree_id,
                                                       final List<String> tsuids) {
    Tree.validateTreeID(tree_id);
    return tsdb_store.fetchNotMatched(tree_id, tsuids);
  }

  /**
   * Attempts to flush the collisions to storage. The storage call is a PUT so
   * it will overwrite any existing columns, but since each column is the TSUID
   * it should only exist once and the data shouldn't change.
   * <b>Note:</b> This will also clear the {@link Tree#collisions} map
   *
   * @param tree The Tree to flush to storage.
   * @return A meaningless deferred (will always be true since we need to group
   * it with tree store calls) for the caller to wait on
   * @throws HBaseException if there was an issue
   */
  public Deferred<Boolean> flushTreeCollisions(final Tree tree) {
    if (!tree.getStoreFailures()) {
      tree.getCollisions().clear();
      return Deferred.fromResult(true);
    }

    return tsdb_store.flushTreeCollisions(tree);
  }
  /**
   * Attempts to flush the non-matches to storage. The storage call is a PUT so
   * it will overwrite any existing columns, but since each column is the TSUID
   * it should only exist once and the data shouldn't change.
   * <b>Note:</b> This will also clear the local {@link Tree#not_matched} map
   * @param tree The Tree to flush to storage.
   * @return A meaningless deferred (will always be true since we need to group
   * it with tree store calls) for the caller to wait on
   * @throws HBaseException if there was an issue
   */
  public Deferred<Boolean> flushTreeNotMatched(final Tree tree) {
    if (!tree.getStoreFailures()) {
      tree.getNotMatched().clear();
      return Deferred.fromResult(true);
    }
    return tsdb_store.flushTreeNotMatched(tree);
  }

  /**
   * Attempts to write the leaf to storage using a CompareAndSet call. We expect
   * the stored value to be null. If it's not, we fetched the stored leaf. If
   * the stored value is the TSUID as the local leaf, we return true since the
   * caller is probably reprocessing a timeseries. If the stored TSUID is
   * different, we store a collision in the tree and return false.
   * <b>Note:</b> You MUST write the tree to storage after calling this as there
   * may be a new collision. Check the tree's collision set.
   * @param leaf The Leaf to put to storage
   * @param branch The branch this leaf belongs to
   * @param tree Tree the leaf and branch belong to
   * @return True if the leaf was stored successful or already existed, false
   * if there was a collision
   * @throws HBaseException if there was an issue
   * @throws JSONException if the object could not be serialized
   */
  public Deferred<Boolean> storeLeaf(final Leaf leaf, final Branch branch,
                                     final Tree tree) {

    return tsdb_store.storeLeaf(leaf, branch, tree);
  }

  /**
   * Attempts to write the branch definition and optionally child leaves to
   * storage via CompareAndSets.
   * Each returned deferred will be a boolean regarding whether the CAS call
   * was successful or not. This will be a mix of the branch call and leaves.
   * Some of these may be false, which is OK, because if the branch
   * definition already exists, we don't need to re-write it. Leaves will
   * return false if there was a collision.
   * @param tree The tree to record collisions to
   * @param branch The branch to be stored
   * @param store_leaves Whether or not child leaves should be written to
   * storage
   * @return A list of deferreds to wait on for completion.
   * @throws HBaseException if there was an issue
   * @throws IllegalArgumentException if the tree ID was missing or data was
   * missing
   */
  public Deferred<ArrayList<Boolean>> storeBranch(final Tree tree,
                                                  final Branch branch,
                                                  final boolean store_leaves) {
    Tree.validateTreeID(branch.getTreeId());

    return tsdb_store.storeBranch(tree, branch, store_leaves);
  }

  /**
   * Attempts to fetch only the branch definition object from storage. This is
   * much faster than scanning many rows for child branches as per the
   * {@link #fetchBranch} call. Useful when building trees, particularly to
   * fetch the root branch.
   * @param branch_id ID of the branch to retrieve
   * @return A branch if found, null if it did not exist
   * @throws JSONException if the object could not be deserialized
   */
  public Deferred<Branch> fetchBranchOnly(final byte[] branch_id) {
    return tsdb_store.fetchBranchOnly(branch_id);
  }

  /**
   * Attempts to fetch the branch, it's leaves and all child branches.
   * The UID names for each leaf may also be loaded if configured.
   * @param branch_id ID of the branch to retrieve
   * @param load_leaf_uids Whether or not to load UID names for each leaf
   * @return A branch if found, null if it did not exist
   * @throws JSONException if the object could not be deserialized
   */
  public Deferred<Branch> fetchBranch(final byte[] branch_id,
                                      final boolean load_leaf_uids) {
    return tsdb_store.fetchBranch(branch_id, load_leaf_uids, this);
  }
  /**
   * Attempts to write the rule to storage via CompareAndSet, merging changes
   * with an existing rule.
   * <b>Note:</b> If the local object didn't have any fields set by the caller
   * or there weren't any changes, then the data will not be written and an
   * exception will be thrown.
   * <b>Note:</b> This method also validates the rule, making sure that proper
   * combinations of data exist before writing to storage.
   * @param rule The TreeRule to be stored
   * @param overwrite When the RPC method is PUT, will overwrite all user
   * accessible fields
   * @return True if the CAS call succeeded, false if the stored data was
   * modified in flight. This should be retried if that happens.
   * @throws HBaseException if there was an issue
   * @throws IllegalArgumentException if parsing failed or the tree ID was
   * invalid or validation failed
   * @throws IllegalStateException if the data hasn't changed. This is OK!
   * @throws JSONException if the object could not be serialized
   */
  public Deferred<Boolean> syncTreeRuleToStorage(final TreeRule rule,
                                         final boolean overwrite) {
    Tree.validateTreeID(rule.getTreeId());

    return tsdb_store.syncTreeRuleToStorage(rule, overwrite);
  }

  /**
   * Attempts to retrieve the specified tree rule from storage.
   * @param tree_id ID of the tree the rule belongs to
   * @param level Level where the rule resides
   * @param order Order where the rule resides
   * @return A TreeRule object if found, null if it does not exist
   * @throws HBaseException if there was an issue
   * @throws IllegalArgumentException if the one of the required parameters was
   * missing
   * @throws JSONException if the object could not be serialized
   */
  public Deferred<TreeRule> fetchTreeRule(final int tree_id, final int level,
                                          final int order) {

    TreeRule.validateTreeRule(tree_id, level, order);

    return tsdb_store.fetchTreeRule(tree_id, level, order);
  }
  /**
   * Attempts to delete the specified rule from storage
   * @param tree_id ID of the tree the rule belongs to
   * @param level Level where the rule resides
   * @param order Order where the rule resides
   * @return A deferred without meaning. The response may be null and should
   * only be used to track completion.
   * @throws HBaseException if there was an issue
   * @throws IllegalArgumentException if the one of the required parameters was
   * missing
   */
  public Deferred<Object> deleteTreeRule(final int tree_id,
                                                final int level,
                                                final int order) {
    TreeRule.validateTreeRule(tree_id, level, order);

    return tsdb_store.deleteTreeRule(tree_id, level, order);
  }

  /**
   * Attempts to delete all rules belonging to the given tree.
   * @param tree_id ID of the tree the rules belongs to
   * @return A deferred to wait on for completion. The value has no meaning and
   * may be null.
   * @throws HBaseException if there was an issue
   * @throws IllegalArgumentException if the one of the required parameters was
   * missing
   */
  public Deferred<Object> deleteAllTreeRules(final int tree_id) {

    Tree.validateTreeID(tree_id);

    return tsdb_store.deleteAllTreeRule(tree_id);
  }

}
