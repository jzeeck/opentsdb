// This file is part of OpenTSDB.
// Copyright (C) 2013  The OpenTSDB Authors.
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

import static net.opentsdb.uid.UniqueId.UniqueIdType.*;
import static org.junit.Assert.*;

import java.lang.reflect.Field;
import java.util.HashMap;

import net.opentsdb.meta.UIDMeta;
import net.opentsdb.storage.MemoryStore;
import net.opentsdb.tree.Branch;
import net.opentsdb.tree.Tree;
import net.opentsdb.tree.TreeRule;
import net.opentsdb.uid.NoSuchUniqueId;
import net.opentsdb.uid.NoSuchUniqueName;
import net.opentsdb.utils.Config;

import org.hbase.async.Bytes;

import org.junit.Before;
import org.junit.Test;

public final class TestTSDB {
  private Config config;
  private TSDB tsdb;
  private MemoryStore tsdb_store;
  
  @Before
  public void before() throws Exception {
    config = new Config(false);
    config.setFixDuplicates(true); // TODO(jat): test both ways
    tsdb_store = new MemoryStore();
    tsdb = new TSDB(tsdb_store, config);
  }
  
  @Test
  public void initializePluginsDefaults() {
    // no configured plugin path, plugins disabled, no exceptions
    tsdb.initializePlugins(true);
  }
  
  @Test
  public void initializePluginsPathSet() throws Exception {
    Field properties = config.getClass().getDeclaredField("properties");
    properties.setAccessible(true);
    @SuppressWarnings("unchecked")
    HashMap<String, String> props = 
      (HashMap<String, String>) properties.get(config);
    props.put("tsd.core.plugin_path", "./");
    properties.setAccessible(false);
    tsdb.initializePlugins(true);
  }
  
  @Test (expected = RuntimeException.class)
  public void initializePluginsPathBad() throws Exception {
    Field properties = config.getClass().getDeclaredField("properties");
    properties.setAccessible(true);
    @SuppressWarnings("unchecked")
    HashMap<String, String> props = 
      (HashMap<String, String>) properties.get(config);
    props.put("tsd.core.plugin_path", "./doesnotexist");
    properties.setAccessible(false);
    tsdb.initializePlugins(true);
  }
  
  @Test
  public void initializePluginsSearch() throws Exception {
    Field properties = config.getClass().getDeclaredField("properties");
    properties.setAccessible(true);
    @SuppressWarnings("unchecked")
    HashMap<String, String> props = 
      (HashMap<String, String>) properties.get(config);
    props.put("tsd.core.plugin_path", "./");
    props.put("tsd.search.enable", "true");
    props.put("tsd.search.plugin", "net.opentsdb.search.DummySearchPlugin");
    props.put("tsd.search.DummySearchPlugin.hosts", "localhost");
    props.put("tsd.search.DummySearchPlugin.port", "42");
    properties.setAccessible(false);
    tsdb.initializePlugins(true);
  }
  
  @Test (expected = RuntimeException.class)
  public void initializePluginsSearchNotFound() throws Exception {
    Field properties = config.getClass().getDeclaredField("properties");
    properties.setAccessible(true);
    @SuppressWarnings("unchecked")
    HashMap<String, String> props = 
      (HashMap<String, String>) properties.get(config);
    props.put("tsd.search.enable", "true");
    props.put("tsd.search.plugin", "net.opentsdb.search.DoesNotExist");
    properties.setAccessible(false);
    tsdb.initializePlugins(true);
  }
  
  @Test
  public void getClient() {
    assertNotNull(tsdb.getTsdbStore());
  }
  
  @Test
  public void getConfig() {
    assertNotNull(tsdb.getConfig());
  }
  
  @Test
  public void getUidNameMetric() throws Exception {
    setGetUidName();
    assertEquals("sys.cpu.0", tsdb.getUidName(METRIC,
        new byte[] { 0, 0, 1 }).joinUninterruptibly());
  }
  
  @Test
  public void getUidNameTagk() throws Exception {
    setGetUidName();
    assertEquals("host", tsdb.getUidName(TAGK,
        new byte[] { 0, 0, 1 }).joinUninterruptibly());
  }
  
  @Test
  public void getUidNameTagv() throws Exception {
    setGetUidName();
    assertEquals("web01", tsdb.getUidName(TAGV,
        new byte[] { 0, 0, 1 }).joinUninterruptibly());
  }
  
  @Test (expected = NoSuchUniqueId.class)
  public void getUidNameMetricNSU() throws Exception {
    setGetUidName();
    tsdb.getUidName(METRIC, new byte[] { 0, 0, 2 })
    .joinUninterruptibly();
  }
  
  @Test (expected = NoSuchUniqueId.class)
  public void getUidNameTagkNSU() throws Exception {
    setGetUidName();
    tsdb.getUidName(TAGK, new byte[] { 0, 0, 2 })
    .joinUninterruptibly();
  }
  
  @Test (expected = NoSuchUniqueId.class)
  public void getUidNameTagvNSU() throws Exception {
    setGetUidName();
    tsdb.getUidName(TAGV, new byte[] { 0, 0, 2 })
    .joinUninterruptibly();
  }
  
  @Test (expected = NullPointerException.class)
  public void getUidNameNullType() throws Exception {
    setGetUidName();
    tsdb.getUidName(null, new byte[] { 0, 0, 2 }).joinUninterruptibly();
  }
  
  @Test (expected = IllegalArgumentException.class)
  public void getUidNameNullUID() throws Exception {
    setGetUidName();
    tsdb.getUidName(TAGV, null).joinUninterruptibly();
  }
  
  @Test
  public void getUIDMetric() throws Exception {
    setupAssignUid();
    assertArrayEquals(new byte[] { 0, 0, 1 }, 
        tsdb.getUID(METRIC, "sys.cpu.0").joinUninterruptibly());
  }
  
  @Test
  public void getUIDTagk() throws Exception {
    setupAssignUid();
    assertArrayEquals(new byte[] { 0, 0, 1 }, 
        tsdb.getUID(TAGK, "host").joinUninterruptibly());
  }
  
  @Test
  public void getUIDTagv() throws Exception {
    setupAssignUid();
    assertArrayEquals(new byte[] { 0, 0, 1 }, 
        tsdb.getUID(TAGV, "localhost").joinUninterruptibly());
  }
  
  @Test (expected = NoSuchUniqueName.class)
  public void getUIDMetricNSU() throws Exception {
    setupAssignUid();
    tsdb.getUID(METRIC, "sys.cpu.2").joinUninterruptibly();
  }
  
  @Test (expected = NoSuchUniqueName.class)
  public void getUIDTagkNSU() throws Exception {
    setupAssignUid();
    tsdb.getUID(TAGK, "region").joinUninterruptibly();
  }
  
  @Test (expected = NoSuchUniqueName.class)
  public void getUIDTagvNSU() throws Exception {
    setupAssignUid();
    tsdb.getUID(TAGV, "yourserver").joinUninterruptibly();
  }
  
  @Test (expected = NullPointerException.class)
  public void getUIDNullType() {
    setupAssignUid();
    tsdb.getUID(null, "sys.cpu.1");
  }
  
  @Test (expected = IllegalArgumentException.class)
  public void getUIDNullName() {
    setupAssignUid();
    tsdb.getUID(TAGV, null);
  }
  
  @Test (expected = IllegalArgumentException.class)
  public void getUIDEmptyName() {
    setupAssignUid();
    tsdb.getUID(TAGV, "");
  }
  
  @Test
  public void assignUidMetric() {
    setupAssignUid();
    assertArrayEquals(new byte[] { 0, 0, 3 },
        tsdb.assignUid(METRIC, "sys.cpu.2"));
  }
  
  @Test (expected = IllegalArgumentException.class)
  public void assignUidMetricExists() {
    setupAssignUid();
    tsdb.assignUid(METRIC, "sys.cpu.0");
  }
  
  @Test
  public void assignUidTagk() {
    setupAssignUid();
    assertArrayEquals(new byte[] {0, 0, 3},
        tsdb.assignUid(TAGK, "region"));
  }
  
  @Test (expected = IllegalArgumentException.class)
  public void assignUidTagkExists() {
    setupAssignUid();
    tsdb.assignUid(TAGK, "host");
  }
  
  @Test
  public void assignUidTagv() {
    setupAssignUid();
    assertArrayEquals(new byte[] {0, 0, 3},
        tsdb.assignUid(TAGV, "yourserver"));
  }
  
  @Test (expected = IllegalArgumentException.class)
  public void assignUidTagvExists() {
    setupAssignUid();
    tsdb.assignUid(TAGV, "localhost");
  }
  
  @Test (expected = NullPointerException.class)
  public void assignUidNullType() {
    setupAssignUid();
    tsdb.assignUid(null, "localhost");
  }
  
  @Test (expected = IllegalArgumentException.class)
  public void assignUidNullName() {
    setupAssignUid();
    tsdb.assignUid(METRIC, null);
  }
  
  @Test (expected = IllegalArgumentException.class)
  public void assignUidInvalidCharacter() {
    setupAssignUid();
    tsdb.assignUid(METRIC, "Not!A:Valid@Name");
  }
  
  @Test
  public void uidTable() {
    assertNotNull(tsdb.uidTable());
    assertArrayEquals("tsdb-uid".getBytes(), tsdb.uidTable());
  }

  @Test
  public void addPointLong1Byte() throws Exception {
    setupAddPointStorage();
    HashMap<String, String> tags = new HashMap<String, String>(1);
    tags.put("host", "web01");
    tsdb.addPoint("sys.cpu.user", 1356998400, 42, tags).joinUninterruptibly();
    final byte[] row = new byte[] { 0, 0, 1, 0x50, (byte) 0xE2, 0x27, 0, 
        0, 0, 1, 0, 0, 1};
    final byte[] value = tsdb_store.getColumnDataTable(row, new byte[] { 0, 0 });
    assertNotNull(value);
    assertEquals(42, value[0]);
  }
  
  @Test
  public void addPointLong1ByteNegative() throws Exception {
    setupAddPointStorage();
    HashMap<String, String> tags = new HashMap<String, String>(1);
    tags.put("host", "web01");
    tsdb.addPoint("sys.cpu.user", 1356998400, -42, tags).joinUninterruptibly();
    final byte[] row = new byte[] { 0, 0, 1, 0x50, (byte) 0xE2, 0x27, 0, 
        0, 0, 1, 0, 0, 1};
    final byte[] value = tsdb_store.getColumnDataTable(row, new byte[] { 0, 0 });
    assertNotNull(value);
    assertEquals(-42, value[0]);
  }
  
  @Test
  public void addPointLong2Bytes() throws Exception {
    setupAddPointStorage();
    HashMap<String, String> tags = new HashMap<String, String>(1);
    tags.put("host", "web01");
    tsdb.addPoint("sys.cpu.user", 1356998400, 257, tags).joinUninterruptibly();
    final byte[] row = new byte[] { 0, 0, 1, 0x50, (byte) 0xE2, 0x27, 0, 
        0, 0, 1, 0, 0, 1};
    final byte[] value = tsdb_store.getColumnDataTable(row, new byte[] { 0, 1 });
    assertNotNull(value);
    assertEquals(257, Bytes.getShort(value));
  }
  
  @Test
  public void addPointLong2BytesNegative() throws Exception {
    setupAddPointStorage();
    HashMap<String, String> tags = new HashMap<String, String>(1);
    tags.put("host", "web01");
    tsdb.addPoint("sys.cpu.user", 1356998400, -257, tags).joinUninterruptibly();
    final byte[] row = new byte[] { 0, 0, 1, 0x50, (byte) 0xE2, 0x27, 0, 
        0, 0, 1, 0, 0, 1};
    final byte[] value = tsdb_store.getColumnDataTable(row, new byte[] { 0, 1 });
    assertNotNull(value);
    assertEquals(-257, Bytes.getShort(value));
  }
  
  @Test
  public void addPointLong4Bytes() throws Exception {
    setupAddPointStorage();
    HashMap<String, String> tags = new HashMap<String, String>(1);
    tags.put("host", "web01");
    tsdb.addPoint("sys.cpu.user", 1356998400, 65537, tags).joinUninterruptibly();
    final byte[] row = new byte[] { 0, 0, 1, 0x50, (byte) 0xE2, 0x27, 0, 
        0, 0, 1, 0, 0, 1};
    final byte[] value = tsdb_store.getColumnDataTable(row, new byte[] { 0, 3 });
    assertNotNull(value);
    assertEquals(65537, Bytes.getInt(value));
  }
  
  @Test
  public void addPointLong4BytesNegative() throws Exception {
    setupAddPointStorage();
    HashMap<String, String> tags = new HashMap<String, String>(1);
    tags.put("host", "web01");
    tsdb.addPoint("sys.cpu.user", 1356998400, -65537, tags).joinUninterruptibly();
    final byte[] row = new byte[] { 0, 0, 1, 0x50, (byte) 0xE2, 0x27, 0, 
        0, 0, 1, 0, 0, 1};
    final byte[] value = tsdb_store.getColumnDataTable(row, new byte[] { 0, 3 });
    assertNotNull(value);
    assertEquals(-65537, Bytes.getInt(value));
  }
  
  @Test
  public void addPointLong8Bytes() throws Exception {
    setupAddPointStorage();
    HashMap<String, String> tags = new HashMap<String, String>(1);
    tags.put("host", "web01");
    tsdb.addPoint("sys.cpu.user", 1356998400, 4294967296L, tags).joinUninterruptibly();
    final byte[] row = new byte[] { 0, 0, 1, 0x50, (byte) 0xE2, 0x27, 0, 
        0, 0, 1, 0, 0, 1};
    final byte[] value = tsdb_store.getColumnDataTable(row, new byte[] { 0, 7 });
    assertNotNull(value);
    assertEquals(4294967296L, Bytes.getLong(value));
  }
  
  @Test
  public void addPointLong8BytesNegative() throws Exception {
    setupAddPointStorage();
    HashMap<String, String> tags = new HashMap<String, String>(1);
    tags.put("host", "web01");
    tsdb.addPoint("sys.cpu.user", 1356998400, -4294967296L, tags).joinUninterruptibly();
    final byte[] row = new byte[] { 0, 0, 1, 0x50, (byte) 0xE2, 0x27, 0, 
        0, 0, 1, 0, 0, 1};
    final byte[] value = tsdb_store.getColumnDataTable(row, new byte[] { 0, 7 });
    assertNotNull(value);
    assertEquals(-4294967296L, Bytes.getLong(value));
  }
  
  @Test
  public void addPointLongMs() throws Exception {
    setupAddPointStorage();
    HashMap<String, String> tags = new HashMap<String, String>(1);
    tags.put("host", "web01");
    tsdb.addPoint("sys.cpu.user", 1356998400500L, 42, tags).joinUninterruptibly();
    final byte[] row = new byte[] { 0, 0, 1, 0x50, (byte) 0xE2, 0x27, 0, 
        0, 0, 1, 0, 0, 1};
    final byte[] value = tsdb_store.getColumnDataTable(row,
        new byte[] { (byte) 0xF0, 0, 0x7D, 0 });
    assertNotNull(value);
    assertEquals(42, value[0]);
  }
  
  @Test
  public void addPointLongMany() throws Exception {
    setupAddPointStorage();
    HashMap<String, String> tags = new HashMap<String, String>(1);
    tags.put("host", "web01");
    long timestamp = 1356998400;
    for (int i = 1; i <= 50; i++) {
      tsdb.addPoint("sys.cpu.user", timestamp++, i, tags).joinUninterruptibly();
    }
    final byte[] row = new byte[] { 0, 0, 1, 0x50, (byte) 0xE2, 0x27, 0, 
        0, 0, 1, 0, 0, 1};
    final byte[] value = tsdb_store.getColumnDataTable(row, new byte[] { 0, 0 });
    assertNotNull(value);
    assertEquals(1, value[0]);
    assertEquals(50, tsdb_store.numColumnsDataTable(row));
  }
  
  @Test
  public void addPointLongManyMs() throws Exception {
    setupAddPointStorage();
    HashMap<String, String> tags = new HashMap<String, String>(1);
    tags.put("host", "web01");
    long timestamp = 1356998400500L;
    for (int i = 1; i <= 50; i++) {
      tsdb.addPoint("sys.cpu.user", timestamp++, i, tags).joinUninterruptibly();
    }
    final byte[] row = new byte[] { 0, 0, 1, 0x50, (byte) 0xE2, 0x27, 0, 
        0, 0, 1, 0, 0, 1};
    final byte[] value = tsdb_store.getColumnDataTable(row,
        new byte[] { (byte) 0xF0, 0, 0x7D, 0 });
    assertNotNull(value);
    assertEquals(1, value[0]);
    assertEquals(50, tsdb_store.numColumnsDataTable(row));
  }
  
  @Test
  public void addPointLongEndOfRow() throws Exception {
    setupAddPointStorage();
    HashMap<String, String> tags = new HashMap<String, String>(1);
    tags.put("host", "web01");
    tsdb.addPoint("sys.cpu.user", 1357001999, 42, tags).joinUninterruptibly();
    final byte[] row = new byte[] { 0, 0, 1, 0x50, (byte) 0xE2, 0x27, 0, 
        0, 0, 1, 0, 0, 1};
    final byte[] value = tsdb_store.getColumnDataTable(row, new byte[] { (byte) 0xE0,
        (byte) 0xF0 });
    assertNotNull(value);
    assertEquals(42, value[0]);
  }
  
  @Test
  public void addPointLongOverwrite() throws Exception {
    setupAddPointStorage();
    HashMap<String, String> tags = new HashMap<String, String>(1);
    tags.put("host", "web01");
    tsdb.addPoint("sys.cpu.user", 1356998400, 42, tags).joinUninterruptibly();
    tsdb.addPoint("sys.cpu.user", 1356998400, 24, tags).joinUninterruptibly();
    final byte[] row = new byte[] { 0, 0, 1, 0x50, (byte) 0xE2, 0x27, 0, 
        0, 0, 1, 0, 0, 1};
    final byte[] value = tsdb_store.getColumnDataTable(row, new byte[] { 0, 0 });
    assertNotNull(value);
    assertEquals(24, value[0]);
  }
  
  @SuppressWarnings("unchecked")
  @Test (expected = NoSuchUniqueName.class)
  public void addPointNoAutoMetric() throws Exception {
    setupAddPointStorage();
    HashMap<String, String> tags = new HashMap<String, String>(1);
    tags.put("host", "web01");
    tsdb.addPoint("sys.cpu.user.0", 1356998400, 42, tags).joinUninterruptibly();
  }

  @Test
  public void addPointSecondZero() throws Exception {
    // Thu, 01 Jan 1970 00:00:00 GMT
    setupAddPointStorage();
    HashMap<String, String> tags = new HashMap<String, String>(1);
    tags.put("host", "web01");
    tsdb.addPoint("sys.cpu.user", 0, 42, tags).joinUninterruptibly();
    final byte[] row = new byte[] { 0, 0, 1, 0, 0, 0, 0, 0, 0, 1, 0, 0, 1};
    final byte[] value = tsdb_store.getColumnDataTable(row, new byte[] { 0, 0 });
    assertNotNull(value);
    assertEquals(42, value[0]);
  }
  
  @Test
  public void addPointSecondOne() throws Exception {
    // hey, it's valid *shrug* Thu, 01 Jan 1970 00:00:01 GMT
    setupAddPointStorage();
    HashMap<String, String> tags = new HashMap<String, String>(1);
    tags.put("host", "web01");
    tsdb.addPoint("sys.cpu.user", 1, 42, tags).joinUninterruptibly();
    final byte[] row = new byte[] { 0, 0, 1, 0, 0, 0, 0, 0, 0, 1, 0, 0, 1};
    final byte[] value = tsdb_store.getColumnDataTable(row, new byte[] { 0, 16 });
    assertNotNull(value);
    assertEquals(42, value[0]);
  }
  
  @Test
  public void addPointSecond2106() throws Exception {
    // Sun, 07 Feb 2106 06:28:15 GMT
    setupAddPointStorage();
    HashMap<String, String> tags = new HashMap<String, String>(1);
    tags.put("host", "web01");
    tsdb.addPoint("sys.cpu.user", 4294967295L, 42, tags).joinUninterruptibly();
    final byte[] row = new byte[] { 0, 0, 1, (byte) 0xFF, (byte) 0xFF, (byte) 0xF9, 
        0x60, 0, 0, 1, 0, 0, 1};
    final byte[] value = tsdb_store.getColumnDataTable(row, new byte[] { 0x69, (byte) 0xF0 });
    assertNotNull(value);
    assertEquals(42, value[0]);
  }
  
  @Test (expected = IllegalArgumentException.class)
  public void addPointSecondNegative() throws Exception {
    // Fri, 13 Dec 1901 20:45:52 GMT
    // may support in the future, but 1.0 didn't
    setupAddPointStorage();
    HashMap<String, String> tags = new HashMap<String, String>(1);
    tags.put("host", "web01");
    tsdb.addPoint("sys.cpu.user", -2147483648, 42, tags).joinUninterruptibly();
  }
  
  @Test
  public void addPointMS1970() throws Exception {
    // Since it's just over Integer.MAX_VALUE, OpenTSDB will treat this as
    // a millisecond timestamp since it doesn't fit in 4 bytes.
    // Base time is 4294800 which is Thu, 19 Feb 1970 17:00:00 GMT
    // offset = F0A36000 or 167296 ms
    setupAddPointStorage();
    HashMap<String, String> tags = new HashMap<String, String>(1);
    tags.put("host", "web01");
    tsdb.addPoint("sys.cpu.user", 4294967296L, 42, tags).joinUninterruptibly();
    final byte[] row = new byte[] { 0, 0, 1, 0, (byte) 0x41, (byte) 0x88, 
        (byte) 0x90, 0, 0, 1, 0, 0, 1};
    final byte[] value = tsdb_store.getColumnDataTable(row, new byte[] { (byte) 0xF0,
        (byte) 0xA3, 0x60, 0});
    assertNotNull(value);
    assertEquals(42, value[0]);
  }
  
  @Test
  public void addPointMS2106() throws Exception {
    // Sun, 07 Feb 2106 06:28:15.000 GMT
    setupAddPointStorage();
    HashMap<String, String> tags = new HashMap<String, String>(1);
    tags.put("host", "web01");
    tsdb.addPoint("sys.cpu.user", 4294967295000L, 42, tags).joinUninterruptibly();
    final byte[] row = new byte[] { 0, 0, 1, (byte) 0xFF, (byte) 0xFF, (byte) 0xF9, 
        0x60, 0, 0, 1, 0, 0, 1};
    final byte[] value = tsdb_store.getColumnDataTable(row, new byte[] { (byte) 0xF6,
        (byte) 0x77, 0x46, 0 });
    assertNotNull(value);
    assertEquals(42, value[0]);
  }
  
  @Test
  public void addPointMS2286() throws Exception {
    // It's an artificial limit and more thought needs to be put into it
    setupAddPointStorage();
    HashMap<String, String> tags = new HashMap<String, String>(1);
    tags.put("host", "web01");
    tsdb.addPoint("sys.cpu.user", Const.MAX_MS_TIMESTAMP, 42, tags).joinUninterruptibly();
    final byte[] row = new byte[] { 0, 0, 1, (byte) 0x54, (byte) 0x0B, (byte) 0xD9, 
        0x10, 0, 0, 1, 0, 0, 1};
    final byte[] value = tsdb_store.getColumnDataTable(row, new byte[] { (byte) 0xFA,
        (byte) 0xAE, 0x5F, (byte) 0xC0 });
    assertNotNull(value);
    assertEquals(42, value[0]);
  }
  
  @Test  (expected = IllegalArgumentException.class)
  public void addPointMSTooLarge() throws Exception {
    // It's an artificial limit and more thought needs to be put into it
    setupAddPointStorage();
    HashMap<String, String> tags = new HashMap<String, String>(1);
    tags.put("host", "web01");
    tsdb.addPoint("sys.cpu.user", Const.MAX_MS_TIMESTAMP+1, 42, tags).joinUninterruptibly();
  }
  
  @Test (expected = IllegalArgumentException.class)
  public void addPointMSNegative() throws Exception {
    // Fri, 13 Dec 1901 20:45:52 GMT
    // may support in the future, but 1.0 didn't
    setupAddPointStorage();
    HashMap<String, String> tags = new HashMap<String, String>(1);
    tags.put("host", "web01");
    tsdb.addPoint("sys.cpu.user", -2147483648000L, 42, tags).joinUninterruptibly();
  }

  @Test
  public void addPointFloat() throws Exception {
    setupAddPointStorage();
    HashMap<String, String> tags = new HashMap<String, String>(1);
    tags.put("host", "web01");
    tsdb.addPoint("sys.cpu.user", 1356998400, 42.5F, tags).joinUninterruptibly();
    final byte[] row = new byte[] { 0, 0, 1, 0x50, (byte) 0xE2, 0x27, 0, 
        0, 0, 1, 0, 0, 1};
    final byte[] value = tsdb_store.getColumnDataTable(row, new byte[] { 0, 11 });
    assertNotNull(value);
    // should have 7 digits of precision
    assertEquals(42.5F, Float.intBitsToFloat(Bytes.getInt(value)), 0.0000001);
  }
  
  @Test
  public void addPointFloatNegative() throws Exception {
    setupAddPointStorage();
    HashMap<String, String> tags = new HashMap<String, String>(1);
    tags.put("host", "web01");
    tsdb.addPoint("sys.cpu.user", 1356998400, -42.5F, tags).joinUninterruptibly();
    final byte[] row = new byte[] { 0, 0, 1, 0x50, (byte) 0xE2, 0x27, 0, 
        0, 0, 1, 0, 0, 1};
    final byte[] value = tsdb_store.getColumnDataTable(row, new byte[] { 0, 11 });
    assertNotNull(value);
    // should have 7 digits of precision
    assertEquals(-42.5F, Float.intBitsToFloat(Bytes.getInt(value)), 0.0000001);
  }
  
  @Test
  public void addPointFloatMs() throws Exception {
    setupAddPointStorage();
    HashMap<String, String> tags = new HashMap<String, String>(1);
    tags.put("host", "web01");
    tsdb.addPoint("sys.cpu.user", 1356998400500L, 42.5F, tags).joinUninterruptibly();
    final byte[] row = new byte[] { 0, 0, 1, 0x50, (byte) 0xE2, 0x27, 0, 
        0, 0, 1, 0, 0, 1};
    final byte[] value = tsdb_store.getColumnDataTable(row,
        new byte[] { (byte) 0xF0, 0, 0x7D, 11 });
    assertNotNull(value);
    // should have 7 digits of precision
    assertEquals(42.5F, Float.intBitsToFloat(Bytes.getInt(value)), 0.0000001);
  }
  
  @Test
  public void addPointFloatEndOfRow() throws Exception {
    setupAddPointStorage();
    HashMap<String, String> tags = new HashMap<String, String>(1);
    tags.put("host", "web01");
    tsdb.addPoint("sys.cpu.user", 1357001999, 42.5F, tags).joinUninterruptibly();
    final byte[] row = new byte[] { 0, 0, 1, 0x50, (byte) 0xE2, 0x27, 0, 
        0, 0, 1, 0, 0, 1};
    final byte[] value = tsdb_store.getColumnDataTable(row, new byte[] { (byte) 0xE0,
        (byte) 0xFB });
    assertNotNull(value);
    // should have 7 digits of precision
    assertEquals(42.5F, Float.intBitsToFloat(Bytes.getInt(value)), 0.0000001);
  }
  
  @Test
  public void addPointFloatPrecision() throws Exception {
    setupAddPointStorage();
    HashMap<String, String> tags = new HashMap<String, String>(1);
    tags.put("host", "web01");
    tsdb.addPoint("sys.cpu.user", 1356998400, 42.5123459999F, tags)
      .joinUninterruptibly();
    final byte[] row = new byte[] { 0, 0, 1, 0x50, (byte) 0xE2, 0x27, 0, 
        0, 0, 1, 0, 0, 1};
    final byte[] value = tsdb_store.getColumnDataTable(row, new byte[] { 0, 11 });
    assertNotNull(value);
    // should have 7 digits of precision
    assertEquals(42.512345F, Float.intBitsToFloat(Bytes.getInt(value)), 0.0000001);
  }
  
  @Test
  public void addPointFloatOverwrite() throws Exception {
    setupAddPointStorage();
    HashMap<String, String> tags = new HashMap<String, String>(1);
    tags.put("host", "web01");
    tsdb.addPoint("sys.cpu.user", 1356998400, 42.5F, tags).joinUninterruptibly();
    tsdb.addPoint("sys.cpu.user", 1356998400, 25.4F, tags).joinUninterruptibly();
    final byte[] row = new byte[] { 0, 0, 1, 0x50, (byte) 0xE2, 0x27, 0, 
        0, 0, 1, 0, 0, 1};
    final byte[] value = tsdb_store.getColumnDataTable(row, new byte[] { 0, 11 });
    assertNotNull(value);
    // should have 7 digits of precision
    assertEquals(25.4F, Float.intBitsToFloat(Bytes.getInt(value)), 0.0000001);
  }
  
  @Test
  public void addPointBothSameTimeIntAndFloat() throws Exception {
    // this is an odd situation that can occur if the user puts an int and then
    // a float (or vice-versa) with the same timestamp. What happens in the
    // aggregators when this occurs? 
    setupAddPointStorage();
    HashMap<String, String> tags = new HashMap<String, String>(1);
    tags.put("host", "web01");
    tsdb.addPoint("sys.cpu.user", 1356998400, 42, tags).joinUninterruptibly();
    tsdb.addPoint("sys.cpu.user", 1356998400, 42.5F, tags).joinUninterruptibly();
    final byte[] row = new byte[] { 0, 0, 1, 0x50, (byte) 0xE2, 0x27, 0, 
        0, 0, 1, 0, 0, 1};
    byte[] value = tsdb_store.getColumnDataTable(row, new byte[] { 0, 0 });
    assertEquals(2, tsdb_store.numColumnsDataTable(row));
    assertNotNull(value);
    assertEquals(42, value[0]);
    value = tsdb_store.getColumnDataTable(row, new byte[] { 0, 11 });
    assertNotNull(value);
    // should have 7 digits of precision
    assertEquals(42.5F, Float.intBitsToFloat(Bytes.getInt(value)), 0.0000001);
  }
  
  @Test
  public void addPointBothSameTimeIntAndFloatMs() throws Exception {
    // this is an odd situation that can occur if the user puts an int and then
    // a float (or vice-versa) with the same timestamp. What happens in the
    // aggregators when this occurs? 
    setupAddPointStorage();
    HashMap<String, String> tags = new HashMap<String, String>(1);
    tags.put("host", "web01");
    tsdb.addPoint("sys.cpu.user", 1356998400500L, 42, tags).joinUninterruptibly();
    tsdb.addPoint("sys.cpu.user", 1356998400500L, 42.5F, tags).joinUninterruptibly();
    final byte[] row = new byte[] { 0, 0, 1, 0x50, (byte) 0xE2, 0x27, 0, 
        0, 0, 1, 0, 0, 1};
    byte[] value = tsdb_store.getColumnDataTable(row, new byte[] { (byte) 0xF0, 0, 0x7D, 0 });
    assertEquals(2, tsdb_store.numColumnsDataTable(row));
    assertNotNull(value);
    assertEquals(42, value[0]);
    value = tsdb_store.getColumnDataTable(row, new byte[] { (byte) 0xF0, 0, 0x7D, 11 });
    assertNotNull(value);
    // should have 7 digits of precision
    assertEquals(42.5F, Float.intBitsToFloat(Bytes.getInt(value)), 0.0000001);
  }
  
  @Test
  public void addPointBothSameTimeSecondAndMs() throws Exception {
    // this can happen if a second and an ms data point are stored for the same
    // timestamp.
    setupAddPointStorage();
    HashMap<String, String> tags = new HashMap<String, String>(1);
    tags.put("host", "web01");
    tsdb.addPoint("sys.cpu.user", 1356998400L, 42, tags).joinUninterruptibly();
    tsdb.addPoint("sys.cpu.user", 1356998400000L, 42, tags).joinUninterruptibly();
    final byte[] row = new byte[] { 0, 0, 1, 0x50, (byte) 0xE2, 0x27, 0, 
        0, 0, 1, 0, 0, 1};
    byte[] value = tsdb_store.getColumnDataTable(row, new byte[] { 0, 0 });
    assertEquals(2, tsdb_store.numColumnsDataTable(row));
    assertNotNull(value);
    assertEquals(42, value[0]);
    value = tsdb_store.getColumnDataTable(row, new byte[] { (byte) 0xF0, 0, 0, 0 });
    assertNotNull(value);
    // should have 7 digits of precision
    assertEquals(42, value[0]);
  }

  @Test (expected = IllegalArgumentException.class)
  public void storeNewEmptyUID() throws Exception {
    UIDMeta meta = new UIDMeta(METRIC, "");
    try {
      tsdb.add(meta).joinUninterruptibly();
    } catch (Exception e) {
      assertEquals("Missing UID", e.getMessage());
      throw(e);
    }
  }
  @Test (expected = IllegalArgumentException.class)
  public void storeNewNoName() throws Exception {
    UIDMeta meta = new UIDMeta(METRIC, new byte[] { 0, 0, 1 }, "");
    try {
      tsdb.add(meta).joinUninterruptibly();
    } catch (Exception e) {
      assertEquals("Missing name", e.getMessage());
      throw(e);
    }
  }

  @Test (expected = IllegalArgumentException.class)
  public void storeNewNullType() throws Exception {
    UIDMeta meta = new UIDMeta(null, new byte[] { 0, 0, 1 }, "sys.cpu.1");
    try {
      tsdb.add(meta).joinUninterruptibly();
    } catch (Exception e) {
      assertEquals("Missing type", e.getMessage());
      throw(e);
    }
  }
  
  /**
   * Helper to mock the UID caches with valid responses
   */
  private void setupAssignUid() {
    tsdb_store.allocateUID("sys.cpu.0", new byte[]{0, 0, 1}, METRIC);
    tsdb_store.allocateUID("sys.cpu.1", new byte[]{0, 0, 2}, METRIC);

    tsdb_store.allocateUID("host", new byte[]{0, 0, 1}, TAGK);
    tsdb_store.allocateUID("datacenter", new byte[]{0, 0, 2}, TAGK);

    tsdb_store.allocateUID("localhost", new byte[]{0, 0, 1}, TAGV);
    tsdb_store.allocateUID("myserver", new byte[]{0, 0, 2}, TAGV);
  }
  
  /**
   * Helper to mock the UID caches with valid responses
   */
  private void setGetUidName() {
    tsdb_store.allocateUID("sys.cpu.0", new byte[]{0, 0, 1}, METRIC);
    tsdb_store.allocateUID("host", new byte[]{0, 0, 1}, TAGK);
    tsdb_store.allocateUID("web01", new byte[]{0, 0, 1}, TAGV);
  }

  /**
   * Configures storage for the addPoint() tests to validate that we're storing
   * data points correctly.
   */
  private void setupAddPointStorage() throws Exception {
    tsdb_store.allocateUID("sys.cpu.user", new byte[]{0, 0, 1}, METRIC);
    tsdb_store.allocateUID("host", new byte[]{0, 0, 1}, TAGK);
    tsdb_store.allocateUID("web01", new byte[]{0, 0, 1}, TAGV);
  }

  @Test (expected = IllegalArgumentException.class)
  public void testStoreTreeTooLowID() {
    Tree tree = new Tree();
    tree.setTreeId(Const.MIN_TREE_ID_INCLUSIVE - 1);

    tsdb.storeTree(tree, true);
  }

  @Test (expected = IllegalArgumentException.class)
  public void testStoreTreeTooHighID() {
    Tree tree = new Tree();
    tree.setTreeId(Const.MAX_TREE_ID_INCLUSIVE + 1);

    tsdb.storeTree(tree, true);
  }

  @Test (expected = IllegalStateException.class)
  public void testStoreTreeNotChanged() {
    Tree tree = new Tree();
    tree.setTreeId(Const.MAX_TREE_ID_INCLUSIVE);

    tsdb.storeTree(tree, true);
  }
  @Test
  public void testStoreTreeValidID() throws Exception {
    Tree tree = new Tree();
    for (int id = Const.MIN_TREE_ID_INCLUSIVE;
         id <= Const.MAX_TREE_ID_INCLUSIVE; ++id) {

      tree.setTreeId(id);
      // sets the optional Note field do tha this
      // tree will have the status changed
      tree.setNotes("Note");

      assertTrue(tsdb.storeTree(tree, true).joinUninterruptibly());
    }
  }

  @Test (expected = IllegalArgumentException.class)
  public void testFetchTreeTooLowID() {
    tsdb.fetchTree(Const.MIN_TREE_ID_INCLUSIVE - 1);
  }

  @Test (expected = IllegalArgumentException.class)
  public void testFetchTreeTooHighID() {
    tsdb.fetchTree(Const.MAX_TREE_ID_INCLUSIVE + 1);
  }

  @Test
  public void testFetchTree() throws Exception {
    Tree tree = new Tree();
    for (int id = Const.MIN_TREE_ID_INCLUSIVE;
         id <= Const.MAX_TREE_ID_INCLUSIVE; ++id) {

      tree.setTreeId(id);
      // sets the optional Note field do tha this
      // tree will have the status changed
      tree.setNotes("Note");

      tsdb.storeTree(tree, true);//maybe alternate true and false?
      assertEquals(tree, tsdb.fetchTree(id).joinUninterruptibly());
    }
  }

  @Test (expected = IllegalArgumentException.class)
  public void testCreateNewTreeIDAlreadySet() {
    Tree tree = new Tree();
    tree.setTreeId(1);

    tsdb.createNewTree(tree);
  }

  @Test (expected = IllegalArgumentException.class)
  public void testCreateNewTreeNoName() {
    Tree tree = new Tree();

    tsdb.createNewTree(tree);
  }

  @Test (expected = IllegalArgumentException.class)
  public void testCreateNewTreeEmptyName() {
    Tree tree = new Tree();
    tree.setName("");

    tsdb.createNewTree(tree);
  }

  @Test
  public void testCreateNewTree() throws Exception {
    Tree tree = new Tree();
    tree.setName("Valid1");

    assertEquals( new Integer(1), tsdb.createNewTree(tree).joinUninterruptibly());
    Tree tree2 = new Tree();
    tree2.setName("Valid2");
    assertEquals( new Integer(2), tsdb.createNewTree(tree2).joinUninterruptibly());
  }

  @Test (expected = IllegalArgumentException.class)
  public void testDeleteTreeTooLowID() {

    tsdb.deleteTree(Const.MIN_TREE_ID_INCLUSIVE - 1, true);
  }

  @Test (expected = IllegalArgumentException.class)
  public void testDeleteTreeTooHighID() {
    tsdb.deleteTree(Const.MAX_TREE_ID_INCLUSIVE + 1, true);
  }

  @Test
  public void testDeleteTree() throws Exception {
    testStoreTreeValidID();
    for (int id = Const.MIN_TREE_ID_INCLUSIVE;
         id <= Const.MAX_TREE_ID_INCLUSIVE; ++id) {

      assertTrue(tsdb.deleteTree(id, true).joinUninterruptibly());
    }
  }

  @Test (expected = IllegalArgumentException.class)
  public void testFetchCollisionsTooLowID() {

    tsdb.fetchCollisions(Const.MIN_TREE_ID_INCLUSIVE - 1, null);
  }

  @Test (expected = IllegalArgumentException.class)
  public void testFetchCollisionsTooHighID() {
    tsdb.fetchCollisions(Const.MAX_TREE_ID_INCLUSIVE + 1, null);
  }

  @Test
  public void testFetchCollisions() throws Exception {
    testStoreTreeValidID();
    for (int id = Const.MIN_TREE_ID_INCLUSIVE;
         id <= Const.MAX_TREE_ID_INCLUSIVE; ++id) {

      tsdb.fetchCollisions(id, null).joinUninterruptibly();
    }
  }

  @Test (expected = IllegalArgumentException.class)
  public void testFetchNotMatchedTooLowID() {
    tsdb.fetchCollisions(Const.MIN_TREE_ID_INCLUSIVE - 1, null);
  }

  @Test (expected = IllegalArgumentException.class)
  public void testFetchNotMatchedTooHighID() {
    tsdb.fetchCollisions(Const.MAX_TREE_ID_INCLUSIVE + 1, null);
  }

  @Test
  public void testFetchNotMatched() throws Exception {
    for (int id = Const.MIN_TREE_ID_INCLUSIVE;
         id <= Const.MAX_TREE_ID_INCLUSIVE; ++id) {

      tsdb.fetchNotMatched(id, null).joinUninterruptibly();
    }
  }

  @Test
  public void testFlushTreeCollisionsNoStoredFailures() throws Exception {
    final Tree tree = new Tree();
    tree.setStoreFailures(false);
    tree.addCollision("4711", "JustANumber");
    assertTrue(tree.getCollisions().containsKey("4711"));
    assertTrue(tree.getCollisions().containsValue("JustANumber"));
    assertEquals(1, tree.getCollisions().size());

    assertTrue(tsdb.flushTreeCollisions(tree).joinUninterruptibly());

    assertEquals(0, tree.getCollisions().size());
  }
  @Test
  public void testFlushTreeCollisionsFailures() throws Exception {
    final Tree tree = new Tree();
    tree.setStoreFailures(true);
    tree.addCollision("4711", "JustANumber");
    assertTrue(tree.getCollisions().containsKey("4711"));
    assertTrue(tree.getCollisions().containsValue("JustANumber"));
    assertEquals(1, tree.getCollisions().size());

    assertTrue(tsdb.flushTreeCollisions(tree).joinUninterruptibly());
    assertEquals(1, tree.getCollisions().size());
  }

  @Test
  public void testFlushTreeNotMatchedStoreFailures() throws Exception {
    final Tree tree = new Tree();
    tree.setStoreFailures(false);
    tree.addNotMatched("4711", "JustANumber");
    assertTrue(tree.getNotMatched().containsKey("4711"));
    assertTrue(tree.getNotMatched().containsValue("JustANumber"));
    assertEquals(1, tree.getNotMatched().size());

    assertTrue(tsdb.flushTreeNotMatched(tree).joinUninterruptibly());

    assertEquals(0, tree.getNotMatched().size());
  }
  @Test
  public void testFlushTreeNotMatchedNoStoreFailures() throws Exception {
    final Tree tree = new Tree();
    tree.setStoreFailures(true);
    tree.addNotMatched("4711", "JustANumber");
    assertTrue(tree.getNotMatched().containsKey("4711"));
    assertTrue(tree.getNotMatched().containsValue("JustANumber"));
    assertEquals(1, tree.getNotMatched().size());

    assertTrue(tsdb.flushTreeNotMatched(tree).joinUninterruptibly());

    assertEquals(1, tree.getNotMatched().size());
  }

  @Test
  public void testStoreLeaf() {
    /*
     * Placeholder test. The method does nothing but forwards the call to the
     * tsdb_store.
     */
  }

  @Test (expected = IllegalArgumentException.class)
  public void testStoreBranchTooLowID() {
    Branch branch = new Branch();
    branch.setTreeId(Const.MIN_TREE_ID_INCLUSIVE - 1);
    tsdb.storeBranch(null, branch, true);
  }

  @Test (expected = IllegalArgumentException.class)
  public void testStoreBranchTooHighID() {
    Branch branch = new Branch();
    branch.setTreeId(Const.MAX_TREE_ID_INCLUSIVE + 1);
    tsdb.storeBranch(null, branch, true);
  }

  @Test
  public void testStoreBranch() throws Exception{
    Branch branch = new Branch();
    for (int id = Const.MIN_TREE_ID_INCLUSIVE;
         id <= Const.MAX_TREE_ID_INCLUSIVE; ++id) {

      branch.setTreeId(id);

      tsdb.storeBranch(null, branch, true).joinUninterruptibly();
    }
  }

  @Test
  public void testFetchBranchOnly() {
    /*
     * Placeholder test. The method does nothing but forwards the call to the
     * tsdb_store.
     */
  }

  @Test
  public void testFetchBranch() {
    /*
     * Placeholder test. The method does nothing but forwards the call to the
     * tsdb_store.
     */
  }

  @Test (expected = IllegalArgumentException.class)
  public void testSyncTreeRuleToStorageTooLowID() {
    TreeRule rule = new TreeRule(Const.MIN_TREE_ID_INCLUSIVE - 1);
    tsdb.syncTreeRuleToStorage(rule, true);
  }

  @Test (expected = IllegalArgumentException.class)
  public void testSyncTreeRuleToStorageTooHighID() {
    TreeRule rule = new TreeRule(Const.MAX_TREE_ID_INCLUSIVE + 1);
    tsdb.syncTreeRuleToStorage(rule, true);
  }

  @Test
  public void testFestSyncTreeRuleToStorage() {
    for (int id = Const.MIN_TREE_ID_INCLUSIVE;
         id <= Const.MAX_TREE_ID_INCLUSIVE; ++id) {
      TreeRule rule = new TreeRule(id);
      tsdb.syncTreeRuleToStorage(rule, true);
    }

  }

  @Test
  public void testFetchTreeRuleTooLowID() {
    try {
      tsdb.fetchTreeRule(Const.MIN_TREE_ID_INCLUSIVE - 1, 0, 0);
    } catch (IllegalArgumentException e){
      assertEquals("Invalid Tree ID", e.getMessage());
    }
  }

  @Test
  public void testFetchTreeRuleTooHighID() {
    try {
      tsdb.fetchTreeRule(Const.MAX_TREE_ID_INCLUSIVE + 1, 0, 0);
    } catch (IllegalArgumentException e){
      assertEquals("Invalid Tree ID", e.getMessage());
    }
  }

  @Test
  public void testFetchTreeRuleInvalidLevel() {
    for (int id = Const.MIN_TREE_ID_INCLUSIVE;
         id <= Const.MAX_TREE_ID_INCLUSIVE; ++id) {
      try {
        tsdb.fetchTreeRule(id, -1, 0);
      } catch (IllegalArgumentException e){
        assertEquals("Invalid rule level" ,e.getMessage());
      }
    }
  }

  @Test
  public void testFetchTreeRuleInvalidOrder() {
    for (int id = Const.MIN_TREE_ID_INCLUSIVE;
         id <= Const.MAX_TREE_ID_INCLUSIVE; ++id) {
      try {
        tsdb.fetchTreeRule(id, 0, -1);
      } catch (IllegalArgumentException e){
        assertEquals("Invalid rule order" , e.getMessage());
      }
    }
  }

  @Test
  public void testFetchTreeRule() throws Exception {
    for (int id = Const.MIN_TREE_ID_INCLUSIVE;
         id <= Const.MAX_TREE_ID_INCLUSIVE; ++id) {
      assertNull(tsdb.fetchTreeRule(id, 0, 0).joinUninterruptibly());
    }
  }

  @Test
  public void testDeleteTreeRuleTooLowID() {
    try {
      tsdb.deleteTreeRule(Const.MIN_TREE_ID_INCLUSIVE - 1, -1, -1);
    } catch (IllegalArgumentException e) {
      assertEquals("Invalid Tree ID" , e.getMessage());
    }
  }

  @Test
  public void testDeleteTreeRuleTooHighID() {
    try {
      tsdb.deleteTreeRule(Const.MAX_TREE_ID_INCLUSIVE + 1, -1, -1);
    } catch (IllegalArgumentException e) {
      assertEquals("Invalid Tree ID" , e.getMessage());
    }
  }

  @Test
  public void testDeleteTreeRuleInvalidLevel() {
    for (int id = Const.MIN_TREE_ID_INCLUSIVE;
         id <= Const.MAX_TREE_ID_INCLUSIVE; ++id) {
      try {
        tsdb.deleteTreeRule(id, -1, 0);
      } catch (IllegalArgumentException e) {
        assertEquals("Invalid rule level" , e.getMessage());
      }
    }
  }

  @Test
  public void testDeleteTreeRuleInvalidOrder() {
    for (int id = Const.MIN_TREE_ID_INCLUSIVE;
         id <= Const.MAX_TREE_ID_INCLUSIVE; ++id) {
      try {
        tsdb.deleteTreeRule(id, 0, -1);
      } catch (IllegalArgumentException e) {
        assertEquals("Invalid rule order" , e.getMessage());
      }
    }
  }

  @Test
  public void testDeleteTreeRule() {
    for (int id = Const.MIN_TREE_ID_INCLUSIVE;
         id <= Const.MAX_TREE_ID_INCLUSIVE; ++id) {
      tsdb.deleteTreeRule(id, 0, 0);
    }
  }

  @Test (expected = IllegalArgumentException.class)
  public void testDeleteAllTreeRulesTooLowID() {
    tsdb.deleteAllTreeRules(Const.MIN_TREE_ID_INCLUSIVE - 1);
  }

  @Test (expected = IllegalArgumentException.class)
  public void testDeleteAllTreeRulesTooHighID() {
    tsdb.deleteAllTreeRules(Const.MAX_TREE_ID_INCLUSIVE + 1);
  }

  @Test
  public void testDeleteAllTreeRules() {
    for (int id = Const.MIN_TREE_ID_INCLUSIVE;
         id <= Const.MAX_TREE_ID_INCLUSIVE; ++id) {
      tsdb.deleteAllTreeRules(id);
    }
  }
}
