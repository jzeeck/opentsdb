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
package net.opentsdb.tools;

import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.nio.charset.Charset;
import java.util.HashMap;

import net.opentsdb.core.TSDB;
import net.opentsdb.core.WritableDataPoints;
import net.opentsdb.storage.MemoryStore;
import net.opentsdb.storage.TsdbStore;
import net.opentsdb.uid.NoSuchUniqueName;
import net.opentsdb.utils.Config;

import org.hbase.async.Bytes;
import org.hbase.async.KeyValue;
import org.hbase.async.Scanner;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PowerMockIgnore;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import static net.opentsdb.uid.UniqueId.UniqueIdType;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.mockito.Matchers.anyString;

@RunWith(PowerMockRunner.class)
@PowerMockIgnore({"javax.management.*", "javax.xml.*",
  "ch.qos.*", "org.slf4j.*",
  "com.sum.*", "org.xml.*"})
@PrepareForTest({KeyValue.class, Fsck.class, Scanner.class,
        FileInputStream.class, TextImporter.class})
public class TestTextImporter {
  private TSDB tsdb = null;
  private MemoryStore tsdb_store;
  
  private final static Field datapoints;
  static {
    try {
      datapoints = TextImporter.class.getDeclaredField("datapoints");
      datapoints.setAccessible(true);
    } catch (Exception e) {
      throw new RuntimeException("Failed in static initializer", e);
    }
  }
  
  private final static Method importFile;
  static {
    try {
      importFile = TextImporter.class.getDeclaredMethod("importFile", 
          TsdbStore.class, TSDB.class, String.class);
      importFile.setAccessible(true);
    } catch (Exception e) {
      throw new RuntimeException("Failed in static initializer", e);
    }
  }
  
  @Before
  public void before() throws Exception {
    Config config = new Config(false);
    tsdb_store = new MemoryStore();
    tsdb = new TSDB(tsdb_store, config);
    
    PowerMockito.spy(TextImporter.class);
    // we need to purge the hash map before each unit test since it's a static
    // field
    datapoints.set(null, new HashMap<String, WritableDataPoints>());

    tsdb_store.allocateUID("sys.cpu.user", new byte[] {0, 0, 1}, UniqueIdType.METRIC);
    tsdb_store.allocateUID("sys.cpu.nice", new byte[] {0, 0, 2}, UniqueIdType.METRIC);

    tsdb_store.allocateUID("host", new byte[] {0, 0, 1}, UniqueIdType.TAGK);

    tsdb_store.allocateUID("web01", new byte[]{0, 0, 1}, UniqueIdType.TAGV);
    tsdb_store.allocateUID("web02", new byte[]{0, 0, 2}, UniqueIdType.TAGV);
  }
  
  @Test
  public void importFileGoodIntegers1Byte() throws Exception {
    String data = 
      "sys.cpu.user 1356998400 0 host=web01\n" +
      "sys.cpu.user 1356998400 127 host=web02";
    setData(data);
    Integer points = (Integer)importFile.invoke(null, tsdb_store, tsdb, "file");
    assertEquals(2, (int)points);
    
    byte[] row = new byte[] { 0, 0, 1, 0x50, (byte) 0xE2, 0x27, 0, 
        0, 0, 1, 0, 0, 1};
    byte[] value = tsdb_store.getColumn(row, new byte[] { 0, 0 });
    assertNotNull(value);
    assertEquals(0, value[0]);
    row = new byte[] { 0, 0, 1, 0x50, (byte) 0xE2, 0x27, 0, 
        0, 0, 1, 0, 0, 2};
    value = tsdb_store.getColumn(row, new byte[] { 0, 0 });
    assertNotNull(value);
    assertEquals(127, value[0]);
  }
  
  @Test
  public void importFileGoodIntegers1ByteNegative() throws Exception {
    String data = 
      "sys.cpu.user 1356998400 -0 host=web01\n" +
      "sys.cpu.user 1356998400 -128 host=web02";
    setData(data);
    Integer points = (Integer)importFile.invoke(null, tsdb_store, tsdb, "file");
    assertEquals(2, (int)points);
    
    byte[] row = new byte[] { 0, 0, 1, 0x50, (byte) 0xE2, 0x27, 0, 
        0, 0, 1, 0, 0, 1};
    byte[] value = tsdb_store.getColumn(row, new byte[] { 0, 0 });
    assertNotNull(value);
    assertEquals(0, value[0]);
    row = new byte[] { 0, 0, 1, 0x50, (byte) 0xE2, 0x27, 0, 
        0, 0, 1, 0, 0, 2};
    value = tsdb_store.getColumn(row, new byte[] { 0, 0 });
    assertNotNull(value);
    assertEquals(-128, value[0]);
  }
  
  @Test
  public void importFileGoodIntegers2Byte() throws Exception {
    String data = 
      "sys.cpu.user 1356998400 128 host=web01\n" +
      "sys.cpu.user 1356998400 32767 host=web02";
    setData(data);
    Integer points = (Integer)importFile.invoke(null, tsdb_store, tsdb, "file");
    assertEquals(2, (int)points);
    
    byte[] row = new byte[] { 0, 0, 1, 0x50, (byte) 0xE2, 0x27, 0, 
        0, 0, 1, 0, 0, 1};
    byte[] value = tsdb_store.getColumn(row, new byte[] { 0, 1 });
    assertNotNull(value);
    assertEquals(128, Bytes.getShort(value));
    row = new byte[] { 0, 0, 1, 0x50, (byte) 0xE2, 0x27, 0, 
        0, 0, 1, 0, 0, 2};
    value = tsdb_store.getColumn(row, new byte[] { 0, 1 });
    assertNotNull(value);
    assertEquals(32767, Bytes.getShort(value));
  }
  
  @Test
  public void importFileGoodIntegers2ByteNegative() throws Exception {
    String data = 
      "sys.cpu.user 1356998400 -129 host=web01\n" +
      "sys.cpu.user 1356998400 -32768 host=web02";
    setData(data);
    Integer points = (Integer)importFile.invoke(null, tsdb_store, tsdb, "file");
    assertEquals(2, (int)points);
    
    byte[] row = new byte[] { 0, 0, 1, 0x50, (byte) 0xE2, 0x27, 0, 
        0, 0, 1, 0, 0, 1};
    byte[] value = tsdb_store.getColumn(row, new byte[] { 0, 1 });
    assertNotNull(value);
    assertEquals(-129, Bytes.getShort(value));
    row = new byte[] { 0, 0, 1, 0x50, (byte) 0xE2, 0x27, 0, 
        0, 0, 1, 0, 0, 2};
    value = tsdb_store.getColumn(row, new byte[] { 0, 1 });
    assertNotNull(value);
    assertEquals(-32768, Bytes.getShort(value));
  }
  
  @Test
  public void importFileGoodIntegers4Byte() throws Exception {
    String data = 
      "sys.cpu.user 1356998400 32768 host=web01\n" +
      "sys.cpu.user 1356998400 2147483647 host=web02";
    setData(data);
    Integer points = (Integer)importFile.invoke(null, tsdb_store, tsdb, "file");
    assertEquals(2, (int)points);
    
    byte[] row = new byte[] { 0, 0, 1, 0x50, (byte) 0xE2, 0x27, 0, 
        0, 0, 1, 0, 0, 1};
    byte[] value = tsdb_store.getColumn(row, new byte[] { 0, 3 });
    assertNotNull(value);
    assertEquals(32768, Bytes.getInt(value));
    row = new byte[] { 0, 0, 1, 0x50, (byte) 0xE2, 0x27, 0, 
        0, 0, 1, 0, 0, 2};
    value = tsdb_store.getColumn(row, new byte[] { 0, 3 });
    assertNotNull(value);
    assertEquals(2147483647, Bytes.getInt(value));
  }
  
  @Test
  public void importFileGoodIntegers4ByteNegative() throws Exception {
    String data = 
      "sys.cpu.user 1356998400 -32769 host=web01\n" +
      "sys.cpu.user 1356998400 -2147483648 host=web02";
    setData(data);
    Integer points = (Integer)importFile.invoke(null, tsdb_store, tsdb, "file");
    assertEquals(2, (int)points);
    
    byte[] row = new byte[] { 0, 0, 1, 0x50, (byte) 0xE2, 0x27, 0, 
        0, 0, 1, 0, 0, 1};
    byte[] value = tsdb_store.getColumn(row, new byte[] { 0, 3 });
    assertNotNull(value);
    assertEquals(-32769, Bytes.getInt(value));
    row = new byte[] { 0, 0, 1, 0x50, (byte) 0xE2, 0x27, 0, 
        0, 0, 1, 0, 0, 2};
    value = tsdb_store.getColumn(row, new byte[] { 0, 3 });
    assertNotNull(value);
    assertEquals(-2147483648, Bytes.getInt(value));
  }
  
  @Test
  public void importFileGoodIntegers8Byte() throws Exception {
    String data = 
      "sys.cpu.user 1356998400 2147483648 host=web01\n" +
      "sys.cpu.user 1356998400 9223372036854775807 host=web02";
    setData(data);
    Integer points = (Integer)importFile.invoke(null, tsdb_store, tsdb, "file");
    assertEquals(2, (int)points);
    byte[] row = new byte[] { 0, 0, 1, 0x50, (byte) 0xE2, 0x27, 0, 
        0, 0, 1, 0, 0, 1};
    byte[] value = tsdb_store.getColumn(row, new byte[] { 0, 7 });
    assertNotNull(value);
    assertEquals(2147483648L, Bytes.getLong(value));
    row = new byte[] { 0, 0, 1, 0x50, (byte) 0xE2, 0x27, 0, 
        0, 0, 1, 0, 0, 2};
    value = tsdb_store.getColumn(row, new byte[] { 0, 7 });
    assertNotNull(value);
    assertEquals(9223372036854775807L, Bytes.getLong(value));
  }
  
  @Test
  public void importFileGoodIntegers8ByteNegative() throws Exception {
    String data = 
      "sys.cpu.user 1356998400 -2147483649 host=web01\n" +
      "sys.cpu.user 1356998400 -9223372036854775808 host=web02";
    setData(data);
    Integer points = (Integer)importFile.invoke(null, tsdb_store, tsdb, "file");
    assertEquals(2, (int)points);
    
    byte[] row = new byte[] { 0, 0, 1, 0x50, (byte) 0xE2, 0x27, 0, 
        0, 0, 1, 0, 0, 1};
    byte[] value = tsdb_store.getColumn(row, new byte[] { 0, 7 });
    assertNotNull(value);
    assertEquals(-2147483649L, Bytes.getLong(value));
    row = new byte[] { 0, 0, 1, 0x50, (byte) 0xE2, 0x27, 0, 
        0, 0, 1, 0, 0, 2};
    value = tsdb_store.getColumn(row, new byte[] { 0, 7 });
    assertNotNull(value);
    assertEquals(-9223372036854775808L, Bytes.getLong(value));
  }
  
  @Test (expected = RuntimeException.class)
  public void importFileTimestampZero() throws Exception {
    String data = 
      "sys.cpu.user 0 0 host=web01\n" +
      "sys.cpu.user 0 127 host=web02";
    setData(data);
    importFile.invoke(null, tsdb_store, tsdb, "file");
  }
  
  @Test (expected = RuntimeException.class)
  public void importFileTimestampNegative() throws Exception {
    String data = 
      "sys.cpu.user -11356998400 0 host=web01\n" +
      "sys.cpu.user -11356998400 127 host=web02";
    setData(data);
    importFile.invoke(null, tsdb_store, tsdb, "file");
  }
  
  @Test
  public void importFileMaxSecondTimestamp() throws Exception {
    String data = 
      "sys.cpu.user 4294967295 24 host=web01\n" +
      "sys.cpu.user 4294967295 42 host=web02";
    setData(data);
    Integer points = (Integer)importFile.invoke(null, tsdb_store, tsdb, "file");
    assertEquals(2, (int)points);
    
    byte[] row = new byte[] { 0, 0, 1, (byte) 0xFF, (byte) 0xFF, (byte) 0xF9, 
        0x60, 0, 0, 1, 0, 0, 1};
    byte[] value = tsdb_store.getColumn(row, new byte[] { 0x69, (byte) 0xF0 });
    assertNotNull(value);
    assertEquals(24, value[0]);
    row = new byte[] { 0, 0, 1, (byte) 0xFF, (byte) 0xFF, (byte) 0xF9, 
        0x60, 0, 0, 1, 0, 0, 2};
    value = tsdb_store.getColumn(row, new byte[] { 0x69, (byte) 0xF0 });
    assertNotNull(value);
    assertEquals(42, value[0]);
  }
  
  @Test
  public void importFileMinMSTimestamp() throws Exception {
    String data = 
      "sys.cpu.user 4294967296 24 host=web01\n" +
      "sys.cpu.user 4294967296 42 host=web02";
    setData(data);
    Integer points = (Integer)importFile.invoke(null, tsdb_store, tsdb, "file");
    assertEquals(2, (int)points);
    
    byte[] row = new byte[] { 0, 0, 1, 0, (byte) 0x41, (byte) 0x88, (byte) 0x90, 
        0, 0, 1, 0, 0, 1};
    byte[] value = tsdb_store.getColumn(row, new byte[] { (byte) 0xF0, (byte) 0xA3,
        0x60, 0 });
    assertNotNull(value);
    assertEquals(24, value[0]);
    row = new byte[] { 0, 0, 1, 0, (byte) 0x41, (byte) 0x88, (byte) 0x90, 0, 
        0, 1, 0, 0, 2};
    value = tsdb_store.getColumn(row, new byte[] { (byte) 0xF0, (byte) 0xA3,
        0x60, 0 });
    assertNotNull(value);
    assertEquals(42, value[0]);
  }
  
  @Test
  public void importFileMSTimestamp() throws Exception {
    String data = 
      "sys.cpu.user 1356998400500 24 host=web01\n" +
      "sys.cpu.user 1356998400500 42 host=web02";
    setData(data);
    Integer points = (Integer)importFile.invoke(null, tsdb_store, tsdb, "file");
    assertEquals(2, (int)points);
    
    byte[] row = new byte[] { 0, 0, 1, 0x50, (byte) 0xE2, 0x27, 0, 
        0, 0, 1, 0, 0, 1};
    byte[] value = tsdb_store.getColumn(row, new byte[] { (byte) 0xF0, 0, 0x7D, 0 });
    assertNotNull(value);
    assertEquals(24, value[0]);
    row = new byte[] { 0, 0, 1, 0x50, (byte) 0xE2, 0x27, 0, 
        0, 0, 1, 0, 0, 2};
    value = tsdb_store.getColumn(row, new byte[] { (byte) 0xF0, 0, 0x7D, 0 });
    assertNotNull(value);
    assertEquals(42, value[0]);
  }
  
  @Test (expected = IllegalArgumentException.class)
  public void importFileMSTimestampTooBig() throws Exception {
    String data = 
      "sys.cpu.user 13569984005001 24 host=web01\n" +
      "sys.cpu.user 13569984005001 42 host=web02";
    setData(data);
    importFile.invoke(null, tsdb_store, tsdb, "file");
  }
  
  @Test (expected = IllegalArgumentException.class)
  public void importFileMSTimestampNegative() throws Exception {
    String data = 
      "sys.cpu.user -2147483648000L 24 host=web01\n" +
      "sys.cpu.user -2147483648000L 42 host=web02";
    setData(data);
    importFile.invoke(null, tsdb_store, tsdb, "file");
  }
  
  @Test
  public void importFileGoodFloats() throws Exception {
    String data = 
      "sys.cpu.user 1356998400 24.5 host=web01\n" +
      "sys.cpu.user 1356998400 42.5 host=web02";
    setData(data);
    Integer points = (Integer)importFile.invoke(null, tsdb_store, tsdb, "file");
    assertEquals(2, (int)points);
    
    byte[] row = new byte[] { 0, 0, 1, 0x50, (byte) 0xE2, 0x27, 0, 
        0, 0, 1, 0, 0, 1};
    byte[] value = tsdb_store.getColumn(row, new byte[] { 0, 11 });
    assertNotNull(value);
    assertEquals(24.5F, Float.intBitsToFloat(Bytes.getInt(value)), 0.0000001);
    row = new byte[] { 0, 0, 1, 0x50, (byte) 0xE2, 0x27, 0, 
        0, 0, 1, 0, 0, 2};
    value = tsdb_store.getColumn(row, new byte[] { 0, 11 });
    assertNotNull(value);
    assertEquals(42.5F, Float.intBitsToFloat(Bytes.getInt(value)), 0.0000001);
  }
  
  @Test
  public void importFileGoodFloatsNegative() throws Exception {
    String data = 
      "sys.cpu.user 1356998400 -24.5 host=web01\n" +
      "sys.cpu.user 1356998400 -42.5 host=web02";
    setData(data);
    Integer points = (Integer)importFile.invoke(null, tsdb_store, tsdb, "file");
    assertEquals(2, (int)points);
    
    byte[] row = new byte[] { 0, 0, 1, 0x50, (byte) 0xE2, 0x27, 0, 
        0, 0, 1, 0, 0, 1};
    byte[] value = tsdb_store.getColumn(row, new byte[] { 0, 11 });
    assertNotNull(value);
    assertEquals(-24.5F, Float.intBitsToFloat(Bytes.getInt(value)), 0.0000001);
    row = new byte[] { 0, 0, 1, 0x50, (byte) 0xE2, 0x27, 0, 
        0, 0, 1, 0, 0, 2};
    value = tsdb_store.getColumn(row, new byte[] { 0, 11 });
    assertNotNull(value);
    assertEquals(-42.5F, Float.intBitsToFloat(Bytes.getInt(value)), 0.0000001);
  }
  
  @Test (expected = NoSuchUniqueName.class)
  public void importFileNSUTagv() throws Exception {
    String data = 
      "sys.cpu.user 1356998400 24 host=web01\n" +
      "sys.cpu.user 1356998400 42 host=web03";
    setData(data);
    importFile.invoke(null, tsdb_store, tsdb, "file");
  }
  
  @Test (expected = NoSuchUniqueName.class)
  public void importFileNSUTagk() throws Exception {
    String data = 
      "sys.cpu.user 1356998400 24 host=web01\n" +
      "sys.cpu.user 1356998400 42 fqdn=web02";
    setData(data);
    importFile.invoke(null, tsdb_store, tsdb, "file");
  }
  
  @Test (expected = NoSuchUniqueName.class)
  public void importFileNSUMetric() throws Exception {
    String data = 
      "sys.cpu.user 1356998400 24 host=web01\n" +
      "sys.cpu.system 1356998400 42 host=web02";
    setData(data);
    importFile.invoke(null, tsdb_store, tsdb, "file");
  }
  
  @Test (expected = RuntimeException.class)
  public void importFileEmptyMetric() throws Exception {
    String data = 
      "sys.cpu.user 1356998400 24 host=web01\n" +
      " 1356998400 42 host=web03";
    setData(data);
    importFile.invoke(null, tsdb_store, tsdb, "file");
  }
  
  @Test (expected = RuntimeException.class)
  public void importFileEmptyTimestamp() throws Exception {
    String data = 
      "sys.cpu.user 1356998400 24 host=web01\n" +
      "sys.cpu.user  42 host=web03";
    setData(data);
    importFile.invoke(null, tsdb_store, tsdb, "file");
  }
  
  @Test (expected = RuntimeException.class)
  public void importFileEmptyValue() throws Exception {
    String data = 
      "sys.cpu.user 1356998400 24 host=web01\n" +
      "sys.cpu.user 1356998400  host=web03";
    setData(data);
    importFile.invoke(null, tsdb_store, tsdb, "file");
  }
  
  @Test (expected = RuntimeException.class)
  public void importFileEmptyTags() throws Exception {
    String data = 
      "sys.cpu.user 1356998400 24 host=web01\n" +
      "sys.cpu.user 1356998400 42";
    setData(data);
    importFile.invoke(null, tsdb_store, tsdb, "file");
  }
  
  @Test (expected = RuntimeException.class)
  public void importFileEmptyTagv() throws Exception {
    String data = 
      "sys.cpu.user 1356998400 24 host=web01\n" +
      "sys.cpu.user 1356998400 42 host";
    setData(data);
    importFile.invoke(null, tsdb_store, tsdb, "file");
  }
  
  @Test (expected = RuntimeException.class)
  public void importFileEmptyTagvEquals() throws Exception {
    String data = 
      "sys.cpu.user 1356998400 24 host=web01\n" +
      "sys.cpu.user 1356998400 42 host=";
    setData(data);
    importFile.invoke(null, tsdb_store, tsdb, "file");
  }
  
  @Test (expected = RuntimeException.class)
  public void importFile0Timestamp() throws Exception {
    String data = 
      "sys.cpu.user 1356998400 24 host=web01\n" +
      "sys.cpu.user 0 42 host=web02";
    setData(data);
    importFile.invoke(null, tsdb_store, tsdb, "file");
  }
  
  @Test (expected = RuntimeException.class)
  public void importFileNegativeTimestamp() throws Exception {
    String data = 
      "sys.cpu.user 1356998400 24 host=web01\n" +
      "sys.cpu.user -1356998400 42 host=web02";
    setData(data);
    importFile.invoke(null, tsdb_store, tsdb, "file");
  }
  
  @Test (expected = IllegalArgumentException.class)
  public void importFileSameTimestamp() throws Exception {
    String data = 
      "sys.cpu.user 1356998400 24 host=web01\n" +
      "sys.cpu.user 1356998400 42 host=web01";
    setData(data);
    importFile.invoke(null, tsdb_store, tsdb, "file");
  }
  
  @Test (expected = IllegalArgumentException.class)
  public void importFileLessthanTimestamp() throws Exception {
    String data = 
      "sys.cpu.user 1356998400 24 host=web01\n" +
      "sys.cpu.user 1356998300 42 host=web01";
    setData(data);
    importFile.invoke(null, tsdb_store, tsdb, "file");
  }
  
  // doesn't throw an exception, just returns "processed 0 data points"
  @Test
  public void importFileEmptyFile() throws Exception {
    String data = "";
    setData(data);
    Integer points = (Integer)importFile.invoke(null, tsdb_store, tsdb, "file");
    assertEquals(0, (int)points);
  }
  
  @Test (expected = FileNotFoundException.class)
  public void inportFileNotFound() throws Exception {
    PowerMockito.doThrow(new FileNotFoundException()).when(TextImporter.class, 
        PowerMockito.method(TextImporter.class, "open", String.class))
        .withArguments(anyString());
    Integer points = (Integer)importFile.invoke(null, tsdb_store, tsdb, "file");
    assertEquals(0, (int)points);
  }
  
  // TODO - figure out how to trigger a throttling exception
  
  /**
   * Helper to set the reader buffer. Just pass a string to use for the unit test
   * @param data The data to set
   */
  private void setData(final String data) throws Exception {
    final InputStream istream = new ByteArrayInputStream(
        data.getBytes(Charset.forName("UTF-8")));
    BufferedReader reader = new BufferedReader(new InputStreamReader(istream));
    
    PowerMockito.doReturn(reader).when(TextImporter.class, 
        PowerMockito.method(TextImporter.class, "open", String.class))
        .withArguments(anyString());
  }
}
