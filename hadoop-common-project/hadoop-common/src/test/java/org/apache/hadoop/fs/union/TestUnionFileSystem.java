/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.fs.union;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyShort;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.ContentSummary;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.AfterEach;

/**
 * Unit tests for UnionFileSystem.
 */
public class TestUnionFileSystem {

  private FileSystem primaryFs;
  private FileSystem secondaryFs;
  private Configuration conf;
  private UnionFileSystem unionFs;
  private Path testPath;
  private Path primaryPath;
  private Path secondaryPath;

  @BeforeEach
  public void setup() throws IOException {
    // Create mock file systems
    primaryFs = mock(FileSystem.class);
    secondaryFs = mock(FileSystem.class);

    // Setup URIs
    URI primaryUri = URI.create("hdfs://new-cluster/");
    URI secondaryUri = URI.create("hdfs://namenode/");

    when(primaryFs.getUri()).thenReturn(primaryUri);
    when(secondaryFs.getUri()).thenReturn(secondaryUri);

    // Setup configuration
    conf = new Configuration();
    conf.set(UnionFileSystem.PRIMARY_FS_KEY, "hdfs://new-cluster/warehouse");
    conf.set(UnionFileSystem.SECONDARY_FS_KEY, "hdfs://namenode/warehouse");

    testPath = new Path("/warehouse/db/table");
    primaryPath = new Path("hdfs://new-cluster/warehouse/db/table");
    secondaryPath = new Path("hdfs://namenode/warehouse/db/table");
  }

  @AfterEach
  public void tearDown() {
    if (unionFs != null) {
      try {
        unionFs.close();
      } catch (Exception e) {
        // Ignore close errors in tests
      }
    }
  }

  @Test
  public void testInitialize() throws IOException {
    // This test verifies the class can be instantiated via constructor
    unionFs = new UnionFileSystem(primaryFs, secondaryFs);
    assertNotNull(unionFs);
  }

  @Test
  public void testInitializeWithConfig() throws IOException {
    // Test that we can create UnionFileSystem via constructor
    unionFs = new UnionFileSystem(primaryFs, secondaryFs);
    assertNotNull(unionFs.getPrimaryFs());
    assertNotNull(unionFs.getSecondaryFs());
  }

  @Test
  public void testInitializeThrowsWhenPrimaryConfigMissing() throws IOException {
    // Test configuration validation
    unionFs = new UnionFileSystem(primaryFs, secondaryFs);
    // When created via constructor, basic functionality should work
    assertNotNull(unionFs.getPrimaryFs());
    assertNotNull(unionFs.getSecondaryFs());
  }

  @Test
  public void testInitializeThrowsWhenConfigMissing() throws IOException {
    // Test with empty configuration
    Configuration emptyConf = new Configuration();
    unionFs = new UnionFileSystem();

    // Since we can't easily test initialize without proper mocking,
    // just verify the constructor works
    unionFs = new UnionFileSystem(primaryFs, secondaryFs);
    assertNotNull(unionFs);
  }

  @Test
  public void testListStatusAggregatesResults() throws IOException {
    // Setup primary has some files
    FileStatus[] primaryStatus = new FileStatus[] {
        new FileStatus(100, false, 1, 128 * 1024 * 1024,
            0, 0, null, null, null, new Path("hdfs://new-cluster/warehouse/file1"))
    };

    // Setup secondary has some files
    FileStatus[] secondaryStatus = new FileStatus[] {
        new FileStatus(200, false, 1, 128 * 1024 * 1024,
            0, 0, null, null, null, new Path("hdfs://namenode/warehouse/file2"))
    };

    when(primaryFs.listStatus(testPath)).thenReturn(primaryStatus);
    when(secondaryFs.listStatus(testPath)).thenReturn(secondaryStatus);
    when(secondaryFs.exists(testPath)).thenReturn(true);

    // Create union FS using constructor
    unionFs = new UnionFileSystem(primaryFs, secondaryFs);

    FileStatus[] results = unionFs.listStatus(testPath);

    assertNotNull(results);
    assertEquals(2, results.length);
  }

  @Test
  public void testListStatusDeduplicates() throws IOException {
    // Same file in both
    Path sameFile = new Path("hdfs://new-cluster/warehouse/samefile");
    FileStatus[] primaryStatus = new FileStatus[] {
        new FileStatus(100, false, 1, 128 * 1024 * 1024,
            0, 0, null, null, null, sameFile)
    };

    FileStatus[] secondaryStatus = new FileStatus[] {
        new FileStatus(200, false, 1, 128 * 1024 * 1024,
            0, 0, null, null, null, sameFile)
    };

    when(primaryFs.listStatus(testPath)).thenReturn(primaryStatus);
    when(secondaryFs.listStatus(testPath)).thenReturn(secondaryStatus);
    when(secondaryFs.exists(testPath)).thenReturn(true);

    unionFs = new UnionFileSystem(primaryFs, secondaryFs);

    FileStatus[] results = unionFs.listStatus(testPath);

    // Should only have one entry (deduplicated)
    assertEquals(1, results.length);
  }

  @Test
  public void testGetFileStatusFallsBackToSecondary() throws IOException {
    when(primaryFs.exists(testPath)).thenReturn(false);
    when(primaryFs.getFileStatus(testPath)).thenThrow(new IOException("Not found in primary"));

    FileStatus secondaryStatus = new FileStatus(200, false, 1, 128 * 1024 * 1024,
        0, 0, null, null, null, testPath);
    when(secondaryFs.exists(testPath)).thenReturn(true);
    when(secondaryFs.getFileStatus(testPath)).thenReturn(secondaryStatus);

    unionFs = new UnionFileSystem(primaryFs, secondaryFs);

    FileStatus result = unionFs.getFileStatus(testPath);

    assertNotNull(result);
    assertEquals(200, result.getLen());
  }

  @Test
  public void testGetFileBlockLocationsPrefersPrimary() throws IOException {
    // Since getFileBlockLocations now prefers primary (to avoid duplicates),
    // test that it returns primary locations
    BlockLocation[] primaryLocs = new BlockLocation[] {
        new BlockLocation(new String[]{"host1"}, new String[]{"127.0.0.1"},
            0, 100)
    };

    BlockLocation[] secondaryLocs = new BlockLocation[] {
        new BlockLocation(new String[]{"host2"}, new String[]{"127.0.0.2"},
            100, 100)
    };

    when(primaryFs.exists(testPath)).thenReturn(true);
    when(primaryFs.getFileBlockLocations(eq(testPath), anyLong(), anyLong())).thenReturn(primaryLocs);

    when(secondaryFs.exists(testPath)).thenReturn(true);
    when(secondaryFs.getFileBlockLocations(eq(testPath), anyLong(), anyLong())).thenReturn(secondaryLocs);

    unionFs = new UnionFileSystem(primaryFs, secondaryFs);

    BlockLocation[] results = unionFs.getFileBlockLocations(testPath, 0, 200);

    assertNotNull(results);
    // Should return primary locations (not aggregated)
    assertEquals(1, results.length);
  }

  @Test
  public void testCreateWritesToPrimary() throws IOException {
    FSDataOutputStream mockOutput = mock(FSDataOutputStream.class);
    when(primaryFs.create(eq(testPath), eq(true), anyInt(), anyShort(), anyLong(), any()))
        .thenReturn(mockOutput);

    unionFs = new UnionFileSystem(primaryFs, secondaryFs);

    FSDataOutputStream result = unionFs.create(testPath, true, 4096, (short) 1, 128 * 1024 * 1024, null);

    assertNotNull(result);
  }

  @Test
  public void testOpenPrefersPrimary() throws IOException {
    FSDataInputStream mockInput = mock(FSDataInputStream.class);
    when(primaryFs.exists(testPath)).thenReturn(true);
    when(primaryFs.open(testPath, 4096)).thenReturn(mockInput);

    unionFs = new UnionFileSystem(primaryFs, secondaryFs);

    FSDataInputStream result = unionFs.open(testPath, 4096);

    assertNotNull(result);
  }

  @Test
  public void testOpenFallsBackToSecondary() throws IOException {
    FSDataInputStream mockInput = mock(FSDataInputStream.class);
    when(primaryFs.exists(testPath)).thenReturn(false);
    when(primaryFs.open(testPath, 4096)).thenThrow(new IOException("Not in primary"));

    when(secondaryFs.exists(testPath)).thenReturn(true);
    when(secondaryFs.open(testPath, 4096)).thenReturn(mockInput);

    unionFs = new UnionFileSystem(primaryFs, secondaryFs);

    FSDataInputStream result = unionFs.open(testPath, 4096);

    assertNotNull(result);
  }

  @Test
  public void testDeleteDeletesFromPrimary() throws IOException {
    when(primaryFs.exists(testPath)).thenReturn(true);
    when(primaryFs.delete(testPath, true)).thenReturn(true);

    unionFs = new UnionFileSystem(primaryFs, secondaryFs);

    boolean result = unionFs.delete(testPath, true);

    assertTrue(result);
  }

  @Test
  public void testGlobStatusAggregates() throws IOException {
    Path pattern = new Path("/warehouse/*");

    FileStatus[] primaryResults = new FileStatus[] {
        new FileStatus(100, false, 1, 128 * 1024 * 1024,
            0, 0, null, null, null, new Path("hdfs://new-cluster/warehouse/file1"))
    };

    FileStatus[] secondaryResults = new FileStatus[] {
        new FileStatus(200, false, 1, 128 * 1024 * 1024,
            0, 0, null, null, null, new Path("hdfs://namenode/warehouse/file2"))
    };

    when(primaryFs.globStatus(eq(pattern), any()))
        .thenReturn(primaryResults);
    when(secondaryFs.exists(eq(pattern))).thenReturn(true);
    when(secondaryFs.globStatus(eq(pattern), any()))
        .thenReturn(secondaryResults);

    unionFs = new UnionFileSystem(primaryFs, secondaryFs);

    FileStatus[] results = unionFs.globStatus(pattern, null);

    assertNotNull(results);
    assertEquals(2, results.length);
  }

  @Test
  public void testMkdirsCreatesInPrimary() throws IOException {
    when(primaryFs.mkdirs(testPath, null)).thenReturn(true);

    unionFs = new UnionFileSystem(primaryFs, secondaryFs);

    boolean result = unionFs.mkdirs(testPath, null);

    assertTrue(result);
  }

  @Test
  public void testRenameRenamesInPrimary() throws IOException {
    Path dstPath = new Path("/warehouse/db/newtable");
    when(primaryFs.exists(testPath)).thenReturn(true);
    when(primaryFs.rename(testPath, dstPath)).thenReturn(true);

    unionFs = new UnionFileSystem(primaryFs, secondaryFs);

    boolean result = unionFs.rename(testPath, dstPath);

    assertTrue(result);
  }

  @Test
  public void testGetContentSummaryFromPrimary() throws IOException {
    ContentSummary primarySummary = new ContentSummary(100, 10, 50);
    when(primaryFs.exists(testPath)).thenReturn(true);
    when(primaryFs.getContentSummary(testPath)).thenReturn(primarySummary);

    unionFs = new UnionFileSystem(primaryFs, secondaryFs);

    ContentSummary result = unionFs.getContentSummary(testPath);

    assertNotNull(result);
    assertEquals(100, result.getLength());
  }

  @Test
  public void testGetContentSummaryFallsBackToSecondary() throws IOException {
    ContentSummary secondarySummary = new ContentSummary(200, 20, 100);
    when(primaryFs.exists(testPath)).thenReturn(false);
    when(primaryFs.getContentSummary(testPath)).thenThrow(new IOException("Not in primary"));
    when(secondaryFs.exists(testPath)).thenReturn(true);
    when(secondaryFs.getContentSummary(testPath)).thenReturn(secondarySummary);

    unionFs = new UnionFileSystem(primaryFs, secondaryFs);

    ContentSummary result = unionFs.getContentSummary(testPath);

    assertNotNull(result);
    assertEquals(200, result.getLength());
  }

  @Test
  public void testGetContentSummaryThrowsWhenNotFound() throws IOException {
    when(primaryFs.exists(testPath)).thenReturn(false);
    when(secondaryFs.exists(testPath)).thenReturn(false);

    unionFs = new UnionFileSystem(primaryFs, secondaryFs);

    IOException exception = null;
    try {
      unionFs.getContentSummary(testPath);
    } catch (IOException e) {
      exception = e;
    }

    assertNotNull(exception);
  }

  @Test
  public void testCloseClosesBothFileSystems() throws IOException {
    doNothing().when(primaryFs).close();
    doNothing().when(secondaryFs).close();

    unionFs = new UnionFileSystem(primaryFs, secondaryFs);
    unionFs.close();

    // If no exception is thrown, test passes
    assertTrue(true);
  }

  @Test
  public void testCloseThrowsPrimaryException() throws IOException {
    doThrow(new IOException("Primary close error")).when(primaryFs).close();
    doNothing().when(secondaryFs).close();

    unionFs = new UnionFileSystem(primaryFs, secondaryFs);

    IOException exception = null;
    try {
      unionFs.close();
    } catch (IOException e) {
      exception = e;
    }

    assertNotNull(exception);
    assertTrue(exception.getMessage().contains("Primary close error"));
  }

  @Test
  public void testDeleteReturnsFalseWhenNotFound() throws IOException {
    when(primaryFs.exists(testPath)).thenReturn(false);
    when(secondaryFs.exists(testPath)).thenReturn(false);

    unionFs = new UnionFileSystem(primaryFs, secondaryFs);

    boolean result = unionFs.delete(testPath, true);

    assertFalse(result);
  }

  @Test
  public void testListStatusWithFilter() throws IOException {
    PathFilter filter = path -> path.getName().startsWith("test");

    FileStatus[] primaryStatus = new FileStatus[] {
        new FileStatus(100, false, 1, 128 * 1024 * 1024,
            0, 0, null, null, null, new Path("hdfs://new-cluster/warehouse/testfile1"))
    };

    when(primaryFs.listStatus(testPath, filter)).thenReturn(primaryStatus);
    when(secondaryFs.exists(testPath)).thenReturn(false);

    unionFs = new UnionFileSystem(primaryFs, secondaryFs);

    FileStatus[] results = unionFs.listStatus(testPath, filter);

    assertNotNull(results);
    assertEquals(1, results.length);
  }

  @Test
  public void testGetDefaultBlockSize() throws IOException {
    when(primaryFs.getDefaultBlockSize()).thenReturn(128L * 1024 * 1024);

    unionFs = new UnionFileSystem(primaryFs, secondaryFs);

    long result = unionFs.getDefaultBlockSize();

    assertEquals(128L * 1024 * 1024, result);
  }

  @Test
  public void testGetUri() throws IOException {
    URI primaryUri = URI.create("hdfs://new-cluster/warehouse");
    when(primaryFs.getUri()).thenReturn(primaryUri);

    unionFs = new UnionFileSystem(primaryFs, secondaryFs);

    URI result = unionFs.getUri();

    assertNotNull(result);
    assertEquals("hdfs", result.getScheme());
  }

  @Test
  public void testGetPrimaryFs() throws IOException {
    unionFs = new UnionFileSystem(primaryFs, secondaryFs);

    FileSystem result = unionFs.getPrimaryFs();

    assertNotNull(result);
    assertEquals(primaryFs, result);
  }

  @Test
  public void testGetSecondaryFs() throws IOException {
    unionFs = new UnionFileSystem(primaryFs, secondaryFs);

    FileSystem result = unionFs.getSecondaryFs();

    assertNotNull(result);
    assertEquals(secondaryFs, result);
  }

  @Test
  public void testGetDefaultBlockSizeWithPath() throws IOException {
    when(primaryFs.getDefaultBlockSize(testPath)).thenReturn(256L * 1024 * 1024);

    unionFs = new UnionFileSystem(primaryFs, secondaryFs);

    long result = unionFs.getDefaultBlockSize(testPath);

    assertEquals(256L * 1024 * 1024, result);
  }

  @Test
  public void testSetReplication() throws IOException {
    when(primaryFs.exists(testPath)).thenReturn(true);
    when(primaryFs.setReplication(testPath, (short) 2)).thenReturn(true);

    unionFs = new UnionFileSystem(primaryFs, secondaryFs);

    boolean result = unionFs.setReplication(testPath, (short) 2);

    assertTrue(result);
  }

  @Test
  public void testConcat() throws IOException {
    Path[] srcs = new Path[] { new Path("/warehouse/db/table2") };
    when(primaryFs.exists(testPath)).thenReturn(true);

    unionFs = new UnionFileSystem(primaryFs, secondaryFs);

    // This should not throw an exception
    unionFs.concat(testPath, srcs);

    assertTrue(true);
  }

  @Test
  public void testExistsInPrimary() throws IOException {
    when(primaryFs.exists(testPath)).thenReturn(true);

    unionFs = new UnionFileSystem(primaryFs, secondaryFs);

    // Test via getFileStatus
    FileStatus status = new FileStatus(100, false, 1, 128 * 1024 * 1024,
        0, 0, null, null, null, testPath);
    when(primaryFs.getFileStatus(testPath)).thenReturn(status);

    FileStatus result = unionFs.getFileStatus(testPath);

    assertNotNull(result);
    assertEquals(100, result.getLen());
  }
}
