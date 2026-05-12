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

import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.ContentSummary;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FilterFileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.util.Progressable;

/**
 * A UnionFileSystem presents a unified view of two underlying file systems:
 * - Primary FS (e.g., new HDFS cluster): writes go here
 * - Secondary FS (e.g., old HDFS cluster): read-only, contains historical data
 *
 * This enables transparent migration from old HDFS cluster to new HDFS cluster
 * without modifying application configurations. New data is written to the new
 * cluster while historical data on the old cluster remains accessible.
 *
 * <p>Use case: Hadoop cluster version upgrade or migration from one cluster to another.
 * Applications access data via the union path without knowing which cluster stores the data.</p>
 *
 * <p>Configuration:</p>
 * <ul>
 *   <li>fs.union.primary.fs - URI of the primary filesystem (e.g., hdfs://new-cluster/)</li>
 *   <li>fs.union.secondary.fs - URI of the secondary filesystem (e.g., hdfs://old-cluster/)</li>
 *   <li>fs.union.delete.secondary - Whether to delete files in secondary FS when deleted (default: false)</li>
 *   <li>fs.union.sync.rename - Whether to rename files in both FS (default: false)</li>
 * </ul>
 *
 * <p>Usage in core-site.xml:</p>
 * <pre>
 * &lt;property&gt;
 *   &lt;name&gt;fs.union.impl&lt;/name&gt;
 *   &lt;value&gt;org.apache.hadoop.fs.union.UnionFileSystem&lt;/value&gt;
 * &lt;/property&gt;
 * &lt;property&gt;
 *   &lt;name&gt;fs.union.primary.fs&lt;/name&gt;
 *   &lt;value&gt;hdfs://new-namenode:8020/warehouse&lt;/value&gt;
 * &lt;/property&gt;
 * &lt;property&gt;
 *   &lt;name&gt;fs.union.secondary.fs&lt;/name&gt;
 *   &lt;value&gt;hdfs://old-namenode:8020/warehouse&lt;/value&gt;
 * &lt;/property&gt;
 * </pre>
 */
@InterfaceAudience.Public
@InterfaceStability.Evolving
public class UnionFileSystem extends FilterFileSystem {

  public static final String PRIMARY_FS_KEY = "fs.union.primary.fs";
  public static final String SECONDARY_FS_KEY = "fs.union.secondary.fs";
  public static final String DELETE_SECONDARY_KEY = "fs.union.delete.secondary";
  public static final String SYNC_RENAME_KEY = "fs.union.sync.rename";
  public static final boolean DEFAULT_DELETE_SECONDARY = false;
  public static final boolean DEFAULT_SYNC_RENAME = false;

  private FileSystem primaryFs;
  private FileSystem secondaryFs;
  private URI primaryUri;
  private URI secondaryUri;
  private boolean deleteSecondary;
  private boolean syncRename;
  private Path primaryBasePath;
  private Path secondaryBasePath;

  public UnionFileSystem() {
  }

  /**
   * Create a UnionFileSystem with the given file systems.
   *
   * @param primaryFs the primary file system (writes go here)
   * @param secondaryFs the secondary file system (read-only, historical data)
   */
  public UnionFileSystem(FileSystem primaryFs, FileSystem secondaryFs) {
    super(primaryFs);
    this.primaryFs = primaryFs;
    this.secondaryFs = secondaryFs;
    // Initialize URI from primary FS for methods like getUri()
    if (primaryFs != null) {
      try {
        this.primaryUri = primaryFs.getUri();
      } catch (Exception e) {
        // Ignore - uri will be null
      }
    }
  }

  @Override
  public void initialize(URI name, Configuration conf) throws IOException {
    super.initialize(name, conf);

    // Get primary and secondary FS URIs from configuration
    String primaryUriStr = conf.get(PRIMARY_FS_KEY);
    String secondaryUriStr = conf.get(SECONDARY_FS_KEY);

    if (primaryUriStr == null || secondaryUriStr == null) {
      throw new IOException("UnionFileSystem requires both " + PRIMARY_FS_KEY +
          " and " + SECONDARY_FS_KEY + " configuration");
    }

    // Get configuration options
    this.deleteSecondary = conf.getBoolean(DELETE_SECONDARY_KEY, DEFAULT_DELETE_SECONDARY);
    this.syncRename = conf.getBoolean(SYNC_RENAME_KEY, DEFAULT_SYNC_RENAME);

    try {
      this.primaryUri = new URI(primaryUriStr);
      this.secondaryUri = new URI(secondaryUriStr);
    } catch (Exception e) {
      throw new IOException("Invalid URI in configuration", e);
    }

    // Initialize both file systems
    this.primaryFs = FileSystem.get(primaryUri, conf);
    this.secondaryFs = FileSystem.get(secondaryUri, conf);
    this.fs = primaryFs;

    // Set up base paths for path translation
    this.primaryBasePath = new Path(primaryUri);
    this.secondaryBasePath = new Path(secondaryUri);
  }

  /**
   * Get the primary file system (writes go here).
   */
  public FileSystem getPrimaryFs() {
    return primaryFs;
  }

  /**
   * Get the secondary file system (read-only).
   */
  public FileSystem getSecondaryFs() {
    return secondaryFs;
  }

  @Override
  public URI getUri() {
    return primaryUri;
  }

  @Override
  public FileStatus getFileStatus(Path f) throws IOException {
    // Try primary first (new cluster)
    try {
      if (primaryFs.exists(f)) {
        return primaryFs.getFileStatus(f);
      }
    } catch (IOException e) {
      // File not in primary, try secondary
    }

    // Try secondary (HDFS)
    try {
      if (secondaryFs.exists(f)) {
        FileStatus status = secondaryFs.getFileStatus(f);
        // Adjust path to appear as if it's from the unified view
        return adjustFileStatusPath(f, status);
      }
    } catch (IOException e) {
      // Not found in secondary either
    }

    throw new IOException("File not found in either filesystem: " + f);
  }

  /**
   * Adjust the FileStatus path to match the unified view path.
   * Translates paths from secondary FS to appear as union paths.
   */
  private FileStatus adjustFileStatusPath(Path queryPath, FileStatus status) {
    if (status == null) {
      return null;
    }
    // Create new FileStatus with translated path to hide underlying FS details
    try {
      return new FileStatus(
          status.getLen(),
          status.isDirectory(),
          status.getReplication(),
          status.getBlockSize(),
          status.getModificationTime(),
          status.getAccessTime(),
          status.getPermission(),
          status.getOwner(),
          status.getGroup(),
          translatePath(status.getPath(), secondaryBasePath, primaryBasePath));
    } catch (Exception e) {
      // If translation fails, return original
      return status;
    }
  }

  /**
   * Translate a path from secondary FS to unified path.
   * E.g., hdfs://namenode/warehouse/db/table -> /warehouse/db/table
   */
  private Path translatePath(Path originalPath, Path sourceBasePath, Path targetBasePath) {
    if (sourceBasePath == null || targetBasePath == null) {
      return originalPath;
    }
    // Get the relative path from source base
    Path relativePath = sourceBasePath.makeQualified(sourceBasePath.toUri(), null);
    Path originalQualified = originalPath.makeQualified(originalPath.toUri(), null);
    if (originalQualified.toString().startsWith(relativePath.toString())) {
      String suffix = originalQualified.toString().substring(relativePath.toString().length());
      if (suffix.startsWith("/")) {
        return new Path(targetBasePath.toString() + suffix);
      } else if (suffix.isEmpty()) {
        return targetBasePath;
      }
    }
    return originalPath;
  }

  /**
   * Check if a path exists in the primary filesystem.
   */
  private boolean existsInPrimary(Path f) {
    try {
      return primaryFs.exists(f);
    } catch (IOException e) {
      return false;
    }
  }

  /**
   * Check if a path exists in the secondary filesystem.
   */
  private boolean existsInSecondary(Path f) {
    try {
      return secondaryFs.exists(f);
    } catch (IOException e) {
      return false;
    }
  }

  @Override
  public BlockLocation[] getFileBlockLocations(Path f, long start, long len)
      throws IOException {
    // Try primary first - avoid duplicates by preferring primary
    try {
      if (existsInPrimary(f)) {
        return primaryFs.getFileBlockLocations(f, start, len);
      }
    } catch (IOException e) {
      // Try secondary
    }

    // Try secondary
    if (existsInSecondary(f)) {
      return secondaryFs.getFileBlockLocations(f, start, len);
    }

    throw new IOException("File not found: " + f);
  }

  @Override
  public FileStatus[] listStatus(Path f) throws IOException {
    List<FileStatus> results = new ArrayList<>();

    // List from primary (new cluster)
    try {
      FileStatus[] primaryStatus = primaryFs.listStatus(f);
      if (primaryStatus != null) {
        for (FileStatus status : primaryStatus) {
          // Mark files from primary
          results.add(status);
        }
      }
    } catch (IOException e) {
      // Primary might not exist or not accessible
    }

    // List from secondary (HDFS) - only if directory exists
    try {
      if (secondaryFs.exists(f)) {
        FileStatus[] secondaryStatus = secondaryFs.listStatus(f);
        if (secondaryStatus != null) {
          for (FileStatus status : secondaryStatus) {
            // Filter out duplicates - prefer primary version
            if (!existsInResults(results, status.getPath())) {
              results.add(status);
            }
          }
        }
      }
    } catch (IOException e) {
      // Secondary might not exist
    }

    return results.toArray(new FileStatus[0]);
  }

  /**
   * Check if a path already exists in results.
   */
  private boolean existsInResults(List<FileStatus> results, Path path) {
    for (FileStatus status : results) {
      if (status.getPath().equals(path)) {
        return true;
      }
    }
    return false;
  }

  @Override
  public FSDataInputStream open(Path f, int bufferSize) throws IOException {
    // Try primary first
    try {
      if (primaryFs.exists(f)) {
        return primaryFs.open(f, bufferSize);
      }
    } catch (IOException e) {
      // Try secondary
    }

    // Try secondary
    if (secondaryFs.exists(f)) {
      return secondaryFs.open(f, bufferSize);
    }

    throw new IOException("File not found: " + f);
  }

  @Override
  public FSDataOutputStream create(Path f, boolean overwrite,
      int bufferSize, short replication, long blockSize, Progressable progress)
      throws IOException {
    // All writes go to primary (new cluster)
    return primaryFs.create(f, overwrite, bufferSize, replication,
        blockSize, progress);
  }

  @Override
  public boolean delete(Path f, boolean recursive) throws IOException {
    boolean deleted = false;

    // Delete from primary
    try {
      if (existsInPrimary(f)) {
        deleted = primaryFs.delete(f, recursive);
      }
    } catch (IOException e) {
      // Continue to try secondary
    }

    // Delete from secondary if configured
    if (deleteSecondary) {
      try {
        if (existsInSecondary(f)) {
          deleted = secondaryFs.delete(f, recursive) || deleted;
        }
      } catch (IOException e) {
        // Secondary delete failed
      }
    }

    return deleted;
  }

  @Override
  public boolean mkdirs(Path f, FsPermission permission) throws IOException {
    // Create in primary (new cluster)
    return primaryFs.mkdirs(f, permission);
  }

  @Override
  public boolean rename(Path src, Path dst) throws IOException {
    boolean renamed = false;

    // Rename in primary (new cluster)
    if (existsInPrimary(src)) {
      renamed = primaryFs.rename(src, dst);
    }

    // Rename in secondary if configured
    if (syncRename && existsInSecondary(src)) {
      try {
        renamed = secondaryFs.rename(src, dst) || renamed;
      } catch (IOException e) {
        // Secondary rename failed, but primary succeeded
      }
    }

    return renamed;
  }

  @Override
  public FileStatus[] globStatus(Path f, PathFilter filter) throws IOException {
    List<FileStatus> results = new ArrayList<>();

    // Glob from primary
    try {
      FileStatus[] primaryResults = primaryFs.globStatus(f, filter);
      if (primaryResults != null) {
        for (FileStatus status : primaryResults) {
          results.add(status);
        }
      }
    } catch (IOException e) {
      // Ignore
    }

    // Glob from secondary
    try {
      if (secondaryFs.exists(f)) {
        FileStatus[] secondaryResults = secondaryFs.globStatus(f, filter);
        if (secondaryResults != null) {
          for (FileStatus status : secondaryResults) {
            if (!existsInResults(results, status.getPath())) {
              results.add(status);
            }
          }
        }
      }
    } catch (IOException e) {
      // Ignore
    }

    return results.toArray(new FileStatus[0]);
  }

  @Override
  public FileStatus[] listStatus(Path f, PathFilter filter)
      throws IOException {
    List<FileStatus> results = new ArrayList<>();

    // List from primary
    try {
      FileStatus[] primaryStatus = primaryFs.listStatus(f, filter);
      if (primaryStatus != null) {
        for (FileStatus status : primaryStatus) {
          results.add(status);
        }
      }
    } catch (IOException e) {
      // Ignore
    }

    // List from secondary
    try {
      if (secondaryFs.exists(f)) {
        FileStatus[] secondaryStatus = secondaryFs.listStatus(f, filter);
        if (secondaryStatus != null) {
          for (FileStatus status : secondaryStatus) {
            if (!existsInResults(results, status.getPath())) {
              results.add(status);
            }
          }
        }
      }
    } catch (IOException e) {
      // Ignore
    }

    return results.toArray(new FileStatus[0]);
  }

  @Override
  public ContentSummary getContentSummary(Path f) throws IOException {
    ContentSummary primarySummary = null;
    ContentSummary secondarySummary = null;

    // Get from primary
    try {
      if (existsInPrimary(f)) {
        primarySummary = primaryFs.getContentSummary(f);
      }
    } catch (IOException e) {
      // Ignore
    }

    // Get from secondary
    try {
      if (existsInSecondary(f)) {
        secondarySummary = secondaryFs.getContentSummary(f);
      }
    } catch (IOException e) {
      // Ignore
    }

    // If found in either, return aggregated or single result
    if (primarySummary != null && secondarySummary != null) {
      // Aggregate both summaries
      return new ContentSummary.Builder()
          .length(primarySummary.getLength() + secondarySummary.getLength())
          .fileCount(primarySummary.getFileCount() + secondarySummary.getFileCount())
          .directoryCount(primarySummary.getDirectoryCount() + secondarySummary.getDirectoryCount())
          .spaceConsumed(primarySummary.getSpaceConsumed() + secondarySummary.getSpaceConsumed())
          .build();
    } else if (primarySummary != null) {
      return primarySummary;
    } else if (secondarySummary != null) {
      return secondarySummary;
    }

    throw new IOException("Path not found: " + f);
  }

  @Override
  public long getDefaultBlockSize() {
    return primaryFs.getDefaultBlockSize();
  }

  @Override
  public long getDefaultBlockSize(Path f) {
    return primaryFs.getDefaultBlockSize(f);
  }

  @Override
  public FileStatus getFileLinkStatus(Path f) throws IOException {
    // Try primary first
    try {
      if (existsInPrimary(f)) {
        return primaryFs.getFileLinkStatus(f);
      }
    } catch (IOException e) {
      // Try secondary
    }

    // Try secondary
    if (existsInSecondary(f)) {
      return secondaryFs.getFileLinkStatus(f);
    }

    throw new IOException("File not found: " + f);
  }

  @Override
  public boolean truncate(Path f, long newLength) throws IOException {
    // Try primary first
    try {
      if (existsInPrimary(f)) {
        return primaryFs.truncate(f, newLength);
      }
    } catch (IOException e) {
      // Try secondary
    }

    // Try secondary
    if (existsInSecondary(f)) {
      return secondaryFs.truncate(f, newLength);
    }

    return false;
  }

  @Override
  public void concat(Path trg, Path[] srcs) throws IOException {
    // Try primary first
    if (existsInPrimary(trg)) {
      primaryFs.concat(trg, srcs);
      return;
    }

    // Try secondary
    if (existsInSecondary(trg)) {
      secondaryFs.concat(trg, srcs);
      return;
    }

    throw new IOException("Target file not found: " + trg);
  }

  @Override
  public boolean setReplication(Path f, short replication) {
    // Set in primary first
    try {
      if (existsInPrimary(f)) {
        return primaryFs.setReplication(f, replication);
      }
    } catch (IOException e) {
      // Try secondary
    }

    // Try secondary
    try {
      if (existsInSecondary(f)) {
        return secondaryFs.setReplication(f, replication);
      }
    } catch (IOException e) {
      // Ignore
    }

    return false;
  }

  @Override
  public void createSymlink(Path f, Path target, boolean createParent) throws IOException {
    // Try primary first
    if (existsInPrimary(f.getParent())) {
      primaryFs.createSymlink(f, target, createParent);
      return;
    }

    // Try secondary
    if (existsInSecondary(f.getParent())) {
      secondaryFs.createSymlink(f, target, createParent);
      return;
    }

    throw new IOException("Parent directory not found: " + f.getParent());
  }

  @Override
  public void setOwner(Path f, String username, String groupname) throws IOException {
    // Set in primary first
    try {
      if (existsInPrimary(f)) {
        primaryFs.setOwner(f, username, groupname);
        return;
      }
    } catch (IOException e) {
      // Try secondary
    }

    // Try secondary
    if (existsInSecondary(f)) {
      secondaryFs.setOwner(f, username, groupname);
      return;
    }

    throw new IOException("File not found: " + f);
  }

  @Override
  public void setPermission(Path f, FsPermission permission) throws IOException {
    // Set in primary first
    try {
      if (existsInPrimary(f)) {
        primaryFs.setPermission(f, permission);
        return;
      }
    } catch (IOException e) {
      // Try secondary
    }

    // Try secondary
    if (existsInSecondary(f)) {
      secondaryFs.setPermission(f, permission);
      return;
    }

    throw new IOException("File not found: " + f);
  }

  @Override
  public void setTimes(Path f, long mtime, long atime) throws IOException {
    // Set in primary first
    try {
      if (existsInPrimary(f)) {
        primaryFs.setTimes(f, mtime, atime);
        return;
      }
    } catch (IOException e) {
      // Try secondary
    }

    // Try secondary
    if (existsInSecondary(f)) {
      secondaryFs.setTimes(f, mtime, atime);
      return;
    }

    throw new IOException("File not found: " + f);
  }

  @Override
  public boolean supportsSymlinks() {
    // Delegate to primary
    return primaryFs.supportsSymlinks();
  }

  @Override
  public Path getLinkTarget(Path f) throws IOException {
    // Try primary first
    try {
      if (existsInPrimary(f)) {
        return primaryFs.getLinkTarget(f);
      }
    } catch (IOException e) {
      // Try secondary
    }

    // Try secondary
    if (existsInSecondary(f)) {
      return secondaryFs.getLinkTarget(f);
    }

    throw new IOException("Symlink not found: " + f);
  }

  @Override
  public Path getWorkingDirectory() {
    // Delegate to primary
    return primaryFs.getWorkingDirectory();
  }

  @Override
  public void setWorkingDirectory(Path new_dir) {
    // Set in primary
    primaryFs.setWorkingDirectory(new_dir);
  }

  @Override
  public Path getHomeDirectory() {
    // Delegate to primary
    return primaryFs.getHomeDirectory();
  }

  @Override
  public long getUsed() throws IOException {
    // Get from primary
    return primaryFs.getUsed();
  }

  @Override
  public long getUsed(Path path) throws IOException {
    // Try primary first
    try {
      if (existsInPrimary(path)) {
        return primaryFs.getUsed(path);
      }
    } catch (IOException e) {
      // Try secondary
    }

    // Try secondary
    if (existsInSecondary(path)) {
      return secondaryFs.getUsed(path);
    }

    return 0;
  }

  @Override
  public RemoteIterator<LocatedFileStatus> listLocatedStatus(final Path f)
      throws IOException {
    // Try primary first
    try {
      if (existsInPrimary(f)) {
        return primaryFs.listLocatedStatus(f);
      }
    } catch (IOException e) {
      // Try secondary
    }

    // Try secondary
    if (existsInSecondary(f)) {
      return secondaryFs.listLocatedStatus(f);
    }

    throw new IOException("Path not found: " + f);
  }

  @Override
  public RemoteIterator<FileStatus> listStatusIterator(final Path p)
      throws IOException {
    // Try primary first
    try {
      if (existsInPrimary(p)) {
        return primaryFs.listStatusIterator(p);
      }
    } catch (IOException e) {
      // Try secondary
    }

    // Try secondary
    if (existsInSecondary(p)) {
      return secondaryFs.listStatusIterator(p);
    }

    throw new IOException("Path not found: " + p);
  }

  @Override
  public boolean exists(Path f) throws IOException {
    // Check primary first
    if (existsInPrimary(f)) {
      return true;
    }
    // Check secondary
    if (existsInSecondary(f)) {
      return true;
    }
    return false;
  }

  @Override
  public void close() throws IOException {
    IOException ioe = null;

    try {
      if (primaryFs != null) {
        primaryFs.close();
      }
    } catch (IOException e) {
      ioe = e;
    }

    try {
      if (secondaryFs != null) {
        secondaryFs.close();
      }
    } catch (IOException e) {
      if (ioe == null) {
        ioe = e;
      }
    }

    try {
      super.close();
    } catch (IOException e) {
      if (ioe == null) {
        ioe = e;
      }
    }

    if (ioe != null) {
      throw ioe;
    }
  }
}