# UnionFileSystem

A Hadoop FileSystem implementation that presents a unified view of two underlying file systems.

## Purpose

Enable transparent migration from old HDFS cluster to new HDFS cluster without modifying application configurations. Applications access data via the union path without knowing which cluster stores the data.

## Architecture

- **Primary FS**: New HDFS cluster where all writes go
- **Secondary FS**: Old HDFS cluster, read-only, contains historical data

## Configuration

Add to `core-site.xml`:

```xml
<property>
  <name>fs.union.impl</name>
  <value>org.apache.hadoop.fs.union.UnionFileSystem</value>
</property>

<property>
  <name>fs.union.primary.fs</name>
  <value>hdfs://new-namenode:8020/warehouse</value>
</property>

<property>
  <name>fs.union.secondary.fs</name>
  <value>hdfs://old-namenode:8020/warehouse</value>
</property>

<!-- Optional: delete files in secondary when deleted in primary -->
<property>
  <name>fs.union.delete.secondary</name>
  <value>false</value>
</property>

<!-- Optional: sync rename operations -->
<property>
  <name>fs.union.sync.rename</name>
  <value>false</value>
</property>
```

## Usage Example

```java
Configuration conf = new Configuration();
conf.set("fs.union.impl", "org.apache.hadoop.fs.union.UnionFileSystem");
conf.set("fs.union.primary.fs", "hdfs://new-cluster:8020/warehouse");
conf.set("fs.union.secondary.fs", "hdfs://old-cluster:8020/warehouse");

Path unionPath = new Path("/warehouse/db/table");
FileSystem fs = FileSystem.get(unionPath.toUri(), conf);

// Read existing data from old cluster (transparently)
// Write new data to new cluster
FSDataOutputStream out = fs.create(new Path("/warehouse/db/table/newfile"));
```

## Features

- **Transparent Read**: Files not in primary automatically fetched from secondary
- **Write Isolation**: New data always written to primary cluster
- **List Aggregation**: Lists files from both clusters, deduplicates by path
- **Path Translation**: Internal cluster paths hidden from applications
- **Configurable Sync**: Optional delete/rename synchronization

## Supported Operations

| Operation | Behavior |
|-----------|---------|
| read | Primary first, fallback to secondary |
| write | Primary only |
| delete | Primary (configurable secondary) |
| rename | Primary (configurable secondary) |
| listStatus | Aggregate + deduplicate |
| globStatus | Aggregate + deduplicate |
| getFileStatus | Primary first, fallback to secondary |
| ContentSummary | Aggregate both |
| mkdirs | Primary only |
| getDefaultBlockSize | From primary |
| getUri | Returns primary URI |

## Testing

Run tests:
```bash
mvn test -Dtest=TestUnionFileSystem
```

## Use Cases

1. **Hadoop Cluster Upgrade**: Migrate from old cluster to new without application changes
2. **Cluster Migration**: Move data between clusters gradually
3. **Hybrid Storage**: Keep recent data on fast cluster, historical on cheaper storage