package org.gbif.multimedia;


import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.stream.Collectors;

import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.io.compress.Compression;
import org.apache.hadoop.hbase.io.encoding.DataBlockEncoding;
import org.apache.hadoop.hbase.util.Bytes;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.expressions.Window;
import org.apache.spark.sql.expressions.WindowSpec;
import org.apache.spark.sql.functions;
import org.gbif.multimedia.meta.ZKMapMetastore;

/**
 * A utility to build an HBase table mapping taxonKey+mediaType to multimedia records.
 * <p>
 * The table is designed for efficient retrieval of multimedia records by taxonKey and mediaType.
 * The row key is constructed as: "<taxonKey>#<mediaType>#<salt>", where salt is a hash-based
 * value to distribute rows evenly across the cluster.
 * <p>
 * Each row contains a single column family "media" with a fixed qualifier "mediaInfos" that holds
 * a comma-separated list of multimedia records.
 * <p>
 * Usage:
 * <pre>
 *   java -cp your-jar-with-dependencies.jar org.gbif.multimedia.OccurrenceSpecieMultiMediaTableBuilder \
 *     <sourceCatalog> <hbaseTable> <saltBuckets> <hbase.zookeeper.quorum> <zookeeper.znode.parent>
 * </pre>
 * Example:
 * <pre>
 *   java -cp your-jar-with-dependencies.jar org.gbif.multimedia.OccurrenceSpecieMultiMediaTableBuilder \
 *     iceberg.prod occurrence_species_multimedia 64 zk1,zk2,zk3 /hbase
 * </pre>
 */
@Slf4j
public class OccurrenceSpecieMultiMediaTableBuilder {

  // Column family & qualifier conventions
  private static final byte[] COLUMN_FAMILY = Bytes.toBytes("media"); // HBase
  private static final byte[] MEDIA_INFOS_QUALIFIER = Bytes.toBytes("media_infos");
  private static final byte[] MULTIMEDIA_COUNT_QUALIFIER = Bytes.toBytes("multimedia_count");
  private static final byte[] TOTAL_MULTIMEDIA_COUNT_QUALIFIER = Bytes.toBytes("total_multimedia_count");
  // CF
  private static final int BATCH_PUT_SIZE = 1000;      // tune per cluster

  private static final int MAX_MEDIAINFOS_PER_CELL = 300;

  private static final int DEFAULT_PARTITIONS = 600;

  private static final ObjectMapper MAPPER = new ObjectMapper();
  public static final int RETRY_INTERVAL_MS = 3000;


  public static void main(String[] args) throws Exception {
    if (args.length < 7) {
      System.err.println("Usage: OccurrenceSpecieMultiMediaTableBuilder <sourceCatalog> <hbaseTable> <saltBuckets> <hbase.zookeeper.quorum> <zookeeper.znode.parent> <zkMetastorePath> <timestamp>");
      System.exit(1);
    }

    final String sourceCatalog = args[0];              // e.g. "iceberg.prod"
    final String baseHbaseTable = args[1];                 // e.g. "occurrence_media"
    final int saltBuckets = Integer.parseInt(args[2]); // e.g. 64
    final String zkQuorum = args[3];                   // e.g. "zk1,zk2,zk3"
    final String znodeParent = args[4];                // e.g. "/znode-93f9cdb5-d146-46da-9f80-e8546468b0fe/hbase"
    final String zkMetastorePath = args[5];          // e.g. "/dev/meta/occurrence_species_multimedia_table"
    final String timestamp = args[6];                  // expected format "YYYYMMDD_HHMM"

    final String hbaseTable = baseHbaseTable + "_" + timestamp;

    SparkSession spark = createSparkSession("OccurrenceSpecieMultiMediaTableBuilder");

    // Recreate HBase table
    log.info("(Re)Creating HBase table: {}", hbaseTable);
    spark.sparkContext().setJobGroup("hbase-table", "(Re)Creating HBase table " + hbaseTable, true);
    recreateHBaseTable(hbaseTable, saltBuckets, zkQuorum, znodeParent);
    spark.sparkContext().clearJobGroup();

    Integer partitions = getShufflePartitions(spark);

    log.info("Using {} shuffle partitions", partitions);

    //1. Create Dataset with taxonKey, media type and aggregated multimedia identifiers
    log.info("Creating aggregated Dataset from source catalog: {}", sourceCatalog);
    spark.sparkContext().setJobGroup("occurrence-query", "Getting the aggregated multimedia identifiers by taxonKey and media type", true);
    Dataset<Row> aggDf = createSpeciesMediaDataset(spark, sourceCatalog, partitions);

    spark.sqlContext().setConf("spark.sql.shuffle.partitions", partitions.toString());

    //2. Convert to JavaRDD<Row> and write per partition to HBase
    JavaRDD<Row> rdd = aggDf.javaRDD();
    spark.sparkContext().clearJobGroup();

    //3. Write to HBase per partition
    log.info("Writing to HBase table: {}", hbaseTable);
    spark.sparkContext().setJobGroup("hbase-write", "Writing the aggregated multimedia identifiers to HBase table " + hbaseTable, true);

    rdd.foreachPartition((VoidFunction<Iterator<Row>>) rows -> {
      BufferedMutatorParams params = new BufferedMutatorParams(TableName.valueOf(hbaseTable));
      //params.writeBufferSize(4 * 1024 * 1024); // 4MB if needed, default is 2MB
      try (Connection conn = createHBaseConnection(zkQuorum, znodeParent);
           BufferedMutator mutator = conn.getBufferedMutator(params)) {

        List<Put> batch = new ArrayList<>(BATCH_PUT_SIZE);
        SaltedKeyGenerator saltedKeyGenerator = new SaltedKeyGenerator(saltBuckets);
        while (rows.hasNext()) {

          Row r = rows.next();
          String taxonKey = r.getAs("taxonkey");
          String mediaTypeValue = r.getAs("mediaType");
          String mediaType = mediaTypeValue != null ? mediaTypeValue.toLowerCase(Locale.ROOT) : "";
          Long chunkIndex = r.getAs("chunkIndex");
          List<Row> mediaInfos = r.getList(r.fieldIndex("mediaInfos"));
          long totalMultimediaCount = r.getLong(r.fieldIndex("totalMultimediaCount"));

          byte[] rowKeyBytes = saltedKeyGenerator.computeKey(taxonKey + mediaType + chunkIndex);

          // Serialize MediaInfo rows to JSON array string
          List<String> mediaInfoJsons = mediaInfos.stream()
              .map(OccurrenceSpecieMultiMediaTableBuilder::rowToJsonMediaInfo)
              .collect(Collectors.toList());

          String mediaInfosStr = "[" + String.join(",", mediaInfoJsons) + "]";
          byte[] value = mediaInfosStr.getBytes(StandardCharsets.UTF_8);
          Put put = new Put(rowKeyBytes);
          put.addColumn(COLUMN_FAMILY, MEDIA_INFOS_QUALIFIER, value);
          put.addColumn(COLUMN_FAMILY, MULTIMEDIA_COUNT_QUALIFIER, Bytes.toBytes(mediaInfoJsons.size()));
          put.addColumn(COLUMN_FAMILY, TOTAL_MULTIMEDIA_COUNT_QUALIFIER, Bytes.toBytes(totalMultimediaCount));
          batch.add(put);

        if (batch.size() >= BATCH_PUT_SIZE) {
          // submit batch
          for (Put p : batch) {
            mutator.mutate(p);
          }
          mutator.flush();
          batch.clear();
        }
      }

      // flush remaining
      if (!batch.isEmpty()) {
        for (Put p: batch) {
          mutator.mutate(p);
        }
        mutator.flush();
        batch.clear();
      }
      }
    });
    log.info("HBase table {} population completed.", hbaseTable);
    spark.sparkContext().clearJobGroup();

    spark.stop();
    updateMeta(zkQuorum, RETRY_INTERVAL_MS, zkMetastorePath, hbaseTable, znodeParent);
    log.info("Updated metastore at {} with table name {}", zkMetastorePath, hbaseTable);
  }

  private static void updateMeta(String zkEnsemble, int retryIntervalMs, String zkNodePath, String newTableName, String hbaseZNodeParent) throws Exception {
    try (ZKMapMetastore mapMetastore = new ZKMapMetastore(zkEnsemble, retryIntervalMs, zkNodePath)) {
      String currentTableName = mapMetastore.getCurrentTableName();
      log.info("Current table in metastore: {}", currentTableName);
      mapMetastore.update(newTableName);
      log.info("Metastore at {} updated to new table: {}", zkNodePath, newTableName);
      if (currentTableName != null && !currentTableName.isEmpty()) {
        deleteHBaseTable(currentTableName, zkEnsemble, hbaseZNodeParent);
      }
    }
  }


  @SneakyThrows
  private static String rowToJsonMediaInfo(Row row) {
    Map<String,Object> map = new HashMap<>();
    map.put("identifier", row.getAs("identifier"));
    map.put("title", row.getAs("title"));
    map.put("occurrenceKey", row.getAs("gbifid"));
    map.put("rightsHolder", row.getAs("rightsholder"));
    map.put("license", row.getAs("license"));
    return MAPPER.writeValueAsString(map);
  }

  /**
   * Creates a Spark session with Hive and Iceberg support enabled.
   *
   * @param appName the name of the Spark application
   * @return a SparkSession instance
   */
  public static SparkSession createSparkSession(String appName) {
    SparkConf sparkConf = new SparkConf();
    SparkSession.Builder sparkBuilder =
        SparkSession.builder()
            .appName(appName)
            .config(sparkConf)
            .enableHiveSupport()
            .config("spark.sql.catalog.iceberg.type", "hive")
            .config("spark.sql.catalog.iceberg", "org.apache.iceberg.spark.SparkCatalog");
    return sparkBuilder.getOrCreate();
  }

  /**
   * Retrieves the number of shuffle partitions from Spark configuration or defaults to DEFAULT_PARTITIONS.
   *
   * @param spark the SparkSession instance
   * @return the number of shuffle partitions
   */
  private static Integer getShufflePartitions(SparkSession spark) {
    return spark.sqlContext().getConf("spark.sql.shuffle.partitions", null) != null ?
        Integer.parseInt(spark.sqlContext().getConf("spark.sql.shuffle.partitions")) : DEFAULT_PARTITIONS;
  }

  /**
   * Creates a Dataset<Row> with taxonKey, media type, and aggregated multimedia identifiers.
   *
   * @param spark         the SparkSession instance
   * @param sourceCatalog the source catalog name (e.g., "iceberg.prod")
   * @param partitions    the number of partitions for repartitioning
   * @return a Dataset<Row> with columns: taxonKey, type, identifiers (array of strings)
   */
  private static Dataset<Row> createSpeciesMediaDataset(SparkSession spark, String sourceCatalog, Integer partitions) {
    // honor requested shuffle partitions (if provided) to control parallelism for aggregations
    if (partitions != null) {
      spark.sqlContext().setConf("spark.sql.shuffle.partitions", partitions.toString());
    }

    // Read occurrence and occurrence_multimedia as separate tables and join explicitly to avoid
    // nested planner ambiguities. Select and alias columns explicitly so Spark has no ambiguity
    // about which relation a column belongs to.
    Dataset<Row> occ = spark.table(sourceCatalog + ".occurrence").select("gbifid", "taxonkey");
    Dataset<Row> mm = spark.table(sourceCatalog + ".occurrence_multimedia").select(
        "gbifid", "datasetkey", "identifier", "type", "format", "title", "license", "rightsHolder");

    Dataset<Row> df = occ.join(mm, occ.col("gbifid").equalTo(mm.col("gbifid")), "inner")
        .filter(mm.col("identifier").isNotNull().and(occ.col("taxonkey").isNotNull()))
        .select(
            occ.col("taxonkey"),
            mm.col("datasetkey"),
            functions.coalesce(mm.col("identifier"), functions.lit("")).alias("identifier"),
            functions.coalesce(mm.col("type"), functions.lit("")).alias("mediaType"),
            mm.col("format"),
            mm.col("title"),
            mm.col("gbifid"),
            mm.col("rightsHolder").alias("rightsholder"),
            mm.col("license")
        );

    // Compute total multimedia count per taxon/mediaType (kept for the HBase metadata column)
    Dataset<Row> globalTotals = df.select("taxonkey", "mediaType", "identifier")
        .groupBy("taxonkey", "mediaType")
        .agg(functions.count(functions.col("identifier")).alias("totalMultimediaCount"));

    // For simplicity and to avoid complex planner joins that can cause ambiguous aggregate
    // resolution, we do not compute per-format counts here. The intra-chunk ordering will
    // rely on per-row attributes (format null, image-like, license null) and a random tie-breaker.
    // Use `df` directly for downstream processing.

    // Per-dataset sequence number (randomized per dataset to interleave rows)
    WindowSpec perDatasetWindow = Window.partitionBy("taxonkey", "mediaType", "datasetkey").orderBy(functions.rand());
    Dataset<Row> withDatasetSequence = df.withColumn("datasetRowNum", functions.row_number().over(perDatasetWindow));

    // Deterministic dataset ordering per taxon/mediaType using hash(datasetkey)
    Dataset<Row> datasetOrder = withDatasetSequence.select("taxonkey", "mediaType", "datasetkey").distinct()
        .withColumn("datasetOrder", functions.row_number().over(
            Window.partitionBy("taxonkey", "mediaType").orderBy(functions.abs(functions.hash(functions.col("datasetkey"))))
        ));

    Dataset<Row> withDatasetOrder = withDatasetSequence.join(datasetOrder, new String[]{"taxonkey", "mediaType", "datasetkey"}, "left");

    // Assign chunkIndex by strict round-robin across datasets (datasetRowNum, datasetOrder)
    WindowSpec roundRobinWindow = Window.partitionBy("taxonkey", "mediaType")
        .orderBy(functions.col("datasetRowNum"), functions.col("datasetOrder"), functions.rand());

    Dataset<Row> withChunkIndex = withDatasetOrder.withColumn("chunkIndex",
        functions.floor(functions.row_number().over(roundRobinWindow).minus(1).divide(MAX_MEDIAINFOS_PER_CELL)));

    // Build mediaInfo struct and compute within-dataset rank (by preferences) inside each chunk
    Dataset<Row> withMediaInfo = withChunkIndex.withColumn("mediaInfoStruct",
        functions.struct(functions.col("identifier"), functions.col("title"), functions.col("gbifid"), functions.col("rightsholder"), functions.col("license")));

    Dataset<Row> withWithinDatasetRank = withMediaInfo.withColumn("withinDatasetRank",
        functions.row_number().over(
            Window.partitionBy("taxonkey", "mediaType", "chunkIndex", "datasetkey").orderBy(
                functions.expr("format IS NULL").asc(),
                functions.expr("lower(format) LIKE 'image%'").desc(),
                functions.expr("license IS NULL").asc(),
                functions.rand()
            )
        )
    );

    // Aggregate per chunk: collect tuples (r,d,rnd,mi), sort them and extract mediaInfo structs in order
    Dataset<Row> chunked = withWithinDatasetRank.groupBy("taxonkey", "mediaType", "chunkIndex")
        .agg(functions.collect_list(functions.struct(
            functions.col("withinDatasetRank").alias("r"),
            functions.col("datasetOrder").alias("d"),
            functions.rand().alias("rnd"),
            functions.col("mediaInfoStruct").alias("mi")
        )).alias("items"))
        .withColumn("mediaInfos", functions.expr("transform(sort_array(items), x -> x.mi)"))
        .select(functions.col("taxonkey"), functions.col("mediaType"), functions.col("chunkIndex"), functions.col("mediaInfos"));

    return chunked.join(globalTotals, new String[]{"taxonkey", "mediaType"}, "inner");
  }

  /**
   * Creates an HBase connection using the provided Zookeeper quorum and znode parent.
   *
   * @param zkQuorum    the Zookeeper quorum (comma-separated hostnames)
   * @param znodeParent the Zookeeper znode parent path
   * @return an HBase Connection instance
   * @throws IOException if an error occurs while creating the connection
   */
  private static Connection createHBaseConnection(String zkQuorum, String znodeParent) throws IOException {
    Configuration conf = HBaseConfiguration.create();
    conf.set("hbase.zookeeper.quorum", zkQuorum);
    conf.set("zookeeper.znode.parent", znodeParent);
    return ConnectionFactory.createConnection(conf);
  }


  /**
   * Recreates an HBase table with the specified name and salt buckets. If the table already exists,
   * it will be deleted and recreated.
   *
   * @param tableName   the name of the HBase table
   * @param saltBuckets the number of salt buckets (pre-splits)
   * @param zkQuorum    the Zookeeper quorum (comma-separated hostnames)
   * @param znodeParent the Zookeeper znode parent path
   * @throws Exception if an error occurs while recreating the table
   */
  public static void recreateHBaseTable(String tableName, int saltBuckets, String zkQuorum, String znodeParent) throws Exception {

    try (Connection connection = createHBaseConnection(zkQuorum, znodeParent);
         Admin admin = connection.getAdmin()) {

      TableName tn = TableName.valueOf(tableName);

      // Drop table if exists
      if (admin.tableExists(tn)) {
        throw new RuntimeException("Table " + tableName + " already exists. Please delete it first before recreating.");
      }

      // Prepare splits
      byte[][] splits = new byte[saltBuckets - 1][];
      for (int i = 0; i < saltBuckets - 1; i++) {
        splits[i] = Bytes.toBytes(String.format("%02d", i + 1));
      }

      // Define table descriptor
      TableDescriptor tableDesc = TableDescriptorBuilder
          .newBuilder(TableName.valueOf(tableName))
          .setRegionSplitPolicyClassName("org.apache.hadoop.hbase.regionserver.DisabledRegionSplitPolicy")
          .setMergeEnabled(false)
          .setColumnFamily(ColumnFamilyDescriptorBuilder.newBuilder(Bytes.toBytes("media"))
                            .setCompressionType(Compression.Algorithm.SNAPPY)
                            .setDataBlockEncoding(DataBlockEncoding.FAST_DIFF)
                            .build())
          .build();

      // Create table with splits
      admin.createTable(tableDesc, splits);
      log.info("Table {} created with {} salt buckets", tableName, saltBuckets);
    }
  }

  /**
   * Deletes an HBase table with the specified name.
   *
   * @param tableName   the name of the HBase table
   * @param zkQuorum    the Zookeeper quorum (comma-separated hostnames)
   * @param znodeParent the Zookeeper znode parent path
   * @throws Exception if an error occurs while deleting the table
   */
  public static void deleteHBaseTable(String tableName, String zkQuorum, String znodeParent) throws Exception {
    try (Connection connection = createHBaseConnection(zkQuorum, znodeParent);
         Admin admin = connection.getAdmin()) {
      TableName tn = TableName.valueOf(tableName);
      // Drop table if exists
      if (admin.tableExists(tn)) {
        log.info("Table {} exists, deleting it first", tableName);
        if (admin.isTableEnabled(tn)) {
          log.info("Disabling table {}", tableName);
          admin.disableTable(tn);
        }
        admin.deleteTable(tn);
      }
    }
  }
}
