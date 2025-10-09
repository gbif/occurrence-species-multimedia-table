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

/**
 * A utility to build an HBase table mapping speciesKey+mediaType to multimedia identifiers.
 * <p>
 * The table is designed for efficient retrieval of multimedia identifiers by speciesKey and mediaType.
 * The row key is constructed as: "<speciesKey>#<mediaType>#<salt>", where salt is a hash-based
 * value to distribute rows evenly across the cluster.
 * <p>
 * Each row contains a single column family "media" with a fixed qualifier "identifiers" that holds
 * a comma-separated list of multimedia identifiers.
 * <p>
 * Usage:
 * <pre>
 *   java -cp your-jar-with-dependencies.jar org.gbif.multimedia.MultiMediaTableBuilder \
 *     <sourceCatalog> <hbaseTable> <saltBuckets> <hbase.zookeeper.quorum> <zookeeper.znode.parent>
 * </pre>
 * Example:
 * <pre>
 *   java -cp your-jar-with-dependencies.jar org.gbif.multimedia.MultiMediaTableBuilder \
 *     iceberg.prod occurrence_species_multimedia 64 zk1,zk2,zk3 /hbase
 * </pre>
 */
@Slf4j
public class OccurrenceSpecieMultiMediaTableBuilder {

  // Column family & qualifier conventions
  private static final String COLUMN_FAMILY = "media"; // HBase CF
  private static final int BATCH_PUT_SIZE = 1000;      // tune per cluster

  private static final int MAX_MEDIAINFOS_PER_CELL = 1000;

  private static final int DEFAULT_PARTITIONS = 200;

  private static final ObjectMapper MAPPER = new ObjectMapper();


  public static void main(String[] args) throws Exception {
    if (args.length < 5) {
      System.err.println("Usage: MediaTableBuilder <sourceCatalog> <hbaseTable> <saltBuckets> <hbase.zookeeper.quorum> <zookeeper.znode.parent>");
      System.exit(1);
    }

    final String sourceCatalog = args[0];              // e.g. "iceberg.prod"
    final String hbaseTable = args[1];                 // e.g. "occurrence_media"
    final int saltBuckets = Integer.parseInt(args[2]); // e.g. 64
    final String zkQuorum = args[3];                   // e.g. "zk1,zk2,zk3"
    final String znodeParent = args[4];                // e.g. "/znode-93f9cdb5-d146-46da-9f80-e8546468b0fe/hbase"

    SparkSession spark = createSparkSession("MediaTableBuilder");

    // Recreate HBase table
    log.info("(Re)Creating HBase table: {}", hbaseTable);
    spark.sparkContext().setJobGroup("hbase-table", "(Re)Creating HBase table " + hbaseTable, true);
    recreateHBaseTable(hbaseTable, saltBuckets, zkQuorum, znodeParent);
    spark.sparkContext().clearJobGroup();

    Integer partitions = getShufflePartitions(spark);

    log.info("Using {} shuffle partitions", partitions);

    //1. Create Dataset with speciesKey, media type and aggregated multimedia identifiers
    log.info("Creating aggregated Dataset from source catalog: {}", sourceCatalog);
    spark.sparkContext().setJobGroup("occurrence-query", "Getting the aggregated multimedia identifiers by speciesKey and media type", true);
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
        byte[] mediaInfosQualifier = Bytes.toBytes("media_infos");
        byte[] multimediaCountQualifier = Bytes.toBytes("multimedia_count");
        byte[] totalMultimediaCountQualifier = Bytes.toBytes("total_multimedia_count");
        while (rows.hasNext()) {

          Row r = rows.next();
          String speciesKey = r.getAs("speciesKey");
          String mediaType = r.getAs("type");
          Long chunkIndex = r.getAs("chunkIndex");
          List<Row> mediaInfos = r.getList(r.fieldIndex("mediaInfos"));

          // Append a postfix to the row key if there are multiple cells for the same speciesKey+mediaType
          String identifierPostKey = chunkIndex != null ? ("#" + chunkIndex) : "";

          byte[] keyPrefix = saltedKeyGenerator.computeKey(speciesKey + mediaType.toUpperCase(Locale.ROOT));
          byte[] postKeyBytes = identifierPostKey.getBytes(StandardCharsets.UTF_8);
          byte[] rowKeyBytes = new byte[keyPrefix.length + postKeyBytes.length];

          // Compute salted row key
          System.arraycopy(keyPrefix, 0, rowKeyBytes, 0, keyPrefix.length);
          System.arraycopy(postKeyBytes, 0, rowKeyBytes, keyPrefix.length, postKeyBytes.length);


          // Serialize MediaInfo rows to JSON array string
          List<String> mediaInfoJsons = mediaInfos.stream()
              .map(OccurrenceSpecieMultiMediaTableBuilder::rowToJsonMediaInfo)
              .collect(Collectors.toList());

          String mediaInfosStr = "[" + String.join(",", mediaInfoJsons) + "]";
          byte[] value = mediaInfosStr.getBytes(StandardCharsets.UTF_8);
          Put put = new Put(rowKeyBytes);
          put.addColumn(Bytes.toBytes(COLUMN_FAMILY), mediaInfosQualifier, value);
          put.addColumn(Bytes.toBytes(COLUMN_FAMILY), multimediaCountQualifier, Bytes.toBytes(mediaInfoJsons.size()));
          put.addColumn(Bytes.toBytes(COLUMN_FAMILY), totalMultimediaCountQualifier, Bytes.toBytes(mediaInfos.size()));
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
        for (Put p : batch) {
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
  }


  @SneakyThrows
  private static String rowToJsonMediaInfo(Row row) {
    Map<String,Object> map = new HashMap<>();
    map.put("identifier", row.getAs("identifier"));
    map.put("title", row.getAs("title"));
    map.put("gbifid", row.getAs("gbifid"));
    map.put("rightsholder", row.getAs("rightsholder"));
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
   * Creates a Dataset<Row> with speciesKey, media type, and aggregated multimedia identifiers.
   *
   * @param spark         the SparkSession instance
   * @param sourceCatalog the source catalog name (e.g., "iceberg.prod")
   * @param partitions    the number of partitions for repartitioning
   * @return a Dataset<Row> with columns: speciesKey, type, identifiers (array of strings)
   */
  private static Dataset<Row> createSpeciesMediaDataset(SparkSession spark, String sourceCatalog, Integer partitions) {

    // Read from Iceberg/Hive table
    Dataset<Row> df = spark.sql(String.format("SELECT o.speciesKey, m.identifier, m.type, m.title, m.gbifid, m.rightsholder, m.license " +
        "FROM %1$s.occurrence o " +
        "JOIN %1$s.occurrence_multimedia m ON o.gbifId = m.gbifId", sourceCatalog) );

    // Repartition for parallelism
    Dataset<Row> repartitionedDf = df.repartition(partitions, functions.col("speciesKey"), functions.col("type"));

    // Window for row_number per group
    WindowSpec w = Window.partitionBy("speciesKey", "type").orderBy(functions.col("identifier"));
    Dataset<Row> withChunkIndex = repartitionedDf.withColumn(
        "chunkIndex",
        functions.floor(
            functions.row_number().over(w).minus(1)
                .divide(MAX_MEDIAINFOS_PER_CELL)
        )
    );

    // Now wrap mediaInfo struct
    Dataset<Row> withMediaInfo = withChunkIndex.select(
        functions.col("speciesKey"),
        functions.col("type"),
        functions.col("chunkIndex"),
        functions.struct(
            functions.col("identifier"),
            functions.col("title"),
            functions.col("gbifid"),
            functions.col("rightsholder"),
            functions.col("license")
        ).alias("mediaInfo")
    );

   // Group by speciesKey, type, chunkIndex
    return withMediaInfo.groupBy("speciesKey", "type", "chunkIndex")
        .agg(functions.collect_list("mediaInfo").alias("mediaInfos"));
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
        log.info("Table {} exists, deleting it first", tableName);
        if (admin.isTableEnabled(tn)) {
          log.info("Disabling table {}", tableName);
          admin.disableTable(tn);
        }
        admin.deleteTable(tn);
      }

      // Prepare splits
      byte[][] splits = new byte[saltBuckets - 1][];
      for (int i = 0; i < saltBuckets - 1; i++) {
        splits[i] = Bytes.toBytes(String.format("%02d", i + 1));
      }

      // Define table descriptor
      TableDescriptor tableDesc = TableDescriptorBuilder
          .newBuilder(TableName.valueOf(tableName))
          .setColumnFamily(ColumnFamilyDescriptorBuilder.of("media"))
          .build();

      // Create table with splits
      admin.createTable(tableDesc, splits);
      log.info("Table {} created with {} salt buckets", tableName, saltBuckets);
    }
  }

}
