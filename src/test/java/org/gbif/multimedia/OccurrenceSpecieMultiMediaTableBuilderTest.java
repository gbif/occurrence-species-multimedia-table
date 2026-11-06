package org.gbif.multimedia;

import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DecoderFactory;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import org.xerial.snappy.Snappy;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Verifies that {@link OccurrenceSpecieMultiMediaTableBuilder#createSpeciesMediaDataset} caps the
 * number of rows contributed by any single dataset within a checklistKey+classificationTaxonKey+
 * mediaType group, so that a broad ancestor taxon (which every occurrence in a large clade shares
 * in its classification hierarchy) cannot blow up the round-robin chunking window into a single
 * massive, OOM-inducing partition.
 */
class OccurrenceSpecieMultiMediaTableBuilderTest {

  private static final String CATALOG = "test_catalog";
  private static final String CHECKLIST_KEY = "CL1";
  private static final String BROAD_TAXON_KEY = "TAXON_BROAD";
  private static final String MEDIA_TYPE = "StillImage";

  @TempDir
  static Path warehouseDir;

  private static SparkSession spark;

  @BeforeAll
  static void setUpSpark() {
    spark = SparkSession.builder()
        .appName("OccurrenceSpecieMultiMediaTableBuilderTest")
        .master("local[2]")
        .config("spark.sql.warehouse.dir", warehouseDir.toString())
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.ui.enabled", "false")
        .getOrCreate();

    spark.sql("CREATE DATABASE IF NOT EXISTS " + CATALOG);
  }

  @AfterAll
  static void tearDownSpark() {
    if (spark != null) {
      spark.stop();
    }
  }

  private static Dataset<Row> buildOccurrenceDataset(long startGbifId, long count) {
    List<Row> rows = new ArrayList<>();
    for (long i = 0; i < count; i++) {
      rows.add(RowFactory.create(startGbifId + i));
    }
    StructType schema = new StructType(new StructField[]{
        new StructField("gbifid", DataTypes.LongType, false, Metadata.empty())
    });
    // Every occurrence shares the same single-entry classification hierarchy: checklistKey ->
    // [taxonKey]. In production, a broad ancestor taxon like this would appear for millions of
    // occurrences across many datasets, which is the skew this test simulates at smaller scale.
    return spark.createDataFrame(rows, schema)
        .withColumn("classifications",
            functions.map(functions.lit(CHECKLIST_KEY), functions.array(functions.lit(BROAD_TAXON_KEY))));
  }

  private static Dataset<Row> buildMultimediaDataset(String datasetKey, long startGbifId, long count) {
    List<Row> rows = new ArrayList<>();
    for (long i = 0; i < count; i++) {
      long gbifId = startGbifId + i;
      rows.add(RowFactory.create(
          gbifId,
          datasetKey,
          "https://example.org/" + datasetKey + "/" + gbifId,
          MEDIA_TYPE,
          "image/jpeg",
          "title-" + gbifId,
          "CC0",
          "rightsHolder-" + datasetKey
      ));
    }
    StructType schema = new StructType(new StructField[]{
        new StructField("gbifid", DataTypes.LongType, false, Metadata.empty()),
        new StructField("datasetkey", DataTypes.StringType, false, Metadata.empty()),
        new StructField("identifier", DataTypes.StringType, false, Metadata.empty()),
        new StructField("type", DataTypes.StringType, false, Metadata.empty()),
        new StructField("format", DataTypes.StringType, false, Metadata.empty()),
        new StructField("title", DataTypes.StringType, false, Metadata.empty()),
        new StructField("license", DataTypes.StringType, false, Metadata.empty()),
        new StructField("rightsHolder", DataTypes.StringType, false, Metadata.empty())
    });
    return spark.createDataFrame(rows, schema);
  }

  @Test
  void capsRowsPerDatasetWithinSkewedTaxonGroup() {
    // ds1 alone exceeds the per-dataset cap; ds2 and ds3 stay well under it, simulating a broad
    // ancestor taxon shared by a dominant dataset plus a couple of smaller contributors.
    long ds1Count = OccurrenceSpecieMultiMediaTableBuilder.MAX_ROWS_PER_DATASET_PER_GROUP + 500L; // 2500
    long ds2Count = 50L;
    long ds3Count = 10L;
    long uncappedTotal = ds1Count + ds2Count + ds3Count; // 2560
    long expectedCappedTotal = OccurrenceSpecieMultiMediaTableBuilder.MAX_ROWS_PER_DATASET_PER_GROUP + ds2Count + ds3Count; // 2060

    Dataset<Row> occurrence = buildOccurrenceDataset(1, ds1Count)
        .unionByName(buildOccurrenceDataset(10_000, ds2Count))
        .unionByName(buildOccurrenceDataset(20_000, ds3Count));

    Dataset<Row> multimedia = buildMultimediaDataset("ds1", 1, ds1Count)
        .unionByName(buildMultimediaDataset("ds2", 10_000, ds2Count))
        .unionByName(buildMultimediaDataset("ds3", 20_000, ds3Count));

    occurrence.write().mode("overwrite").saveAsTable(CATALOG + ".occurrence");
    multimedia.write().mode("overwrite").saveAsTable(CATALOG + ".occurrence_multimedia");

    AppConfig.Config config = new AppConfig.Config();
    config.setSourceCatalog(CATALOG);

    Dataset<Row> result = OccurrenceSpecieMultiMediaTableBuilder
        .createSpeciesMediaDataset(spark, CATALOG, 4, config)
        .persist();

    System.out.println("PLAN>>>\n" + result.queryExecution().executedPlan().toString());
    // The per-dataset cap must be applied map-side (before the shuffle) via WindowGroupLimit;
    // otherwise a skewed group is shuffled and sorted in full by a single task.
    assertTrue(result.queryExecution().executedPlan().toString().contains("WindowGroupLimit"),
        "per-dataset cap should be planned as a WindowGroupLimit");

    long totalStoredMediaInfos = result.collectAsList().stream()
        .mapToLong(r -> r.getList(r.fieldIndex("mediaInfos")).size())
        .sum();

    assertEquals(expectedCappedTotal, totalStoredMediaInfos,
        "stored mediaInfos should be bounded by the per-dataset cap, not the raw skewed row count");
    assertTrue(totalStoredMediaInfos < uncappedTotal,
        "capped total should be strictly less than the uncapped skewed total");

    // totalMultimediaCount is the HBase metadata column and must reflect the true, uncapped count
    // (it's computed from globalTotals before the per-dataset cap is applied).
    long totalMultimediaCount = result.select("totalMultimediaCount")
        .distinct()
        .collectAsList()
        .get(0)
        .getLong(0);
    assertEquals(uncappedTotal, totalMultimediaCount,
        "totalMultimediaCount must remain the full uncapped count, unaffected by the per-dataset cap");
  }

  @Test
  void mediaInfosCarryDatasetKeyThroughAvro() throws Exception {
    Dataset<Row> occurrence = buildOccurrenceDataset(1, 3);
    Dataset<Row> multimedia = buildMultimediaDataset("ds-inat", 1, 3);
    occurrence.write().mode("overwrite").saveAsTable(CATALOG + ".occurrence");
    multimedia.write().mode("overwrite").saveAsTable(CATALOG + ".occurrence_multimedia");

    AppConfig.Config config = new AppConfig.Config();
    config.setSourceCatalog(CATALOG);

    List<Row> rows = OccurrenceSpecieMultiMediaTableBuilder
        .createSpeciesMediaDataset(spark, CATALOG, 4, config)
        .collectAsList();
    assertEquals(1, rows.size());
    List<Row> mediaInfos = rows.get(0).getList(rows.get(0).fieldIndex("mediaInfos"));
    assertFalse(mediaInfos.isEmpty());

    byte[] avro = Snappy.uncompress(MediaInfosAvroSerializer.serializeMediaInfosAvro(mediaInfos));
    GenericData.Array<GenericRecord> decoded = new GenericDatumReader<GenericData.Array<GenericRecord>>(
        MediaInfosAvroSerializer.MEDIA_INFO_ARRAY_SCHEMA)
        .read(null, DecoderFactory.get().binaryDecoder(avro, null));

    assertEquals(mediaInfos.size(), decoded.size());
    for (GenericRecord rec : decoded) {
      assertEquals("ds-inat", rec.get("datasetKey").toString());
    }
  }
}
