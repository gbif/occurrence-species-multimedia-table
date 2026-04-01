package org.gbif.multimedia;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.List;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericData.Array;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.EncoderFactory;
import org.apache.spark.sql.Row;
import org.xerial.snappy.Snappy;

public class MediaInfosAvroSerializer {

  // Define schema once (can also load from a .avsc file)
  private static final String MEDIA_INFO_SCHEMA_JSON = """
    {
      "type": "record",
      "name": "MediaInfo",
      "fields": [
        {"name": "identifier",   "type": ["null", "string"], "default": null},
        {"name": "title",        "type": ["null", "string"], "default": null},
        {"name": "occurrenceKey","type": ["null", "string"], "default": null},
        {"name": "rightsHolder", "type": ["null", "string"], "default": null},
        {"name": "license",      "type": ["null", "string"], "default": null}
      ]
    }
    """;

  private static final String MEDIA_INFO_ARRAY_SCHEMA_JSON =
      "{\"type\": \"array\", \"items\": " + MEDIA_INFO_SCHEMA_JSON + "}";

  private static final Schema MEDIA_INFO_SCHEMA = new Schema.Parser().parse(MEDIA_INFO_SCHEMA_JSON);
  private static final Schema MEDIA_INFO_ARRAY_SCHEMA = new Schema.Parser().parse(MEDIA_INFO_ARRAY_SCHEMA_JSON);

  // Writer — reuse encoder across calls within a partition
  public static byte[] serializeMediaInfosAvro(List<Row> mediaInfos) throws IOException {
    GenericData.Array<GenericRecord> array =
        new GenericData.Array<>(mediaInfos.size(), MEDIA_INFO_ARRAY_SCHEMA);

    for (Row row : mediaInfos) {
      GenericRecord rec = new GenericData.Record(MEDIA_INFO_SCHEMA);
      rec.put("identifier",   row.getAs("identifier"));
      rec.put("title",        row.getAs("title"));
      rec.put("occurrenceKey", row.getAs("gbifid") != null ? row.getAs("gbifid").toString() : null);
      rec.put("rightsHolder", row.getAs("rightsholder"));
      rec.put("license",      row.getAs("license"));
      array.add(rec);
    }

    ByteArrayOutputStream baos = new ByteArrayOutputStream(mediaInfos.size() * 100);
    GenericDatumWriter<Array<GenericRecord>> writer =
        new GenericDatumWriter<>(MEDIA_INFO_ARRAY_SCHEMA);
    BinaryEncoder encoder = EncoderFactory.get().binaryEncoder(baos, null);
    writer.write(array, encoder);
    encoder.flush();
    return Snappy.compress(baos.toByteArray());
  }
}
