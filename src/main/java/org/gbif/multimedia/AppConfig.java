package org.gbif.multimedia;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.functions;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.util.Collections;
import java.util.List;

/**
 * Application configuration loaded from a YAML file.
 * <p>
 * Contains all settings for the Occurrence Species Multimedia Table Builder
 * except the runtime timestamp, which may be provided as a CLI argument.
 */
@Slf4j
public class AppConfig {

  private static final String DEFAULT_RESOURCE = "application.yaml";
  private static final ObjectMapper YAML_MAPPER = new ObjectMapper(new YAMLFactory());

  @Data
  @JsonIgnoreProperties(ignoreUnknown = true)
  public static class HBaseConfig {
    /** Base HBase table name; the runtime timestamp is appended as {@code <table>_<timestamp>}. */
    private String table = "occurrence_species_multimedia";
    /** Number of salt buckets (pre-split regions). */
    private int saltBuckets = 64;
    /** Zookeeper quorum (comma-separated hostnames). */
    private String zookeeperQuorum;
    /** Zookeeper znode parent path for HBase. */
    private String znodeParent;
  }

  @Data
  @JsonIgnoreProperties(ignoreUnknown = true)
  public static class Config {
    /** Source catalog for Iceberg tables (e.g. "iceberg.prod"). */
    private String sourceCatalog;
    /** HBase connection and table settings. */
    private HBaseConfig hbase = new HBaseConfig();
    /** Zookeeper path used by the metastore to track the current active table. */
    private String zkMetastorePath;
    /** Regex patterns for identifier URLs of known offline/inaccessible media. */
    private List<String> offlineUrlPatterns = Collections.emptyList();
    /** Dataset keys that should be prioritized first, in exact configured order. */
    private List<String> datasetKeyPriorityOrder = Collections.emptyList();
  }

  /**
   * Loads configuration from the default classpath resource ({@value DEFAULT_RESOURCE}).
   *
   * @return the loaded Config
   * @throws RuntimeException if the resource is missing or cannot be parsed
   */
  public static Config load() {
    return load(DEFAULT_RESOURCE);
  }

  /**
   * Loads configuration from the given classpath resource.
   *
   * @param resource classpath resource path
   * @return the loaded Config
   * @throws RuntimeException if the resource is missing or cannot be parsed
   */
  public static Config load(String resource) {
    try (InputStream is = AppConfig.class.getClassLoader().getResourceAsStream(resource)) {
      if (is == null) {
        throw new RuntimeException("Configuration resource '" + resource + "' not found on classpath");
      }
      Config config = YAML_MAPPER.readValue(is, Config.class);
      log.info("Loaded configuration from '{}': sourceCatalog={}, hbase.table={}, saltBuckets={}, offlineUrlPatterns={}, datasetKeyPriorityOrder={}",
          resource,
          config.getSourceCatalog(),
          config.getHbase().getTable(),
          config.getHbase().getSaltBuckets(),
          config.getOfflineUrlPatterns().size(),
          config.getDatasetKeyPriorityOrder().size());
      return config;
    } catch (IOException e) {
      throw new RuntimeException("Failed to load configuration from '" + resource + "'", e);
    }
  }

  /**
   * Loads configuration from a file on the filesystem.
   *
   * @param filePath absolute or relative path to the YAML configuration file
   * @return the loaded Config
   * @throws RuntimeException if the file is missing or cannot be parsed
   */
  public static Config loadFromFile(String filePath) {
    File file = new File(filePath);
    if (!file.exists()) {
      throw new RuntimeException("Configuration file '" + filePath + "' not found");
    }
    try {
      Config config = YAML_MAPPER.readValue(file, Config.class);
      log.info("Loaded configuration from '{}': sourceCatalog={}, hbase.table={}, saltBuckets={}, offlineUrlPatterns={}, datasetKeyPriorityOrder={}",
          filePath,
          config.getSourceCatalog(),
          config.getHbase().getTable(),
          config.getHbase().getSaltBuckets(),
          config.getOfflineUrlPatterns().size(),
          config.getDatasetKeyPriorityOrder().size());
      return config;
    } catch (IOException e) {
      throw new RuntimeException("Failed to load configuration from '" + filePath + "'", e);
    }
  }

  /**
   * Builds a Spark Column expression that evaluates to {@code true} when the given
   * (already-lowercased) identifier column matches any of the configured offline URL patterns.
   * Returns a {@code lit(false)} column if no patterns are configured.
   *
   * @param lowercasedIdCol a Column containing the lowercased identifier
   * @param config          the loaded Config
   * @return a boolean Column expression
   */
  public static Column buildIsOfflineColumn(Column lowercasedIdCol, Config config) {
    List<String> patterns = config.getOfflineUrlPatterns();
    if (patterns == null || patterns.isEmpty()) {
      return functions.lit(false);
    }
    Column result = lowercasedIdCol.rlike(patterns.get(0));
    for (int i = 1; i < patterns.size(); i++) {
      result = result.or(lowercasedIdCol.rlike(patterns.get(i)));
    }
    return result;
  }
}
