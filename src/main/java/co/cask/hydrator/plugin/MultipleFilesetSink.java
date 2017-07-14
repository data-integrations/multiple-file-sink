package co.cask.hydrator.plugin;

import co.cask.cdap.api.annotation.Description;
import co.cask.cdap.api.annotation.Macro;
import co.cask.cdap.api.annotation.Name;
import co.cask.cdap.api.annotation.Plugin;
import co.cask.cdap.api.data.batch.OutputFormatProvider;
import co.cask.cdap.api.data.format.StructuredRecord;
import co.cask.cdap.api.data.schema.Schema;
import co.cask.cdap.api.data.schema.UnsupportedTypeException;
import co.cask.cdap.api.dataset.DatasetProperties;
import co.cask.cdap.api.dataset.lib.FileSetProperties;
import co.cask.cdap.api.dataset.lib.KeyValue;
import co.cask.cdap.etl.api.Emitter;
import co.cask.cdap.etl.api.batch.BatchRuntimeContext;
import co.cask.hydrator.common.HiveSchemaConverter;
import co.cask.hydrator.plugin.common.FileSetUtil;
import co.cask.hydrator.plugin.common.StructuredToAvroTransformer;
import org.apache.avro.SchemaParseException;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.mapreduce.AvroKeyInputFormat;
import org.apache.avro.mapreduce.AvroKeyOutputFormat;
import parquet.avro.AvroParquetInputFormat;
import parquet.avro.AvroParquetOutputFormat;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import javax.annotation.Nullable;


/**
 * {@link SnapshotFileBatchSink} that stores data in Parquet format.
 */
@Plugin(type = "batchsink")
@Name("SnapshotParquet")
@Description("Sink for a SnapshotFileSet that writes data in Parquet format.")
public class MultipleFilesetSink extends SnapshotFileBatchSink<Void, GenericRecord> {
  private final MultipleFilesetSink.MultipleFilesetSinkConfig config;

  private StructuredToAvroTransformer recordTransformer;

  public MultipleFilesetSink (MultipleFilesetSinkConfig config) {
    super(config);
    this.config = config;
  }

  @Override
  public void initialize(BatchRuntimeContext context) throws Exception {
    super.initialize(context);
    recordTransformer = new StructuredToAvroTransformer(config.schema);
  }

  @Override
  public void transform(StructuredRecord input,
                        Emitter<KeyValue<Void, GenericRecord>> emitter) throws Exception {
    emitter.emit(new KeyValue<Void, GenericRecord>(null, recordTransformer.transform(input)));
  }

  @Override
  protected void addFileProperties(FileSetProperties.Builder propertiesBuilder) {
    // parse it to make sure its valid
    try {
      new org.apache.avro.Schema.Parser().parse(config.schema);
    } catch (SchemaParseException e) {
      throw new IllegalArgumentException("Could not parse schema: " + e.getMessage(), e);
    }
    propertiesBuilder.addAll(FileSetUtil.getAvroCompressionConfiguration(config.compressionCodec, config.schema,
                                                                         true));

    propertiesBuilder
      .setInputFormat(AvroKeyInputFormat.class)
      .setOutputFormat(AvroKeyOutputFormat.class)
      .setEnableExploreOnCreate(true)
      .setSerDe("org.apache.hadoop.hive.serde2.avro.AvroSerDe")
      .setExploreInputFormat("org.apache.hadoop.hive.ql.io.avro.AvroContainerInputFormat")
      .setExploreOutputFormat("org.apache.hadoop.hive.ql.io.avro.AvroContainerOutputFormat")
      .setTableProperty("avro.schema.literal", config.schema)
      .add(DatasetProperties.SCHEMA, config.schema);
  }

  /**
   * Config for SnapshotFileBatchAvroSink
   */
  public static class MultipleFilesetSinkConfig extends SnapshotFileBatchSink.SnapshotFileSetBatchSinkConfig {
    @Description("The Avro schema of the record being written to the Sink as a JSON Object.")
    @Macro
    private String schema;

    @Nullable
    @Description("Used to specify the compression codec to be used for the final dataset.")
    private String compressionCodec;

    public MultipleFilesetSinkConfig(String name, @Nullable String basePath, String schema,
                              @Nullable String compressionCodec) {
      super(name, basePath, null);
      this.schema = schema;
      this.compressionCodec = compressionCodec;
    }

    @Override
    public void validate() {
      super.validate();
      try {
        if (schema != null) {
          co.cask.cdap.api.data.schema.Schema.parseJson(schema);
        }
      } catch (IOException e) {
        throw new IllegalArgumentException("Unable to parse schema: " + e.getMessage());
      }
    }
  }

  private class MultipleFilesetOutputFormatProvider implements OutputFormatProvider {

    private final Map<String, String> conf;

    MultipleFilesetOutputFormatProvider(MultipleFilesetSinkConfig config, Configuration configuration) {
      this.conf = new HashMap<>();
      conf.put("key", "value");

    }

    @Override
    public String getOutputFormatClassName() {
      return "";
    }

    @Override
    public Map<String, String> getOutputFormatConfiguration() {
      return conf;
    }
  }

}