package co.cask.hydrator.plugin;

import co.cask.cdap.api.annotation.Description;
import co.cask.cdap.api.annotation.Macro;
import co.cask.cdap.api.annotation.Name;
import co.cask.cdap.api.annotation.Plugin;
import co.cask.cdap.api.data.batch.Output;
import co.cask.cdap.api.data.batch.OutputFormatProvider;
import co.cask.cdap.api.data.format.StructuredRecord;
import co.cask.cdap.api.data.schema.Schema;
import co.cask.cdap.api.data.schema.UnsupportedTypeException;
import co.cask.cdap.api.dataset.DatasetProperties;
import co.cask.cdap.api.dataset.lib.FileSetProperties;
import co.cask.cdap.api.dataset.lib.KeyValue;
import co.cask.cdap.api.dataset.lib.PartitionedFileSet;
import co.cask.cdap.etl.api.Emitter;
import co.cask.cdap.etl.api.batch.BatchRuntimeContext;
import co.cask.cdap.etl.api.batch.BatchSink;
import co.cask.cdap.etl.api.batch.BatchSinkContext;
import co.cask.hydrator.common.HiveSchemaConverter;
import co.cask.hydrator.common.batch.JobUtils;
import co.cask.hydrator.plugin.common.FileSetUtil;
import co.cask.hydrator.plugin.common.StructuredToAvroTransformer;
import co.cask.hydrator.plugin.dataset.SnapshotFileSet;
import org.apache.avro.SchemaParseException;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.mapreduce.AvroKeyInputFormat;
import org.apache.avro.mapreduce.AvroKeyOutputFormat;
import org.apache.hadoop.mapreduce.Job;
import parquet.avro.AvroParquetInputFormat;
import parquet.avro.AvroParquetOutputFormat;
import org.apache.hadoop.conf.Configuration;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import javax.annotation.Nullable;


/**
 * {@link SnapshotFileBatchSink} that stores data in Parquet format.
 */
@Plugin(type = BatchSink.PLUGIN_TYPE)
@Name("MultipleFilesetSink")
@Description("Sink for a MultiFileSet that writes data in Parquet format and multiple fileset")
public class MultipleFilesetSink extends SnapshotFileBatchSink<Void, GenericRecord> {
  private final MultipleFilesetSink.MultipleFilesetSinkConfig config;

  private StructuredToAvroTransformer recordTransformer;

  public MultipleFilesetSink (MultipleFilesetSinkConfig config) {
    super(config);
    this.config = config;
  }

  @Override
  public void prepareRun(BatchSinkContext context) throws Exception {

    super.prepareRunAddition(context);
    Job job;
    ClassLoader oldClassLoader = Thread.currentThread().getContextClassLoader();
    // Switch the context classloader to plugin class' classloader (PluginClassLoader) so that
    // when Job/Configuration is created, it uses PluginClassLoader to load resources (hbase-default.xml)
    // which is present in the plugin jar and is not visible in the CombineClassLoader (which is what oldClassLoader
    // points to).
    Thread.currentThread().setContextClassLoader(getClass().getClassLoader());
    try {
      job = JobUtils.createInstance();
    } finally {
      // Switch back to the original
      Thread.currentThread().setContextClassLoader(oldClassLoader);
    }

    job.setOutputFormatClass(MultipleFilesetOutputFormat.class);
    Configuration conf = job.getConfiguration();

    context.addOutput(Output.of(config.getName(), new MultipleFilesetOutputFormatProvider(config, conf)));
  }

  @Override
  public void initialize(BatchRuntimeContext context) throws Exception {
    super.initialize(context);
    recordTransformer = new StructuredToAvroTransformer(config.schema, null);
  }

  @Override
  public void transform(StructuredRecord input,
                        Emitter<KeyValue<Void, GenericRecord>> emitter) throws Exception {
    emitter.emit(new KeyValue<Void, GenericRecord>(null, recordTransformer.transform(input)));
  }

  @Override
  protected void addFileProperties(FileSetProperties.Builder propertiesBuilder) {
    String schema = config.schema.toLowerCase();
    // parse to make sure it's valid
    new org.apache.avro.Schema.Parser().parse(schema);
    String hiveSchema;
    try {
      hiveSchema = HiveSchemaConverter.toHiveSchema(Schema.parseJson(schema));
    } catch (UnsupportedTypeException | IOException e) {
      throw new RuntimeException("Error: Schema is not valid ", e);
    }
    propertiesBuilder.addAll(FileSetUtil.getParquetCompressionConfiguration(config.compressionCodec, config.schema,
                                                                            true));

    propertiesBuilder
      .setInputFormat(AvroParquetInputFormat.class)
      .setOutputFormat(AvroParquetOutputFormat.class)
      .setEnableExploreOnCreate(true)
      .setExploreFormat("parquet")
      .setExploreSchema(hiveSchema.substring(1, hiveSchema.length() - 1))
      .add(DatasetProperties.SCHEMA, schema);
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
      return MultipleFilesetOutputFormat.class.getName();
    }

    @Override
    public Map<String, String> getOutputFormatConfiguration() {
      return conf;
    }
  }

}