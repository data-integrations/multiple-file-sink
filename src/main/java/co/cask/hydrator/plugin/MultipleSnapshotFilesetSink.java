/*
 * Copyright © 2017 Cask Data, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package co.cask.hydrator.plugin;

import co.cask.cdap.api.annotation.Description;
import co.cask.cdap.api.annotation.Macro;
import co.cask.cdap.api.annotation.Name;
import co.cask.cdap.api.annotation.Plugin;
import co.cask.cdap.api.data.format.StructuredRecord;
import co.cask.cdap.api.dataset.DatasetManagementException;
import co.cask.cdap.api.dataset.DatasetProperties;
import co.cask.cdap.api.dataset.lib.FileSetProperties;
import co.cask.cdap.api.dataset.lib.KeyValue;
import co.cask.cdap.etl.api.Emitter;
import co.cask.cdap.etl.api.batch.BatchRuntimeContext;
import co.cask.cdap.etl.api.batch.BatchSink;
import co.cask.cdap.etl.api.batch.BatchSinkContext;
import co.cask.hydrator.plugin.common.FileSetUtil;
import co.cask.hydrator.plugin.common.StructuredToAvroTransformer;
import co.cask.hydrator.plugin.model.MultipleFileSets;
import co.cask.hydrator.plugin.model.OutputFileSet;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import org.apache.avro.Schema;
import org.apache.avro.SchemaParseException;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.mapred.AvroKey;
import org.apache.avro.mapreduce.AvroKeyInputFormat;
import org.apache.avro.mapreduce.AvroKeyOutputFormat;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.NullWritable;

import java.io.IOException;
import javax.annotation.Nullable;

import java.util.List;
import java.util.Map;
import java.util.HashMap;

import co.cask.cdap.api.data.batch.OutputFormatProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * {@link CustomizedSnapshotFileBatchSink} that stores data in Avro format.
 */
@Plugin(type =  BatchSink.PLUGIN_TYPE)
@Name("MultipleSnapshotFileset")
@Description("Sink for a  Multiple SnapshotFileSets that writes data in Avro format.")
public class MultipleSnapshotFilesetSink extends CustomizedSnapshotFileBatchSink<AvroKey<GenericRecord>, NullWritable> {
  private StructuredToAvroTransformer recordTransformer;
  private final MultipleSnapshotFilesetSinkConfig config;
  private static final Logger LOG = LoggerFactory.getLogger(MultipleSnapshotFilesetSink.class);

  public MultipleSnapshotFilesetSink(MultipleSnapshotFilesetSinkConfig config) {
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
                        Emitter<KeyValue<AvroKey<GenericRecord>, NullWritable>> emitter) throws Exception {
    emitter.emit(new KeyValue<>(new AvroKey<>(recordTransformer.transform(input)), NullWritable.get()));
  }

  @Override
  protected void addFileProperties(FileSetProperties.Builder propertiesBuilder, String schema) {
    // parse it to make sure its valid
    try {
      new Schema.Parser().parse(schema);
    } catch (SchemaParseException e) {
      throw new IllegalArgumentException("Could not parse schema: " + e.getMessage(), e);
    }
    propertiesBuilder.addAll(FileSetUtil.getAvroCompressionConfiguration(config.compressionCodec, schema,
                                                                         true));
//    LOG.info("MultipleSnapshotFilesetSink.addFileProperties:");
//    LOG.info("MultipleSnapshotFilesetSink.addFileProperties:"+config.schema);
    propertiesBuilder
      .setInputFormat(AvroKeyInputFormat.class)
      .setOutputFormat(AvroKeyOutputFormat.class)
      .setEnableExploreOnCreate(true)
      .setSerDe("org.apache.hadoop.hive.serde2.avro.AvroSerDe")
      .setExploreInputFormat("org.apache.hadoop.hive.ql.io.avro.AvroContainerInputFormat")
      .setExploreOutputFormat("org.apache.hadoop.hive.ql.io.avro.AvroContainerOutputFormat")
      .setTableProperty("avro.schema.literal", schema)
      .add(DatasetProperties.SCHEMA, schema);
  }

  /**
   * Config for SnapshotFileBatchAvroSink
   */
  public static class MultipleSnapshotFilesetSinkConfig extends CustomizedSnapshotFileSetBatchSinkConfig {
    @Description("The Avro schema of the record being written to the Sink as a JSON Object.")
    @Macro
    private String schema;

    @Nullable
    @Description("Used to specify the compression codec to be used for the final dataset.")
    private String compressionCodec;

    public MultipleSnapshotFilesetSinkConfig(String schema, @Nullable String compressionCodec) {
      //super(null);
//      this.schema = schema;
      this.compressionCodec = compressionCodec;
    }

//    @Override
//    public void validate() {
//      super.validate();
//      try {
//        if (schema != null) {
//          co.cask.cdap.api.data.schema.Schema.parseJson(schema);
//        }
//      } catch (IOException e) {
//        throw new IllegalArgumentException("Unable to parse schema: " + e.getMessage());
//      }
//    }
  }

//   private class MultipleSnapshotFilesetSinkOutputFormatProvider implements OutputFormatProvider {
//
//    private final Map<String, String> conf;
//
//     MultipleSnapshotFilesetSinkOutputFormatProvider(MultipleSnapshotFilesetSinkConfig config, Configuration configuration) {
//      this.conf = new HashMap<>();
//      conf.put("key", "value");
//    }
//
//    @Override
//    public String getOutputFormatClassName() {
////      return MultipleSnapshotFilesetSinkOutputFormat.class.getName();
//      return null;
//    }
//
//    @Override
//    public Map<String, String> getOutputFormatConfiguration() {
//      return conf;
//    }
//  }
}
