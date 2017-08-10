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
import co.cask.cdap.api.data.batch.Output;
import co.cask.cdap.api.data.format.StructuredRecord;
import co.cask.cdap.api.data.schema.Schema;
import co.cask.cdap.api.dataset.DatasetManagementException;
import co.cask.cdap.api.dataset.lib.FileSetProperties;
import co.cask.cdap.api.dataset.lib.PartitionedFileSet;
import co.cask.cdap.etl.api.PipelineConfigurer;
import co.cask.cdap.etl.api.batch.BatchSink;
import co.cask.cdap.etl.api.batch.BatchSinkContext;
import co.cask.hydrator.common.TimeParser;
import co.cask.hydrator.plugin.common.SnapshotFileSetConfig;
import co.cask.hydrator.plugin.dataset.SnapshotFileSet;
import co.cask.hydrator.plugin.model.MultipleFileSets;
import co.cask.hydrator.plugin.model.OutputFileSet;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.reflect.TypeToken;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.lang.reflect.Type;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import javax.annotation.Nullable;

/**
 * Sink that stores snapshots on HDFS, and keeps track of which snapshot is the latest snapshot.
 *
 * @param <KEY_OUT> the type of key the sink outputs
 * @param <VAL_OUT> the type of value the sink outputs
 */
public abstract class CustomizedSnapshotFileBatchSink<KEY_OUT, VAL_OUT> extends BatchSink<StructuredRecord, KEY_OUT, VAL_OUT> {
  private static final Logger LOG = LoggerFactory.getLogger(CustomizedSnapshotFileBatchSink.class);
  private static final Gson GSON = new Gson();
  private static final Type MAP_TYPE = new TypeToken<Map<String, String>>() { }.getType();
  private static Gson gson;
  private List<Schema> schemaList;

  private final CustomizedSnapshotFileSetBatchSinkConfig config;
  private CustomizedSnapshotFileSet customizedSnapshotFileSet;

  public CustomizedSnapshotFileBatchSink(CustomizedSnapshotFileSetBatchSinkConfig config) {
    this.config = config;
  }

  @Override
  public void configurePipeline(PipelineConfigurer pipelineConfigurer) {
    super.configurePipeline(pipelineConfigurer);
    Schema inputSchema = pipelineConfigurer.getStageConfigurer().getInputSchema();
  }

  @Override
  public void prepareRun(BatchSinkContext context) throws DatasetManagementException, IOException {

    gson = new GsonBuilder().setPrettyPrinting().create();
    MultipleFileSets multipleFileSets= gson.fromJson(config.getFileProperties(), MultipleFileSets.class);

    // if macros were provided, the dataset still needs to be created
    //config.validate();
    for(OutputFileSet outputFileSet : multipleFileSets.getOutputFileSets()){
      if (!context.datasetExists(outputFileSet.getDatasetName())) {
        //FileSetProperties.Builder fileProperties = CustomizedSnapshotFileSet.getBaseProperties(config);
        FileSetProperties.Builder fileProperties = CustomizedSnapshotFileSet.getBaseProperties(outputFileSet);
        addFileProperties(fileProperties, Schema.parseJson(outputFileSet.getSchema().toString()).toString());
        schemaList.add(Schema.parseJson(outputFileSet.getSchema().toString()));
        context.createDataset(outputFileSet.getDatasetName(),
                              PartitionedFileSet.class.getName(), fileProperties.build());
      }
      PartitionedFileSet files = context.getDataset(outputFileSet.getDatasetName());
      customizedSnapshotFileSet = new CustomizedSnapshotFileSet(files);

      Map<String, String> arguments = new HashMap<>();

      if (outputFileSet.getFilesetProperties() != null) {
        arguments = GSON.fromJson(outputFileSet.getFilesetProperties(), MAP_TYPE);
      }
      context.addOutput(Output.ofDataset(outputFileSet.getDatasetName(), customizedSnapshotFileSet.
        getOutputArguments(context.getLogicalStartTime(), arguments)));
    }

    config.getFileProperties();
  }

  /**
   * add all fileset properties specific to the type of sink, such as schema and output format.
   */
  protected abstract void addFileProperties(FileSetProperties.Builder propertiesBuilder, String schema);

  /**
   * Config for CustomizedSnapshotFileBatchSink
   */
  public static class CustomizedSnapshotFileSetBatchSinkConfig extends CustomizedSnapshotFileSetConfig {
//    @Description("Optional property that configures the sink to delete old partitions after successful runs. " +
//      "If set, when a run successfully finishes, the sink will subtract this amount of time from the runtime and " +
//      "delete any partitions older than that time. " +
//      "The format is expected to be a number followed by an 's', 'm', 'h', or 'd' specifying the time unit, with 's' " +
//      "for seconds, 'm' for minutes, 'h' for hours, and 'd' for days. For example, if the pipeline is scheduled to " +
//      "run at midnight of January 1, 2016, and this property is set to 7d, the sink will delete any partitions " +
//      "for time partitions older than midnight Dec 25, 2015.")
//    @Nullable
//    @Macro
//    protected String cleanPartitionsOlderThan;
//
//    public CustomizedSnapshotFileSetBatchSinkConfig() {
//
//    }
//
//    public CustomizedSnapshotFileSetBatchSinkConfig(
//                                          @Nullable String cleanPartitionsOlderThan) {
////      super(null);
//      this.cleanPartitionsOlderThan = cleanPartitionsOlderThan;
//    }
//
//    public String getCleanPartitionsOlderThan() {
//      return cleanPartitionsOlderThan;
//    }
//
//    public void validate() {
//      if (cleanPartitionsOlderThan != null) {
//        TimeParser.parseDuration(cleanPartitionsOlderThan);
//      }
//    }


  }
}