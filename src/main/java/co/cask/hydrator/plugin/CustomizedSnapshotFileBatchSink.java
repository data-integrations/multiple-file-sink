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

import co.cask.cdap.api.data.batch.Output;
import co.cask.cdap.api.data.format.StructuredRecord;
import co.cask.cdap.api.data.schema.Schema;
import co.cask.cdap.api.dataset.DatasetManagementException;
import co.cask.cdap.api.dataset.lib.FileSetProperties;
import co.cask.cdap.etl.api.PipelineConfigurer;
import co.cask.cdap.etl.api.batch.BatchSink;
import co.cask.cdap.etl.api.batch.BatchSinkContext;
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
import java.util.Map;

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

    for(OutputFileSet outputFileSet : multipleFileSets.getOutputFileSets()) {
      if (!context.datasetExists(outputFileSet.getDatasetName())) {
        //FileSetProperties.Builder fileProperties = CustomizedSnapshotFileSet.getBaseProperties(config);
        FileSetProperties.Builder fileProperties = CustomizedSnapshotFileSet.getBaseProperties(outputFileSet);
        Schema cdapSchema = Schema.parseJson(outputFileSet.getSchema().toString());
        //addFileProperties(fileProperties, cdapSchema.toString());
        addFileProperties(fileProperties, null);
        Map<String, String> arguments = new HashMap<>();
        if (config.getFileProperties() != null) {
          arguments = GSON.fromJson(config.getFileProperties(), MAP_TYPE);
        }
        context.addOutput(Output.ofDataset(
          outputFileSet.getDatasetName(),
          customizedSnapshotFileSet.getOutputArguments(context.getLogicalStartTime(), arguments)));

      }


    }
  }

  /**
   * add all fileset properties specific to the type of sink, such as schema and output format.
   */
  protected abstract void addFileProperties(FileSetProperties.Builder propertiesBuilder, String schema);

  /**
   * Config for CustomizedSnapshotFileBatchSink
   */
  public static class CustomizedSnapshotFileSetBatchSinkConfig extends CustomizedSnapshotFileSetConfig {


  }


}