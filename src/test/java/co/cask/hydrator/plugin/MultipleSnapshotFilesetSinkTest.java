/*
 * Copyright © 2016 Cask Data, Inc.
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

import co.cask.cdap.api.artifact.ArtifactSummary;
import co.cask.cdap.api.common.Bytes;
import co.cask.cdap.api.data.format.StructuredRecord;
import co.cask.cdap.api.data.schema.Schema;
import co.cask.cdap.api.dataset.lib.KeyValueTable;
import co.cask.cdap.api.dataset.table.Table;
import co.cask.cdap.datapipeline.DataPipelineApp;
import co.cask.cdap.datapipeline.SmartWorkflow;
import co.cask.cdap.etl.api.batch.BatchSink;
import co.cask.cdap.etl.api.batch.BatchSource;
import co.cask.cdap.etl.mock.batch.MockSink;
import co.cask.cdap.etl.mock.batch.MockSource;
import co.cask.cdap.etl.mock.test.HydratorTestBase;
import co.cask.cdap.etl.proto.v2.ETLBatchConfig;
import co.cask.cdap.etl.proto.v2.ETLPlugin;
import co.cask.cdap.etl.proto.v2.ETLStage;
import co.cask.cdap.proto.ProgramRunStatus;
import co.cask.cdap.proto.artifact.AppRequest;
import co.cask.cdap.proto.id.ApplicationId;
import co.cask.cdap.proto.id.ArtifactId;
import co.cask.cdap.proto.id.NamespaceId;
import co.cask.cdap.test.ApplicationManager;
import co.cask.cdap.test.DataSetManager;
import co.cask.cdap.test.TestConfiguration;
import co.cask.cdap.test.WorkflowManager;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import kafka.common.TopicAndPartition;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

/**
 * Unit tests for our plugins.
 */
public class MultipleSnapshotFilesetSinkTest extends HydratorTestBase {
  private static final ArtifactSummary APP_ARTIFACT = new ArtifactSummary("multiple-snapshot-artifact", "1.0.0");
  @ClassRule
  public static final TestConfiguration CONFIG = new TestConfiguration("explore.enabled", false);

  @BeforeClass
  public static void setupTestClass() throws Exception {
    ArtifactId parentArtifact = NamespaceId.DEFAULT.artifact(APP_ARTIFACT.getName(), APP_ARTIFACT.getVersion());

    // add the data-pipeline artifact and mock plugins
    setupBatchArtifacts(parentArtifact, DataPipelineApp.class);

    // add our plugins artifact with the data-pipeline artifact as its parent.
    // this will make our plugins available to data-pipeline.
    addPluginArtifact(NamespaceId.DEFAULT.artifact("example-plugins", "1.0.0"),
                      parentArtifact,
                      MultipleSnapshotFilesetSink.class);

  }

  @Test
  public void testMultipleSnapshot() throws Exception {
    DataSetManager<Table> inputManager = getDataset("inputParquet");
    Schema recordSchema = Schema.recordOf("record",
                                           Schema.Field.of("id", Schema.of(Schema.Type.STRING)),
                                           Schema.Field.of("first_name", Schema.of(Schema.Type.STRING)),
                                           Schema.Field.of("last_name", Schema.of(Schema.Type.STRING)),
                                           Schema.Field.of("sex", Schema.of(Schema.Type.STRING)),
                                           Schema.Field.of("address", Schema.of(Schema.Type.STRING)),
                                           Schema.Field.of("salary", Schema.of(Schema.Type.DOUBLE)));
    // write input data
    List<StructuredRecord> input = ImmutableList.of(
      StructuredRecord.builder(recordSchema)
        .set("id", "201EMPLPIM")
        .set("first_name", "Sean")
        .set("last_name", "Froula")
        .set("sex", "M")
        .set("address", "2105 8th St, Uhome, WY")
        .set("salary", 7000)
        .build()
    );
    MockSource.writeInput(inputManager, input);



    // create the pipeline config
    String inputName = "sourceTestInput";
    String outputName = "sourceTestOutput";

    Map<String, String> sourceProperties = new HashMap<>();

    sourceProperties.put("referenceName", "MultipleFilesetTest");
    sourceProperties.put("topic", "users");
    sourceProperties.put("format", "csv");

    ETLStage mockSource= new ETLStage("source", MockSource.getPlugin(outputName));

    Map<String, String> properties = ImmutableMap.of("schema", recordSchema.toString(),
                                 "name", "outputParquet");
    ETLStage sink =
      new ETLStage("sink", new ETLPlugin("MultipleSnapshotFilesetSink",
                                         BatchSink.PLUGIN_TYPE, properties, null));

    ETLStage mockink = new ETLStage("sink", MockSink.getPlugin(outputName));

    ETLBatchConfig pipelineConfig = ETLBatchConfig.builder("* * * * *")
      .addStage(mockSource)
      .addStage(sink)
      .addConnection("MockSource", sink.getName())
      .build();

    // create the pipeline
    ApplicationId pipelineId = NamespaceId.DEFAULT.app("testMultipleFilesetSink");
    ApplicationManager appManager = deployApplication(pipelineId, new AppRequest<>(APP_ARTIFACT, pipelineConfig));



    WorkflowManager workflowManager = appManager.getWorkflowManager(SmartWorkflow.NAME);
    workflowManager.start();
    workflowManager.waitForRun(ProgramRunStatus.COMPLETED, 1, TimeUnit.MINUTES);

    // check the pipeline output
    DataSetManager<Table> outputManager = getDataset(outputName);
    Set<StructuredRecord> outputRecords = new HashSet<>();
    outputRecords.addAll(MockSink.readOutput(outputManager));

  }


}