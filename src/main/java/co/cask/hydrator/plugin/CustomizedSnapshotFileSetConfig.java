/*
 * Copyright © 2015 Cask Data, Inc.
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
import co.cask.cdap.api.plugin.PluginConfig;
import co.cask.hydrator.plugin.common.Properties;

import javax.annotation.Nullable;

/**
 * {@link PluginConfig} for snapshot fileset sources and sinks.
 */
public abstract class CustomizedSnapshotFileSetConfig extends PluginConfig {

  @Name(Properties.SnapshotFileSetSink.FILE_PROPERTIES)
  @Nullable
  @Description("Advanced feature to specify any additional properties that should be used with the sink, " +
    "specified as a JSON object of string to string. These properties are set on the dataset if one is created. " +
    "The properties are also passed to the dataset at runtime as arguments.")
  @Macro
  protected String fileProperties;

  public CustomizedSnapshotFileSetConfig() {

  }

  public CustomizedSnapshotFileSetConfig(@Nullable String fileProperties) {
    this.fileProperties = fileProperties;
  }

  @Nullable
  public String getFileProperties() {return fileProperties;
  }
}
