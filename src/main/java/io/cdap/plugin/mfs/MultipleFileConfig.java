package io.cdap.plugin.mfs;

import io.cdap.cdap.api.annotation.Macro;
import io.cdap.cdap.api.annotation.Name;
import io.cdap.cdap.api.plugin.PluginConfig;

import javax.annotation.Nullable;

/**
 * Class description here.
 */
public class MultipleFileConfig extends PluginConfig {
  @Name("prefix")
  @Macro
  private String prefix;

  @Name("path")
  @Macro
  private String path;

  @Name("type")
  @Macro
  @Nullable
  private String type;

  @Name("format")
  @Macro
  @Nullable
  private String format;

  @Name("filters")
  @Macro
  @Nullable
  private String filters;

  @Name("filters-type")
  @Macro
  @Nullable
  private String filtersType;

  public MultipleFileConfig(String prefix, String path, String type, String format, String filters, String filtersType) {
    this.prefix = prefix;
    this.path = path;
    this.type = type;
    this.format = format;
    this.filtersType = filtersType;
    this.filters = filters;
  }

  public String getPrefix() {
    return prefix;
  }

  public String getPath() {
    return path;
  }

  public String getType() {
    if (type == null) {
      return "Text";
    }
    return type;
  }

  public String getFormat() {
    if (format == null) {
      return "CSV";
    }
    return format;
  }

  public String getFiltersType() {
    if (filtersType == null) {
      return "Expression";
    }
    return filtersType;
  }

  public String[] getFiltersAsList() {
    return getFilters().split(",");
  }

  public String getFilters() {
    if (filters == null) {
      filters = "None";
    }
    return filters;
  }
}
