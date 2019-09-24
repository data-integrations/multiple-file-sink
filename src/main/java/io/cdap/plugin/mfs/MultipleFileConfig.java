package io.cdap.plugin.mfs;

import com.google.common.base.Strings;
import io.cdap.cdap.api.annotation.Macro;
import io.cdap.cdap.api.annotation.Name;
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.cdap.api.plugin.PluginConfig;
import io.cdap.cdap.etl.api.FailureCollector;
import io.cdap.plugin.mfs.expression.EL;
import io.cdap.plugin.mfs.expression.ELException;

import java.util.stream.Stream;
import javax.annotation.Nullable;

/**
 * Class description here.
 */
public class MultipleFileConfig extends PluginConfig {
  public static final String PROPERTY_OUTPUT_TYPE = "type";
  public static final String PROPERTY_OUTPUT_FORMAT = "format";
  public static final String PROPERTY_FILTERS = "filters";
  public static final String PROPERTY_FILTERS_TYPE = "filtersType";

  @Name("prefix")
  @Macro
  private String prefix;

  @Name("path")
  @Macro
  private String path;

  @Name(PROPERTY_OUTPUT_TYPE)
  @Macro
  @Nullable
  private String type;

  @Name(PROPERTY_OUTPUT_FORMAT)
  @Macro
  @Nullable
  private String format;

  @Name(PROPERTY_FILTERS)
  @Macro
  @Nullable
  private String filters;

  @Name(PROPERTY_FILTERS_TYPE)
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

  private MultipleFileConfig(Builder builder) {
    this.prefix = builder.prefix;
    this.path = builder.path;
    this.type = builder.type;
    this.format = builder.format;
    this.filtersType = builder.filtersType;
    this.filters = builder.filters;
  }

  public String getPrefix() {
    return prefix;
  }

  public String getPath() {
    return path;
  }

  public OutputType getType() {
    return getEnumValueByString(OutputType.class, type, PROPERTY_OUTPUT_TYPE);
  }

  public OutputFormat getFormat() {
    return getEnumValueByString(OutputFormat.class, format, PROPERTY_OUTPUT_FORMAT);
  }

  public FilterType getFiltersType() {
    return getEnumValueByString(FilterType.class, filtersType, PROPERTY_FILTERS_TYPE);
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

  public void validate(FailureCollector failureCollector, Schema inputSchema) {
    // trigger getters, so that they fail if value cannot be converted to enum.
    try {
      getType();
    } catch (IllegalStateException ex) {
      failureCollector.addFailure(ex.getMessage(), null)
        .withConfigProperty(PROPERTY_OUTPUT_TYPE);
    }

    try {
      getFormat();
    } catch (IllegalStateException ex) {
      failureCollector.addFailure(ex.getMessage(), null)
        .withConfigProperty(PROPERTY_OUTPUT_FORMAT);
    }

    try {
      getFiltersType();
    } catch (IllegalStateException ex) {
      failureCollector.addFailure(ex.getMessage(), null)
        .withConfigProperty(PROPERTY_FILTERS_TYPE);
      return; // if invalid filters type further validations does not make sense.
    }

    if (getFiltersType().equals(FilterType.EXPRESSION)) {
      if (Strings.isNullOrEmpty(filters)) {
        failureCollector.addFailure("Filter conditions must be set, when assigment type is \"Expression\"",
                             null).withConfigProperty(PROPERTY_FILTERS);
      } else {
        EL el = new EL(new EL.DefaultFunctions());

        for (String expression : getFiltersAsList()) {
          try {
            el.compile(expression);
          } catch (ELException e) {
            failureCollector.addFailure(String.format("Filter conditions contain an invalid expression. Reason: \"%s\"",
                                               e.getMessage()), null)
              .withConfigProperty(PROPERTY_FILTERS);
          }

          for (String variableName : el.variables()) {
            if (inputSchema.getField(variableName) == null) {
              failureCollector.addFailure(String.format(
                "Filter conditions contain an invalid expression. Variable '%s' in not in the schema",
                variableName), null)
                .withConfigProperty(PROPERTY_FILTERS)
                .withInputSchemaField(variableName);
            }
          }

        }
      }
    } else {
      if (!Strings.isNullOrEmpty(filters)) {
        failureCollector.addFailure(String.format("Filter conditions must not be set, when assigment type is \"%s\"",
                                           filtersType), null).withConfigProperty(PROPERTY_FILTERS);
      }
    }
  }

  public static <T extends EnumWithValue> T getEnumValueByString(Class<T> enumClass, String stringValue,
                                                                 String propertyName) {
    return Stream.of(enumClass.getEnumConstants())
      .filter(keyType -> keyType.getValue().equalsIgnoreCase(stringValue))
      .findAny()
      .orElseThrow(() -> new IllegalStateException(
        String.format("Unsupported value for '%s': '%s'", propertyName, stringValue)));
  }

  public static Builder builder() {
    return new Builder();
  }

  public static Builder builder(MultipleFileConfig copy) {
    return new Builder()
      .setPrefix(copy.getPrefix())
      .setPath(copy.getPath())
      .setType(copy.getType().toString())
      .setFormat(copy.getFormat().toString())
      .setFiltersType(copy.getFiltersType().toString())
      .setFilters(copy.getFilters());
  }

  public static final class Builder {
    private String prefix;
    private String path;
    private String type;
    private String format;
    private String filtersType;
    private String filters;

    private Builder() {
    }

    public Builder setPrefix(String prefix) {
      this.prefix = prefix;
      return this;
    }

    public Builder setPath(String path) {
      this.path = path;
      return this;
    }

    public Builder setType(String type) {
      this.type = type;
      return this;
    }

    public Builder setFormat(String format) {
      this.format = format;
      return this;
    }

    public Builder setFiltersType(String filtersType) {
      this.filtersType = filtersType;
      return this;
    }

    public Builder setFilters(String filters) {
      this.filters = filters;
      return this;
    }

    public MultipleFileConfig build() {
      return new MultipleFileConfig(this);
    }
  }
}
