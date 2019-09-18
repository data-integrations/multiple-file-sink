package io.cdap.plugin.mfs.writer;

import io.cdap.plugin.mfs.MultipleFileConfig;
import io.cdap.cdap.api.data.batch.OutputFormatProvider;
import io.cdap.cdap.api.dataset.DatasetProperties;
import io.cdap.cdap.etl.api.batch.BatchSinkContext;
import io.cdap.plugin.mfs.filter.ExpressionFilter;
import com.google.gson.Gson;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;

/**
 * Class description here.
 */
public class MultipleFileOutputProvider implements OutputFormatProvider {
  private static final Logger LOG = LoggerFactory.getLogger(MultipleFileOutputProvider.class);
  private final int partitionId;
  private final int count;
  private final Map<String, String> properties = new HashMap<>();
  private final MultipleFileConfig config;
  private final BatchSinkContext context;
  private final Gson gson = new Gson();

  public MultipleFileOutputProvider(int partitionId, int count, MultipleFileConfig config, BatchSinkContext context) {
    this.partitionId = partitionId;
    this.count = count;
    this.config = config;
    this.context = context;

    String path = getPath(config.getPath(), config.getPrefix());
    LOG.info(String.format(
      "Initializing partition with id %d - Output Type '%s', Filter Type '%s', Formatter Type '%s', Path '%s'"
      , partitionId, config.getType(), config.getFiltersType(), config.getFormat(), path
    ));

    Map<String, String> arguments = context.getArguments().asMap();
    properties.put(OutputFormatDelegator.MFS_RUNTIME_ARGUMENTS, gson.toJson(arguments));
    properties.put(OutputFormatDelegator.MFS_OUTPUT_TYPE, config.getType().toString());
    properties.put(OutputFormatDelegator.MFS_FILTER_TYPE, config.getFiltersType().toString());
    properties.put(ExpressionFilter.EXPRESSION_CONFIG, config.getFiltersAsList()[partitionId]);
    properties.put(OutputFormatDelegator.MFS_FORMATTER_TYPE, config.getFormat().toString());
    properties.put(DatasetProperties.SCHEMA, context.getInputSchema().toString());
    properties.put(OutputFormatDelegator.MFS_PARTITION_ID, String.valueOf(partitionId));
    properties.put(FileOutputFormat.OUTDIR, path);
  }

  private String getPath(String base, String prefix) {
    if (base.charAt(base.length() - 1) == '/' && base.length() > 1) {
      base = base.substring(0, base.length() - 1);
    }
    return String.format("%s/%s/%05d", base, prefix, partitionId);
  }

  @Override
  public String getOutputFormatClassName() {
    return OutputFormatDelegator.class.getName();
  }

  @Override
  public Map<String, String> getOutputFormatConfiguration() {
    return properties;
  }
}
