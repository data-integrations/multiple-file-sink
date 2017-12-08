package co.cask.writer;

import co.cask.cdap.api.data.format.StructuredRecord;
import co.cask.filter.ExpressionFilter;
import co.cask.filter.NoFilter;
import co.cask.filter.RecordFilter;
import co.cask.formatter.CSVFormatter;
import co.cask.formatter.GenericRecordFormatter;
import co.cask.formatter.JsonFormatter;
import co.cask.formatter.RecordFormatter;
import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.mapreduce.AvroKeyOutputFormat;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.OutputCommitter;
import org.apache.hadoop.mapreduce.OutputFormat;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.parquet.avro.AvroParquetOutputFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Map;

/**
 * Class description here.
 */
public class OutputFormatDelegator extends OutputFormat<NullWritable, StructuredRecord> {
  private static final Logger LOG = LoggerFactory.getLogger(OutputFormatDelegator.class);
  public static final String MFS_OUTPUT_TYPE = "output.type";
  public static final String MFS_FILTER_TYPE = "filter.type";
  public static final String MFS_FORMATTER_TYPE = "formatter.type";
  public static final String MFS_PARTITION_ID = "part.id";
  public static final String MFS_RUNTIME_ARGUMENTS = "runtime.arguments";
  private Gson gson = new Gson();

  @Override
  public RecordWriter<NullWritable, StructuredRecord> getRecordWriter(TaskAttemptContext ctx)
    throws IOException, InterruptedException {
    Configuration configuration = ctx.getConfiguration();
    OutputFormat output = getOutput(configuration);

    String args = configuration.get(OutputFormatDelegator.MFS_RUNTIME_ARGUMENTS);
    Map<String, String> arguments = gson.fromJson(args, new TypeToken<Map<String, String>>(){}.getType());

    int partitionId = configuration.getInt(OutputFormatDelegator.MFS_PARTITION_ID, -1);
    String filterType = configuration.get(MFS_FILTER_TYPE, "None");
    RecordFilter filter = new NoFilter();
    if (filterType.equalsIgnoreCase("None")) {
      filter = new NoFilter();
    } else if (filterType.equalsIgnoreCase("Expression")) {
      String expression = configuration.get(ExpressionFilter.EXPRESSION_CONFIG);
      filter = new ExpressionFilter(expression);
    }
    filter.configure(partitionId, arguments, configuration);

    String formatterType = configuration.get(MFS_FORMATTER_TYPE, "CSV");
    RecordFormatter formatter = new CSVFormatter();
    if (formatterType.equalsIgnoreCase("CSV")) {
      formatter = new CSVFormatter();
    } else if (formatterType.equalsIgnoreCase("JSON")) {
      formatter = new JsonFormatter();
    } else if (formatterType.equalsIgnoreCase("Generic Record")) {
      formatter = new GenericRecordFormatter();
    }
    formatter.configure(partitionId, arguments, configuration);

    return new RecordWriterWrappper(
      shouldWriteValueAsKey(configuration),
      output.getRecordWriter(ctx),
      filter,
      formatter
    );
  }

  private OutputFormat getOutput(Configuration configuration) {
    String outputType = configuration.get(MFS_OUTPUT_TYPE, "Text");
    OutputFormat output = new TextOutputFormat<NullWritable, Text>();
    if (outputType.equalsIgnoreCase("Text")) {
      output = new TextOutputFormat<NullWritable, Text>();
    } else if (outputType.equalsIgnoreCase("Avro")) {
      output = new AvroKeyOutputFormat<GenericRecord>();
    } else if (outputType.equalsIgnoreCase("Avro Parquet")) {
      output = new AvroParquetOutputFormat();
    } else if (outputType.equalsIgnoreCase("Sequence File")) {
      output = new SequenceFileOutputFormat();
    }
    return output;
  }

  private boolean shouldWriteValueAsKey(Configuration configuration) {
    String outputType = configuration.get(MFS_OUTPUT_TYPE, "Text");
    boolean output = false;
    if (outputType.equalsIgnoreCase("Avro")) {
      output = true;
    } else if (outputType.equalsIgnoreCase("Avro Parquet")) {
      output = true;
    }
    return output;
  }

  @Override
  public void checkOutputSpecs(JobContext ctx) throws IOException, InterruptedException {
    getOutput(ctx.getConfiguration()).checkOutputSpecs(ctx);
  }

  @Override
  public OutputCommitter getOutputCommitter(TaskAttemptContext ctx) throws IOException, InterruptedException {
    return getOutput(ctx.getConfiguration()).getOutputCommitter(ctx);
  }
}
