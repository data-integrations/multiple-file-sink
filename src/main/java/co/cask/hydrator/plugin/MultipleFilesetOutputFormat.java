package co.cask.hydrator.plugin;

import co.cask.cdap.api.data.format.StructuredRecord;
import co.cask.cdap.api.data.schema.Schema;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;

import java.io.DataOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import javax.annotation.Nullable;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

public class MultipleFilesetOutputFormat extends FileOutputFormat<NullWritable, StructuredRecord> {

  private static final String PATH_FIELD = "path.tracking.path.field";
  private static final String FILENAME_ONLY = "path.tracking.filename.only";

  /**
   * Configure the input format to use the specified schema and optional path field.
   */
  public static void configure(Configuration conf, @Nullable String pathField, boolean filenameOnly) {
    if (pathField != null) {
      conf.set(PATH_FIELD, pathField);
    }
    conf.setBoolean(FILENAME_ONLY, filenameOnly);
  }

  public static Schema getOutputSchema(@Nullable String pathField) {
    List<Schema.Field> fields = new ArrayList<>();
    fields.add(Schema.Field.of("offset", Schema.of(Schema.Type.LONG)));
    fields.add(Schema.Field.of("body", Schema.nullableOf(Schema.of(Schema.Type.STRING))));
    if (pathField != null) {
      fields.add(Schema.Field.of(pathField, Schema.nullableOf(Schema.of(Schema.Type.STRING))));
    }
    return Schema.recordOf("file.record", fields);
  }



  protected static class MultipleFilesetOutputFormatRecordWriter extends RecordWriter<Text, IntWritable> {

    private final RecordWriter<LongWritable, Text> delegate;
    private final Schema schema;
    private final String pathField;
    private final String path;
    private DataOutputStream out;


    private MultipleFilesetOutputFormatRecordWriter (RecordWriter<LongWritable, Text> delegate, @Nullable String pathField,
                                     String path, DataOutputStream out) {

      this.delegate = delegate;
      this.pathField = pathField;
      this.path = path;
      schema = getOutputSchema(pathField);
      this.out = out;

    }

    public synchronized void write(Text key, IntWritable value) throws IOException

    {

      out.writeBytes("<record>\n");

      this.writeStyle("key", key.toString());

      this.writeStyle("value", value.toString());

      out.writeBytes("</record>\n");

    }

    private void writeStyle(String xml_tag,String tag_value) throws IOException{

      out.writeBytes("<"+xml_tag+">"+tag_value+"</"+xml_tag+">\n");

    }


    public synchronized void close(TaskAttemptContext job)

      throws IOException

    {

      try {

        out.writeBytes("</Output>\n");

      } finally {

        out.close();

      }

    }

  }

  public RecordWriter<NullWritable, StructuredRecord> getRecordWriter throws InterruptedException(

    TaskAttemptContext context)

    throws IOException {

    String file_extension = ".xml";

    Path file = getDefaultWorkFile(context, file_extension);

    FileSystem fs = file.getFileSystem(context.getConfiguration());

    FSDataOutputStream fileOut = fs.create(file, false);


    RecordWriter<NullWritable, StructuredRecord> delegate = new TextOutputFormat<NullWritable, StructuredRecord>().getRecordWriter(context);


    String pathField = context.getConfiguration().get(PATH_FIELD);


    return new MultipleFilesetOutputFormatRecordWriter(delegate, pathField, pathField, fileOut);
  }



}