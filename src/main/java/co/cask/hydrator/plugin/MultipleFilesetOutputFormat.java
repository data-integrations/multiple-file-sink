package co.cask.hydrator.plugin;

import java.io.DataOutputStream;

import java.io.IOException;

import org.apache.hadoop.fs.FSDataOutputStream;

import org.apache.hadoop.fs.FileSystem;

import org.apache.hadoop.fs.Path;

import org.apache.hadoop.io.*;

import org.apache.hadoop.mapreduce.RecordWriter;

import org.apache.hadoop.mapreduce.TaskAttemptContext;

import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class MultipleFilesetOutputFormat extends FileOutputFormat<Text, IntWritable> {

  protected static class MultipleFilesetRecordWriter extends RecordWriter<Text, IntWritable> {

    private DataOutputStream out;

    public MultipleFilesetRecordWriter(DataOutputStream out) throws IOException

    {

      this.out = out;

      out.writeBytes("<Output>\n");

    }

    private void writeStyle(String xml_tag,String tag_value) throws IOException{

      out.writeBytes("<"+xml_tag+">"+tag_value+"</"+xml_tag+">\n");

    }

    public synchronized void write(Text key, IntWritable value) throws IOException

    {

      out.writeBytes("<record>\n");

      this.writeStyle("key", key.toString());

      this.writeStyle("value", value.toString());

      out.writeBytes("</record>\n");

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

  public RecordWriter<Text, IntWritable> getRecordWriter(

    TaskAttemptContext job)

    throws IOException {

    String file_extension = ".xml";

    Path file = getDefaultWorkFile(job, file_extension);

    FileSystem fs = file.getFileSystem(job.getConfiguration());

    FSDataOutputStream fileOut = fs.create(file, false);

    return new MultipleFilesetRecordWriter(fileOut);

  }



}