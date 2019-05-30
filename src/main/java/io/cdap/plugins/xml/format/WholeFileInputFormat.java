package io.cdap.plugins.xml.format;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;

import java.io.IOException;

/**
 * An input format that reads the whole file content as one record.
 */
public class WholeFileInputFormat extends FileInputFormat<NullWritable, BytesWritable> {

  @Override
  protected boolean isSplitable(JobContext context, Path filename) {
    return false;
  }

  @Override
  public WhileFileRecordReader createRecordReader(InputSplit inputSplit,
                                                  TaskAttemptContext context) throws IOException, InterruptedException {
    WhileFileRecordReader reader = new WhileFileRecordReader();
    reader.initialize(inputSplit, context);
    return reader;
  }
}
