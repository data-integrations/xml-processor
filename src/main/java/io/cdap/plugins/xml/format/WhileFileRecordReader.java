package io.cdap.plugins.xml.format;

import com.google.common.io.ByteStreams;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.IOException;

/**
 * A {@link RecordReader} that reads the full file content.
 */
final class WhileFileRecordReader extends RecordReader<NullWritable, BytesWritable> {

  private BytesWritable value;
  private FileSplit inputSplit;
  private Configuration hConf;

  @Override
  public void initialize(InputSplit inputSplit,
                         TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException {
    if (!(inputSplit instanceof FileSplit)) {
      // This shouldn't happen
      throw new IllegalArgumentException("Input split should be instance of FileSplit: " + inputSplit.getClass());
    }
    if (inputSplit.getLength() > Integer.MAX_VALUE) {
      throw new IllegalArgumentException("Content cannot be larger than " + Integer.MAX_VALUE / 1024 / 1024 + "MB");
    }
    this.inputSplit = (FileSplit) inputSplit;
    this.hConf = taskAttemptContext.getConfiguration();
  }

  @Override
  public boolean nextKeyValue() throws IOException, InterruptedException {
    if (value != null) {
      return false;
    }

    Path path = inputSplit.getPath();
    FileSystem fs = path.getFileSystem(hConf);

    try (FSDataInputStream input = fs.open(path)) {
      byte[] content = new byte[(int) inputSplit.getLength()];
      ByteStreams.readFully(input, content);
      value = new BytesWritable(content);
    }
    return true;
  }

  @Override
  public NullWritable getCurrentKey() throws IOException, InterruptedException {
    return NullWritable.get();
  }

  @Override
  public BytesWritable getCurrentValue() throws IOException, InterruptedException {
    return value == null ? new BytesWritable() : value;
  }

  @Override
  public float getProgress() throws IOException, InterruptedException {
    return value == null ? 0.0f : 1.0f;
  }

  @Override
  public void close() throws IOException {
    // no-op
  }
}
