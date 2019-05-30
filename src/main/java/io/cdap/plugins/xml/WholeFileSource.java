package io.cdap.plugins.xml;

import io.cdap.cdap.api.annotation.Description;
import io.cdap.cdap.api.annotation.Macro;
import io.cdap.cdap.api.annotation.Name;
import io.cdap.cdap.api.annotation.Plugin;
import io.cdap.cdap.api.data.batch.Input;
import io.cdap.cdap.api.data.batch.InputFormatProvider;
import io.cdap.cdap.api.data.format.StructuredRecord;
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.cdap.api.dataset.lib.KeyValue;
import io.cdap.cdap.api.plugin.PluginConfig;
import io.cdap.cdap.etl.api.Emitter;
import io.cdap.cdap.etl.api.PipelineConfigurer;
import io.cdap.cdap.etl.api.batch.BatchSource;
import io.cdap.cdap.etl.api.batch.BatchSourceContext;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.security.UserGroupInformation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import io.cdap.plugins.xml.format.WholeFileInputFormat;

import java.io.IOException;
import java.util.Collections;
import java.util.Map;

/**
 * A source that uses the {@link WholeFileInputFormat} to read whole file as one record.
 *
 * TODO: Move this to hydrator-plugins and make it extends from FileBatchSource to get the full configurability.
 */
@Plugin(type = BatchSource.PLUGIN_TYPE)
@Name("WholeFileReader")
@Description("Reads content of the whole file as one record")
public class WholeFileSource extends BatchSource<NullWritable, BytesWritable, StructuredRecord> {

  private static final Logger LOG = LoggerFactory.getLogger(WholeFileSource.class);

  private final Config config;
  private final Schema outputSchema;

  public WholeFileSource(Config config) {
    this.config = config;
    this.outputSchema = createOutputSchema();
  }

  @Override
  public void configurePipeline(PipelineConfigurer configurer) {
    configurer.getStageConfigurer().setOutputSchema(outputSchema);
  }

  @Override
  public void prepareRun(BatchSourceContext context) throws Exception {
    Job job = createJob();

    FileInputFormat.setInputPaths(job, config.path);
    final String inputDir = job.getConfiguration().get(FileInputFormat.INPUT_DIR);

    context.setInput(Input.of(config.referenceName, new InputFormatProvider() {
      @Override
      public String getInputFormatClassName() {
        return WholeFileInputFormat.class.getName();
      }

      @Override
      public Map<String, String> getInputFormatConfiguration() {
        return Collections.singletonMap(FileInputFormat.INPUT_DIR, inputDir);
      }
    }));
  }

  @Override
  public void transform(KeyValue<NullWritable, BytesWritable> input,
                        Emitter<StructuredRecord> emitter) throws Exception {
    emitter.emit(StructuredRecord.builder(outputSchema).set("body", input.getValue().getBytes()).build());
  }

  private Schema createOutputSchema() {
    return Schema.recordOf("output", Schema.Field.of("body", Schema.of(Schema.Type.BYTES)));
  }

  private static Job createJob() throws IOException {
    try {
      Job job = Job.getInstance();

      LOG.info("Job new instance");

      // some input formats require the credentials to be present in the job. We don't know for
      // sure which ones (HCatalog is one of them), so we simply always add them. This has no other
      // effect, because this method is only used at configure time and will be ignored later on.
      if (UserGroupInformation.isSecurityEnabled()) {
        Credentials credentials = UserGroupInformation.getCurrentUser().getCredentials();
        job.getCredentials().addAll(credentials);
      }

      return job;
    } catch (Exception e) {
      LOG.error("Exception ", e);
      throw e;
    }
  }

  /**
   * Configurations for the {@link WholeFileSource} plugin.
   */
  public static final class Config extends PluginConfig {

    @Description(
      "This will be used to uniquely identify this source/sink for lineage, annotating metadata, etc."
    )
    private String referenceName;

    @Description(
      "Path to file(s) to be read. If a directory is specified, " +
        "terminate the path name with a \'/\'. For distributed file system such as HDFS, file system name should come" +
        " from 'fs.DefaultFS' property in the 'core-site.xml'. For example, 'hdfs://mycluster.net:8020/input', where" +
        " value of the property 'fs.DefaultFS' in the 'core-site.xml' is 'hdfs://mycluster.net:8020'. The path uses " +
        "filename expansion (globbing) to read files."
    )
    @Macro
    private String path;

    @Override
    public String toString() {
      return "WholeFileSourceConfig{" +
        "referenceName='" + referenceName + '\'' +
        ", path='" + path + '\'' +
        '}';
    }
  }
}
