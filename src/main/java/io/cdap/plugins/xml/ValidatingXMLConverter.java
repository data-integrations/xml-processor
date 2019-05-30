package io.cdap.plugins.xml;

import io.cdap.cdap.api.annotation.Description;
import io.cdap.cdap.api.annotation.Macro;
import io.cdap.cdap.api.annotation.Name;
import io.cdap.cdap.api.annotation.Plugin;
import io.cdap.cdap.api.data.format.StructuredRecord;
import io.cdap.cdap.api.data.schema.Schema;
import io.cdap.cdap.api.plugin.PluginConfig;
import io.cdap.cdap.etl.api.Emitter;
import io.cdap.cdap.etl.api.InvalidEntry;
import io.cdap.cdap.etl.api.PipelineConfigurer;
import io.cdap.cdap.etl.api.Transform;
import io.cdap.cdap.etl.api.TransformContext;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.xml.sax.ErrorHandler;
import org.xml.sax.SAXException;
import org.xml.sax.SAXParseException;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;
import java.util.TimeZone;
import javax.annotation.Nullable;
import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;

/**
 *
 */
@Plugin(type = Transform.PLUGIN_TYPE)
@Name("ValidatingXMLConverter")
@Description("Validate XML content with a XML schema, and convert the xml document into StructuredRecord.")
public class ValidatingXMLConverter extends Transform<StructuredRecord, StructuredRecord> {

  private static final Logger LOG = LoggerFactory.getLogger(ValidatingXMLConverter.class);

  private final Config config;
  private final Map<org.apache.avro.Schema, Schema> schemaMap;
  private StructuredRecordCreator structuredRecordCreator;
  private Schema.Field inputField;
  private SchemaProvider.SchemaInfo schemaInfo;
  private DocumentBuilder documentBuilder;
  private DataMasker dataMasker;

  public ValidatingXMLConverter(Config config) {
    this.config = config;
    this.schemaMap = new HashMap<>();
  }

  @Override
  public void configurePipeline(PipelineConfigurer configurer) throws IllegalArgumentException {
    // Validate the input schema that should have the xml field
    Schema inputSchema = configurer.getStageConfigurer().getInputSchema();
    if (inputSchema != null) {
      findInputField(inputSchema);
    }

    // If provided, try to initialize the object to catch any regex errors if any before runtime
    if (config != null) {
      dataMasker = new DataMasker(config.dataMasks);
    }
  }

  @Override
  public void initialize(TransformContext context) throws Exception {
    dataMasker = new DataMasker(config.dataMasks);
    this.structuredRecordCreator = config.timezone == null
      ? new StructuredRecordCreator(dataMasker)
      : new StructuredRecordCreator(dataMasker, TimeZone.getTimeZone(config.timezone));

    if (context.getInputSchema() != null) {
      this.inputField = findInputField(context.getInputSchema());
    }

    schemaInfo = new SchemaProvider(config.ignoreSchemaError).getSchemaInfo(new Path(config.xsdPath));
    documentBuilder = createDocumentBuilder(schemaInfo);

  }

  @Override
  public void transform(StructuredRecord input, Emitter<StructuredRecord> emitter) throws Exception {
    // If input field is null, meaning the previous stage doesn't provide a schema. So we have to find the
    // input field from the current input record
    Schema.Field inputField = this.inputField;
    if (inputField == null) {
      this.inputField = inputField = findInputField(input.getSchema());
    }

    try (InputStream is = createXMLInputStream(input.get(inputField.getName()), inputField.getSchema().getType())) {
      Document document = documentBuilder.parse(is);
      Element docElement = document.getDocumentElement();

      org.apache.avro.Schema avroSchema = schemaInfo.getSchema(docElement.getLocalName());
      // This shouldn't happen
      if (avroSchema == null) {
        emitter.emitError(new InvalidEntry<>(1, "No schema found for document element " + docElement.getLocalName(),
                                             input));
      } else {
        StructuredRecord record = structuredRecordCreator.create(document, avroSchema, getSchema(avroSchema));
        LOG.info("Got Schema : {}", record.getSchema());
        emitter.emit(record);
      }
    } catch (Exception e) {
      emitter.emitError(new InvalidEntry<>(2, "Failed to parse document due to " + e.getMessage(), input));
    }
  }

  private InputStream createXMLInputStream(Object object, Schema.Type type) {
    if (type == Schema.Type.STRING) {
      return new ByteArrayInputStream(((String) object).getBytes(StandardCharsets.UTF_8));
    } else {
      // Must be BYTES type, as it's been checked in findInputField method
      return new ByteArrayInputStream((byte[]) object);
    }
  }

  private Schema.Field findInputField(Schema inputSchema) {
    if (inputSchema.getType() != Schema.Type.RECORD) {
      throw new IllegalArgumentException("Input schema must be a record");
    }

    if (config.fieldName != null) {
      Schema.Field field = inputSchema.getField(config.fieldName);
      if (field == null) {
        throw new IllegalArgumentException("Input schema does not contain a field named '" + config.fieldName + "'");
      }
      Schema schema = field.getSchema();
      if (schema.getType() != Schema.Type.STRING && schema.getType() != Schema.Type.BYTES) {
        throw new IllegalArgumentException("Input schema field '" + config.fieldName +
                                             "' must be of type 'string' or 'bytes'");
      }
      return field;
    }


    for (Schema.Field field : inputSchema.getFields()) {
      Schema.Type type = field.getSchema().getType();
      if (type == Schema.Type.STRING || type == Schema.Type.BYTES) {
        return field;
      }
    }
    throw new IllegalArgumentException("Unable to find a field of type 'string' or 'bytes' for XML data");
  }

  private DocumentBuilder createDocumentBuilder(SchemaProvider.SchemaInfo schemaInfo) throws Exception {
    DocumentBuilderFactory dbf = DocumentBuilderFactory.newInstance();
    dbf.setNamespaceAware(true);
    dbf.setSchema(schemaInfo.getXmlSchema());

    DocumentBuilder documentBuilder = dbf.newDocumentBuilder();
    documentBuilder.setErrorHandler(new ErrorHandler() {
      @Override
      public void warning(SAXParseException exception) throws SAXException {
        LOG.warn("warning: {}", (Object) exception);
      }

      @Override
      public void error(SAXParseException exception) throws SAXException {
        if (exception != null) {
          throw exception;
        }
      }

      @Override
      public void fatalError(SAXParseException exception) throws SAXException {
        if (exception != null) {
          throw exception;
        }
      }
    });

    return documentBuilder;
  }

  private Schema getSchema(org.apache.avro.Schema avroSchema) {
    Schema schema = schemaMap.get(avroSchema);
    if (schema != null) {
      return schema;
    }
    schema = SchemaConverter.fromAvro(avroSchema);
    schemaMap.put(avroSchema, schema);
    return schema;
  }

  /**
   * Configuration for the xml converter plugin
   */
  public static final class Config extends PluginConfig {

    @Description(
      "A path to the XML schema file"
    )
    @Macro
    private String xsdPath;

    @Nullable
    @Description(
      "The field name in the input record that contains the XML content; " +
        "if not specified, the first field with type 'string' or 'bytes' will be used"
    )
    @Macro
    private String fieldName;

    @Description(
      "Set to 'true' such that errors encountered during parsing of the xml schema file will not result in failure; " +
        "default value is 'true'"
    )
    private boolean ignoreSchemaError = true;

    @Nullable
    @Description(
      "Timezone for date time conversion; if not provided, the default timezone will be used"
    )
    @Macro
    private String timezone;

    @Description(DataMasker.DESCRIPTION)
    @Nullable
    private String dataMasks;
  }
}
