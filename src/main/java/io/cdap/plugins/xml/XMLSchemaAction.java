package io.cdap.plugins.xml;

import io.cdap.cdap.api.annotation.Description;
import io.cdap.cdap.api.annotation.Macro;
import io.cdap.cdap.api.annotation.Name;
import io.cdap.cdap.api.annotation.Plugin;
import io.cdap.cdap.api.plugin.PluginConfig;
import io.cdap.cdap.etl.api.action.Action;
import io.cdap.cdap.etl.api.action.ActionContext;
import com.google.common.base.Function;
import com.google.common.base.Predicates;
import com.google.common.base.Splitter;
import com.google.common.base.Strings;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import org.apache.avro.Schema;
import org.apache.hadoop.fs.Path;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import javax.annotation.Nullable;

/**
 * An {@link Action} that converts XML schema into Avro Schema and CDAP Schema
 */
@Plugin(type = Action.PLUGIN_TYPE)
@Name("XMLSchemaAction")
@Description("An action that takes a XML schema and generate CDAP and Avro Schema from it. " +
  "If there are more than one top level records, the generated schemas will be of UNION type, " +
  "with all the records defined by the XML schema in it.")
public class XMLSchemaAction extends Action {

  private final Config config;

  public XMLSchemaAction(Config config) {
    this.config = config;
  }

  @Override
  public void run(ActionContext context) throws Exception {
    SchemaProvider schemaProvider = new SchemaProvider(config.ignoreSchemaError);
    final SchemaProvider.SchemaInfo schemaInfo = schemaProvider.getSchemaInfo(new Path(config.xsdPath));

    Schema avroSchema;
    if (!Strings.isNullOrEmpty(config.acceptRecords)) {
      Set<String> records = Sets.newHashSet(Splitter.on(",").split(config.acceptRecords));

      // Get the schemas from the given set of records
      List<Schema> schemas = Lists.newArrayList(Iterables.filter(
        Iterables.transform(records, new Function<String, Schema>() {
          @Nullable
          @Override
          public Schema apply(String record) {
            return schemaInfo.getSchema(record);
          }
        }), Predicates.<Schema>notNull()));

      if (schemas.isEmpty()) {
        throw new IllegalArgumentException(
          "No record schema matching the list provided by the acceptRecords config: " + config.acceptRecords);
      }
      avroSchema = Schema.createUnion(new ArrayList<>(schemas));
    } else {
      avroSchema = schemaInfo.getUnionSchema();
    }

    if (avroSchema.getTypes().size() == 1) {
      avroSchema = avroSchema.getTypes().iterator().next();
    }

    context.getArguments().set(config.avroSchemaName, avroSchema.toString());
    context.getArguments().set(config.schemaName, SchemaConverter.fromAvro(avroSchema).toString());
  }

  /**
   * Configuration class for this action.
   */
  public static final class Config extends PluginConfig {

    @Description(
      "A path to the XML schema file"
    )
    @Macro
    private String xsdPath;

    @Description(
      "Set to 'true' such that errors encountered during parsing of the xml schema file will not result in failure; " +
        "default value is 'true'"
    )
    private boolean ignoreSchemaError = true;

    @Nullable
    @Description(
      "A comma separate list of record names that will have schema generated; if not specified, all records found in" +
        "the xml schema file will have schema generated"
    )
    @Macro
    private String acceptRecords;

    @Description("The argument name for storing the avro schema")
    private String avroSchemaName;

    @Description("The argument name for storing the schema")
    private String schemaName;
  }
}
