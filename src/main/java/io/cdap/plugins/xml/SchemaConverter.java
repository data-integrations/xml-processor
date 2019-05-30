package io.cdap.plugins.xml;

import io.cdap.cdap.api.data.schema.Schema;
import com.google.common.base.Function;
import com.google.common.collect.Iterables;

/**
 * Helper class to convert Avro schema into CDAP schema.
 */
public final class SchemaConverter {

  private static final Function<org.apache.avro.Schema, Schema> SCHEMA_CONVERTER =
    new Function<org.apache.avro.Schema, Schema>() {
      @Override
      public Schema apply(org.apache.avro.Schema input) {
        return fromAvro(input);
      }
    };

  private static final Function<org.apache.avro.Schema.Field, Schema.Field> FIELD_CONVERTER =
    new Function<org.apache.avro.Schema.Field, Schema.Field>() {
      @Override
      public Schema.Field apply(org.apache.avro.Schema.Field input) {
        return Schema.Field.of(input.name(), SCHEMA_CONVERTER.apply(input.schema()));
      }
    };


  public static Schema fromAvro(org.apache.avro.Schema avroSchema) {
    switch (avroSchema.getType()) {
      case NULL:
        return Schema.of(Schema.Type.NULL);
      case BOOLEAN:
        return Schema.of(Schema.Type.BOOLEAN);
      case INT:
        return Schema.of(Schema.Type.INT);
      case LONG:
        return Schema.of(Schema.Type.LONG);
      case FLOAT:
        return Schema.of(Schema.Type.FLOAT);
      case DOUBLE:
        return Schema.of(Schema.Type.DOUBLE);
      case STRING:
        return Schema.of(Schema.Type.STRING);
      case BYTES:
        return Schema.of(Schema.Type.BYTES);
      case FIXED:
        return Schema.of(Schema.Type.BYTES);
      case ENUM:
        return Schema.enumWith(avroSchema.getEnumSymbols());
      case ARRAY:
        return Schema.arrayOf(fromAvro(avroSchema.getElementType()));
      case MAP:
        return Schema.mapOf(Schema.of(Schema.Type.STRING), fromAvro(avroSchema.getValueType()));
      case RECORD:
        return Schema.recordOf(avroSchema.getName(), Iterables.transform(avroSchema.getFields(), FIELD_CONVERTER));
      case UNION:
        return Schema.unionOf(Iterables.transform(avroSchema.getTypes(), SCHEMA_CONVERTER));
    }

    // This shouldn't happen.
    throw new IllegalArgumentException("Unsupported Avro schema type " + avroSchema.getType());
  }



  private SchemaConverter() {

  }
}
