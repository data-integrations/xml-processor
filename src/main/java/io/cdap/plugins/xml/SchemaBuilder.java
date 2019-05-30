package io.cdap.plugins.xml;

import org.apache.avro.Schema;
import org.apache.xerces.impl.xs.XSComplexTypeDecl;
import org.apache.xerces.xs.XSAttributeDeclaration;
import org.apache.xerces.xs.XSAttributeUse;
import org.apache.xerces.xs.XSComplexTypeDefinition;
import org.apache.xerces.xs.XSConstants;
import org.apache.xerces.xs.XSElementDeclaration;
import org.apache.xerces.xs.XSModel;
import org.apache.xerces.xs.XSModelGroup;
import org.apache.xerces.xs.XSNamedMap;
import org.apache.xerces.xs.XSObject;
import org.apache.xerces.xs.XSObjectList;
import org.apache.xerces.xs.XSParticle;
import org.apache.xerces.xs.XSSimpleTypeDefinition;
import org.apache.xerces.xs.XSTerm;
import org.apache.xerces.xs.XSTypeDefinition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import javax.annotation.Nullable;

/**
 *
 */
public class SchemaBuilder {

  private static final Logger LOG = LoggerFactory.getLogger(SchemaBuilder.class);
  private static final Schema NULL_SCHEMA = Schema.create(Schema.Type.NULL);
  private static final Map<Short, Schema.Type> PRIMITIVES = new HashMap<>();

  static {
    PRIMITIVES.put(XSConstants.BOOLEAN_DT, Schema.Type.BOOLEAN);

    PRIMITIVES.put(XSConstants.INT_DT, Schema.Type.INT);
    PRIMITIVES.put(XSConstants.BYTE_DT, Schema.Type.INT);
    PRIMITIVES.put(XSConstants.SHORT_DT, Schema.Type.INT);
    PRIMITIVES.put(XSConstants.UNSIGNEDBYTE_DT, Schema.Type.INT);
    PRIMITIVES.put(XSConstants.UNSIGNEDSHORT_DT, Schema.Type.INT);

    PRIMITIVES.put(XSConstants.INTEGER_DT, Schema.Type.STRING);
    PRIMITIVES.put(XSConstants.NEGATIVEINTEGER_DT, Schema.Type.STRING);
    PRIMITIVES.put(XSConstants.NONNEGATIVEINTEGER_DT, Schema.Type.STRING);
    PRIMITIVES.put(XSConstants.POSITIVEINTEGER_DT, Schema.Type.STRING);
    PRIMITIVES.put(XSConstants.NONPOSITIVEINTEGER_DT, Schema.Type.STRING);

    PRIMITIVES.put(XSConstants.LONG_DT, Schema.Type.LONG);
    PRIMITIVES.put(XSConstants.UNSIGNEDINT_DT, Schema.Type.LONG);

    PRIMITIVES.put(XSConstants.FLOAT_DT, Schema.Type.FLOAT);

    PRIMITIVES.put(XSConstants.DOUBLE_DT, Schema.Type.DOUBLE);
    PRIMITIVES.put(XSConstants.DECIMAL_DT, Schema.Type.DOUBLE);

    PRIMITIVES.put(XSConstants.DATETIME_DT, Schema.Type.LONG);
  }

  private final Map<String, Schema> schemas = new LinkedHashMap<>();
  private int typeName;


  /**
   * Returns a {@link Schema} of {@link Schema.Type#UNION} which contains all record schemas as parsed from the
   * given {@link XSModel}.
   */
  public Schema createSchema(XSModel model) {
    schemas.clear();

    Map<String, Schema> schemas = new LinkedHashMap<>();
    XSNamedMap rootEls = model.getComponents(XSConstants.ELEMENT_DECLARATION);

    for (int i = 0; i < rootEls.getLength(); i++) {
      XSElementDeclaration el = (XSElementDeclaration) rootEls.item(i);
      XSTypeDefinition type = el.getTypeDefinition();

      Schema schema = createTypeSchema(type, false, false);
      String fullName = schema.getFullName();
      if (schema.getType() == Schema.Type.RECORD && schemas.containsKey(fullName)) {
        if (!schema.equals(schemas.get(fullName))) {
          LOG.warn("Ignore record schema of name {} due to a schema with the same name already existed. " +
                     "Existing: {}, new: {}", fullName, schemas.get(fullName), schema); 
        }
        continue;
      }

      Source source = new Source(el.getName());
      schema.addProp(Source.SOURCE, source.toString());
      schemas.put(fullName, schema);
    }

    if (schemas.size() == 0) {
      throw new IllegalArgumentException("No root element declaration");
    }

    return Schema.createUnion(new ArrayList<>(schemas.values()));
  }

  private Schema createTypeSchema(XSTypeDefinition type, boolean optional, boolean array) {
    Schema schema;

    if (type.getTypeCategory() == XSTypeDefinition.SIMPLE_TYPE) {
      schema = Schema.create(getPrimitiveType((XSSimpleTypeDefinition) type));
    } else {
      String name = complexTypeName(type);

      schema = schemas.get(name);
      if (schema == null) {
        schema = createRecordSchema(name, (XSComplexTypeDefinition) type);
      }
    }

    if (array || isGroupTypeWithMultipleOccurs(type)) {
      schema = Schema.createArray(schema);
    } else if (optional) {
      schema = Schema.createUnion(Arrays.asList(NULL_SCHEMA, schema));
    }

    return schema;
  }

  private boolean isGroupTypeWithMultipleOccurs(XSTypeDefinition type) {
    return type instanceof XSComplexTypeDefinition &&
      isGroupTypeWithMultipleOccurs(((XSComplexTypeDefinition) type).getParticle());
  }

  private boolean isGroupTypeWithMultipleOccurs(XSParticle particle) {
    if (particle == null) {
      return false;
    }

    XSTerm term = particle.getTerm();
    if (term.getType() != XSConstants.MODEL_GROUP) {
      return false;
    }

    XSModelGroup group = (XSModelGroup) term;
    final short compositor = group.getCompositor();
    switch (compositor) {
      case XSModelGroup.COMPOSITOR_CHOICE:
      case XSModelGroup.COMPOSITOR_SEQUENCE:
        return particle.getMaxOccurs() > 1 || particle.getMaxOccursUnbounded();
      default:
        return false;
    }
  }

  private Schema createGroupSchema(String name, XSModelGroup groupTerm) {
    Schema record = Schema.createRecord(name, null, null, false);
    schemas.put(name, record);

    Map<String, Schema.Field> fields = new HashMap<>();
    createGroupFields(groupTerm, fields, false);
    record.setFields(new ArrayList<>(fields.values()));

    return Schema.createArray(record);
  }

  private Schema createRecordSchema(String name, XSComplexTypeDefinition type) {
    Schema record = Schema.createRecord(name, null, null, false);
    schemas.put(name, record);

    record.setFields(createFields(type));
    return record;
  }

  private List<Schema.Field> createFields(XSComplexTypeDefinition type) {
    final Map<String, Schema.Field> fields = new LinkedHashMap<>();

    // Define fields for attributes
    XSObjectList attrUses = type.getAttributeUses();
    for (int i = 0; i < attrUses.getLength(); i++) {
      XSAttributeUse attrUse = (XSAttributeUse) attrUses.item(i);
      XSAttributeDeclaration attrDecl = attrUse.getAttrDeclaration();

      boolean optional = !attrUse.getRequired();
      Schema.Field field = createField(fields.values(), attrDecl, attrDecl.getTypeDefinition(), optional, false);
      fields.put(field.getProp(Source.SOURCE), field);
    }

    // Define fields for children elements
    XSParticle particle = type.getParticle();
    if (particle != null) {
      XSTerm term = particle.getTerm();
      if (term.getType() != XSConstants.MODEL_GROUP) {
        throw new IllegalStateException("Unsupported term type " + term.getType());
      }

      XSModelGroup group = (XSModelGroup) term;
      createGroupFields(group, fields, false);
    }

    // Define field for element body. Only applicable to type with simpleContent.
    XSSimpleTypeDefinition simpleType = type.getSimpleType();
    if (simpleType != null) {
      String fieldName = uniqueFieldName(fields.values(), "body");
      Schema bodySchema = Schema.createUnion(Arrays.asList(NULL_SCHEMA, Schema.create(getPrimitiveType(simpleType))));
      Schema.Field field = new Schema.Field(fieldName, bodySchema, null, null);
      field.addProp(Source.SIMPLE_CONTENT, Boolean.toString(true));
      fields.put(fieldName, field);
    }

    return new ArrayList<>(fields.values());
  }

  private void createGroupFields(XSModelGroup group, Map<String, Schema.Field> fields, boolean forceOptional) {
    XSObjectList particles = group.getParticles();

    for (int j = 0; j < particles.getLength(); j++) {
      XSParticle particle = (XSParticle) particles.item(j);
      boolean insideChoice = group.getCompositor() == XSModelGroup.COMPOSITOR_CHOICE;

      boolean optional = insideChoice || particle.getMinOccurs() == 0;
      boolean array = particle.getMaxOccurs() > 1 || particle.getMaxOccursUnbounded();

      XSTerm term = particle.getTerm();

      switch (term.getType()) {
        case XSConstants.ELEMENT_DECLARATION:
          XSElementDeclaration el = (XSElementDeclaration) term;
          Schema.Field field = createField(fields.values(), el, el.getTypeDefinition(), forceOptional || optional,
                                           array);
          fields.put(field.getProp(Source.SOURCE), field);
          break;
        case XSConstants.MODEL_GROUP:
          XSModelGroup subGroup = (XSModelGroup) term;
          if (particle.getMaxOccurs() <= 1 && !particle.getMaxOccursUnbounded()) {
            createGroupFields(subGroup, fields, forceOptional || insideChoice);
          } else {
            String fieldName = nextTypeName();
            fields.put(fieldName, new Schema.Field(fieldName, createGroupSchema(nextTypeName(), subGroup), null, null));
          }
          break;
        case XSConstants.WILDCARD:
          field = createField(fields.values(), term, null, forceOptional || optional, array);
          fields.put(field.getProp(Source.SOURCE), field);
          break;
        default:
          throw new IllegalStateException("Unsupported term type " + term.getType());
      }
    }
  }

  private Schema.Field createField(Iterable<Schema.Field> fields, XSObject source, XSTypeDefinition type, boolean
    optional, boolean array) {
    List<Short> supportedTypes = Arrays.asList(XSConstants.ELEMENT_DECLARATION, XSConstants.ATTRIBUTE_DECLARATION,
                                               XSConstants.WILDCARD);
    if (!supportedTypes.contains(source.getType())) {
      throw new IllegalStateException("Invalid source object type " + source.getType());
    }

    boolean wildcard = source.getType() == XSConstants.WILDCARD;
    if (wildcard) {
      Schema schema = Schema.createUnion(Arrays.asList(Schema.create(Schema.Type.NULL),
                                                       Schema.create(Schema.Type.STRING)));
      return new Schema.Field(Source.WILDCARD, schema, null, null);
    }

    Schema fieldSchema = createTypeSchema(type, optional, array);

    String name = validName(source.getName());
    name = uniqueFieldName(fields, name);

    Schema.Field field = new Schema.Field(name, fieldSchema, null, null);

    boolean attribute = source.getType() == XSConstants.ATTRIBUTE_DECLARATION;
    field.addProp(Source.SOURCE, new Source(source.getName(), attribute).toString());

    return field;
  }

  private Schema.Type getPrimitiveType(XSSimpleTypeDefinition type) {
    Schema.Type avroType = PRIMITIVES.get(type.getBuiltInKind());
    return avroType == null ? Schema.Type.STRING : avroType;
  }

  private String uniqueFieldName(Iterable<Schema.Field> fields, String name) {
    int duplicates = 0;

    for (Schema.Field field : fields) {
      if (field.name().equals(name)) {
        duplicates++;
      }
    }

    return name + (duplicates > 0 ? duplicates - 1 : "");
  }

  private String complexTypeName(XSTypeDefinition type) {
    String name = validName(((XSComplexTypeDecl) type).getTypeName());
    return name != null ? name : nextTypeName();
  }

  @Nullable
  private String validName(@Nullable String name) {
    if (name == null) {
      return null;
    }

    char[] chars = name.toCharArray();
    char[] result = new char[chars.length];

    int p = 0;
    for (char c : chars) {
      boolean valid =
        c >= 'a' && c <= 'z' ||
          c >= 'A' && c <= 'z' ||
          c >= '0' && c <= '9' ||
          c == '_';

      boolean separator = c == '.' || c == '-';

      if (valid) {
        result[p] = c;
        p++;
      } else if (separator) {
        result[p] = '_';
        p++;
      }
    }

    String s = new String(result, 0, p);

    // handle built-in types
    try {
      Schema.Type.valueOf(s.toUpperCase());
      s += typeName++;
    } catch (IllegalArgumentException ignore) {
    }

    return s;
  }

  private String nextTypeName() {
    return "type" + typeName++;
  }
}
