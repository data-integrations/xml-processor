package io.cdap.plugins.xml;

import io.cdap.cdap.api.data.format.StructuredRecord;
import com.google.common.base.Preconditions;
import com.google.common.base.Predicate;
import com.google.common.base.Strings;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.PeekingIterator;
import org.apache.avro.Schema;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.NamedNodeMap;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;

import java.io.StringWriter;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Collections;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Objects;
import java.util.Set;
import java.util.TimeZone;
import javax.annotation.Nullable;
import javax.xml.bind.DatatypeConverter;
import javax.xml.transform.OutputKeys;
import javax.xml.transform.Transformer;
import javax.xml.transform.TransformerException;
import javax.xml.transform.TransformerFactory;
import javax.xml.transform.dom.DOMSource;
import javax.xml.transform.stream.StreamResult;

/**
 * This class creates {@link StructuredRecord} from XML {@link Document}.
 */
public class StructuredRecordCreator {

  private static final Predicate<Schema> IS_NULL_TYPE = new Predicate<Schema>() {
    @Override
    public boolean apply(Schema schema) {
      return schema.getType() == Schema.Type.NULL;
    }
  };
  private static final Set<String> IGNORE_NAMESPACES = ImmutableSet.of(
    "http://www.w3.org/2000/xmlns/", "http://www.w3.org/2001/XMLSchema-instance"
  );

  private final TimeZone timezone;
  private final DataMasker dataMasker;

  public StructuredRecordCreator(DataMasker dataMasker) {
    this(dataMasker, TimeZone.getDefault());
  }

  public StructuredRecordCreator(DataMasker dataMasker, TimeZone timezone) {
    this.dataMasker = dataMasker;
    this.timezone = timezone;
  }

  public StructuredRecord create(Document document, Schema avroSchema,
                                 io.cdap.cdap.api.data.schema.Schema schema) {
    Preconditions.checkArgument(avroSchema.getType() == Schema.Type.RECORD, "Schema must be of type RECORD");
    Element docElement = document.getDocumentElement();
    return createValue(docElement, avroSchema, schema, "");
  }

  @SuppressWarnings("unchecked")
  @Nullable
  private <T> T createValue(Node node, Schema avroSchema, io.cdap.cdap.api.data.schema.Schema schema, String path) {
    switch (avroSchema.getType()) {
      case NULL:
        return null;
      case BOOLEAN: {
        String content = node.getTextContent();
        return Strings.isNullOrEmpty(content) ? null : (T) Boolean.valueOf(DatatypeConverter.parseBoolean(content));
      }
      case INT: {
        String content = node.getTextContent();
        return Strings.isNullOrEmpty(content) ? null : (T) Integer.valueOf(DatatypeConverter.parseInt(content));
      }
      case LONG:
        return (T) getLong(node.getTextContent());
      case FLOAT: {
        String content = node.getTextContent();
        return Strings.isNullOrEmpty(content) ? null : (T) Float.valueOf(DatatypeConverter.parseFloat(content));
      }
      case DOUBLE: {
        String content = node.getTextContent();
        return Strings.isNullOrEmpty(content) ? null : (T) Double.valueOf(DatatypeConverter.parseDouble(content));
      }
      case STRING:
        return (T) node.getTextContent();
      case ARRAY: {
        List<Object> array = new ArrayList<>();
        Node element = node;
        while (element != null) {
          array.add(createValue(element, avroSchema.getElementType(), schema.getComponentSchema(), path));
          element = getNextArrayElement(element);
        }
        return (T) array;
      }
      case RECORD: {
        StructuredRecord.Builder builder = StructuredRecord.builder(SchemaConverter.fromAvro(avroSchema));
        PeekingIterator<Node> childIterator = new ElementNodeIterator(node.getChildNodes());

        for (Schema.Field field : avroSchema.getFields()) {
          Schema fieldSchema = field.schema();
          String newPath = appendPath(path, field.name());

          // Look for the field in the attribute first
          if (setFieldByAttribute(node.getAttributes(), field, builder, newPath)) {
            continue;
          }

          // If it is the simpleContent body field, get it from the content nodes
          if (Boolean.parseBoolean(field.getProp(Source.SIMPLE_CONTENT))) {
            builder.convertAndSet(field.name(), getTextOnlyContent(node));
            continue;
          }

          // If no more child node, try to set the field to null
          if (!childIterator.hasNext()) {
            setField(builder, avroSchema.getName(), field, null, newPath);
            continue;
          }

          // See if the field comes from the given node
          String fieldSource = new Source(childIterator.peek().getLocalName()).toString();

          if (fieldSource.equals(field.getProp(Source.SOURCE))) {
            setField(builder, avroSchema.getName(), field, createValue(childIterator.next(), fieldSchema,
                                                                       schema.getField(field.name()).getSchema(),
                                                                       newPath), newPath);
          } else if (field.name().equals(Source.WILDCARD)) {
            // This is the special case for xsd:any type
            setField(builder, avroSchema.getName(), field, getFullTextContent(childIterator.next()), newPath);
          } else {
            setField(builder, avroSchema.getName(), field, null, newPath);
          }
        }
        return (T) builder.build();
      }
      case UNION: {
        Schema matchingAvroSchema = null;
        int matchingIdx = 0;

        for (int i = 0; i < avroSchema.getTypes().size(); i++) {
          matchingAvroSchema = avroSchema.getTypes().get(i);
          matchingIdx = i;

          switch (matchingAvroSchema.getType()) {
            case NULL:
              continue;
            case RECORD:
              if (matchingAvroSchema.getName().equals(node.getLocalName())) {
                break;
              }
              break;
            case ARRAY: {
              Schema componentSchema = matchingAvroSchema.getElementType();
              if (componentSchema.getType() == Schema.Type.RECORD
                && componentSchema.getName().equals(node.getLocalName())) {
                break;
              }
            }
          }
        }
        if (matchingAvroSchema == null || matchingAvroSchema.getType() == Schema.Type.NULL) {
          throw new IllegalArgumentException("Failed to find a ");
        }
        return createValue(node, matchingAvroSchema, schema.getUnionSchemas().get(matchingIdx), path);
      }

      case FIXED:
      case BYTES:
      case ENUM:
      case MAP:
        throw new IllegalArgumentException("Schema type " + avroSchema.getType().getName() +
                                             " is not supported in XML");
    }

    // This shouldn't happen
    throw new IllegalArgumentException("Unsupported type " + avroSchema.getType());
  }

  private String appendPath(String oldPath, String addn) {
    if (Strings.isNullOrEmpty(oldPath)) {
      return addn;
    }
    return String.format("%s.%s", oldPath, addn);
  }

  @Nullable
  private Long getLong(@Nullable String content) {
    if (Strings.isNullOrEmpty(content)) {
      return null;
    }
    try {
      return DatatypeConverter.parseLong(content);
    } catch (NumberFormatException e) {
      // If failed to parse it as a long, assume it is the xs:dateTime format, as it is what generated by the
      // SchemaBuilder
      Calendar calendar = DatatypeConverter.parseDateTime(content);
      calendar.setTimeZone(timezone);
      return calendar.getTimeInMillis();
    }
  }

  private String getFullTextContent(Node node) {
    if (node.getTextContent().length() == 0) {
      return "";
    }

    StringWriter writer = new StringWriter();
    try {
      Transformer transformer = TransformerFactory.newInstance().newTransformer();
      transformer.setOutputProperty(OutputKeys.METHOD, "xml");
      transformer.setOutputProperty(OutputKeys.OMIT_XML_DECLARATION, "yes");
      transformer.transform(new DOMSource(node), new StreamResult(writer));
    } catch (TransformerException e) {
      // This shouldn't happen
      throw new IllegalArgumentException(e);
    }

    return writer.toString();
  }

  /**
   * Returns the text content of the immediate text or cdap nodes of the given node.
   */
  private String getTextOnlyContent(Node node) {
    StringBuilder content = new StringBuilder();

    NodeList children = node.getChildNodes();
    for (int i = 0; i < children.getLength(); i++) {
      Node child = children.item(i);
      switch (child.getNodeType()) {
        case Node.TEXT_NODE:
        case Node.CDATA_SECTION_NODE:
          content.append(child.getTextContent());
      }
    }
    return content.toString();
  }

  private boolean setFieldByAttribute(NamedNodeMap attributes, Schema.Field field, StructuredRecord.Builder builder,
                                      String path) {
    for (int i = 0; i < attributes.getLength(); i++) {
      Node attribute = attributes.item(i);
      String attributeSource = new Source(attribute.getLocalName(), true).toString();
      if (IGNORE_NAMESPACES.contains(attribute.getNamespaceURI())
        || !attributeSource.equals(field.getProp(Source.SOURCE))) {
        continue;
      }
      if (hasType(field.schema(), Schema.Type.LONG)) {
        builder.set(field.name(), getLong(attribute.getTextContent()));
      } else {
        String value = attribute.getTextContent();
        if (field.schema().getType().equals(Schema.Type.STRING)) {
          value = checkAndReplace(path, value);
        }
        builder.convertAndSet(field.name(), value);
      }
      return true;
    }
    return false;
  }

  private <T> void setField(StructuredRecord.Builder builder, String recordName, Schema.Field field, T value,
                            String path) {
    // If it is not null, just set it.
    if (value != null) {
      if (field.schema().getType().equals(Schema.Type.STRING)) {
        //noinspection unchecked
        value = (T) checkAndReplace(path, (String) value);
      }
      builder.set(field.name(), value);
      return;
    }

    // Otherwise set null/empty collection if allowed
    Schema fieldSchema = field.schema();
    if (isNullable(fieldSchema)) {
      builder.set(field.name(), null);
    } else if (fieldSchema.getType() == Schema.Type.ARRAY) {
      builder.set(field.name(), Collections.emptyList());
    } else if (fieldSchema.getType() == Schema.Type.MAP) {
      builder.set(field.name(), Collections.emptyMap());
    } else {
      throw new IllegalArgumentException("Null value is not allowed for field " + field.name() +
                                           " in record " + recordName + " at path " + path);
    }
  }

  private String checkAndReplace(String path, String value) {
    for (MaskInfo maskInfo : dataMasker.getDataMasks()) {
      if (maskInfo.getFullPath().equals(path)) {
        return maskInfo.doReplace(value);
      }
    }
    return value;
  }

  /**
   * Returns the sibling element {@link Node} of the given {@link Node} which has the same element name.
   */
  @Nullable
  private Node getNextArrayElement(Node node) {
    Node element = node.getNextSibling();
    while (element != null && !hasSameElementName(node, element)) {
      element = element.getNextSibling();
    }
    return element;
  }

  /**
   * Returns {@code true} if both nodes are elements and has the same name.
   */
  private boolean hasSameElementName(Node node1, Node node2) {
    return node1.getNodeType() == Node.ELEMENT_NODE
      && node2.getNodeType() == Node.ELEMENT_NODE &&
      Objects.equals(node1.getLocalName(), node2.getLocalName())
      && Objects.equals(node1.getNamespaceURI(), node2.getNamespaceURI());
  }

  private boolean isNullable(Schema schema) {
    return schema.getType() == Schema.Type.NULL
      || (schema.getType() == Schema.Type.UNION && Iterables.any(schema.getTypes(), IS_NULL_TYPE));
  }

  private boolean hasType(Schema schema, Schema.Type type) {
    if (schema.getType() == type) {
      return true;
    }
    if (schema.getType() != Schema.Type.UNION) {
      return false;
    }
    for (Schema s : schema.getTypes()) {
      if (s.getType() == type) {
        return true;
      }
    }
    return false;
  }

  /**
   * A {@link PeekingIterator} that iterates XML {@link Node} of {@link Node#ELEMENT_NODE} type.
   */
  private static final class ElementNodeIterator implements PeekingIterator<Node> {

    private final NodeList nodeList;
    private int idx;

    private ElementNodeIterator(NodeList nodeList) {
      this.nodeList = nodeList;
    }

    @Override
    public Node peek() {
      if (!hasNext()) {
        throw new NoSuchElementException();
      }
      return nodeList.item(idx);
    }

    @Override
    public boolean hasNext() {
      while (idx < nodeList.getLength() && nodeList.item(idx).getNodeType() != Node.ELEMENT_NODE) {
        idx++;
      }
      return idx < nodeList.getLength();
    }

    @Override
    public Node next() {
      if (!hasNext()) {
        throw new NoSuchElementException();
      }
      return nodeList.item(idx++);
    }

    @Override
    public void remove() {
      throw new UnsupportedOperationException();
    }
  }
}
