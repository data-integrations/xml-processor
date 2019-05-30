package io.cdap.plugins.xml;

import com.google.common.base.Throwables;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import org.apache.avro.Schema;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.MRJobConfig;
import org.apache.xerces.dom.DOMInputImpl;
import org.apache.xerces.impl.xs.XMLSchemaLoader;
import org.apache.xerces.jaxp.validation.XMLSchemaFactory;
import org.apache.xerces.util.XMLGrammarPoolImpl;
import org.apache.xerces.xni.XMLResourceIdentifier;
import org.apache.xerces.xni.XNIException;
import org.apache.xerces.xni.grammars.Grammar;
import org.apache.xerces.xni.grammars.XMLGrammarDescription;
import org.apache.xerces.xni.grammars.XMLGrammarPool;
import org.apache.xerces.xni.parser.XMLEntityResolver;
import org.apache.xerces.xni.parser.XMLErrorHandler;
import org.apache.xerces.xni.parser.XMLInputSource;
import org.apache.xerces.xni.parser.XMLParseException;
import org.apache.xerces.xs.XSModel;
import org.apache.xerces.xs.XSNamespaceItemList;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.xml.sax.SAXException;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

/**
 *
 */
public class SchemaProvider {

  private static final Logger LOG = LoggerFactory.getLogger(SchemaProvider.class);

  private final LoadingCache<Path, SchemaInfo> schemaCache;
  private final FileSystem fs;

  public SchemaProvider(final boolean ignoreXsdError) throws IOException {
    Configuration hConf = new Configuration();
    hConf.addResource(new Path(MRJobConfig.JOB_CONF_FILE));
    this.fs = FileSystem.get(hConf);
    this.schemaCache = CacheBuilder.newBuilder()
      .build(new CacheLoader<Path, SchemaInfo>() {
        @Override
        public SchemaInfo load(Path path) throws Exception {
          return new SchemaInfo(loadXSModel(path, ignoreXsdError));
        }
      });
  }

  public SchemaInfo getSchemaInfo(Path path) throws IOException {
    try {
      return schemaCache.get(fs.makeQualified(path));
    } catch (ExecutionException e) {
      Throwables.propagateIfPossible(e, IOException.class);
      throw Throwables.propagate(e);
    }
  }

  private XSModel loadXSModel(final Path path, final boolean ignoreError) throws Exception {
    XMLSchemaLoader loader = new XMLSchemaLoader();
    loader.setErrorHandler(new XMLErrorHandler() {
      @Override
      public void warning(String domain, String key, @Nonnull XMLParseException exception) throws XNIException {
        if (ignoreError) {
          LOG.warn("Exception raised when parsing xsd: [warn] {} {} {}", domain, key, exception.toString());
        } else {
          throw exception;
        }
      }

      @Override
      public void error(String domain, String key, @Nonnull XMLParseException exception) throws XNIException {
        if (ignoreError) {
          LOG.warn("Exception raised when parsing xsd: [error] {} {} {}", domain, key, exception.toString());
        } else {
          throw exception;
        }
      }

      @Override
      public void fatalError(String domain, String key, @Nonnull XMLParseException exception) throws XNIException {
        if (ignoreError) {
          LOG.warn("Exception raised when parsing xsd: [fatalError] {} {} {}", domain, key, exception.toString());
        } else {
          throw exception;
        }
      }
    });

    // This is needed because the XMLSchemaLoader resolves resource against current directory
    final URI userURI = new File(System.getProperty("user.dir")).toURI();

    loader.setEntityResolver(new XMLEntityResolver() {
      @Override
      public XMLInputSource resolveEntity(XMLResourceIdentifier id) throws XNIException, IOException {
        String resourceURI = id.getExpandedSystemId();
        if (resourceURI == null) {
          return null;
        }
        try {
          URI relativeURI = userURI.relativize(new URI(resourceURI));
          Path resourcePath = new Path(path.getParent(), relativeURI.getPath());
          XMLInputSource source = new XMLInputSource(id);
          source.setByteStream(fs.open(resourcePath));
          return source;
        } catch (Exception e) {
          return null;
        }
      }
    });

    // Load the XSD
    DOMInputImpl input = new DOMInputImpl();
    input.setByteStream(fs.open(path));
    return loader.load(input);
  }

  /**
   * Contains information about XML schema and the corresponding Avro schema.
   */
  public static final class SchemaInfo {
    private final javax.xml.validation.Schema xmlSchema;
    private final Schema unionSchema;
    private final Map<String, Schema> schemas;

    private SchemaInfo(XSModel xsModel) throws SAXException {
      XSNamespaceItemList nsItemList = xsModel.getNamespaceItems();
      Grammar[] grammars = (Grammar[]) nsItemList.toArray(new Grammar[nsItemList.getLength()]);
      XMLGrammarPool pool = new XMLGrammarPoolImpl();
      pool.cacheGrammars(XMLGrammarDescription.XML_SCHEMA, grammars);

      this.xmlSchema = new XMLSchemaFactory().newSchema(pool);
      this.schemas = new HashMap<>();

      this.unionSchema = new SchemaBuilder().createSchema(xsModel);

      // the createSchema always return an union schema
      for (Schema schema : unionSchema.getTypes()) {
        if (schema.getType() != Schema.Type.RECORD || schema.getFields().isEmpty()) {
          continue;
        }
        schemas.put(schema.getName(), schema);
      }
    }

    public javax.xml.validation.Schema getXmlSchema() {
      return xmlSchema;
    }

    /**
     * Returns the schema generated for the given document element name.
     */
    @Nullable
    public Schema getSchema(String docElementName) {
      String docSource = new Source(docElementName).toString();
      for (Schema schema : unionSchema.getTypes()) {
        if (docElementName.equals(schema.getName()) || docSource.equals(schema.getProp(Source.SOURCE))) {
          return schema;
        }
      }

      return null;
    }

    public Schema getUnionSchema() {
      return unionSchema;
    }
  }
}
