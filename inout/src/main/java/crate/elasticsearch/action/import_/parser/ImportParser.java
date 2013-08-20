package crate.elasticsearch.action.import_.parser;

import java.util.HashMap;
import java.util.Map;

import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.collect.ImmutableMap;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.search.SearchParseException;

import crate.elasticsearch.action.import_.ImportContext;

public class ImportParser implements IImportParser {

    private final ImmutableMap<String, ImportParseElement> elementParsers;

    public ImportParser() {
        Map<String, ImportParseElement> elementParsers = new HashMap<String, ImportParseElement>();
        elementParsers.put("directory", new DirectoryParseElement());
        elementParsers.put("compression", new ImportCompressionParseElement());
        elementParsers.put("file_pattern", new FilePatternParseElement());
        elementParsers.put("mappings", new ImportMappingsParseElement());
        elementParsers.put("settings", new ImportSettingsParseElement());
        this.elementParsers = ImmutableMap.copyOf(elementParsers);
    }

    /**
     * Main method of this class to parse given payload of _export action
     *
     * @param context
     * @param source
     * @throws SearchParseException
     */
    public void parseSource(ImportContext context, BytesReference source) throws ImportParseException {
        XContentParser parser = null;
        try {
            if (source != null && source.length() != 0) {
                parser = XContentFactory.xContent(source).createParser(source);
                XContentParser.Token token;
                while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
                    if (token == XContentParser.Token.FIELD_NAME) {
                        String fieldName = parser.currentName();
                        parser.nextToken();
                        ImportParseElement element = elementParsers.get(fieldName);
                        if (element == null) {
                            throw new ImportParseException(context, "No parser for element [" + fieldName + "]");
                        }
                        element.parse(parser, context);
                    } else if (token == null) {
                        break;
                    }
                }
            }
            validate(context);
        } catch (Exception e) {
            String sSource = "_na_";
            try {
                sSource = XContentHelper.convertToJson(source, false);
            } catch (Throwable e1) {
                // ignore
            }
            throw new ImportParseException(context, "Failed to parse source [" + sSource + "]", e);
        } finally {
            if (parser != null) {
                parser.close();
            }
        }
    }

    /**
     * validate given payload
     *
     * @param context
     */
    private void validate(ImportContext context) {
        if (context.directory() == null || context.directory().isEmpty()) {
            throw new ImportParseException(context, "No directory defined");
        }
    }
}
