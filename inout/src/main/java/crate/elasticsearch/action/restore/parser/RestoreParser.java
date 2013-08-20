package crate.elasticsearch.action.restore.parser;

import crate.elasticsearch.action.dump.parser.DumpParser;
import crate.elasticsearch.action.import_.ImportContext;
import crate.elasticsearch.action.import_.parser.DirectoryParseElement;
import crate.elasticsearch.action.import_.parser.IImportParser;
import crate.elasticsearch.action.import_.parser.ImportParseElement;
import crate.elasticsearch.action.import_.parser.ImportParseException;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.collect.ImmutableMap;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.common.xcontent.XContentParser;

import java.util.HashMap;
import java.util.Map;
import java.util.regex.Pattern;

public class RestoreParser implements IImportParser {

    private final ImmutableMap<String, ImportParseElement> elementParsers;

    public static final String FILE_PATTERN = ".*_.*_.*\\.json\\.gz";

    public RestoreParser() {
        Map<String, ImportParseElement> elementParsers = new HashMap<String, ImportParseElement>();
        elementParsers.put("directory", new DirectoryParseElement());
        this.elementParsers = ImmutableMap.copyOf(elementParsers);
    }

    /**
     * Main method of this class to parse given payload of _restore action
     *
     * @param context
     * @param source
     * @throws org.elasticsearch.search.SearchParseException
     */
    public void parseSource(ImportContext context, BytesReference source) throws ImportParseException {
        XContentParser parser = null;
        this.setDefaults(context);
        context.settings(true);
        context.mappings(true);
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
            if (context.directory() == null) {
                context.directory(DumpParser.DEFAULT_DIR);
            }
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
     * Set restore specific default values to the context like compression and file_pattern
     *
     * @param context
     */
    private void setDefaults(ImportContext context) {
        context.compression(true);
        Pattern p = Pattern.compile(FILE_PATTERN);
        context.file_pattern(p);
    }
}
