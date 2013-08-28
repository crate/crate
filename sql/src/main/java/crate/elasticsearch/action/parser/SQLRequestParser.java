package crate.elasticsearch.action.parser;

import com.google.common.collect.ImmutableMap;
import crate.elasticsearch.sql.SQLParseException;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.search.SearchParseException;

import java.util.HashMap;
import java.util.Map;

/**
 * Parser for SQL statements in JSON format
 *
 * {
 *     "stmt": "select * from...."
 * }
 */
public class SQLRequestParser {

    private final ImmutableMap<String, SQLParseElement> elementParsers;

    static final class Fields {
        static final String STMT = "stmt";
    }

    public SQLRequestParser() {
        /**
         * The stock elasticsearch parsers make use of SearchParseElements and a SearchContext;
         * But the SQLRequestParser here operates on a higher level and hasn't access to the SearchContext
         *
         * Therefore SqlParseElements and an SQLContext is used.
         */

        Map<String, SQLParseElement> elementParsers = new HashMap<String, SQLParseElement>();
        elementParsers.put(Fields.STMT, new SQLStmtParseElement());

        this.elementParsers = ImmutableMap.copyOf(elementParsers);
    }

    private void validate(SQLContext context) throws SearchParseException {
    }

    public void parseSource(SQLContext context, BytesReference source) throws SQLParseException {
        XContentParser parser = null;
        try {
            if (source != null && source.length() != 0) {
                parser = XContentFactory.xContent(source).createParser(source);
                XContentParser.Token token;
                while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
                    if (token == XContentParser.Token.FIELD_NAME) {
                        String fieldName = parser.currentName();
                        parser.nextToken();
                        SQLParseElement element = elementParsers.get(fieldName);
                        if (element == null) {
                            throw new SQLParseException("No parser for element [" + fieldName + "]");
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
            throw new SQLParseException("Failed to parse source [" + sSource + "]", e);
        } finally {
            if (parser != null) {
                parser.close();
            }
        }
    }
}
