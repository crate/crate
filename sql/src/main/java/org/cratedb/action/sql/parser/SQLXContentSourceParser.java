package org.cratedb.action.sql.parser;

import com.google.common.collect.ImmutableMap;
import org.cratedb.sql.SQLParseException;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentHelper;
import org.elasticsearch.common.xcontent.XContentParser;

/**
 * Parser for SQL statements in JSON and other XContent formats
 * <p/>
 * {
 * "stmt": "select * from...."
 * }
 */
public class SQLXContentSourceParser {

    private final SQLXContentSourceContext context;

    static final class Fields {
        static final String STMT = "stmt";
        static final String ARGS = "args";
    }

    private static final ImmutableMap<String, SQLParseElement> elementParsers = ImmutableMap.of(
            Fields.STMT, (SQLParseElement) new SQLStmtParseElement(),
            Fields.ARGS, (SQLParseElement) new SQLArgsParseElement()
    );

    public SQLXContentSourceParser(SQLXContentSourceContext context) {
        this.context = context;
    }

    private void validate() throws SQLParseSourceException {
        if (context.stmt() == null) {
            throw new SQLParseSourceException(context, "Field [stmt] was not defined");
        }
    }

    public void parseSource(BytesReference source) throws SQLParseException {
        XContentParser parser = null;
        try {
            if (source != null && source.length() != 0) {
                parser = XContentFactory.xContent(source).createParser(source);
                parse(parser);
            }
            validate();
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

    public void parse(XContentParser parser) throws Exception {
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
}
