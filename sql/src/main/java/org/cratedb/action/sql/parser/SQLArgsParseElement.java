package org.cratedb.action.sql.parser;

import org.elasticsearch.common.xcontent.XContentParser;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class SQLArgsParseElement implements SQLParseElement {

    @Override
    public void parse(XContentParser parser, SQLXContentSourceContext context) throws Exception {
        XContentParser.Token token = parser.currentToken();

        if (token != XContentParser.Token.START_ARRAY) {
            throw new SQLParseSourceException(context, "Field [" + parser.currentName() + "] has an invalid value");
        }

        Object[] params = parseSubArray(context, parser);
        context.args(params);
    }

    private Object[] parseSubArray(SQLXContentSourceContext context, XContentParser parser)
        throws IOException
    {
        XContentParser.Token token;
        List<Object> subList = new ArrayList<Object>();

        while ((token = parser.nextToken()) != XContentParser.Token.END_ARRAY) {
            if (token.isValue()) {
                subList.add(parser.objectText());
            } else if (token == XContentParser.Token.START_ARRAY) {
                subList.add(parseSubArray(context, parser));
            } else if (token == XContentParser.Token.START_OBJECT) {
                subList.add(parser.map());
            } else {
                throw new SQLParseSourceException(context, "Field [" + parser.currentName() + "] has an invalid value");
            }
        }

        return subList.toArray(new Object[subList.size()]);
    }
}
