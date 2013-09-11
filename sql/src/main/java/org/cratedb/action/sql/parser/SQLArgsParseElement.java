package org.cratedb.action.sql.parser;

import org.elasticsearch.common.xcontent.XContentParser;

import java.util.ArrayList;
import java.util.List;

public class SQLArgsParseElement implements SQLParseElement {

    @Override
    public void parse(XContentParser parser, SQLXContentSourceContext context) throws Exception {
        XContentParser.Token token = parser.currentToken();
        List<Object> params = new ArrayList<Object>();
        List<Object> subList;

        if (token != XContentParser.Token.START_ARRAY) {
            throw new SQLParseSourceException(context, "Field [" + parser.currentName() + "] has an invalid value");
        }

        while ((token = parser.nextToken()) != XContentParser.Token.END_ARRAY) {
            if (token.isValue()) {
                params.add(parser.objectText());
            } else if (token == XContentParser.Token.START_ARRAY) {
                subList = new ArrayList<Object>();
                while ((token = parser.nextToken()) != XContentParser.Token.END_ARRAY) {
                    if (token.isValue()) {
                        subList.add(parser.objectText());
                    } else {
                        throw new SQLParseSourceException(context, "Field [" + parser.currentName() + "] has an invalid value");
                    }
                }
                params.add(subList.toArray(new Object[subList.size()]));
            } else {
                throw new SQLParseSourceException(context, "Field [" + parser.currentName() + "] has an invalid value");
            }
        }

        context.args(params.toArray(new Object[params.size()]));
    }
}
