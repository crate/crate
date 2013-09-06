package org.cratedb.action.sql.parser;

import org.elasticsearch.common.xcontent.XContentParser;

/**
 * used to parse the "stmt" element that is expected to be in requests parsed by the {@link org.cratedb.action.sql.parser
 * .SQLXContentSourceParser}
 * <p/>
 * Fills the stmt in the {@link org.cratedb.action.sql.parser.SQLXContentSourceContext}.
 */
public class SQLStmtParseElement implements SQLParseElement {

    @Override
    public void parse(XContentParser parser, SQLXContentSourceContext context) throws Exception {
        XContentParser.Token token = parser.currentToken();

        if (!token.isValue()) {
            throw new SQLParseSourceException(context, "Field [" + parser.currentName() + "] has an invalid value");
        }
        String stmt = parser.text();
        if (stmt == null || stmt.length() == 0) {
            throw new SQLParseSourceException(context, "Field [" + parser.currentName() + "] has no value");
        }
        context.stmt(parser.text());
    }
}
