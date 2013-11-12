package org.cratedb.action.import_.parser;

import org.elasticsearch.common.xcontent.XContentParser;

import org.cratedb.action.import_.ImportContext;

public class PathParseElement implements ImportParseElement {

    @Override
    public void parse(XContentParser parser, ImportContext context)
            throws Exception {
        XContentParser.Token token = parser.currentToken();
        if (token.isValue()) {
            context.path(parser.text());
        }
    }

}
