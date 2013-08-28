package crate.elasticsearch.action.import_.parser;

import org.elasticsearch.common.xcontent.XContentParser;

import crate.elasticsearch.action.import_.ImportContext;

/**
 * Parser for token ``settings``.
 */
public class ImportSettingsParseElement implements ImportParseElement {

    @Override
    public void parse(XContentParser parser, ImportContext context)
            throws Exception {
        XContentParser.Token token = parser.currentToken();
        if (token.isValue()) {
            ((ImportContext)context).settings(parser.booleanValue());
        }
    }

}
