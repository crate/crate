package org.cratedb.action.export.parser;

import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.search.SearchParseElement;
import org.elasticsearch.search.internal.SearchContext;

import org.cratedb.action.export.ExportContext;

/**
 * Parser for token ``settings``. Makes sense if output_file was defined.
 */
public class ExportSettingsParseElement implements SearchParseElement {

    @Override
    public void parse(XContentParser parser, SearchContext context)
            throws Exception {
        XContentParser.Token token = parser.currentToken();
        if (token.isValue()) {
            ((ExportContext)context).settings(parser.booleanValue());
        }
    }

}
