package crate.elasticsearch.action.export.parser;

import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.search.SearchParseElement;
import org.elasticsearch.search.internal.SearchContext;

import crate.elasticsearch.action.export.ExportContext;

/**
 * Parser for token ``mappings``. Makes sense if output_file was defined.
 */
public class ExportMappingsParseElement implements SearchParseElement {

    @Override
    public void parse(XContentParser parser, SearchContext context)
            throws Exception {
        XContentParser.Token token = parser.currentToken();
        if (token.isValue()) {
            ((ExportContext)context).mappings(parser.booleanValue());
        }
    }

}
