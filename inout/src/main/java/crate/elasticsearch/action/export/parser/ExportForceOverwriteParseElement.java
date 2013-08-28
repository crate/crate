package crate.elasticsearch.action.export.parser;

import crate.elasticsearch.action.export.ExportContext;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.search.SearchParseElement;
import org.elasticsearch.search.internal.SearchContext;

/**
 * Parser for token ``force_overwrite``. Make sense if output_file was defined.
 */
public class ExportForceOverwriteParseElement implements SearchParseElement {

    @Override
    public void parse(XContentParser parser, SearchContext context) throws Exception {
        XContentParser.Token token = parser.currentToken();
        if (token.isValue()) {
            ((ExportContext)context).forceOverride(parser.booleanValue());
        }
    }
}
