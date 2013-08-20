package crate.elasticsearch.action.export.parser;

import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.search.SearchParseElement;
import org.elasticsearch.search.SearchParseException;
import org.elasticsearch.search.internal.SearchContext;

import crate.elasticsearch.action.export.ExportContext;

public class ExportCompressionParseElement implements SearchParseElement {

    @Override
    public void parse(XContentParser parser, SearchContext context)
            throws Exception {
        XContentParser.Token token = parser.currentToken();
        if (token.isValue()) {
            String lower = parser.text().toLowerCase();
            if (lower.equals("gzip")) {
                ((ExportContext) context).compression(true);
            } else if (!lower.isEmpty()) {
                throw new SearchParseException(context,
                        "Compression format '" + lower + "' unknown or not supported.");
            }
        }
    }

}
