package crate.elasticsearch.action.export.parser;

import crate.elasticsearch.action.export.ExportContext;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.search.SearchParseElement;
import org.elasticsearch.search.internal.SearchContext;

/**
 * Parser for token ``output_file``. The value of the token must be a String.
 * <p/>
 * <pre>
 *     "output_file": "/tmp/out"
 * </pre>
 */
public class ExportOutputFileParseElement implements SearchParseElement {

    @Override
    public void parse(XContentParser parser, SearchContext context) throws Exception {
        XContentParser.Token token = parser.currentToken();
        if (token.isValue()) {
            ((ExportContext) context).outputFile(parser.text());
        }
    }
}
