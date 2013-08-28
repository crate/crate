package crate.elasticsearch.action.import_.parser;

import org.elasticsearch.common.xcontent.XContentParser;

import crate.elasticsearch.action.import_.ImportContext;

public class ImportCompressionParseElement implements ImportParseElement {

    @Override
    public void parse(XContentParser parser, ImportContext context)
            throws Exception {
        XContentParser.Token token = parser.currentToken();
        if (token.isValue()) {
            String lower = parser.text().toLowerCase();
            if (lower.equals("gzip")) {
                ((ImportContext) context).compression(true);
            } else if (!lower.isEmpty()) {
                throw new ImportParseException(context,
                        "Compression format '" + lower + "' unknown or not supported.");
            }
        }
    }

}
