package crate.elasticsearch.action.import_.parser;

import java.util.regex.Pattern;

import org.elasticsearch.common.xcontent.XContentParser;

import crate.elasticsearch.action.import_.ImportContext;

public class FilePatternParseElement implements ImportParseElement {

    @Override
    public void parse(XContentParser parser, ImportContext context)
            throws Exception {
        XContentParser.Token token = parser.currentToken();
        if (token.isValue()) {
            Pattern p = Pattern.compile(parser.text());
            context.file_pattern(p);
        }
    }

}
