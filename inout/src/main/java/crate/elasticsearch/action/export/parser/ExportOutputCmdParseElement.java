package crate.elasticsearch.action.export.parser;

import crate.elasticsearch.action.export.ExportContext;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.search.SearchParseElement;
import org.elasticsearch.search.internal.SearchContext;

import java.util.ArrayList;
import java.util.List;

/**
 * Parser for token ``output_cmd``. The value of the token might be a String
 * containing the command or an array containing the command and all
 * arguments as seperated parts.
 * <p/>
 * <pre>
 * "output_cmd": "gzip > /tmp/out"
 *
 * or
 *
 * "output_cmd": ["gzip", ">" "/tmp/out"]
 * </pre>
 */
public class ExportOutputCmdParseElement implements SearchParseElement {

    @Override
    public void parse(XContentParser parser, SearchContext context) throws Exception {
        XContentParser.Token token = parser.currentToken();
        if (token.isValue()) {
            ((ExportContext) context).outputCmd(parser.text());
        } else if (token == XContentParser.Token.START_ARRAY) {
            List<String> cmds = new ArrayList<String>(4);
            while ((token = parser.nextToken()) != XContentParser.Token.END_ARRAY) {
                cmds.add(parser.text());
            }
            ((ExportContext) context).outputCmdArray(cmds);
        }
    }
}
