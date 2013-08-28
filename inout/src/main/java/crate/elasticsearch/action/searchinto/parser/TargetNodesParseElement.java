package crate.elasticsearch.action.searchinto.parser;

import crate.elasticsearch.action.searchinto.SearchIntoContext;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.search.SearchParseElement;
import org.elasticsearch.search.internal.SearchContext;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * parses the targetNode field which looks like
 * <p/>
 * "targetNode": ["host:9300", "host:9301"]
 *  <p/>
 * or
 *  <p/>
 * "targetNode": "host:9300"
 *
 */
public class TargetNodesParseElement implements SearchParseElement {

    private Pattern PATTERN = Pattern.compile("^\\s*(.*?):(\\d+)\\s*$");

    @Override
    public void parse(XContentParser parser, SearchContext context) throws Exception {
        XContentParser.Token token = parser.currentToken();
        if (token == XContentParser.Token.START_ARRAY) {
            boolean added = false;
            SearchIntoContext ctx = (SearchIntoContext)context;
            while ((token = parser.nextToken()) != XContentParser.Token.END_ARRAY) {
                addAddress(ctx, parser.text());
            }
            if (!added) {
                ctx.emptyTargetNodes();
            }
        } else if (token == XContentParser.Token.VALUE_STRING) {
            addAddress((SearchIntoContext)context, parser.text());
        }
    }

    private void addAddress(SearchIntoContext context, String nodeAddress) {
        Matcher m = PATTERN.matcher(nodeAddress);
        if (m.matches()) {
            String host = m.group(1);
            int port = Integer.parseInt(m.group(2));
            InetSocketTransportAddress isa = new InetSocketTransportAddress(host, port);
            context.targetNodes().add(isa);
        } else {
            throw new InvalidNodeAddressException(context, nodeAddress);
        }
    }
}
