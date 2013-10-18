package org.cratedb.action;

import com.google.common.collect.ImmutableMap;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.search.SearchParseElement;
import org.elasticsearch.search.SearchParseException;
import org.elasticsearch.search.internal.SearchContext;
import org.elasticsearch.search.query.QueryPhase;

import java.util.Map;

import static com.google.common.collect.Maps.newHashMap;

/**
 * Used to populate the SearchContext (to build a lucene Query)
 * using a BytesReference that contains a XContent Query
 *
 * Utilizes the QueryPhase elementParsers for parsing the "query", "filter", ... elements...
 */
public class SQLXContentQueryParser {

    private final ImmutableMap<String, SearchParseElement> elementParsers;

    @Inject
    public SQLXContentQueryParser(QueryPhase queryPhase) {
        Map<String, SearchParseElement> elementParsers = newHashMap();
        elementParsers.putAll(queryPhase.parseElements());
        this.elementParsers = ImmutableMap.copyOf(elementParsers);
    }

    public void parse(SearchContext context, BytesReference xcontentQuery) throws Exception {
        XContentParser parser = null;

        try {
            if (xcontentQuery == null) {
                return;
            }
            parser = XContentFactory.xContent(xcontentQuery).createParser(xcontentQuery);
            XContentParser.Token token;
            while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
                if (token == XContentParser.Token.FIELD_NAME) {
                    String fieldName = parser.currentName();
                    parser.nextToken();
                    SearchParseElement element = elementParsers.get(fieldName);
                    if (element == null) {
                        throw new SearchParseException(context,
                            "No parser for element [" + fieldName + "]");
                    }
                    element.parse(parser, context);
                } else if (token == null) {
                    break;
                }
            }
        } finally {
            if (parser != null) {
                parser.close();
            }
        }
    }
}
