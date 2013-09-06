package org.cratedb.action.reindex;

import java.util.HashMap;
import java.util.Map;

import com.google.common.collect.ImmutableMap;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.search.SearchParseElement;
import org.elasticsearch.search.SearchParseException;
import org.elasticsearch.search.fetch.FetchPhase;
import org.elasticsearch.search.fetch.explain.ExplainParseElement;
import org.elasticsearch.search.query.QueryPhase;

import org.cratedb.action.searchinto.SearchIntoContext;
import org.cratedb.action.searchinto.parser.AbstractSearchIntoParser;
import org.cratedb.action.searchinto.parser.ISearchIntoParser;

/**
 * Parser for pay load given to _reindex action.
 */
public class ReindexParser extends AbstractSearchIntoParser implements ISearchIntoParser {

    private final ImmutableMap<String, SearchParseElement> elementParsers;

    @Inject
    public ReindexParser(QueryPhase queryPhase, FetchPhase fetchPhase) {
        Map<String, SearchParseElement> elementParsers = new HashMap<String,
                SearchParseElement>();
        elementParsers.putAll(queryPhase.parseElements());
        elementParsers.put("explain", new ExplainParseElement());
        this.elementParsers = ImmutableMap.copyOf(elementParsers);
    }

    @Override
    protected ImmutableMap<String, SearchParseElement> getElementParsers() {
        return elementParsers;
    }

    @Override
    public void parseSource(SearchIntoContext context, BytesReference source)
            throws SearchParseException {
        context.fieldNames().add("_id");
        context.fieldNames().add("_source");
        context.outputNames().put("_id", "_id");
        context.outputNames().put("_source", "_source");
        super.parseSource(context, source);
    }
}
