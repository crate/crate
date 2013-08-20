package crate.elasticsearch.action.searchinto.parser;

import java.util.HashMap;
import java.util.Map;

import org.elasticsearch.common.collect.ImmutableMap;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.index.mapper.FieldMapper;
import org.elasticsearch.search.SearchParseElement;
import org.elasticsearch.search.SearchParseException;
import org.elasticsearch.search.fetch.FetchPhase;
import org.elasticsearch.search.fetch.explain.ExplainParseElement;
import org.elasticsearch.search.query.QueryPhase;

import crate.elasticsearch.action.searchinto.SearchIntoContext;

/**
 * Parser for payload given to _search_into action.
 */
public class SearchIntoParser extends AbstractSearchIntoParser implements ISearchIntoParser {

    private final ImmutableMap<String, SearchParseElement> elementParsers;

    @Inject
    public SearchIntoParser(QueryPhase queryPhase, FetchPhase fetchPhase) {
        Map<String, SearchParseElement> elementParsers = new HashMap<String,
                SearchParseElement>();
        elementParsers.putAll(queryPhase.parseElements());
        elementParsers.put("fields", new FieldsParseElement());
        elementParsers.put("targetNodes", new TargetNodesParseElement());
        elementParsers.put("explain", new ExplainParseElement());
        this.elementParsers = ImmutableMap.copyOf(elementParsers);
    }

    @Override
    protected void validate(SearchIntoContext context) {
        if (!context.hasFieldNames()) {
            throw new SearchParseException(context, "No fields defined");
        }

        for (String field : context.fieldNames()) {
            FieldMapper<?> mapper = context.mapperService().smartNameFieldMapper(
                    field);
            if (mapper == null && !field.equals(
                    "_version") && !field.startsWith(
                    FieldsParseElement.SCRIPT_FIELD_PREFIX)) {
                throw new SearchParseException(context,
                        "SearchInto field [" + field + "] does not exist in " +
                                "the mapping");
            }
        }
        super.validate(context);
    }

    @Override
    protected ImmutableMap<String, SearchParseElement> getElementParsers() {
        return elementParsers;
    }


}