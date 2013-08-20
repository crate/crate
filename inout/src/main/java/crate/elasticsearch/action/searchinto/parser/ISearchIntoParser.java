package crate.elasticsearch.action.searchinto.parser;

import org.elasticsearch.common.bytes.BytesReference;

import crate.elasticsearch.action.searchinto.SearchIntoContext;

/**
 * Interface for search into parsers.
 *
 * Known implementations: @SearchIntoParser, @ReindexParser
 */
public interface ISearchIntoParser {

    /**
     * Main method of this class to parse given payload of _search_into action
     *
     * @param context
     * @param source
     * @throws org.elasticsearch.search.SearchParseException
     *
     */
    void parseSource(SearchIntoContext context, BytesReference source);

}
