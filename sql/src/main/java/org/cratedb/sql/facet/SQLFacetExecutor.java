package org.cratedb.sql.facet;

import org.cratedb.action.sql.ParsedStatement;
import org.elasticsearch.action.update.TransportUpdateAction;
import org.elasticsearch.search.facet.FacetExecutor;
import org.elasticsearch.search.facet.InternalFacet;
import org.elasticsearch.search.internal.SearchContext;

public class SQLFacetExecutor extends FacetExecutor {


    private final SearchContext searchContext;
    private final ParsedStatement stmt;
    private final TransportUpdateAction updateAction;
    private final UpdateCollector collector;

    public SQLFacetExecutor(
            ParsedStatement stmt,
            SearchContext searchContext,
            TransportUpdateAction updateAction) {
        this.stmt = stmt;
        this.updateAction = updateAction;
        this.searchContext = searchContext;
        // TODO: remove hard coded update collector, look at stmt
        this.collector = new UpdateCollector(
                stmt.updateDoc(),
                updateAction,
                searchContext);
    }

    /**
     * Executed once per shard after all records are processed
     */
    @Override
    public InternalFacet buildFacet(String facetName) {
        InternalSQLFacet facet = new InternalSQLFacet(facetName);
        facet.rowCount(collector.rowCount());
        return facet;
    }

    @Override
    public Collector collector() {
        return collector;
    }


}
