package org.cratedb.sql.facet;

import org.cratedb.action.sql.ParsedStatement;
import org.cratedb.sql.PartialSQLResult;
import org.elasticsearch.search.facet.Facet;


public interface SQLFacet extends Facet, PartialSQLResult {

    /**
     * The type of the facet.
     */
    public static final String TYPE = "sql";

    /**
     * This is the "real" reduce hook for sql facets, where the sub-results of the shards are
     * merged together based on the given statement.
     *
     * @param stmt the parsed statement of the sql query
     */
    public void reduce(ParsedStatement stmt);


}
