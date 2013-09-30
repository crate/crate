package org.cratedb.sql;

import org.cratedb.action.sql.ParsedStatement;
import org.elasticsearch.search.facet.Facet;


public interface SQLResult extends PartialSQLResult {

    /**
     * Returns the result column names of the result
     */
    public String[] cols();

}
