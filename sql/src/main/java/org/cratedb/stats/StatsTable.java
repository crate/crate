package org.cratedb.stats;

import org.cratedb.action.groupby.GroupByKey;
import org.cratedb.action.groupby.GroupByRow;
import org.cratedb.action.groupby.key.Rows;
import org.cratedb.action.sql.ParsedStatement;

import java.util.List;
import java.util.Map;

public interface StatsTable {

    public Rows queryGroupBy(int numReducers,
            ParsedStatement stmt,
            StatsInfo statsInfo) throws Exception;

    public List<List<Object>> query(ParsedStatement stmt, StatsInfo statsInfo) throws Exception;

}
