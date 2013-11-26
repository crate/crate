package org.cratedb.action.groupby;

/**
 * provides fast direct access to the value inside a GroupByRow.
 *
 * use {@link GroupByHelper#buildFieldExtractor(org.cratedb.action.sql.ParsedStatement, org.cratedb.sql.types.SQLFieldMapper)}
 * to get an array of GroupByFieldExtractor
 *
 * then use the array[resultColumnListIdx].getValue(row) to get values.
 */
public abstract class GroupByFieldExtractor {

    protected final int idx;

    public GroupByFieldExtractor(int idx) {
        this.idx = idx;
    }

    public abstract Object getValue(GroupByRow row);
}
