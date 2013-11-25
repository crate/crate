package org.cratedb.action;

import org.apache.lucene.index.AtomicReaderContext;
import org.cratedb.sql.GroupByOnArrayUnsupportedException;

import java.io.IOException;

/**
 * Used to lookup fields by the {@link org.cratedb.action.groupby.SQLGroupingCollector}
 */
public interface GroupByFieldLookup {

    public void setNextDocId(int doc);
    public void setNextReader(AtomicReaderContext context);
    public Object lookupField(final String columnName) throws IOException, GroupByOnArrayUnsupportedException;
}
