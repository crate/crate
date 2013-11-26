package org.cratedb.action;

import org.apache.lucene.index.AtomicReaderContext;
import org.cratedb.sql.CrateException;

import java.io.IOException;

/**
 * Used to lookup fields by the {@link org.cratedb.action.groupby.SQLGroupingCollector} and
 * {@link org.cratedb.action.sql.SQLFetchCollector}
 */
public interface FieldLookup {

    public void setNextDocId(int doc);
    public void setNextReader(AtomicReaderContext context);
    public Object lookupField(final String columnName) throws IOException, CrateException;
}
