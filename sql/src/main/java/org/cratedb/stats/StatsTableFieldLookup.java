package org.cratedb.stats;

import org.apache.lucene.index.AtomicReaderContext;
import org.cratedb.action.FieldLookup;
import org.cratedb.sql.CrateException;

import java.io.IOException;
import java.util.Map;

public class StatsTableFieldLookup implements FieldLookup {

    private final Map<String, Object> fields;

    public StatsTableFieldLookup(Map<String, Object> fields) {
        this.fields = fields;
    }

    @Override
    public void setNextDocId(int doc) {
    }

    @Override
    public void setNextReader(AtomicReaderContext context) {
    }

    @Override
    public Object lookupField(String columnName) throws IOException, CrateException {
        return fields.get(columnName);
    }
}
