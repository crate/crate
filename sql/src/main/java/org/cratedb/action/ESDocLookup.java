package org.cratedb.action;

import org.apache.lucene.index.AtomicReaderContext;
import org.cratedb.sql.GroupByOnArrayUnsupportedException;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.index.fielddata.AtomicFieldData;
import org.elasticsearch.index.fielddata.ScriptDocValues;
import org.elasticsearch.search.lookup.DocLookup;
import org.elasticsearch.search.lookup.FieldLookup;
import org.elasticsearch.search.lookup.SearchLookup;

import java.util.List;

/**
 * Wrapper around {@link SearchLookup} that implements {@link GroupByFieldLookup}
 *
 * This is used in the {@link org.cratedb.action.groupby.SQLGroupingCollector} and done so
 * that the GroupingCollector can be tested without depending on SearchLookup.
 */
public class ESDocLookup implements GroupByFieldLookup {

    DocLookup docLookup;

    @Inject
    public ESDocLookup(DocLookup searchLookup) {
        this.docLookup = searchLookup;
    }

    public void setNextDocId(int doc) {
        docLookup.setNextDocId(doc);
    }

    public void setNextReader(AtomicReaderContext context) {
        docLookup.setNextReader(context);
    }

    public Object lookupField(String columnName) throws GroupByOnArrayUnsupportedException {
        ScriptDocValues docValues = (ScriptDocValues)docLookup.get(columnName);
        if (docValues.isEmpty())
            return null;
        List<?> values = docValues.getValues();
        if (values.size() > 1) {
            throw new GroupByOnArrayUnsupportedException(columnName);
        }

        return values.get(0);
    }
}
