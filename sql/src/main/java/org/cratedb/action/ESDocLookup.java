package org.cratedb.action;

import org.apache.lucene.index.AtomicReaderContext;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.index.fielddata.AtomicFieldData;
import org.elasticsearch.index.fielddata.ScriptDocValues;
import org.elasticsearch.search.lookup.DocLookup;
import org.elasticsearch.search.lookup.FieldLookup;
import org.elasticsearch.search.lookup.SearchLookup;

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

    public Object[] getValues(String columnName) {
        ScriptDocValues docValues = (ScriptDocValues)docLookup.get(columnName);
        if (docValues.isEmpty())
            return new Object[] { null };
        return docValues.getValues().toArray(new Object[docValues.getValues().size()]);
    }
}
