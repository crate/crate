package org.cratedb.action;

import org.apache.lucene.index.AtomicReaderContext;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.search.lookup.FieldLookup;
import org.elasticsearch.search.lookup.SearchLookup;

/**
 * Wrapper around {@link SearchLookup} that implements {@link GroupByFieldLookup}
 *
 * This is used in the {@link org.cratedb.action.groupby.SQLGroupingCollector} and done so
 * that the GroupingCollector can be tested without depending on SearchLookup.
 */
public class ESSearchLookup implements GroupByFieldLookup {

    SearchLookup searchLookupDelegate;

    @Inject
    public ESSearchLookup(SearchLookup searchLookup) {
        this.searchLookupDelegate = searchLookup;
    }

    public void setNextDocId(int doc) {
        searchLookupDelegate.setNextDocId(doc);
    }

    public void setNextReader(AtomicReaderContext context) {
        searchLookupDelegate.setNextReader(context);
    }

    public Object lookupField(String columnName) {
        return ((FieldLookup)searchLookupDelegate.fields().get(columnName)).getValue();
    }
}
