package org.cratedb.action.collect;

import org.cratedb.action.FieldLookup;
import org.elasticsearch.cache.recycler.CacheRecycler;
import org.elasticsearch.search.internal.SearchContext;

public class CollectorContext {

    private SearchContext searchContext;
    private FieldLookup fieldLookup;
    private CacheRecycler cacheRecycler;

    public CollectorContext() {
    }

    public SearchContext searchContext() {
        return searchContext;
    }

    public CollectorContext searchContext(SearchContext searchContext) {
        this.searchContext = searchContext;
        if (searchContext().cacheRecycler()!=null){
            this.cacheRecycler(searchContext.cacheRecycler());
        }
        return this;
    }

    public FieldLookup fieldLookup() {
        return fieldLookup;
    }

    public CollectorContext fieldLookup(FieldLookup fieldLookup) {
        this.fieldLookup = fieldLookup;
        return this;
    }

    public CacheRecycler cacheRecycler() {
        return cacheRecycler;
    }

    public CollectorContext cacheRecycler(CacheRecycler cacheRecycler) {
        this.cacheRecycler = cacheRecycler;
        return this;
    }
}
