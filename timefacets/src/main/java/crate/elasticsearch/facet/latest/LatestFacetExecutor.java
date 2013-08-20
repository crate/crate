package crate.elasticsearch.facet.latest;

import org.apache.lucene.index.AtomicReaderContext;
import org.elasticsearch.common.CacheRecycler;
import org.elasticsearch.common.trove.ExtTLongObjectHashMap;
import org.elasticsearch.index.fielddata.FieldDataType;
import org.elasticsearch.index.fielddata.IndexNumericFieldData;
import org.elasticsearch.index.fielddata.LongValues;
import org.elasticsearch.search.facet.FacetExecutor;
import org.elasticsearch.search.facet.InternalFacet;
import org.elasticsearch.search.facet.LongFacetAggregatorBase;

import java.io.IOException;

public class LatestFacetExecutor extends FacetExecutor {

    public static final FieldDataType keyDataType = new FieldDataType("long");
    public static final FieldDataType tsDataType = new FieldDataType("long");

    private final IndexNumericFieldData keyFieldName;
    private final IndexNumericFieldData valueFieldName;
    private final IndexNumericFieldData tsFieldName;

    private final Aggregator aggregator;

    protected int size = 10;
    protected int start = 0;

    public LatestFacetExecutor(IndexNumericFieldData keyField, IndexNumericFieldData valueField,
                               IndexNumericFieldData tsField, int size, int start) {
        super();
        this.size = size;
        this.start = start;

        this.keyFieldName = keyField;
        this.valueFieldName = valueField;
        this.tsFieldName = tsField;

        this.aggregator = new Aggregator();
    }

    @Override
    public InternalFacet buildFacet(String facetName) {
        InternalLatestFacet f = new InternalLatestFacet(facetName, size, start,
                aggregator.entries.size());
        f.insert(aggregator.entries);
        return f;
    }

    @Override
    public Collector collector() {
        return new Collector();
    }

    class Collector extends FacetExecutor.Collector {

        private LongValues keyValues;

        @Override
        public void postCollection() {
            //Nothing to do here
        }

        @Override
        public void collect(int doc) throws IOException {
            aggregator.onDoc(doc, keyValues);
        }

        @Override
        public void setNextReader(AtomicReaderContext context) throws IOException {
            keyValues = keyFieldName.load(context).getLongValues();
            aggregator.valueValues  = valueFieldName.load(context).getLongValues();
            aggregator.tsValues  = tsFieldName.load(context).getLongValues();
        }
    }


    public static class Aggregator extends LongFacetAggregatorBase {

        final ExtTLongObjectHashMap<InternalLatestFacet.Entry> entries = CacheRecycler
                .popLongObjectMap();

        LongValues valueValues;
        LongValues tsValues;

        @Override
        public void onValue(int docId, long key) {
            InternalLatestFacet.Entry entry = entries.get(key);
            long ts = tsValues.getValue(docId);
            if (entry == null || entry.ts < ts) {
                int value = (int)valueValues.getValue(docId);
                if (entry == null) {
                    entry = new InternalLatestFacet.Entry(ts, value);
                    entries.put(key, entry);
                } else {
                    entry.ts = ts;
                    entry.value = value;
                }
            }
        }
    }
}
