package crate.elasticsearch.facet.distinct;

import org.apache.lucene.index.AtomicReaderContext;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.CacheRecycler;
import org.elasticsearch.common.joda.time.MutableDateTime;
import org.elasticsearch.common.trove.ExtTLongObjectHashMap;
import org.elasticsearch.index.fielddata.BytesValues;
import org.elasticsearch.index.fielddata.LongValues;
import org.elasticsearch.index.fielddata.plain.LongArrayIndexFieldData;
import org.elasticsearch.index.fielddata.plain.PagedBytesIndexFieldData;
import org.elasticsearch.search.facet.FacetExecutor;
import org.elasticsearch.search.facet.InternalFacet;
import org.elasticsearch.search.facet.datehistogram.DateHistogramFacet;
import org.elasticsearch.search.facet.terms.strings.HashedAggregator;

import java.io.IOException;

/**
 * Collect the distinct values per time interval.
 */
public class StringDistinctDateHistogramFacetExecutor extends FacetExecutor {

    private final LongArrayIndexFieldData keyIndexFieldData;
    private final PagedBytesIndexFieldData distinctIndexFieldData;


    private MutableDateTime dateTime;
    private final long interval;
    private final DateHistogramFacet.ComparatorType comparatorType;
    final ExtTLongObjectHashMap<InternalDistinctDateHistogramFacet.DistinctEntry> entries;

    public StringDistinctDateHistogramFacetExecutor(LongArrayIndexFieldData keyIndexFieldData,
                                                    PagedBytesIndexFieldData distinctIndexFieldData,
                                                    MutableDateTime dateTime, long interval, DateHistogramFacet.ComparatorType comparatorType) {
        this.comparatorType = comparatorType;
        this.keyIndexFieldData = keyIndexFieldData;
        this.distinctIndexFieldData = distinctIndexFieldData;
        this.entries = CacheRecycler.popLongObjectMap();
        this.dateTime = dateTime;
        this.interval = interval;
    }

    @Override
    public Collector collector() {
        return new Collector();
    }

    @Override
    public InternalFacet buildFacet(String facetName) {
        return new StringInternalDistinctDateHistogramFacet(facetName, comparatorType, entries, true);
    }

    /*
     * Similar to the Collector from the ValueDateHistogramFacetExecutor
     *
     * Only difference is that dateTime and interval is passed to DateHistogramProc instead of tzRounding
     */
    class Collector extends FacetExecutor.Collector {

        private LongValues keyValues;
        private final DateHistogramProc histoProc;

        public Collector() {
            this.histoProc = new DateHistogramProc(entries, dateTime, interval);
        }

        @Override
        public void setNextReader(AtomicReaderContext context) throws IOException {
            keyValues = keyIndexFieldData.load(context).getLongValues();
            histoProc.valueValues = distinctIndexFieldData.load(context).getBytesValues();
        }

        @Override
        public void collect(int doc) throws IOException {
            histoProc.onDoc(doc, keyValues);
        }

        @Override
        public void postCollection() {
        }
    }


    /**
     * Collect the time intervals in value aggregators for each time interval found.
     * The value aggregator finally contains the facet entry.
     */
    public static class DateHistogramProc  {

        private int total;
        private int missing;
        BytesValues.WithOrdinals valueValues;
        private final long interval;
        private MutableDateTime dateTime;
        final ExtTLongObjectHashMap<InternalDistinctDateHistogramFacet.DistinctEntry> entries;

        final ValueAggregator valueAggregator = new ValueAggregator();

        public DateHistogramProc(ExtTLongObjectHashMap<InternalDistinctDateHistogramFacet.DistinctEntry>  entries, MutableDateTime dateTime, long interval) {
            this.dateTime = dateTime;
            this.entries = entries;
            this.interval = interval;
        }

        /*
         * Pass a dateTime to onValue to account for the interval and rounding that is set in the Parser
         */
        public void onDoc(int docId, LongValues values) {
            if (values.hasValue(docId)) {
                final LongValues.Iter iter = values.getIter(docId);
                while (iter.hasNext()) {
                    dateTime.setMillis(iter.next());
                    onValue(docId, dateTime);
                    total++;
                }
            } else {
                missing++;
            }
        }

        protected void onValue(int docId, MutableDateTime dateTime) {
            long time = dateTime.getMillis();
            onValue(docId, time);
        }

        /*
         * for each time interval an entry is created in which the distinct values are aggregated
         */
        protected void onValue(int docId, long time) {
            if (interval != 1) {
                time = ((time / interval) * interval);
            }

            InternalDistinctDateHistogramFacet.DistinctEntry entry = entries.get(time);
            if (entry == null) {
                entry = new InternalDistinctDateHistogramFacet.DistinctEntry(time);
                entries.put(time, entry);
            }
            valueAggregator.entry = entry;
            valueAggregator.onDoc(docId, valueValues);
        }

        public final int total() {
            return total;
        }

        public final int missing() {
            return missing;
        }

        /*
         * aggregates the values in a set
         */
        public final static class ValueAggregator extends HashedAggregator {

            InternalDistinctDateHistogramFacet.DistinctEntry entry;

            @Override
            protected void onValue(int docId, BytesRef value, int hashCode, BytesValues values) {
                entry.getValues().add(value.utf8ToString());
            }
        }
    }
}
