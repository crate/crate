/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.elasticsearch.search.aggregations.bucket.composite;

import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.CollectionTerminatedException;
import org.apache.lucene.search.DocIdSet;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.Scorer;
import org.apache.lucene.search.Weight;
import org.apache.lucene.util.RoaringDocIdSet;
import org.elasticsearch.common.lease.Releasables;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.search.DocValueFormat;
import org.elasticsearch.search.aggregations.Aggregator;
import org.elasticsearch.search.aggregations.AggregatorFactories;
import org.elasticsearch.search.aggregations.BucketCollector;
import org.elasticsearch.search.aggregations.InternalAggregation;
import org.elasticsearch.search.aggregations.InternalAggregations;
import org.elasticsearch.search.aggregations.LeafBucketCollector;
import org.elasticsearch.search.aggregations.MultiBucketCollector;
import org.elasticsearch.search.aggregations.bucket.BucketsAggregator;
import org.elasticsearch.search.aggregations.pipeline.PipelineAggregator;
import org.elasticsearch.search.aggregations.support.ValuesSource;
import org.elasticsearch.search.internal.SearchContext;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.function.LongUnaryOperator;
import java.util.stream.Collectors;

final class CompositeAggregator extends BucketsAggregator {
    private final int size;
    private final SortedDocsProducer sortedDocsProducer;
    private final List<String> sourceNames;
    private final int[] reverseMuls;
    private final List<DocValueFormat> formats;

    private final SingleDimensionValuesSource<?>[] sources;
    private final CompositeValuesCollectorQueue queue;

    private final List<Entry> entries = new ArrayList<>();
    private LeafReaderContext currentLeaf;
    private RoaringDocIdSet.Builder docIdSetBuilder;
    private BucketCollector deferredCollectors;

    CompositeAggregator(String name, AggregatorFactories factories, SearchContext context, Aggregator parent,
                        List<PipelineAggregator> pipelineAggregators, Map<String, Object> metaData,
                        int size, CompositeValuesSourceConfig[] sourceConfigs, CompositeKey rawAfterKey) throws IOException {
        super(name, factories, context, parent, pipelineAggregators, metaData);
        this.size = size;
        this.sourceNames = Arrays.stream(sourceConfigs).map(CompositeValuesSourceConfig::name).collect(Collectors.toList());
        this.reverseMuls = Arrays.stream(sourceConfigs).mapToInt(CompositeValuesSourceConfig::reverseMul).toArray();
        this.formats = Arrays.stream(sourceConfigs).map(CompositeValuesSourceConfig::format).collect(Collectors.toList());
        this.sources = new SingleDimensionValuesSource[sourceConfigs.length];
        for (int i = 0; i < sourceConfigs.length; i++) {
            this.sources[i] = createValuesSource(context.bigArrays(), context.searcher().getIndexReader(),
                context.query(), sourceConfigs[i], size, i);
        }
        this.queue = new CompositeValuesCollectorQueue(context.bigArrays(), sources, size, rawAfterKey);
        this.sortedDocsProducer = sources[0].createSortedDocsProducerOrNull(context.searcher().getIndexReader(), context.query());
    }

    @Override
    protected void doClose() {
        Releasables.close(queue);
        Releasables.close(sources);
    }

    @Override
    protected void doPreCollection() throws IOException {
        List<BucketCollector> collectors = Arrays.asList(subAggregators);
        deferredCollectors = MultiBucketCollector.wrap(collectors);
        collectableSubAggregators = BucketCollector.NO_OP_COLLECTOR;
    }

    @Override
    protected void doPostCollection() throws IOException {
        finishLeaf();
    }

    @Override
    public InternalAggregation buildAggregation(long zeroBucket) throws IOException {
        assert zeroBucket == 0L;
        consumeBucketsAndMaybeBreak(queue.size());

        if (deferredCollectors != NO_OP_COLLECTOR) {
            // Replay all documents that contain at least one top bucket (collected during the first pass).
            runDeferredCollections();
        }

        int num = Math.min(size, queue.size());
        final InternalComposite.InternalBucket[] buckets = new InternalComposite.InternalBucket[num];
        int pos = 0;
        for (int slot : queue.getSortedSlot()) {
            CompositeKey key = queue.toCompositeKey(slot);
            InternalAggregations aggs = bucketAggregations(slot);
            int docCount = queue.getDocCount(slot);
            buckets[pos++] = new InternalComposite.InternalBucket(sourceNames, formats, key, reverseMuls, docCount, aggs);
        }
        CompositeKey lastBucket = num > 0 ? buckets[num-1].getRawKey() : null;
        return new InternalComposite(name, size, sourceNames, formats, Arrays.asList(buckets), lastBucket, reverseMuls,
            pipelineAggregators(), metaData());
    }

    @Override
    public InternalAggregation buildEmptyAggregation() {
        return new InternalComposite(name, size, sourceNames, formats, Collections.emptyList(), null, reverseMuls,
            pipelineAggregators(), metaData());
    }

    private void finishLeaf() {
        if (currentLeaf != null) {
            DocIdSet docIdSet = docIdSetBuilder.build();
            entries.add(new Entry(currentLeaf, docIdSet));
            currentLeaf = null;
            docIdSetBuilder = null;
        }
    }

    @Override
    protected LeafBucketCollector getLeafCollector(LeafReaderContext ctx, LeafBucketCollector sub) throws IOException {
        finishLeaf();
        boolean fillDocIdSet = deferredCollectors != NO_OP_COLLECTOR;
        if (sortedDocsProducer != null) {
            /*
              The producer will visit documents sorted by the leading source of the composite definition
              and terminates when the leading source value is guaranteed to be greater than the lowest
              composite bucket in the queue.
             */
            DocIdSet docIdSet = sortedDocsProducer.processLeaf(context.query(), queue, ctx, fillDocIdSet);
            if (fillDocIdSet) {
                entries.add(new Entry(ctx, docIdSet));
            }

            /*
              We can bypass search entirely for this segment, all the processing has been done in the previous call.
              Throwing this exception will terminate the execution of the search for this root aggregation,
              see {@link org.apache.lucene.search.MultiCollector} for more details on how we handle early termination in aggregations.
             */
            throw new CollectionTerminatedException();
        } else {
            if (fillDocIdSet) {
                currentLeaf = ctx;
                docIdSetBuilder = new RoaringDocIdSet.Builder(ctx.reader().maxDoc());
            }
            final LeafBucketCollector inner = queue.getLeafCollector(ctx, getFirstPassCollector(docIdSetBuilder));
            return new LeafBucketCollector() {
                @Override
                public void collect(int doc, long zeroBucket) throws IOException {
                    assert zeroBucket == 0L;
                    inner.collect(doc);
                }
            };
        }
    }

    /**
     * The first pass selects the top composite buckets from all matching documents.
     */
    private LeafBucketCollector getFirstPassCollector(RoaringDocIdSet.Builder builder) {
        return new LeafBucketCollector() {
            int lastDoc = -1;

            @Override
            public void collect(int doc, long bucket) throws IOException {
                int slot = queue.addIfCompetitive();
                if (slot != -1) {
                    if (builder != null && lastDoc != doc) {
                        builder.add(doc);
                        lastDoc = doc;
                    }
                }
            }
        };
    }

    /**
     * Replay the documents that might contain a top bucket and pass top buckets to
     * the {@link #deferredCollectors}.
     */
    private void runDeferredCollections() throws IOException {
        final boolean needsScores = needsScores();
        Weight weight = null;
        if (needsScores) {
            Query query = context.query();
            weight = context.searcher().createNormalizedWeight(query, true);
        }
        deferredCollectors.preCollection();
        for (Entry entry : entries) {
            DocIdSetIterator docIdSetIterator = entry.docIdSet.iterator();
            if (docIdSetIterator == null) {
                continue;
            }
            final LeafBucketCollector subCollector = deferredCollectors.getLeafCollector(entry.context);
            final LeafBucketCollector collector = queue.getLeafCollector(entry.context, getSecondPassCollector(subCollector));
            DocIdSetIterator scorerIt = null;
            if (needsScores) {
                Scorer scorer = weight.scorer(entry.context);
                if (scorer != null) {
                    scorerIt = scorer.iterator();
                    subCollector.setScorer(scorer);
                }
            }
            int docID;
            while ((docID = docIdSetIterator.nextDoc()) != DocIdSetIterator.NO_MORE_DOCS) {
                if (needsScores) {
                    assert scorerIt != null && scorerIt.docID() < docID;
                    scorerIt.advance(docID);
                    // aggregations should only be replayed on matching documents
                    assert scorerIt.docID() == docID;
                }
                collector.collect(docID);
            }
        }
        deferredCollectors.postCollection();
    }

    /**
     * Replay the top buckets from the matching documents.
     */
    private LeafBucketCollector getSecondPassCollector(LeafBucketCollector subCollector) {
        return new LeafBucketCollector() {
            @Override
            public void collect(int doc, long zeroBucket) throws IOException {
                assert zeroBucket == 0;
                Integer slot = queue.compareCurrent();
                if (slot != null) {
                    // The candidate key is a top bucket.
                    // We can defer the collection of this document/bucket to the sub collector
                    subCollector.collect(doc, slot);
                }
            }
        };
    }

    private SingleDimensionValuesSource<?> createValuesSource(BigArrays bigArrays, IndexReader reader, Query query,
                                                              CompositeValuesSourceConfig config, int sortRank, int size) {

        final int reverseMul = config.reverseMul();
        if (config.valuesSource() instanceof ValuesSource.Bytes.WithOrdinals && reader instanceof DirectoryReader) {
            ValuesSource.Bytes.WithOrdinals vs = (ValuesSource.Bytes.WithOrdinals) config.valuesSource();
            SingleDimensionValuesSource<?> source = new GlobalOrdinalValuesSource(
                bigArrays,
                config.fieldType(),
                vs::globalOrdinalsValues,
                config.format(),
                config.missingBucket(),
                config.missing(),
                size,
                reverseMul
            );

            if (sortRank == 0 && source.createSortedDocsProducerOrNull(reader, query) != null) {
                // this the leading source and we can optimize it with the sorted docs producer but
                // we don't want to use global ordinals because the number of visited documents
                // should be low and global ordinals need one lookup per visited term.
                Releasables.close(source);
                return new BinaryValuesSource(
                    bigArrays,
                    this::addRequestCircuitBreakerBytes,
                    config.fieldType(),
                    vs::bytesValues,
                    config.format(),
                    config.missingBucket(),
                    config.missing(),
                    size,
                    reverseMul
                );
            } else {
                return source;
            }
        } else if (config.valuesSource() instanceof ValuesSource.Bytes) {
            ValuesSource.Bytes vs = (ValuesSource.Bytes) config.valuesSource();
            return new BinaryValuesSource(
                bigArrays,
                this::addRequestCircuitBreakerBytes,
                config.fieldType(),
                vs::bytesValues,
                config.format(),
                config.missingBucket(),
                config.missing(),
                size,
                reverseMul
            );

        } else if (config.valuesSource() instanceof ValuesSource.Numeric) {
            final ValuesSource.Numeric vs = (ValuesSource.Numeric) config.valuesSource();
            if (vs.isFloatingPoint()) {
                return new DoubleValuesSource(
                    bigArrays,
                    config.fieldType(),
                    vs::doubleValues,
                    config.format(),
                    config.missingBucket(),
                    config.missing(),
                    size,
                    reverseMul
                );

            } else {
                final LongUnaryOperator rounding;
                if (vs instanceof RoundingValuesSource) {
                    rounding = ((RoundingValuesSource) vs)::round;
                } else {
                    rounding = LongUnaryOperator.identity();
                }
                return new LongValuesSource(
                    bigArrays,
                    config.fieldType(),
                    vs::longValues,
                    rounding,
                    config.format(),
                    config.missingBucket(),
                    config.missing(),
                    size,
                    reverseMul
                );
            }
        } else {
            throw new IllegalArgumentException("Unknown values source type: " + config.valuesSource().getClass().getName() +
                " for source: " + config.name());
        }
    }

    private static class Entry {
        final LeafReaderContext context;
        final DocIdSet docIdSet;

        Entry(LeafReaderContext context, DocIdSet docIdSet) {
            this.context = context;
            this.docIdSet = docIdSet;
        }
    }
}

