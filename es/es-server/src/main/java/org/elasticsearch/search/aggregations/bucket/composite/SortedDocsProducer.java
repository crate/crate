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

import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.DocIdSet;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.Query;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.DocIdSetBuilder;
import org.elasticsearch.common.Nullable;
import org.elasticsearch.search.aggregations.LeafBucketCollector;

import java.io.IOException;

/**
 * A producer that visits composite buckets in the order of the value indexed in the leading source of the composite
 * definition. It can be used to control which documents should be collected to produce the top composite buckets
 * without visiting all documents in an index.
 */
abstract class SortedDocsProducer {
    protected final String field;

    SortedDocsProducer(String field) {
        this.field = field;
    }

    /**
     * Visits all non-deleted documents in <code>iterator</code> and fills the provided <code>queue</code>
     * with the top composite buckets extracted from the collection.
     * Documents that contain a top composite bucket are added in the provided <code>builder</code> if it is not null.
     *
     * Returns true if the queue is full and the current <code>leadSourceBucket</code> did not produce any competitive
     * composite buckets.
     */
    protected boolean processBucket(CompositeValuesCollectorQueue queue, LeafReaderContext context, DocIdSetIterator iterator,
                                    Comparable<?> leadSourceBucket, @Nullable DocIdSetBuilder builder) throws IOException {
        final int[] topCompositeCollected = new int[1];
        final boolean[] hasCollected = new boolean[1];
        final LeafBucketCollector queueCollector = new LeafBucketCollector() {
            int lastDoc = -1;

            // we need to add the matching document in the builder
            // so we build a bulk adder from the approximate cost of the iterator
            // and rebuild the adder during the collection if needed
            int remainingBits = (int) Math.min(iterator.cost(), Integer.MAX_VALUE);
            DocIdSetBuilder.BulkAdder adder = builder == null ? null : builder.grow(remainingBits);

            @Override
            public void collect(int doc, long bucket) throws IOException {
                hasCollected[0] = true;
                int slot = queue.addIfCompetitive();
                if (slot != -1) {
                    topCompositeCollected[0]++;
                    if (adder != null && doc != lastDoc) {
                        if (remainingBits == 0) {
                            // the cost approximation was lower than the real size, we need to grow the adder
                            // by some numbers (128) to ensure that we can add the extra documents
                            adder = builder.grow(128);
                            remainingBits = 128;
                        }
                        adder.add(doc);
                        remainingBits --;
                        lastDoc = doc;
                    }
                }
            }
        };
        final Bits liveDocs = context.reader().getLiveDocs();
        final LeafBucketCollector collector = queue.getLeafCollector(leadSourceBucket, context, queueCollector);
        while (iterator.nextDoc() != DocIdSetIterator.NO_MORE_DOCS) {
            if (liveDocs == null || liveDocs.get(iterator.docID())) {
                collector.collect(iterator.docID());
            }
        }
        if (queue.isFull() &&
                hasCollected[0] &&
                topCompositeCollected[0] == 0) {
            return true;
        }
        return false;
    }

    /**
     * Populates the queue with the composite buckets present in the <code>context</code>.
     * Returns the {@link DocIdSet} of the documents that contain a top composite bucket in this leaf or
     * {@link DocIdSet#EMPTY} if <code>fillDocIdSet</code> is false.
     */
    abstract DocIdSet processLeaf(Query query, CompositeValuesCollectorQueue queue,
                                  LeafReaderContext context, boolean fillDocIdSet) throws IOException;
}
