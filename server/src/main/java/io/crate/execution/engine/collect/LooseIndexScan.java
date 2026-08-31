/*
 * Licensed to Crate.io GmbH ("Crate") under one or more contributor
 * license agreements.  See the NOTICE file distributed with this work for
 * additional information regarding copyright ownership.  Crate licenses
 * this file to you under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.  You may
 * obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.  See the
 * License for the specific language governing permissions and limitations
 * under the License.
 *
 * However, if you have executed another commercial license agreement
 * with Crate these terms will supersede the license and you may use the
 * software solely pursuant to the terms of the relevant commercial agreement.
 */

package io.crate.execution.engine.collect;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.PriorityQueue;
import java.util.Set;

import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.PointValues;
import org.apache.lucene.index.PointValues.IntersectVisitor;
import org.apache.lucene.index.PointValues.PointTree;
import org.apache.lucene.index.PointValues.Relation;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.util.ArrayUtil;
import org.apache.lucene.util.ArrayUtil.ByteArrayComparator;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.RamUsageEstimator;
import org.apache.lucene.util.bkd.BKDConfig;
import org.jspecify.annotations.Nullable;

import io.crate.common.concurrent.Killable.Token;
import io.crate.data.BatchIterator;
import io.crate.data.InMemoryBatchIterator;
import io.crate.data.Row;
import io.crate.data.RowN;
import io.crate.data.SentinelRow;
import io.crate.data.breaker.RamAccounting;
import io.crate.metadata.IndexType;
import io.crate.metadata.Reference;
import io.crate.types.ByteType;
import io.crate.types.DataType;
import io.crate.types.DateType;
import io.crate.types.DoubleType;
import io.crate.types.FloatType;
import io.crate.types.IntegerType;
import io.crate.types.LongType;
import io.crate.types.ShortType;
import io.crate.types.StorageSupport;
import io.crate.types.TimestampType;

/**
 * Loose index scan (a.k.a. skip scan) over the BKD points index of a single numeric column. This can be used to serve
 * a keys-only GROUP BY without reading documents.
 * <p>
 * A BKD tree with one dimension (which is what is used for numeric columns) has all the values in leaf blocks sorted.
 * With that, a left-to-right traversal yields every point in ascending value order.
 * <p>
 * Deduplication is then a comparison against the previously emitted value. The key optimization is when finding a node
 * for which {@code min == max}. This means that the whole subtree holds a single distinct value. That is the "loose"
 * part.
 * <p>
 * Applicability is decided by {@link GroupByOptimizedIterator#tryUseLooseIndexScan}.
 */
final class LooseIndexScan {

    /// Types CrateDB indexes as a single point dimension, and whose [StorageSupport#decode(byte[])]
    /// can turn a packed point back into a value.
    ///
    /// `ip` and NUMERIC(19..38) are 1-dimension points too, but are left out for now: they need more
    /// complex logic for comparison and decoding. `boolean` and `text` write no points at all.
    private static final Set<Integer> SUPPORTED_TYPE_IDS = Set.of(
        ByteType.ID,
        ShortType.ID,
        IntegerType.ID,
        LongType.ID,
        FloatType.ID,
        DoubleType.ID,
        DateType.ID,
        TimestampType.ID_WITH_TZ,
        TimestampType.ID_WITHOUT_TZ
    );

    static boolean supportsType(DataType<?> type) {
        return SUPPORTED_TYPE_IDS.contains(type.id());
    }

    private LooseIndexScan() {
    }

    /**
     * Distinct values of `keyRef`, read from the points index.
     */
    static BatchIterator<Row> iterator(IndexSearcher searcher,
                                       Reference keyRef,
                                       int bytesPerValue,
                                       RamAccounting ramAccounting,
                                       Token killToken) {
        return InMemoryBatchIterator.of(
            rows(searcher, keyRef, bytesPerValue, ramAccounting, killToken),
            SentinelRow.SENTINEL,
            // distinct values loaded lazily from each segment
            true
        );
    }

    // The distinct values as single cell rows, produced lazily.
    private static Iterable<Row> rows(IndexSearcher searcher,
                                      Reference keyRef,
                                      int bytesPerValue,
                                      RamAccounting ramAccounting,
                                      Token killToken) {
        // moveToStart() re-invokes this Iterable. Without closing the previous pass first, each
        // rewind would account the same memory again.
        ShardDistinctValues[] previous = new ShardDistinctValues[1];
        return () -> {
            try {
                if (previous[0] != null) {
                    previous[0].close();
                    previous[0] = null;
                }

                boolean returnNull = keyRef.isNullable()
                    && GroupByOptimizedIterator.countNullValues(keyRef, searcher) > 0;

                ShardDistinctValues distinctValues = new ShardDistinctValues(
                    keyRef,
                    returnNull,
                    bytesPerValue,
                    createSegmentCursors(
                        searcher, keyRef, bytesPerValue, ramAccounting, killToken
                    ),
                    ramAccounting
                );
                previous[0] = distinctValues;
                return distinctValues;
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        };
    }

    /**
     * An iterator over distinct values in a shard. The values are returned NULL first and then in ascending order.
     * <p>
     * {@code ShardDistinctValues} reads distinct values from individual segments. The values returned from each segment
     * are already sorted, so {@code ShardDistinctValues} needs a heap of size {@code number_of_segments}.
     * <p>
     * Lucene's MergedIterator does the same thing, but it caches each sub-iterator's last value in its heap,
     * so sub-iterators may not reuse the returned object. That's not true for our segment cursors.
     */
    static final class ShardDistinctValues implements Iterator<Row> {

        private final PriorityQueue<SegmentCursor> queue;
        private final ByteArrayComparator cmp;
        private final int bytesPerValue;
        private final StorageSupport<?> storageSupport;
        private final Object[] cells = new Object[1];
        private final Row row = new RowN(cells);
        private final RamAccounting ramAccounting;
        private final long accountedBytes;

        private byte[] current;
        /// A value sits in `current`, not yet handed out by `next()`.
        private boolean nextAvailable;
        /// No point is written for NULL, so the tree cannot report it. It is emitted first.
        private boolean returnNull;

        ShardDistinctValues(Reference keyRef,
                            boolean returnNull,
                            int bytesPerValue,
                            List<SegmentCursor> cursors,
                            RamAccounting ramAccounting) throws IOException {
            this.bytesPerValue = bytesPerValue;
            this.cmp = ArrayUtil.getUnsignedComparator(bytesPerValue);

            this.storageSupport = keyRef.valueType().storageSupportSafe();
            assert supportsType(keyRef.valueType())
                : "ShardDistinctValues can only be used with types that decode points";

            this.ramAccounting = ramAccounting;
            // bytesPerValue for the `current` field
            this.accountedBytes = bytesPerValue + (long) cursors.size() * RamUsageEstimator.NUM_BYTES_OBJECT_REF;
            ramAccounting.addBytes(accountedBytes);
            this.returnNull = returnNull;
            this.queue = new PriorityQueue<>(
                Math.max(1, cursors.size()),
                (a, b) -> cmp.compare(a.value(), 0, b.value(), 0)
            );
            // Only cursors that actually hold values can get into the queue (the comparator requires a value).
            for (var c : cursors) {
                if (c.next()) {
                    queue.add(c);
                }
            }
        }

        /**
         * Releases the bytes accounted for this instance, and closes every cursor still in the queue.
         * A cursor that had already exhausted itself was closed by {@link #advance()} already.
         */
        void close() {
            ramAccounting.addBytes(-accountedBytes);
            while (!queue.isEmpty()) {
                queue.poll().close();
            }
        }

        @Override
        public boolean hasNext() {
            if (returnNull || nextAvailable) {
                return true;
            }

            try {
                nextAvailable = advance();
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }

            return nextAvailable;
        }

        @Override
        public Row next() {
            if (!hasNext()) {
                throw new NoSuchElementException("All distinct values have been consumed");
            }
            if (returnNull) {
                returnNull = false;
                cells[0] = null;
            } else {
                nextAvailable = false;
                cells[0] = storageSupport.decode(current);
            }
            return row;
        }

        // Advances `current` to the next distinct value. Returns false once every segment is exhausted.
        private boolean advance() throws IOException {
            while (!queue.isEmpty()) {
                SegmentCursor cursor = queue.poll();
                // NB: value() returns the cursor's own array, and the cursor may overwrite it.
                boolean found = current == null || cmp.compare(cursor.value(), 0, current, 0) != 0;
                if (found) {
                    if (current == null) {
                        current = new byte[bytesPerValue];
                    }
                    System.arraycopy(cursor.value(), 0, current, 0, bytesPerValue);
                }

                if (cursor.next()) {
                    queue.add(cursor);
                } else {
                    // Exhausted, so we can close it/release used memory right away.
                    cursor.close();
                }

                if (!found) {
                    continue; // the same value, from another segment
                }
                return true;
            }
            return false;
        }
    }

    // One cursor per segment that has points for `keyRef`.
    private static List<SegmentCursor> createSegmentCursors(IndexSearcher searcher,
                                                            Reference keyRef,
                                                            int bytesPerValue,
                                                            RamAccounting ramAccounting,
                                                            Token killToken) throws IOException {
        assert keyRef.indexType() != IndexType.NONE
            : "SegmentCursors can be created only for indexed columns";

        String field = keyRef.storageIdent();
        List<SegmentCursor> cursors = new ArrayList<>(searcher.getLeafContexts().size());
        for (var leaf : searcher.getLeafContexts()) {
            LeafReader reader = leaf.reader();
            PointValues values = reader.getPointValues(field);
            if (values == null || values.size() == 0) {
                // The column was added after this segment was written, or every doc in it has a
                // NULL value, so no points were written for it here.
                continue;
            }
            cursors.add(new SegmentCursor(values, reader.getLiveDocs(), bytesPerValue, ramAccounting, killToken));
        }
        return cursors;
    }

    /**
     * A cursor for distinct values in a segment. Values are ordered in ascending order.
     * <p>
     * This is done by traversing a {@link PointTree}, depth first, left to right.
     * At each node:
     * <p>(1) if {@code min == max}, the whole subtree contains identical values, we just take {@code min} and skip the
     * subtree;
     * <p>(2) otherwise descend into subtree
     * <p>(3) if at a leaf, copy the leaf block's points.
     */
    static final class SegmentCursor {

        private final PointTree tree;
        @Nullable
        private final Bits liveDocs;
        // Number of bytes needed to store a value.
        private final int bytesPerValue;
        private final ByteArrayComparator cmp;
        private final RamAccounting ramAccounting;
        private final Token killToken;

        /// The value most recently returned by `next()`.
        private byte @Nullable [] current;

        // The distinct values of one leaf block.
        private byte[] buffer;
        // Number of packed values in buffer.
        private int numValues;
        private int nextValueIndex;
        private boolean treeFullyVisited;
        // Running total of bytes accounted via `ramAccounting`, including buffer growth.
        private long accountedBytes;

        private final CopyLeafValuesVisitor copyLeafValuesVisitor = new CopyLeafValuesVisitor();
        private final LiveDocVisitor liveDocVisitor = new LiveDocVisitor();

        SegmentCursor(PointValues values,
                      @Nullable Bits liveDocs,
                      int bytesPerValue,
                      RamAccounting ramAccounting,
                      Token killToken) throws IOException {
            this.tree = values.getPointTree();
            this.liveDocs = liveDocs;
            this.bytesPerValue = bytesPerValue;
            this.cmp = ArrayUtil.getUnsignedComparator(bytesPerValue);
            this.ramAccounting = ramAccounting;
            this.killToken = killToken;
            // Buffer's max size:
            //
            // The buffer is always filled with either 1 value (in internal nodes) or with all the values from a leaf.
            // All the leaves (except for maybe the right-most) have the same size.
            // This size is never exceeded because, as soon as the buffer is filled with at least one value,
            // it's not filled again, until all the values have been returned from the cursor.
            // This implies that the buffer's max size is the size of the first leaf,
            // which is likely BKDConfig.DEFAULT_MAX_POINTS_IN_LEAF_NODE.
            // With 4-8 bytes per point, this is currently 2-4kb.
            //
            // Buffer's growth:
            //
            // Given the above, it's expected to grow once, at most, which is
            // from the initial size to the number of points in the first leaf.
            //
            // With all the above, 2-4kb is a relatively small value for which there will be no buffer growth
            // in most cases, so we choose that.
            this.buffer = new byte[bytesPerValue * BKDConfig.DEFAULT_MAX_POINTS_IN_LEAF_NODE];
            // bytesPerValue for the `current` field, which is allocated on the first next()
            this.accountedBytes = buffer.length + bytesPerValue;
            ramAccounting.addBytes(accountedBytes);
        }

        // Appends a value unless it's not already in the buffer.
        private void append(byte[] packed) {
            // Values arrive ascending, so a duplicate can only equal the value appended just before it,
            // or - when the buffer has been drained - the last one returned.
            if (numValues > 0) {
                if (cmp.compare(packed, 0, buffer, (numValues - 1) * bytesPerValue) == 0) {
                    return;
                }
            } else if (current != null && cmp.compare(packed, 0, current, 0) == 0) {
                return;
            }
            maybeGrowBuffer(1);
            System.arraycopy(packed, 0, buffer, numValues * bytesPerValue, bytesPerValue);
            numValues++;
        }

        private void maybeGrowBuffer(int numNewValue) {
            int required = (numValues + numNewValue) * bytesPerValue;
            if (buffer.length < required) {
                int before = buffer.length;
                buffer = ArrayUtil.growExact(buffer, required);
                long grown = buffer.length - before;
                accountedBytes += grown;
                ramAccounting.addBytes(grown);
            }
        }

        /**
         * Releases the bytes this cursor accounted for via {@code ramAccounting}, including any
         * buffer growth since construction.
         */
        void close() {
            ramAccounting.addBytes(-accountedBytes);
            accountedBytes = 0;
        }

        // Advances to the next distinct value. Returns false once the segment is exhausted.
        public boolean next() throws IOException {
            while (nextValueIndex == numValues) {
                if (treeFullyVisited) {
                    return false;
                }
                fill();
            }
            if (current == null) {
                current = new byte[bytesPerValue];
            }
            System.arraycopy(buffer, nextValueIndex * bytesPerValue, current, 0, bytesPerValue);
            nextValueIndex++;
            return true;
        }

        /**
         * The current value, valid only after {@link #next()} returned true.
         * The array is the cursor's own and is overwritten by the next call. Copy it if it has to outlive that.
         */
        public byte[] value() {
            return current;
        }

        // Walks the tree until the buffer holds something, or the tree runs out.
        private void fill() throws IOException {
            nextValueIndex = 0;
            numValues = 0;
            while (numValues == 0 && !treeFullyVisited) {
                killToken.raiseIfKilled();
                if (cmp.compare(tree.getMinPackedValue(), 0, tree.getMaxPackedValue(), 0) == 0) {
                    // The whole subtree holds a single value.
                    // getMinPackedValue() returns the array the cursor mutates while navigating, but
                    // passing it straight in is safe: append() copies it out immediately, and step(),
                    // which overwrites it, only runs afterwards.
                    // Points of deleted documents linger until segments merge, so the value is only a
                    // candidate until a live document is found for it.
                    if (liveDocs == null || hasLiveDoc()) {
                        append(tree.getMinPackedValue());
                    }
                    step();
                } else if (!tree.moveToChild()) {
                    // No child, so this is a leaf: buffer its points.
                    // The number of points in a leaf is an int
                    // (see: org.apache.lucene.util.bkd.BKDConfig.maxPointsInLeafNode),
                    // so it's safe to cast to `int` here.
                    maybeGrowBuffer((int) tree.size());
                    tree.visitDocValues(copyLeafValuesVisitor);
                    step();
                }
                // else: moveToChild() descended, the loop inspects the child next
            }
        }

        // Moves to the next node in depth-first order, after the current subtree.
        private void step() throws IOException {
            while (!tree.moveToSibling()) {
                if (!tree.moveToParent()) {
                    treeFullyVisited = true;
                    return;
                }
            }
        }

        /**
         * True if any document below the current node is live. Visits doc ids only, so no point values
         * are decoded, and stops at the first live one.
         * <p>
         * The probe runs on a clone rooted at the current node, never on {@code tree} itself. The reason is:
         * IntersectVisitor cannot signal "stop", so leaving early means we need to throw an exception.
         * The exception is not handled in the {@code BKDReader} that implements the {@code PointTree}, so the
         * tree may be left in an inconsistent state.
         */
        private boolean hasLiveDoc() throws IOException {
            liveDocVisitor.found = false;
            PointTree probe = tree.clone();
            try {
                probe.visitDocIDs(liveDocVisitor);
            } catch (StopVisiting stop) {
                // a live document was found, nothing left to check
            }
            return liveDocVisitor.found;
        }

        private final class LiveDocVisitor implements IntersectVisitor {

            boolean found;

            @Override
            public void visit(int docID) {
                assert liveDocs != null : "Only used when the segment has deletes";
                if (liveDocs.get(docID)) {
                    found = true;
                    throw StopVisiting.INSTANCE;
                }
            }

            @Override
            public void visit(int docID, byte[] packedValue) {
                visit(docID);
            }

            @Override
            public Relation compare(byte[] minPackedValue, byte[] maxPackedValue) {
                return Relation.CELL_CROSSES_QUERY;
            }
        }

        /**
         * Copies a leaf block's values. Ascending order comes from Lucene, per the contract of
         * {@link IntersectVisitor#visit(int, byte[])}: "In the 1D case, values are visited in
         * increasing order, and in the case of ties, in increasing docID order."
         */
        private final class CopyLeafValuesVisitor implements IntersectVisitor {

            @Override
            public void visit(int docID) {
                throw new IllegalStateException("compare() never reports CELL_INSIDE_QUERY");
            }

            @Override
            public void visit(int docID, byte[] packedValue) {
                if (liveDocs == null || liveDocs.get(docID)) {
                    append(packedValue);
                }
            }

            @Override
            public Relation compare(byte[] minPackedValue, byte[] maxPackedValue) {
                // CELL_CROSSES_QUERY forces a visit per point, which is what we need: the values.
                return Relation.CELL_CROSSES_QUERY;
            }
        }
    }

    /**
     * Used to leave an {@link IntersectVisitor} early.
     * It carries no stack trace and no suppression, so throwing it costs nothing.
     */
    private static final class StopVisiting extends RuntimeException {

        static final StopVisiting INSTANCE = new StopVisiting();

        private StopVisiting() {
            super(null, null, false, false);
        }
    }
}
