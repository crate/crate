/*
 * Licensed to Crate under one or more contributor license agreements.
 * See the NOTICE file distributed with this work for additional
 * information regarding copyright ownership.  Crate licenses this file
 * to you under the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.  You may
 * obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied.  See the License for the specific language governing
 * permissions and limitations under the License.
 *
 * However, if you have executed another commercial license agreement
 * with Crate these terms will supersede the license and you may use the
 * software solely pursuant to the terms of the relevant commercial
 * agreement.
 */

package io.crate.execution.engine.fetch;

import java.util.List;

import javax.annotation.Nullable;

import com.carrotsearch.hppc.IntContainer;
import com.carrotsearch.hppc.IntObjectHashMap;
import com.carrotsearch.hppc.IntObjectMap;
import com.carrotsearch.hppc.IntSet;
import com.carrotsearch.hppc.cursors.IntCursor;
import com.carrotsearch.hppc.cursors.IntObjectCursor;
import com.carrotsearch.hppc.cursors.ObjectCursor;

import io.crate.data.Bucket;

public class ReaderBuckets {

    private final IntObjectHashMap<ReaderBucket> readerBuckets = new IntObjectHashMap<>();

    public ReaderBuckets() {
    }

    @Nullable
    ReaderBucket getReaderBucket(int readerId) {
        return readerBuckets.get(readerId);
    }

    ReaderBucket require(long fetchId) {
        int readerId = FetchId.decodeReaderId(fetchId);
        int docId = FetchId.decodeDocId(fetchId);
        ReaderBucket readerBucket = readerBuckets.get(readerId);
        if (readerBucket == null) {
            readerBucket = new ReaderBucket();
            readerBuckets.put(readerId, readerBucket);
        }
        readerBucket.require(docId);
        return readerBucket;
    }

    void clearBuckets() {
        for (ObjectCursor<ReaderBucket> bucketCursor : readerBuckets.values()) {
            bucketCursor.value.docs.clear();
        }
    }

    public void applyResults(List<IntObjectMap<? extends Bucket>> resultsByReader) {
        for (IntObjectMap<? extends Bucket> result : resultsByReader) {
            if (result == null) {
                continue;
            }
            for (IntObjectCursor<? extends Bucket> cursor : result) {
                ReaderBucket readerBucket = getReaderBucket(cursor.key);
                assert readerBucket != null
                    : "If we get a result for a reader, there must be a readerBucket for it";
                readerBucket.fetched(cursor.value);
            }
        }
    }

    public IntObjectHashMap<IntContainer> generateToFetch(IntSet readerIds) {
        IntObjectHashMap<IntContainer> toFetch = new IntObjectHashMap<>(readerIds.size());
        for (IntCursor readerIdCursor : readerIds) {
            ReaderBucket readerBucket = getReaderBucket(readerIdCursor.value);
            if (readerBucket != null && readerBucket.docs.size() > 0) {
                toFetch.put(readerIdCursor.value, readerBucket.docs.keys());
            }
        }
        return toFetch;
    }
}
