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

package org.elasticsearch.index.translog;

import com.carrotsearch.hppc.LongHashSet;
import com.carrotsearch.hppc.LongSet;
import org.elasticsearch.ElasticsearchException;
import org.hamcrest.Description;
import org.hamcrest.Matcher;
import org.hamcrest.TypeSafeMatcher;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;


public final class SnapshotMatchers {
    private SnapshotMatchers() {
    }

    /**
     * Consumes a snapshot and make sure it's size is as expected
     */
    public static Matcher<Translog.Snapshot> size(int size) {
        return new SizeMatcher(size);
    }

    /**
     * Consumes a snapshot and makes sure that its operations have all seqno between minSeqNo(inclusive) and maxSeqNo(inclusive).
     */
    public static Matcher<Translog.Snapshot> containsSeqNoRange(long minSeqNo, long maxSeqNo) {
        return new ContainingSeqNoRangeMatcher(minSeqNo, maxSeqNo);
    }

    public static class SizeMatcher extends TypeSafeMatcher<Translog.Snapshot> {

        private final int size;

        public SizeMatcher(int size) {
            this.size = size;
        }

        @Override
        public boolean matchesSafely(Translog.Snapshot snapshot) {
            int count = 0;
            try {
                while (snapshot.next() != null) {
                    count++;
                }
            } catch (IOException ex) {
                throw new ElasticsearchException("failed to advance snapshot", ex);
            }
            return size == count;
        }

        @Override
        public void describeTo(Description description) {
            description.appendText("a snapshot with size ").appendValue(size);
        }
    }

    static class ContainingSeqNoRangeMatcher extends TypeSafeMatcher<Translog.Snapshot> {
        private final long minSeqNo;
        private final long maxSeqNo;
        private final List<Long> notFoundSeqNo = new ArrayList<>();

        ContainingSeqNoRangeMatcher(long minSeqNo, long maxSeqNo) {
            this.minSeqNo = minSeqNo;
            this.maxSeqNo = maxSeqNo;
        }

        @Override
        protected boolean matchesSafely(Translog.Snapshot snapshot) {
            try {
                final LongSet seqNoList = new LongHashSet();
                Translog.Operation op;
                while ((op = snapshot.next()) != null) {
                    seqNoList.add(op.seqNo());
                }
                for (long i = minSeqNo; i <= maxSeqNo; i++) {
                    if (seqNoList.contains(i) == false) {
                        notFoundSeqNo.add(i);
                    }
                }
                return notFoundSeqNo.isEmpty();
            } catch (IOException ex) {
                throw new ElasticsearchException("failed to read snapshot content", ex);
            }
        }

        @Override
        protected void describeMismatchSafely(Translog.Snapshot snapshot, Description mismatchDescription) {
            mismatchDescription
                .appendText("not found seqno ").appendValueList("[", ", ", "]", notFoundSeqNo);
        }

        @Override
        public void describeTo(Description description) {
            description.appendText("snapshot contains all seqno from [" + minSeqNo + " to " + maxSeqNo + "]");
        }
    }
}
