/*
 * Licensed to Crate.io GmbH ("Crate") under one or more contributor
 * license agreements.  See the NOTICE file distributed with this work for
 * additional information regarding copyright ownership.  Crate licenses
 * this file to you under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.  You may
 * obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
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

package io.crate.testing;

import static org.assertj.core.api.AssertionsForClassTypes.assertThat;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import org.assertj.core.api.AbstractAssert;
import org.assertj.core.api.Assertions;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.index.translog.Translog;

import com.carrotsearch.hppc.LongHashSet;
import com.carrotsearch.hppc.LongSet;

public class TranslogSnapshotAssert extends AbstractAssert<TranslogSnapshotAssert, Translog.Snapshot> {


    protected TranslogSnapshotAssert(Translog.Snapshot actual) {
        super(actual, TranslogSnapshotAssert.class);
    }

    public TranslogSnapshotAssert hasSize(int size) {
        int count = 0;
        try {
            while (actual.next() != null) {
                count++;
            }
        } catch (IOException ex) {
            throw new ElasticsearchException("failed to advance snapshot", ex);
        }
        assertThat(count)
            .as("snapshot size is not expected")
            .isEqualTo(size);
        return this;
    }

    public TranslogSnapshotAssert containsSeqNoRange(long minSeqNo, long maxSeqNo) {
        describedAs("snapshot contains all sequence numbers from [" + minSeqNo + " to " + maxSeqNo + "]");
        try {
            List<Long> notFoundSeqNo = new ArrayList<>();
            final LongSet seqNoList = new LongHashSet();
            Translog.Operation op;
            while ((op = actual.next()) != null) {
                seqNoList.add(op.seqNo());
            }
            for (long i = minSeqNo; i <= maxSeqNo; i++) {
                if (seqNoList.contains(i) == false) {
                    notFoundSeqNo.add(i);
                }
            }
            Assertions.assertThat(notFoundSeqNo)
                .as("not found sequence numbers: " + notFoundSeqNo)
                .isEmpty();
        } catch (IOException ex) {
            throw new ElasticsearchException("failed to read snapshot content", ex);
        }
        return this;
    }

    public TranslogSnapshotAssert equalsTo(List<Translog.Operation> expectedOps) {
        return equalsTo(expectedOps.toArray(new Translog.Operation[]{}));
    }

    public TranslogSnapshotAssert equalsTo(Translog.Operation... expectedOps) {
        try {
            Translog.Operation op;
            int i;
            for (i = 0, op = actual.next(); op != null && i < expectedOps.length; i++, op = actual.next()) {
                assertThat (expectedOps[i])
                    .as("position [" + i + "] expected [" + expectedOps[i] + "] but found [" + op + "]")
                    .isEqualTo(op);
            }

            assertThat(i)
                .as("expected [" + expectedOps.length + "] ops but only found [" + i + "]")
                .isGreaterThanOrEqualTo(expectedOps.length);

            int count = 0;
            if (op != null) {
                count++; // account for the op we already read
                while (actual.next() != null) {
                    count++;
                }
            }
            assertThat(count)
                .as("expected [" + expectedOps.length + "] ops but got [" + (expectedOps.length + count) + "]")
                .isEqualTo(0);
        } catch (IOException ex) {
            throw new ElasticsearchException("failed to read snapshot content", ex);
        }
        return this;
    }

    public TranslogSnapshotAssert containsOperationsInAnyOrder(Collection<Translog.Operation> expectedOps) {
        describedAs("snapshot contains: %s in any order", expectedOps);

        List<Translog.Operation> notFoundOps;
        List<Translog.Operation> notExpectedOps;
        try {
            List<Translog.Operation> actualOps = drainAll(actual);
            notFoundOps = expectedOps.stream()
                .filter(o -> actualOps.contains(o) == false)
                .toList();
            notExpectedOps = actualOps.stream()
                .filter(o -> expectedOps.contains(o) == false)
                .toList();
            assertThat(notFoundOps.isEmpty())
                .as("not found: %s", notFoundOps)
                .isTrue();
            assertThat(notExpectedOps.isEmpty())
                .as("not expected: %s", notExpectedOps)
                .isTrue();
        } catch (IOException ex) {
            throw new ElasticsearchException("failed to read snapshot content", ex);
        }
        return this;
    }

    private static List<Translog.Operation> drainAll(Translog.Snapshot snapshot) throws IOException {
        final List<Translog.Operation> actualOps = new ArrayList<>();
        Translog.Operation op;
        while ((op = snapshot.next()) != null) {
            actualOps.add(op);
        }
        return actualOps;
    }
}
