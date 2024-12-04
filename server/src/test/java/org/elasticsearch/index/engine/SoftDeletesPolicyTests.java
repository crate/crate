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

package org.elasticsearch.index.engine;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.OptionalLong;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Supplier;

import org.elasticsearch.index.seqno.RetentionLease;
import org.elasticsearch.index.seqno.RetentionLeases;
import org.elasticsearch.test.ESTestCase;
import org.junit.Test;

public class SoftDeletesPolicyTests extends ESTestCase {

    @Test
    public void testWhenGlobalCheckpointDictatesThePolicy() {
        final int retentionOperations = randomIntBetween(0, 1024);
        final AtomicLong globalCheckpoint = new AtomicLong(randomLongBetween(0, Long.MAX_VALUE - 2));
        final Collection<RetentionLease> leases = new ArrayList<>();
        final int numberOfLeases = randomIntBetween(0, 16);
        for (int i = 0; i < numberOfLeases; i++) {
            // setup leases where the minimum retained sequence number is more than the policy dictated by the global checkpoint
            leases.add(new RetentionLease(
                    Integer.toString(i),
                    randomLongBetween(1 + globalCheckpoint.get() - retentionOperations + 1, Long.MAX_VALUE),
                    randomNonNegativeLong(), "test"));
        }
        final long primaryTerm = randomNonNegativeLong();
        final long version = randomNonNegativeLong();
        final Supplier<RetentionLeases> leasesSupplier =
                () -> new RetentionLeases(
                        primaryTerm,
                        version,
                        Collections.unmodifiableCollection(new ArrayList<>(leases)));
        final SoftDeletesPolicy policy =
                new SoftDeletesPolicy(globalCheckpoint::get, 0, retentionOperations, leasesSupplier);
        // set the local checkpoint of the safe commit to more than the policy dicated by the global checkpoint
        final long localCheckpointOfSafeCommit = randomLongBetween(1 + globalCheckpoint.get() - retentionOperations + 1, Long.MAX_VALUE);
        policy.setLocalCheckpointOfSafeCommit(localCheckpointOfSafeCommit);
        assertThat(policy.getMinRetainedSeqNo()).isEqualTo(1 + globalCheckpoint.get() - retentionOperations);
    }

    @Test
    public void testWhenLocalCheckpointOfSafeCommitDictatesThePolicy() {
        final int retentionOperations = randomIntBetween(0, 1024);
        final long localCheckpointOfSafeCommit = randomLongBetween(-1, Long.MAX_VALUE - retentionOperations - 1);
        final AtomicLong globalCheckpoint =
                new AtomicLong(randomLongBetween(Math.max(0, localCheckpointOfSafeCommit + retentionOperations), Long.MAX_VALUE - 1));
        final Collection<RetentionLease> leases = new ArrayList<>();
        final int numberOfLeases = randomIntBetween(0, 16);
        for (int i = 0; i < numberOfLeases; i++) {
            leases.add(new RetentionLease(
                    Integer.toString(i),
                    randomLongBetween(1 + localCheckpointOfSafeCommit + 1, Long.MAX_VALUE), // leases are for more than the local checkpoint
                    randomNonNegativeLong(), "test"));
        }
        final long primaryTerm = randomNonNegativeLong();
        final long version = randomNonNegativeLong();
        final Supplier<RetentionLeases> leasesSupplier =
                () -> new RetentionLeases(
                        primaryTerm,
                        version,
                        Collections.unmodifiableCollection(new ArrayList<>(leases)));

        final SoftDeletesPolicy policy =
                new SoftDeletesPolicy(globalCheckpoint::get, 0, retentionOperations, leasesSupplier);
        policy.setLocalCheckpointOfSafeCommit(localCheckpointOfSafeCommit);
        assertThat(policy.getMinRetainedSeqNo()).isEqualTo(1 + localCheckpointOfSafeCommit);
    }

    @Test
    public void testWhenRetentionLeasesDictateThePolicy() {
        final int retentionOperations = randomIntBetween(0, 1024);
        final Collection<RetentionLease> leases = new ArrayList<>();
        final int numberOfLeases = randomIntBetween(1, 16);
        for (int i = 0; i < numberOfLeases; i++) {
            leases.add(new RetentionLease(
                    Integer.toString(i),
                    randomLongBetween(0, Long.MAX_VALUE - retentionOperations - 1),
                    randomNonNegativeLong(), "test"));
        }
        final OptionalLong minimumRetainingSequenceNumber = leases.stream().mapToLong(RetentionLease::retainingSequenceNumber).min();
        assert minimumRetainingSequenceNumber.isPresent() : leases;
        final long localCheckpointOfSafeCommit = randomLongBetween(minimumRetainingSequenceNumber.getAsLong(), Long.MAX_VALUE - 1);
        final AtomicLong globalCheckpoint =
                new AtomicLong(randomLongBetween(minimumRetainingSequenceNumber.getAsLong() + retentionOperations, Long.MAX_VALUE - 1));
        final long primaryTerm = randomNonNegativeLong();
        final long version = randomNonNegativeLong();
        final Supplier<RetentionLeases> leasesSupplier =
                () -> new RetentionLeases(
                        primaryTerm,
                        version,
                        Collections.unmodifiableCollection(new ArrayList<>(leases)));
        final SoftDeletesPolicy policy =
                new SoftDeletesPolicy(globalCheckpoint::get, 0, retentionOperations, leasesSupplier);
        policy.setLocalCheckpointOfSafeCommit(localCheckpointOfSafeCommit);
        assertThat(policy.getMinRetainedSeqNo()).isEqualTo(minimumRetainingSequenceNumber.getAsLong());
    }
}
