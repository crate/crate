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

package org.elasticsearch.index.seqno;

import static org.elasticsearch.index.seqno.SequenceNumbers.UNASSIGNED_SEQ_NO;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.hasSize;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.LongSupplier;
import java.util.stream.Collectors;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.cluster.routing.AllocationId;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.test.IndexSettingsModule;
import org.hamcrest.Matchers;
import org.junit.Test;

import io.crate.common.collections.Tuple;
import io.crate.common.unit.TimeValue;

public class ReplicationTrackerRetentionLeaseTests extends ReplicationTrackerTestCase {

    @Test
    public void testAddOrRenewRetentionLease() {
        final AllocationId allocationId = AllocationId.newInitializing();
        long primaryTerm = randomLongBetween(1, Long.MAX_VALUE);
        final ReplicationTracker replicationTracker = new ReplicationTracker(
            new ShardId("test", "_na", 0),
            allocationId.getId(),
            IndexSettingsModule.newIndexSettings("test", Settings.EMPTY),
            primaryTerm,
            UNASSIGNED_SEQ_NO,
            value -> {},
            () -> 0L,
            (leases, listener) -> {}
        );
        replicationTracker.updateFromMaster(
            randomNonNegativeLong(),
            Collections.singleton(allocationId.getId()),
            routingTable(Collections.emptySet(), allocationId)
        );
        replicationTracker.activatePrimaryMode(SequenceNumbers.NO_OPS_PERFORMED);
        final int length = randomIntBetween(0, 8);
        final long[] minimumRetainingSequenceNumbers = new long[length];
        for (int i = 0; i < length; i++) {
            minimumRetainingSequenceNumbers[i] = randomLongBetween(SequenceNumbers.NO_OPS_PERFORMED, Long.MAX_VALUE);
            replicationTracker.addRetentionLease(
                Integer.toString(i),
                minimumRetainingSequenceNumbers[i],
                "test-" + i,
                ActionListener.wrap(() -> {})
            );
            if (rarely() && primaryTerm < Long.MAX_VALUE) {
                primaryTerm = randomLongBetween(primaryTerm + 1, Long.MAX_VALUE);
                replicationTracker.setOperationPrimaryTerm(primaryTerm);
            }
            assertRetentionLeases(replicationTracker, i + 1, minimumRetainingSequenceNumbers, () -> 0L, primaryTerm, 1 + i, true);
        }

        for (int i = 0; i < length; i++) {
            minimumRetainingSequenceNumbers[i] = randomLongBetween(minimumRetainingSequenceNumbers[i], Long.MAX_VALUE);
            replicationTracker.renewRetentionLease(Integer.toString(i), minimumRetainingSequenceNumbers[i], "test-" + i);
            if (rarely() && primaryTerm < Long.MAX_VALUE) {
                primaryTerm = randomLongBetween(primaryTerm + 1, Long.MAX_VALUE);
                replicationTracker.setOperationPrimaryTerm(primaryTerm);
            }
            assertRetentionLeases(replicationTracker, length, minimumRetainingSequenceNumbers, () -> 0L, primaryTerm, 1 + length + i, true);
        }

    }

    public void testAddRetentionLeaseCausesRetentionLeaseSync() {
        final AllocationId allocationId = AllocationId.newInitializing();
        final Map<String, Long> retainingSequenceNumbers = new HashMap<>();
        final AtomicBoolean invoked = new AtomicBoolean();
        final AtomicReference<ReplicationTracker> reference = new AtomicReference<>();
        final ReplicationTracker replicationTracker = new ReplicationTracker(
            new ShardId("test", "_na", 0),
            allocationId.getId(),
            IndexSettingsModule.newIndexSettings("test", Settings.EMPTY),
            randomNonNegativeLong(),
            UNASSIGNED_SEQ_NO,
            value -> {},
            () -> 0L,
            (leases, listener) -> {
                // we do not want to hold a lock on the replication tracker in the callback!
                assertFalse(Thread.holdsLock(reference.get()));
                invoked.set(true);
                assertThat(
                    leases.leases()
                        .stream()
                        .collect(Collectors.toMap(RetentionLease::id, RetentionLease::retainingSequenceNumber)),
                    equalTo(retainingSequenceNumbers));
            });
        reference.set(replicationTracker);
        replicationTracker.updateFromMaster(
            randomNonNegativeLong(),
            Collections.singleton(allocationId.getId()),
            routingTable(Collections.emptySet(), allocationId)
        );
        replicationTracker.activatePrimaryMode(SequenceNumbers.NO_OPS_PERFORMED);

        final int length = randomIntBetween(0, 8);
        for (int i = 0; i < length; i++) {
            final String id = randomAlphaOfLength(8);
            final long retainingSequenceNumber = randomLongBetween(SequenceNumbers.NO_OPS_PERFORMED, Long.MAX_VALUE);
            retainingSequenceNumbers.put(id, retainingSequenceNumber);
            replicationTracker.addRetentionLease(id, retainingSequenceNumber, "test", ActionListener.wrap(() -> {}));
            // assert that the new retention lease callback was invoked
            assertTrue(invoked.get());

            // reset the invocation marker so that we can assert the callback was not invoked when renewing the lease
            invoked.set(false);
            replicationTracker.renewRetentionLease(id, retainingSequenceNumber, "test");
            assertFalse(invoked.get());
        }
     }

    @Test
    public void testExpirationOnPrimary() {
        runExpirationTest(true);
    }

    @Test
    public void testExpirationOnReplica() {
        runExpirationTest(false);
    }

    private void runExpirationTest(final boolean primaryMode) {
        final AllocationId allocationId = AllocationId.newInitializing();
        final AtomicLong currentTimeMillis = new AtomicLong(randomLongBetween(0, 1024));
        final long retentionLeaseMillis = randomLongBetween(1, TimeValue.timeValueHours(12).millis());
        final Settings settings = Settings
            .builder()
            .put(IndexSettings.INDEX_SOFT_DELETES_SETTING.getKey(), true)
            .put(
                IndexSettings.INDEX_SOFT_DELETES_RETENTION_LEASE_SETTING.getKey(),
                TimeValue.timeValueMillis(retentionLeaseMillis).getStringRep())
            .build();
        final long primaryTerm = randomLongBetween(1, Long.MAX_VALUE);
        final ReplicationTracker replicationTracker = new ReplicationTracker(
            new ShardId("test", "_na", 0),
            allocationId.getId(),
            IndexSettingsModule.newIndexSettings("test", settings),
            primaryTerm,
            UNASSIGNED_SEQ_NO,
            value -> {},
            currentTimeMillis::get,
            (leases, listener) -> {}
        );
        replicationTracker.updateFromMaster(
            randomNonNegativeLong(),
            Collections.singleton(allocationId.getId()),
            routingTable(Collections.emptySet(), allocationId)
        );
        if (primaryMode) {
            replicationTracker.activatePrimaryMode(SequenceNumbers.NO_OPS_PERFORMED);
        }
        final long[] retainingSequenceNumbers = new long[1];
        retainingSequenceNumbers[0] = randomLongBetween(0, Long.MAX_VALUE);
        if (primaryMode) {
            replicationTracker.addRetentionLease("0", retainingSequenceNumbers[0], "test-0", ActionListener.wrap(() -> {}));
        } else {
            final RetentionLeases retentionLeases = new RetentionLeases(
                primaryTerm,
                1,
                Collections.singleton(new RetentionLease("0", retainingSequenceNumbers[0], currentTimeMillis.get(), "test-0")));
            replicationTracker.updateRetentionLeasesOnReplica(retentionLeases);
        }

        {
            final RetentionLeases retentionLeases = replicationTracker.getRetentionLeases();
            assertThat(retentionLeases.version(), equalTo(1L));
            assertThat(retentionLeases.leases(), hasSize(1));
            final RetentionLease retentionLease = retentionLeases.leases().iterator().next();
            assertThat(retentionLease.timestamp(), equalTo(currentTimeMillis.get()));
            assertRetentionLeases(replicationTracker, 1, retainingSequenceNumbers, currentTimeMillis::get, primaryTerm, 1, primaryMode);
        }

        // renew the lease
        currentTimeMillis.set(currentTimeMillis.get() + randomLongBetween(0, 1024));
        retainingSequenceNumbers[0] = randomLongBetween(retainingSequenceNumbers[0], Long.MAX_VALUE);
        if (primaryMode) {
            replicationTracker.renewRetentionLease("0", retainingSequenceNumbers[0], "test-0");
        } else {
            final RetentionLeases retentionLeases = new RetentionLeases(
                    primaryTerm,
                    2,
                    Collections.singleton(new RetentionLease("0", retainingSequenceNumbers[0], currentTimeMillis.get(), "test-0")));
            replicationTracker.updateRetentionLeasesOnReplica(retentionLeases);
        }

        {
            final RetentionLeases retentionLeases = replicationTracker.getRetentionLeases();
            assertThat(retentionLeases.version(), equalTo(2L));
            assertThat(retentionLeases.leases(), hasSize(1));
            final RetentionLease retentionLease = retentionLeases.leases().iterator().next();
            assertThat(retentionLease.timestamp(), equalTo(currentTimeMillis.get()));
            assertRetentionLeases(replicationTracker, 1, retainingSequenceNumbers, currentTimeMillis::get, primaryTerm, 2, primaryMode);
        }

        // now force the lease to expire
        currentTimeMillis.set(currentTimeMillis.get() + randomLongBetween(retentionLeaseMillis, Long.MAX_VALUE - currentTimeMillis.get()));
        if (primaryMode) {
            assertRetentionLeases(replicationTracker, 0, retainingSequenceNumbers, currentTimeMillis::get, primaryTerm, 3, true);
        } else {
            // leases do not expire on replicas until synced from the primary
            assertRetentionLeases(replicationTracker, 1, retainingSequenceNumbers, currentTimeMillis::get, primaryTerm, 2, false);
        }
    }

    @Test
    public void testRetentionLeaseExpirationCausesRetentionLeaseSync() {
        final AllocationId allocationId = AllocationId.newInitializing();
        final AtomicLong currentTimeMillis = new AtomicLong(randomLongBetween(0, 1024));
        final long retentionLeaseMillis = randomLongBetween(1, TimeValue.timeValueHours(12).millis());
        final Settings settings = Settings
            .builder()
            .put(IndexSettings.INDEX_SOFT_DELETES_SETTING.getKey(), true)
            .put(IndexSettings.INDEX_SOFT_DELETES_RETENTION_LEASE_SETTING.getKey(), TimeValue.timeValueMillis(retentionLeaseMillis).getStringRep())
            .build();
        final Map<String, Tuple<Long, Long>> retentionLeases = new HashMap<>();
        final AtomicBoolean invoked = new AtomicBoolean();
        final AtomicReference<ReplicationTracker> reference = new AtomicReference<>();
        final ReplicationTracker replicationTracker = new ReplicationTracker(
            new ShardId("test", "_na", 0),
            allocationId.getId(),
            IndexSettingsModule.newIndexSettings("test", settings),
            randomNonNegativeLong(),
            UNASSIGNED_SEQ_NO,
            value -> {},
            currentTimeMillis::get,
            (leases, listener) -> {
                // we do not want to hold a lock on the replication tracker in the callback!
                assertFalse(Thread.holdsLock(reference.get()));
                invoked.set(true);
                assertThat(
                    leases.leases()
                        .stream()
                        .collect(Collectors.toMap(RetentionLease::id, ReplicationTrackerRetentionLeaseTests::toTuple)),
                    equalTo(retentionLeases));
            });
        reference.set(replicationTracker);
        replicationTracker.updateFromMaster(
            randomNonNegativeLong(),
            Collections.singleton(allocationId.getId()),
            routingTable(Collections.emptySet(), allocationId)
        );
        replicationTracker.activatePrimaryMode(SequenceNumbers.NO_OPS_PERFORMED);

        final int length = randomIntBetween(0, 8);
        long version = 0;
        for (int i = 0; i < length; i++) {
            final String id = randomAlphaOfLength(8);
            final long retainingSequenceNumber = randomLongBetween(SequenceNumbers.NO_OPS_PERFORMED, Long.MAX_VALUE);
            retentionLeases.put(id, Tuple.tuple(retainingSequenceNumber, currentTimeMillis.get()));
            replicationTracker.addRetentionLease(id, retainingSequenceNumber, "test", ActionListener.wrap(() -> {}));
            version++;
            assertThat(replicationTracker.getRetentionLeases().version(), equalTo(version));
            // assert that the new retention lease callback was invoked
            assertTrue(invoked.get());

            // reset the invocation marker so that we can assert the callback was not invoked when renewing the lease
            invoked.set(false);
            currentTimeMillis.set(1 + currentTimeMillis.get());
            retentionLeases.put(id, Tuple.tuple(retainingSequenceNumber, currentTimeMillis.get()));
            replicationTracker.renewRetentionLease(id, retainingSequenceNumber, "test");
            version++;
            assertThat(replicationTracker.getRetentionLeases().version(), equalTo(version));

            // reset the invocation marker so that we can assert the callback was invoked if any leases are expired
            assertFalse(invoked.get());
            // randomly expire some leases
            final long currentTimeMillisIncrement = randomLongBetween(0, Long.MAX_VALUE - currentTimeMillis.get());
            // calculate the expired leases and update our tracking map
            final List<String> expiredIds = retentionLeases.entrySet()
                .stream()
                .filter(r -> currentTimeMillis.get() + currentTimeMillisIncrement > r.getValue().v2() + retentionLeaseMillis)
                .map(Map.Entry::getKey)
                .collect(Collectors.toList());
            expiredIds.forEach(retentionLeases::remove);
            if (expiredIds.isEmpty() == false) {
                version++;
            }
            currentTimeMillis.set(currentTimeMillis.get() + currentTimeMillisIncrement);
            // getting the leases has the side effect of calculating which leases are expired and invoking the sync callback
            final RetentionLeases current = replicationTracker.getRetentionLeases();
            assertThat(current.version(), equalTo(version));
            // the current leases should equal our tracking map
            assertThat(
                current.leases()
                    .stream()
                    .collect(Collectors.toMap(RetentionLease::id, ReplicationTrackerRetentionLeaseTests::toTuple)),
                equalTo(retentionLeases)
            );
            // the callback should only be invoked if there were expired leases
            assertThat(invoked.get(), equalTo(expiredIds.isEmpty() == false));
        }
        assertThat(replicationTracker.getRetentionLeases().version(), equalTo(version));
    }

    public void testReplicaIgnoresOlderRetentionLeasesVersion() {
        final AllocationId allocationId = AllocationId.newInitializing();
        final ReplicationTracker replicationTracker = new ReplicationTracker(
            new ShardId("test", "_na", 0),
            allocationId.getId(),
            IndexSettingsModule.newIndexSettings("test", Settings.EMPTY),
            randomNonNegativeLong(),
            UNASSIGNED_SEQ_NO,
            value -> {},
            () -> 0L,
            (leases, listener) -> {});
        replicationTracker.updateFromMaster(
            randomNonNegativeLong(),
            Collections.singleton(allocationId.getId()),
            routingTable(Collections.emptySet(), allocationId)
        );
        final int length = randomIntBetween(0, 8);
        final List<RetentionLeases> retentionLeasesCollection = new ArrayList<>(length);
        long primaryTerm = 1;
        long version = 0;
        for (int i = 0; i < length; i++) {
            final int innerLength = randomIntBetween(0, 8);
            final Collection<RetentionLease> leases = new ArrayList<>();
            for (int j = 0; j < innerLength; j++) {
                leases.add(new RetentionLease(
                    i + "-" + j,
                    randomNonNegativeLong(),
                    randomNonNegativeLong(),
                    randomAlphaOfLength(8)
                ));
                version++;
            }
            if (rarely()) {
                primaryTerm++;
            }
            retentionLeasesCollection.add(new RetentionLeases(primaryTerm, version, leases));
        }
        final Collection<RetentionLease> expectedLeases;
        if (length == 0 || retentionLeasesCollection.get(length - 1).leases().isEmpty()) {
            expectedLeases = Collections.emptyList();
        } else {
            expectedLeases = retentionLeasesCollection.get(length - 1).leases();
        }
        Collections.shuffle(retentionLeasesCollection, random());
        for (final RetentionLeases retentionLeases : retentionLeasesCollection) {
            replicationTracker.updateRetentionLeasesOnReplica(retentionLeases);
        }
        assertThat(replicationTracker.getRetentionLeases().version(), equalTo(version));
        if (expectedLeases.isEmpty()) {
            assertThat(replicationTracker.getRetentionLeases().leases(), empty());
        } else {
            assertThat(
                replicationTracker.getRetentionLeases().leases(),
                Matchers.contains(expectedLeases.toArray(new RetentionLease[0])));
        }
     }

    private static Tuple<Long, Long> toTuple(final RetentionLease retentionLease) {
        return Tuple.tuple(retentionLease.retainingSequenceNumber(), retentionLease.timestamp());
    }

    private void assertRetentionLeases(
            final ReplicationTracker replicationTracker,
            final int size,
            final long[] minimumRetainingSequenceNumbers,
            final LongSupplier currentTimeMillisSupplier,
            final long primaryTerm,
            final long version,
            final boolean primaryMode) {
        final RetentionLeases retentionLeases = replicationTracker.getRetentionLeases();
        assertThat(retentionLeases.primaryTerm(), equalTo(primaryTerm));
        assertThat(retentionLeases.version(), equalTo(version));
        final Map<String, RetentionLease> idToRetentionLease = new HashMap<>();
        for (final RetentionLease retentionLease : retentionLeases.leases()) {
            idToRetentionLease.put(retentionLease.id(), retentionLease);
        }

        assertThat(idToRetentionLease.entrySet(), hasSize(size));
        for (int i = 0; i < size; i++) {
            assertThat(idToRetentionLease.keySet(), hasItem(Integer.toString(i)));
            final RetentionLease retentionLease = idToRetentionLease.get(Integer.toString(i));
            assertThat(retentionLease.retainingSequenceNumber(), equalTo(minimumRetainingSequenceNumbers[i]));
            if (primaryMode) {
                // retention leases can be expired on replicas, so we can only assert on primaries here
                assertThat(
                    currentTimeMillisSupplier.getAsLong() - retentionLease.timestamp(),
                    Matchers.lessThanOrEqualTo(replicationTracker.indexSettings().getRetentionLeaseMillis()));
            }
            assertThat(retentionLease.source(), equalTo("test-" + i));
        }
    }

}
