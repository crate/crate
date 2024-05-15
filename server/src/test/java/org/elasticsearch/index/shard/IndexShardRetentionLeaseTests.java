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

package org.elasticsearch.index.shard;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.hasSize;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.cluster.routing.RecoverySource;
import org.elasticsearch.cluster.routing.ShardRoutingHelper;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.index.IndexSettings;
import org.elasticsearch.index.seqno.ReplicationTracker;
import org.elasticsearch.index.seqno.RetentionLease;
import org.elasticsearch.index.seqno.RetentionLeaseStats;
import org.elasticsearch.index.seqno.RetentionLeases;
import org.elasticsearch.index.seqno.SequenceNumbers;
import org.elasticsearch.threadpool.TestThreadPool;
import org.elasticsearch.threadpool.ThreadPool;
import org.junit.Test;

import io.crate.common.collections.Tuple;
import io.crate.common.unit.TimeValue;

public class IndexShardRetentionLeaseTests extends IndexShardTestCase {

    private final AtomicLong currentTimeMillis = new AtomicLong();

    @Override
    protected ThreadPool setUpThreadPool() {
        return new TestThreadPool(getClass().getName(), threadPoolSettings()) {
            @Override
            public long absoluteTimeInMillis() {
                return currentTimeMillis.get();
            }
        };
    }

    @Test
    public void testAddOrRenewRetentionLease() throws IOException {
        final IndexShard indexShard = newStartedShard(
            true,
            Settings.builder().put(IndexSettings.INDEX_SOFT_DELETES_SETTING.getKey(), true).build()
        );
        final long primaryTerm = indexShard.getOperationPrimaryTerm();
        try {
            final int length = randomIntBetween(0, 8);
            final long[] minimumRetainingSequenceNumbers = new long[length];
            for (int i = 0; i < length; i++) {
                minimumRetainingSequenceNumbers[i] = randomLongBetween(SequenceNumbers.NO_OPS_PERFORMED, Long.MAX_VALUE);
                indexShard.addRetentionLease(
                    Integer.toString(i), minimumRetainingSequenceNumbers[i], "test-" + i, ActionListener.wrap(() -> {}));
                assertRetentionLeases(
                    indexShard, i + 1, minimumRetainingSequenceNumbers, primaryTerm, 2 + i, true, false);
            }

            for (int i = 0; i < length; i++) {
                minimumRetainingSequenceNumbers[i] = randomLongBetween(minimumRetainingSequenceNumbers[i], Long.MAX_VALUE);
                indexShard.renewRetentionLease(Integer.toString(i), minimumRetainingSequenceNumbers[i], "test-" + i);
                assertRetentionLeases(
                    indexShard,
                    length,
                    minimumRetainingSequenceNumbers,
                    primaryTerm,
                    2 + length + i,
                    true,
                    false);
            }
        } finally {
            closeShards(indexShard);
        }
    }

    public void testRemoveRetentionLease() throws IOException {
        final IndexShard indexShard = newStartedShard(true,
            Settings.builder().put(IndexSettings.INDEX_SOFT_DELETES_SETTING.getKey(), true).build());
        final long primaryTerm = indexShard.getOperationPrimaryTerm();
        try {
            final int length = randomIntBetween(0, 8);
            final long[] minimumRetainingSequenceNumbers = new long[length];
            for (int i = 0; i < length; i++) {
                minimumRetainingSequenceNumbers[i] = randomLongBetween(SequenceNumbers.NO_OPS_PERFORMED, Long.MAX_VALUE);
                indexShard.addRetentionLease(
                        Integer.toString(i), minimumRetainingSequenceNumbers[i], "test-" + i, ActionListener.wrap(() -> {}));
                assertRetentionLeases(
                        indexShard, i + 1, minimumRetainingSequenceNumbers, primaryTerm, 2 + i, true, false);
            }

            for (int i = 0; i < length; i++) {
                indexShard.removeRetentionLease(Integer.toString(length - i - 1), ActionListener.wrap(() -> {}));
                assertRetentionLeases(
                        indexShard,
                        length - i - 1,
                        minimumRetainingSequenceNumbers,
                        primaryTerm,
                        2 + length + i,
                        true,
                        false);
            }
        } finally {
            closeShards(indexShard);
        }
    }

    @Test
    public void testExpirationOnPrimary() throws IOException {
        runExpirationTest(true);
    }

    @Test
    public void testExpirationOnReplica() throws IOException {
        runExpirationTest(false);
    }

    private RetentionLease peerRecoveryRetentionLease(IndexShard indexShard) {
        return new RetentionLease(
            ReplicationTracker.getPeerRecoveryRetentionLeaseId(indexShard.routingEntry()), 0, currentTimeMillis.get(),
            ReplicationTracker.PEER_RECOVERY_RETENTION_LEASE_SOURCE);
    }

    private void runExpirationTest(final boolean primary) throws IOException {
        final long retentionLeaseMillis = randomLongBetween(1, TimeValue.timeValueHours(12).millis());
        final Settings settings = Settings
            .builder()
            .put(IndexSettings.INDEX_SOFT_DELETES_SETTING.getKey(), true)
            .put(IndexSettings.INDEX_SOFT_DELETES_RETENTION_LEASE_PERIOD_SETTING.getKey(), TimeValue.timeValueMillis(retentionLeaseMillis).getStringRep())
            .build();
        // current time is mocked through the thread pool
        final IndexShard indexShard = newStartedShard(primary, settings, List.of());
        final long primaryTerm = indexShard.getOperationPrimaryTerm();
        try {
            final long[] retainingSequenceNumbers = new long[1];
            retainingSequenceNumbers[0] = randomLongBetween(0, Long.MAX_VALUE);
            final long initialVersion;
            if (primary) {
                initialVersion = 2;
                indexShard.addRetentionLease("0", retainingSequenceNumbers[0], "test-0", ActionListener.wrap(() -> {}));
            } else {
                initialVersion = 3;
                final RetentionLeases retentionLeases = new RetentionLeases(
                    primaryTerm,
                    initialVersion,
                    Arrays.asList(
                        peerRecoveryRetentionLease(indexShard),
                        new RetentionLease("0", retainingSequenceNumbers[0], currentTimeMillis.get(), "test-0")
                    )
                );
                indexShard.updateRetentionLeasesOnReplica(retentionLeases);
            }

            {
                final RetentionLeases retentionLeases = indexShard.getEngine().config().retentionLeasesSupplier().get();
                assertThat(retentionLeases.version()).isEqualTo(initialVersion);
                assertThat(retentionLeases.leases()).hasSize(2);
                final RetentionLease retentionLease = retentionLeases.get("0");
                assertThat(retentionLease.timestamp()).isEqualTo(currentTimeMillis.get());
                assertRetentionLeases(indexShard, 1, retainingSequenceNumbers, primaryTerm, initialVersion, primary, false);
            }

            // renew the lease
            currentTimeMillis.set(currentTimeMillis.get() + randomLongBetween(0, 1024));
            retainingSequenceNumbers[0] = randomLongBetween(retainingSequenceNumbers[0], Long.MAX_VALUE);
            if (primary) {
                indexShard.renewRetentionLease("0", retainingSequenceNumbers[0], "test-0");
            } else {
                final RetentionLeases retentionLeases = new RetentionLeases(
                    primaryTerm,
                    initialVersion + 1,
                    Arrays.asList(
                        peerRecoveryRetentionLease(indexShard),
                        new RetentionLease("0", retainingSequenceNumbers[0], currentTimeMillis.get(), "test-0")
                    )
                );
                indexShard.updateRetentionLeasesOnReplica(retentionLeases);
            }

            {
                final RetentionLeases retentionLeases = indexShard.getEngine().config().retentionLeasesSupplier().get();
                assertThat(retentionLeases.version()).isEqualTo(initialVersion + 1);
                assertThat(retentionLeases.leases()).hasSize(2);
                final RetentionLease retentionLease = retentionLeases.get("0");
                assertThat(retentionLease.timestamp()).isEqualTo(currentTimeMillis.get());
                assertRetentionLeases(indexShard, 1, retainingSequenceNumbers, primaryTerm, initialVersion + 1, primary, false);
            }

            // now force the lease to expire
            currentTimeMillis.set(
                    currentTimeMillis.get() + randomLongBetween(retentionLeaseMillis, Long.MAX_VALUE - currentTimeMillis.get()));
            if (primary) {
                assertRetentionLeases(indexShard, 1, retainingSequenceNumbers, primaryTerm, initialVersion + 1, true, false);
                assertRetentionLeases(indexShard, 0, new long[0], primaryTerm, initialVersion + 2, true, true);
            } else {
                assertRetentionLeases(indexShard, 1, retainingSequenceNumbers, primaryTerm, initialVersion + 1, false, false);
            }
        } finally {
            closeShards(indexShard);
        }
    }

    public void testPersistence() throws IOException {
        final Settings settings = Settings.builder()
                .put(IndexSettings.INDEX_SOFT_DELETES_SETTING.getKey(), true)
                .put(IndexSettings.INDEX_SOFT_DELETES_RETENTION_LEASE_PERIOD_SETTING.getKey(), Long.MAX_VALUE, TimeUnit.NANOSECONDS)
                .build();
        final IndexShard indexShard = newStartedShard(
                true,
                settings,
                List.of());
        try {
            final int length = randomIntBetween(0, 8);
            final long[] minimumRetainingSequenceNumbers = new long[length];
            for (int i = 0; i < length; i++) {
                minimumRetainingSequenceNumbers[i] = randomLongBetween(SequenceNumbers.NO_OPS_PERFORMED, Long.MAX_VALUE);
                currentTimeMillis.set(TimeUnit.NANOSECONDS.toMillis(randomNonNegativeLong()));
                indexShard.addRetentionLease(
                        Integer.toString(i), minimumRetainingSequenceNumbers[i], "test-" + i, ActionListener.wrap(() -> {
                        }));
            }

            currentTimeMillis.set(TimeUnit.NANOSECONDS.toMillis(Long.MAX_VALUE));

            // force the retention leases to persist
            indexShard.persistRetentionLeases();

            // the written retention leases should equal our current retention leases
            final RetentionLeases retentionLeases = indexShard.getEngine().config().retentionLeasesSupplier().get();
            final RetentionLeases writtenRetentionLeases = indexShard.loadRetentionLeases();
            assertThat(writtenRetentionLeases.version()).isEqualTo(1L + length);
            assertThat(writtenRetentionLeases.leases(), contains(retentionLeases.leases().toArray(new RetentionLease[0])));

            // when we recover, we should recover the retention leases
            final IndexShard recoveredShard = reinitShard(
                    indexShard,
                    ShardRoutingHelper.initWithSameId(indexShard.routingEntry(), RecoverySource.ExistingStoreRecoverySource.INSTANCE));
            try {
                recoverShardFromStore(recoveredShard);
                final RetentionLeases recoveredRetentionLeases = recoveredShard.getEngine().config().retentionLeasesSupplier().get();
                assertThat(recoveredRetentionLeases.version()).isEqualTo(1L + length);
                assertThat(
                    recoveredRetentionLeases.leases(),
                    contains(retentionLeases.leases().toArray(new RetentionLease[0])));
            } finally {
                closeShards(recoveredShard);
            }

            // we should not recover retention leases when force-allocating a stale primary
            final IndexShard forceRecoveredShard = reinitShard(
                indexShard,
                ShardRoutingHelper.initWithSameId(indexShard.routingEntry(),
                    RecoverySource.ExistingStoreRecoverySource.FORCE_STALE_PRIMARY_INSTANCE));
            try {
                recoverShardFromStore(forceRecoveredShard);
                final RetentionLeases recoveredRetentionLeases = forceRecoveredShard.getEngine().config().retentionLeasesSupplier().get();
                assertThat(recoveredRetentionLeases.leases()).hasSize(1);
                assertThat(recoveredRetentionLeases.leases().iterator().next().id()).isEqualTo(ReplicationTracker.getPeerRecoveryRetentionLeaseId(indexShard.routingEntry()));
                assertThat(recoveredRetentionLeases.version()).isEqualTo(1L);
            } finally {
                closeShards(forceRecoveredShard);
            }
        } finally {
            closeShards(indexShard);
        }
    }

    @Test
    public void testRetentionLeaseStats() throws IOException {
        final IndexShard indexShard = newStartedShard(
            true,
            Settings.builder().put(IndexSettings.INDEX_SOFT_DELETES_SETTING.getKey(), true).build()
        );
        try {
            final int length = randomIntBetween(0, 8);
            final long[] minimumRetainingSequenceNumbers = new long[length];
            for (int i = 0; i < length; i++) {
                minimumRetainingSequenceNumbers[i] = randomLongBetween(SequenceNumbers.NO_OPS_PERFORMED, Long.MAX_VALUE);
                indexShard.addRetentionLease(
                    Integer.toString(i),
                    minimumRetainingSequenceNumbers[i],
                    "test-" + i,
                    ActionListener.wrap(() -> {})
                );
            }
            final RetentionLeaseStats stats = indexShard.getRetentionLeaseStats();
            assertRetentionLeases(
                stats.leases(),
                length,
                minimumRetainingSequenceNumbers,
                indexShard.getOperationPrimaryTerm(),
                length + 1
            );
        } finally {
            closeShards(indexShard);
        }
    }

    @Test
    public void testRetentionLeasesActionsFailWithSoftDeletesDisabled() throws Exception {
        IndexShard shard = newStartedShard(true, Settings.builder().put(IndexSettings.INDEX_SOFT_DELETES_SETTING.getKey(), false).build());
        assertThatThrownBy(() -> shard.addRetentionLease(randomAlphaOfLength(10),
            randomLongBetween(SequenceNumbers.NO_OPS_PERFORMED, Long.MAX_VALUE), "test", ActionListener.wrap(() -> {}))
        ).isExactlyInstanceOf(AssertionError.class)
            .hasMessage("retention leases requires soft deletes but [index] does not have soft deletes enabled");
        assertThatThrownBy(() -> shard.renewRetentionLease(
            randomAlphaOfLength(10), randomLongBetween(SequenceNumbers.NO_OPS_PERFORMED, Long.MAX_VALUE), "test")
        ).isExactlyInstanceOf(AssertionError.class)
            .hasMessage("retention leases requires soft deletes but [index] does not have soft deletes enabled");
        assertThatThrownBy(() -> shard.removeRetentionLease(randomAlphaOfLength(10), ActionListener.wrap(() -> {})))
            .isExactlyInstanceOf(AssertionError.class)
            .hasMessage("retention leases requires soft deletes but [index] does not have soft deletes enabled");
        shard.syncRetentionLeases();
        closeShards(shard);
    }

    private void assertRetentionLeases(
             final IndexShard indexShard,
             final int size,
             final long[] minimumRetainingSequenceNumbers,
             final long primaryTerm,
             final long version,
             final boolean primary,
             final boolean expireLeases) {
        assertTrue(expireLeases == false || primary);
        final RetentionLeases retentionLeases;
        if (expireLeases == false) {
            if (randomBoolean()) {
                retentionLeases = indexShard.getRetentionLeases();
            } else {
                final Tuple<Boolean, RetentionLeases> tuple = indexShard.getRetentionLeases(false);
                assertFalse(tuple.v1());
                retentionLeases = tuple.v2();
            }
        } else {
            final Tuple<Boolean, RetentionLeases> tuple = indexShard.getRetentionLeases(true);
            assertTrue(tuple.v1());
            retentionLeases = tuple.v2();
        }
        assertRetentionLeases(
            retentionLeases,
            size,
            minimumRetainingSequenceNumbers,
            primaryTerm,
            version);
    }

    private void assertRetentionLeases(
            final RetentionLeases retentionLeases,
            final int size,
            final long[] minimumRetainingSequenceNumbers,
            final long primaryTerm,
            final long version) {
        assertThat(retentionLeases.primaryTerm()).isEqualTo(primaryTerm);
        assertThat(retentionLeases.version()).isEqualTo(version);
        final Map<String, RetentionLease> idToRetentionLease = new HashMap<>();
        for (final RetentionLease retentionLease : retentionLeases.leases()) {
            if (ReplicationTracker.PEER_RECOVERY_RETENTION_LEASE_SOURCE.equals(retentionLease.source()) == false) {
                idToRetentionLease.put(retentionLease.id(), retentionLease);
            }
        }

        assertThat(idToRetentionLease.entrySet()).hasSize(size);
        for (int i = 0; i < size; i++) {
            assertThat(idToRetentionLease.keySet(), hasItem(Integer.toString(i)));
            final RetentionLease retentionLease = idToRetentionLease.get(Integer.toString(i));
            assertThat(retentionLease.retainingSequenceNumber()).isEqualTo(minimumRetainingSequenceNumbers[i]);
            assertThat(retentionLease.source()).isEqualTo("test-" + i);
        }
    }

}
