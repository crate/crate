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

package org.elasticsearch.cluster.routing;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.Assert.assertTrue;

import java.io.IOException;

import org.elasticsearch.Version;
import org.elasticsearch.common.UUIDs;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.shard.ShardId;
import org.elasticsearch.repositories.IndexId;
import org.elasticsearch.snapshots.Snapshot;
import org.elasticsearch.snapshots.SnapshotId;
import org.elasticsearch.test.ESTestCase;

public class ShardRoutingTests extends ESTestCase {

    public void testIsSameAllocation() {
        ShardRouting unassignedShard0 = TestShardRouting.newShardRouting("test", 0, null, false, ShardRoutingState.UNASSIGNED);
        ShardRouting unassignedShard1 = TestShardRouting.newShardRouting("test", 1, null, false, ShardRoutingState.UNASSIGNED);
        ShardRouting initializingShard0 =
            TestShardRouting.newShardRouting("test", 0, "1", randomBoolean(), ShardRoutingState.INITIALIZING);
        ShardRouting initializingShard1 =
            TestShardRouting.newShardRouting("test", 1, "1", randomBoolean(), ShardRoutingState.INITIALIZING);
        ShardRouting startedShard0 = initializingShard0.moveToStarted();
        ShardRouting startedShard1 = initializingShard1.moveToStarted();

        // test identity
        assertThat(initializingShard0.isSameAllocation(initializingShard0)).isTrue();

        // test same allocation different state
        assertThat(initializingShard0.isSameAllocation(startedShard0)).isTrue();

        // test unassigned is false even to itself
        assertThat(unassignedShard0.isSameAllocation(unassignedShard0)).isFalse();

        // test different shards/nodes/state
        assertThat(unassignedShard0.isSameAllocation(unassignedShard1)).isFalse();
        assertThat(unassignedShard0.isSameAllocation(initializingShard0)).isFalse();
        assertThat(unassignedShard0.isSameAllocation(initializingShard1)).isFalse();
        assertThat(unassignedShard0.isSameAllocation(startedShard1)).isFalse();
    }

    private ShardRouting randomShardRouting(String index, int shard) {
        ShardRoutingState state = randomFrom(ShardRoutingState.values());
        return TestShardRouting.newShardRouting(index, shard, state == ShardRoutingState.UNASSIGNED ? null : "1",
                                                state == ShardRoutingState.RELOCATING ? "2" : null, state != ShardRoutingState.UNASSIGNED && randomBoolean(), state);
    }

    public void testIsSourceTargetRelocation() {
        ShardRouting unassignedShard0 =
            TestShardRouting.newShardRouting("test", 0, null, false, ShardRoutingState.UNASSIGNED);
        ShardRouting initializingShard0 =
            TestShardRouting.newShardRouting("test", 0, "node1", randomBoolean(), ShardRoutingState.INITIALIZING);
        ShardRouting initializingShard1 =
            TestShardRouting.newShardRouting("test", 1, "node1", randomBoolean(), ShardRoutingState.INITIALIZING);
        assertThat(initializingShard0.isRelocationTarget()).isFalse();
        ShardRouting startedShard0 = initializingShard0.moveToStarted();
        assertThat(startedShard0.isRelocationTarget()).isFalse();
        assertThat(initializingShard1.isRelocationTarget()).isFalse();
        ShardRouting startedShard1 = initializingShard1.moveToStarted();
        assertThat(startedShard1.isRelocationTarget()).isFalse();
        ShardRouting sourceShard0a = startedShard0.relocate("node2", -1);
        assertThat(sourceShard0a.isRelocationTarget()).isFalse();
        ShardRouting targetShard0a = sourceShard0a.getTargetRelocatingShard();
        assertThat(targetShard0a.isRelocationTarget()).isTrue();
        ShardRouting sourceShard0b = startedShard0.relocate("node2", -1);
        ShardRouting sourceShard1 = startedShard1.relocate("node2", -1);

        // test true scenarios
        assertThat(targetShard0a.isRelocationTargetOf(sourceShard0a)).isTrue();
        assertThat(sourceShard0a.isRelocationSourceOf(targetShard0a)).isTrue();

        // test two shards are not mixed
        assertThat(targetShard0a.isRelocationTargetOf(sourceShard1)).isFalse();
        assertThat(sourceShard1.isRelocationSourceOf(targetShard0a)).isFalse();

        // test two allocations are not mixed
        assertThat(targetShard0a.isRelocationTargetOf(sourceShard0b)).isFalse();
        assertThat(sourceShard0b.isRelocationSourceOf(targetShard0a)).isFalse();

        // test different shard states
        assertThat(targetShard0a.isRelocationTargetOf(unassignedShard0)).isFalse();
        assertThat(sourceShard0a.isRelocationTargetOf(unassignedShard0)).isFalse();
        assertThat(unassignedShard0.isRelocationSourceOf(targetShard0a)).isFalse();
        assertThat(unassignedShard0.isRelocationSourceOf(sourceShard0a)).isFalse();

        assertThat(targetShard0a.isRelocationTargetOf(initializingShard0)).isFalse();
        assertThat(sourceShard0a.isRelocationTargetOf(initializingShard0)).isFalse();
        assertThat(initializingShard0.isRelocationSourceOf(targetShard0a)).isFalse();
        assertThat(initializingShard0.isRelocationSourceOf(sourceShard0a)).isFalse();

        assertThat(targetShard0a.isRelocationTargetOf(startedShard0)).isFalse();
        assertThat(sourceShard0a.isRelocationTargetOf(startedShard0)).isFalse();
        assertThat(startedShard0.isRelocationSourceOf(targetShard0a)).isFalse();
        assertThat(startedShard0.isRelocationSourceOf(sourceShard0a)).isFalse();
    }

    public void testEqualsIgnoringVersion() {
        ShardRouting routing = randomShardRouting("test", 0);

        ShardRouting otherRouting = routing;

        Integer[] changeIds = new Integer[]{0, 1, 2, 3, 4, 5, 6};
        for (int changeId : randomSubsetOf(randomIntBetween(1, changeIds.length), changeIds)) {
            boolean unchanged = false;
            switch (changeId) {
                case 0:
                    // change index
                    ShardId shardId = new ShardId(new Index("blubb", randomAlphaOfLength(10)), otherRouting.id());
                    otherRouting = new ShardRouting(shardId, otherRouting.currentNodeId(), otherRouting.relocatingNodeId(),
                                                    otherRouting.primary(), otherRouting.state(), otherRouting.recoverySource(), otherRouting.unassignedInfo(),
                                                    otherRouting.allocationId(), otherRouting.getExpectedShardSize());
                    break;
                case 1:
                    // change shard id
                    otherRouting = new ShardRouting(new ShardId(otherRouting.index(), otherRouting.id() + 1),
                                                    otherRouting.currentNodeId(), otherRouting.relocatingNodeId(),
                                                    otherRouting.primary(), otherRouting.state(), otherRouting.recoverySource(), otherRouting.unassignedInfo(),
                                                    otherRouting.allocationId(), otherRouting.getExpectedShardSize());
                    break;
                case 2:
                    // change current node
                    if (otherRouting.assignedToNode() == false) {
                        unchanged = true;
                    } else {
                        otherRouting = new ShardRouting(otherRouting.shardId(), otherRouting.currentNodeId() + "_1",
                                                        otherRouting.relocatingNodeId(), otherRouting.primary(), otherRouting.state(), otherRouting.recoverySource(),
                                                        otherRouting.unassignedInfo(), otherRouting.allocationId(), otherRouting.getExpectedShardSize());
                    }
                    break;
                case 3:
                    // change relocating node
                    if (otherRouting.relocating() == false) {
                        unchanged = true;
                    } else {
                        otherRouting = new ShardRouting(otherRouting.shardId(), otherRouting.currentNodeId(),
                                                        otherRouting.relocatingNodeId() + "_1", otherRouting.primary(), otherRouting.state(),
                                                        otherRouting.recoverySource(), otherRouting.unassignedInfo(), otherRouting.allocationId(),
                                                        otherRouting.getExpectedShardSize());
                    }
                    break;
                case 4:
                    // change recovery source (only works for inactive primaries)
                    if (otherRouting.active() || otherRouting.primary() == false) {
                        unchanged = true;
                    } else {
                        otherRouting = new ShardRouting(otherRouting.shardId(), otherRouting.currentNodeId(),
                                                        otherRouting.relocatingNodeId(), otherRouting.primary(), otherRouting.state(),
                                                        new RecoverySource.SnapshotRecoverySource(UUIDs.randomBase64UUID(),
                                                        new Snapshot("test",
                                                        new SnapshotId("s1", UUIDs.randomBase64UUID())), Version.CURRENT, new IndexId("test", UUIDs.randomBase64UUID(random()))),
                                                        otherRouting.unassignedInfo(), otherRouting.allocationId(), otherRouting.getExpectedShardSize());
                    }
                    break;
                case 5:
                    // change primary flag
                    otherRouting = TestShardRouting.newShardRouting(otherRouting.getIndexName(), otherRouting.id(),
                                                                    otherRouting.currentNodeId(), otherRouting.relocatingNodeId(), otherRouting.primary() == false,
                                                                    otherRouting.state(), otherRouting.unassignedInfo());
                    break;
                case 6:
                    // change state
                    ShardRoutingState newState;
                    do {
                        newState = randomFrom(ShardRoutingState.values());
                    } while (newState == otherRouting.state());

                    UnassignedInfo unassignedInfo = otherRouting.unassignedInfo();
                    if (unassignedInfo == null && (newState == ShardRoutingState.UNASSIGNED ||
                                                   newState == ShardRoutingState.INITIALIZING)) {
                        unassignedInfo = new UnassignedInfo(UnassignedInfo.Reason.INDEX_CREATED, "test");
                    }

                    otherRouting = TestShardRouting.newShardRouting(otherRouting.getIndexName(), otherRouting.id(),
                                                                    newState == ShardRoutingState.UNASSIGNED ? null :
                                                                        (otherRouting.currentNodeId() == null ? "1" : otherRouting.currentNodeId()),
                                                                    newState == ShardRoutingState.RELOCATING ? "2" : null,
                                                                    otherRouting.primary(), newState, unassignedInfo);
                    break;
            }

            if (randomBoolean()) {
                // change unassigned info
                otherRouting = TestShardRouting.newShardRouting(otherRouting.getIndexName(), otherRouting.id(),
                                                                otherRouting.currentNodeId(), otherRouting.relocatingNodeId(), otherRouting.primary(), otherRouting.state(),
                                                                otherRouting.unassignedInfo() == null ? new UnassignedInfo(UnassignedInfo.Reason.INDEX_CREATED, "test") :
                                                                    new UnassignedInfo(UnassignedInfo.Reason.INDEX_CREATED, otherRouting.unassignedInfo().getMessage() + "_1"));
            }

            if (unchanged == false) {
                logger.debug("comparing\nthis  {} to\nother {}", routing, otherRouting);
                assertThat(routing.equalsIgnoringMetadata(otherRouting)).as("expected non-equality\nthis  " + routing + ",\nother " + otherRouting).isFalse();
            }
        }
    }

    public void testExpectedSize() throws IOException {
        final int iters = randomIntBetween(10, 100);
        for (int i = 0; i < iters; i++) {
            ShardRouting routing = randomShardRouting("test", 0);
            long byteSize = randomIntBetween(0, Integer.MAX_VALUE);
            if (routing.unassigned()) {
                routing = ShardRoutingHelper.initialize(routing, "foo", byteSize);
            } else if (routing.started()) {
                routing = ShardRoutingHelper.relocate(routing, "foo", byteSize);
            } else {
                byteSize = -1;
            }
            if (randomBoolean()) {
                BytesStreamOutput out = new BytesStreamOutput();
                routing.writeTo(out);
                routing = new ShardRouting(out.bytes().streamInput());
            }
            if (routing.initializing() || routing.relocating()) {
                assertEquals(routing.toString(), byteSize, routing.getExpectedShardSize());
                if (byteSize >= 0) {
                    assertThat(routing.toString().contains("expected_shard_size[" + byteSize + "]")).as(routing.toString()).isTrue();
                }
                if (routing.initializing()) {
                    routing = routing.moveToStarted();
                    assertEquals(-1, routing.getExpectedShardSize());
                    assertThat(routing.toString().contains("expected_shard_size[" + byteSize + "]")).as(routing.toString()).isFalse();
                }
            } else {
                assertThat(routing.toString().contains("expected_shard_size [" + byteSize + "]")).as(routing.toString()).isFalse();
                assertEquals(byteSize, routing.getExpectedShardSize());
            }
        }
    }
}
