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

package org.elasticsearch.cluster.coordination;

import org.apache.logging.log4j.Logger;
import org.elasticsearch.Version;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.ClusterStateTaskExecutor;
import org.elasticsearch.cluster.NotMasterException;
import org.elasticsearch.cluster.block.ClusterBlocks;
import org.elasticsearch.cluster.metadata.IndexMetaData;
import org.elasticsearch.cluster.metadata.MetaData;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.cluster.routing.allocation.AllocationService;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.function.BiConsumer;

import static org.elasticsearch.gateway.GatewayService.STATE_NOT_RECOVERED_BLOCK;

public class JoinTaskExecutor implements ClusterStateTaskExecutor<JoinTaskExecutor.Task> {

    private final AllocationService allocationService;

    private final Logger logger;

    public static class Task {

        private final DiscoveryNode node;
        private final String reason;

        public Task(DiscoveryNode node, String reason) {
            this.node = node;
            this.reason = reason;
        }

        public DiscoveryNode node() {
            return node;
        }

        public String reason() {
            return reason;
        }

        @Override
        public String toString() {
            return node != null ? node + " " + reason : reason;
        }

        public boolean isBecomeMasterTask() {
            return reason.equals(BECOME_MASTER_TASK_REASON);
        }

        public boolean isFinishElectionTask() {
            return reason.equals(FINISH_ELECTION_TASK_REASON);
        }

        private static final String BECOME_MASTER_TASK_REASON = "_BECOME_MASTER_TASK_";
        private static final String FINISH_ELECTION_TASK_REASON = "_FINISH_ELECTION_";
    }

    public JoinTaskExecutor(AllocationService allocationService, Logger logger) {
        this.allocationService = allocationService;
        this.logger = logger;
    }

    @Override
    public ClusterTasksResult<Task> execute(ClusterState currentState, List<Task> joiningNodes) throws Exception {
        final ClusterTasksResult.Builder<Task> results = ClusterTasksResult.builder();

        final DiscoveryNodes currentNodes = currentState.nodes();
        boolean nodesChanged = false;
        ClusterState.Builder newState;

        if (joiningNodes.size() == 1 && joiningNodes.get(0).isFinishElectionTask()) {
            return results.successes(joiningNodes).build(currentState);
        } else if (currentNodes.getMasterNode() == null && joiningNodes.stream().anyMatch(Task::isBecomeMasterTask)) {
            assert joiningNodes.stream().anyMatch(Task::isFinishElectionTask)
                : "becoming a master but election is not finished " + joiningNodes;
            // use these joins to try and become the master.
            // Note that we don't have to do any validation of the amount of joining nodes - the commit
            // during the cluster state publishing guarantees that we have enough
            newState = becomeMasterAndTrimConflictingNodes(currentState, joiningNodes);
            nodesChanged = true;
        } else if (currentNodes.isLocalNodeElectedMaster() == false) {
            logger.trace("processing node joins, but we are not the master. current master: {}", currentNodes.getMasterNode());
            throw new NotMasterException("Node [" + currentNodes.getLocalNode() + "] not master for join request");
        } else {
            newState = ClusterState.builder(currentState);
        }

        DiscoveryNodes.Builder nodesBuilder = DiscoveryNodes.builder(newState.nodes());

        assert nodesBuilder.isLocalNodeElectedMaster();

        Version minClusterNodeVersion = newState.nodes().getMinNodeVersion();
        Version maxClusterNodeVersion = newState.nodes().getMaxNodeVersion();
        // we only enforce major version transitions on a fully formed clusters
        final boolean enforceMajorVersion = currentState.getBlocks().hasGlobalBlock(STATE_NOT_RECOVERED_BLOCK) == false;
        // processing any joins
        for (final Task joinTask : joiningNodes) {
            if (joinTask.isBecomeMasterTask() || joinTask.isFinishElectionTask()) {
                // noop
            } else if (currentNodes.nodeExists(joinTask.node())) {
                logger.debug("received a join request for an existing node [{}]", joinTask.node());
            } else {
                final DiscoveryNode node = joinTask.node();
                try {
                    if (enforceMajorVersion) {
                        ensureMajorVersionBarrier(node.getVersion(), minClusterNodeVersion);
                    }
                    ensureNodesCompatibility(node.getVersion(), minClusterNodeVersion, maxClusterNodeVersion);
                    // we do this validation quite late to prevent race conditions between nodes joining and importing dangling indices
                    // we have to reject nodes that don't support all indices we have in this cluster
                    ensureIndexCompatibility(node.getVersion(), currentState.getMetaData());
                    nodesBuilder.add(node);
                    nodesChanged = true;
                    minClusterNodeVersion = Version.min(minClusterNodeVersion, node.getVersion());
                    maxClusterNodeVersion = Version.max(maxClusterNodeVersion, node.getVersion());
                } catch (IllegalArgumentException | IllegalStateException e) {
                    results.failure(joinTask, e);
                    continue;
                }
            }
            results.success(joinTask);
        }
        if (nodesChanged) {
            newState.nodes(nodesBuilder);
            return results.build(allocationService.reroute(newState.build(), "node_join"));
        } else {
            // we must return a new cluster state instance to force publishing. This is important
            // for the joining node to finalize its join and set us as a master
            return results.build(newState.build());
        }
    }

    protected ClusterState.Builder becomeMasterAndTrimConflictingNodes(ClusterState currentState, List<Task> joiningNodes) {
        assert currentState.nodes().getMasterNodeId() == null : currentState;
        DiscoveryNodes currentNodes = currentState.nodes();
        DiscoveryNodes.Builder nodesBuilder = DiscoveryNodes.builder(currentNodes);
        nodesBuilder.masterNodeId(currentState.nodes().getLocalNodeId());

        for (final Task joinTask : joiningNodes) {
            if (joinTask.isBecomeMasterTask() || joinTask.isFinishElectionTask()) {
                // noop
            } else {
                final DiscoveryNode joiningNode = joinTask.node();
                final DiscoveryNode nodeWithSameId = nodesBuilder.get(joiningNode.getId());
                if (nodeWithSameId != null && nodeWithSameId.equals(joiningNode) == false) {
                    logger.debug("removing existing node [{}], which conflicts with incoming join from [{}]", nodeWithSameId, joiningNode);
                    nodesBuilder.remove(nodeWithSameId.getId());
                }
                final DiscoveryNode nodeWithSameAddress = currentNodes.findByAddress(joiningNode.getAddress());
                if (nodeWithSameAddress != null && nodeWithSameAddress.equals(joiningNode) == false) {
                    logger.debug("removing existing node [{}], which conflicts with incoming join from [{}]", nodeWithSameAddress,
                        joiningNode);
                    nodesBuilder.remove(nodeWithSameAddress.getId());
                }
            }
        }


        // now trim any left over dead nodes - either left there when the previous master stepped down
        // or removed by us above
        ClusterState tmpState = ClusterState.builder(currentState).nodes(nodesBuilder).blocks(ClusterBlocks.builder()
            .blocks(currentState.blocks())
            .removeGlobalBlock(NoMasterBlockService.NO_MASTER_BLOCK_ID))
            .build();
        logger.trace("becomeMasterAndTrimConflictingNodes: {}", tmpState.nodes());
        allocationService.cleanCaches();
        return ClusterState.builder(allocationService.disassociateDeadNodes(tmpState, false, "removed dead nodes on election"));
    }

    @Override
    public boolean runOnlyOnMaster() {
        // we validate that we are allowed to change the cluster state during cluster state processing
        return false;
    }

    public static Task newBecomeMasterTask() {
        return new Task(null, Task.BECOME_MASTER_TASK_REASON);
    }

    /**
     * a task that is used to signal the election is stopped and we should process pending joins.
     * it may be used in combination with {@link JoinTaskExecutor#newBecomeMasterTask()}
     */
    public static Task newFinishElectionTask() {
        return new Task(null, Task.FINISH_ELECTION_TASK_REASON);
    }

    /**
     * Ensures that all indices are compatible with the given node version. This will ensure that all indices in the given metadata
     * will not be created with a newer version of elasticsearch as well as that all indices are newer or equal to the minimum index
     * compatibility version.
     * @see Version#minimumIndexCompatibilityVersion()
     * @throws IllegalStateException if any index is incompatible with the given version
     */
    public static void ensureIndexCompatibility(final Version nodeVersion, MetaData metaData) {
        Version supportedIndexVersion = nodeVersion.minimumIndexCompatibilityVersion();
        // we ensure that all indices in the cluster we join are compatible with us no matter if they are
        // closed or not we can't read mappings of these indices so we need to reject the join...
        for (IndexMetaData idxMetaData : metaData) {
            if (idxMetaData.getCreationVersion().after(nodeVersion)) {
                throw new IllegalStateException("index " + idxMetaData.getIndex() + " version not supported: "
                    + idxMetaData.getCreationVersion() + " the node version is: " + nodeVersion);
            }
            if (idxMetaData.getCreationVersion().before(supportedIndexVersion)) {
                throw new IllegalStateException("index " + idxMetaData.getIndex() + " version not supported: "
                    + idxMetaData.getCreationVersion() + " minimum compatible index version is: " + supportedIndexVersion);
            }
        }
    }

    /** ensures that the joining node has a version that's compatible with all current nodes*/
    public static void ensureNodesCompatibility(final Version joiningNodeVersion, DiscoveryNodes currentNodes) {
        final Version minNodeVersion = currentNodes.getMinNodeVersion();
        final Version maxNodeVersion = currentNodes.getMaxNodeVersion();
        ensureNodesCompatibility(joiningNodeVersion, minNodeVersion, maxNodeVersion);
    }

    /** ensures that the joining node has a version that's compatible with a given version range */
    public static void ensureNodesCompatibility(Version joiningNodeVersion, Version minClusterNodeVersion, Version maxClusterNodeVersion) {
        assert minClusterNodeVersion.onOrBefore(maxClusterNodeVersion) : minClusterNodeVersion + " > " + maxClusterNodeVersion;
        if (joiningNodeVersion.isCompatible(maxClusterNodeVersion) == false) {
            throw new IllegalStateException("node version [" + joiningNodeVersion + "] is not supported. " +
                "The cluster contains nodes with version [" + maxClusterNodeVersion + "], which is incompatible.");
        }
        if (joiningNodeVersion.isCompatible(minClusterNodeVersion) == false) {
            throw new IllegalStateException("node version [" + joiningNodeVersion + "] is not supported." +
                "The cluster contains nodes with version [" + minClusterNodeVersion + "], which is incompatible.");
        }
    }

    /**
     * ensures that the joining node's major version is equal or higher to the minClusterNodeVersion. This is needed
     * to ensure that if the master is already fully operating under the new major version, it doesn't go back to mixed
     * version mode
     **/
    public static void ensureMajorVersionBarrier(Version joiningNodeVersion, Version minClusterNodeVersion) {
        final byte clusterMajor = minClusterNodeVersion.major;
        if (joiningNodeVersion.major < clusterMajor) {
            throw new IllegalStateException("node version [" + joiningNodeVersion + "] is not supported. " +
                "All nodes in the cluster are of a higher major [" + clusterMajor + "].");
        }
    }

    public static Collection<BiConsumer<DiscoveryNode,ClusterState>> addBuiltInJoinValidators(
        Collection<BiConsumer<DiscoveryNode, ClusterState>> onJoinValidators) {
        final Collection<BiConsumer<DiscoveryNode, ClusterState>> validators = new ArrayList<>();
        validators.add((node, state) -> {
            ensureNodesCompatibility(node.getVersion(), state.getNodes());
            ensureIndexCompatibility(node.getVersion(), state.getMetaData());
        });
        validators.addAll(onJoinValidators);
        return Collections.unmodifiableCollection(validators);
    }
}
