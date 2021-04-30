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

package io.crate.metadata.cluster;

import io.crate.metadata.PartitionName;
import io.crate.metadata.RelationName;
import org.elasticsearch.cluster.ClusterState;

import java.util.List;

/**
 * Components can implement this interface to hook into DDL statement which are resulting in a changed cluster state.
 * Every implementation must register itself at {@link DDLClusterStateService#addModifier(DDLClusterStateModifier)}.
 *
 * The passed in {@link ClusterState} is an already by the main DDL operation modified state.
 * An implementation should return the given {@link ClusterState} if nothing was modified.
 * Otherwise a new {@link ClusterState} object created by the {@link ClusterState.Builder} must be build and returned.
 */
public interface DDLClusterStateModifier {

    /**
     * Called while a table is closed.
     */
    default ClusterState onCloseTable(ClusterState currentState, RelationName relationName) {
        return currentState;
    }

    /**
     * Called while a single partition is closed
     */
    default ClusterState onCloseTablePartition(ClusterState currentState, PartitionName partitionName) {
        return currentState;
    }

    /**
     * Called while a table is opened.
     */
    default ClusterState onOpenTable(ClusterState currentState, RelationName relationName) {
        return currentState;
    }

    /**
     * Called while a single partition is opened.
     */
    default ClusterState onOpenTablePartition(ClusterState currentState, PartitionName partitionName) {
        return currentState;
    }

    /**
     * Called while a table is dropped.
     */
    default ClusterState onDropTable(ClusterState currentState, RelationName relationName) {
        return currentState;
    }

    /**
     * Called while a table is renamed
     */
    default ClusterState onRenameTable(ClusterState currentState,
                                       RelationName sourceRelationName,
                                       RelationName targetRelationName,
                                       boolean isPartitionedTable) {
        return currentState;
    }

    default ClusterState onSwapRelations(ClusterState currentState,
                                         RelationName source,
                                         RelationName target) {
        return currentState;
    }

    /**
     * Called while a view is dropped.
     */
    default ClusterState onDropView(ClusterState currentState, List<RelationName> relationNames) {
        return currentState;
    }
}
