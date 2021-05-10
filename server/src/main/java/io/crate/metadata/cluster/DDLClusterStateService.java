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
import org.elasticsearch.common.inject.Singleton;

import java.util.ArrayList;
import java.util.List;
import java.util.function.BiFunction;

/**
 * This service makes it possible to hook into cluster state modification caused by DDL statements.
 * One must implement the {@link DDLClusterStateModifier} interface and the related methods, and register it here by
 * {@link #addModifier(DDLClusterStateModifier)}.
 */
@Singleton
public class DDLClusterStateService {

    private List<DDLClusterStateModifier> clusterStateModifiers = new ArrayList<>();

    public void addModifier(DDLClusterStateModifier modifier) {
        clusterStateModifiers.add(modifier);
    }

    public ClusterState onCloseTable(ClusterState currentState, RelationName relationName) {
        return applyOnAllModifiers(currentState,
            (modifier, clusterState) -> modifier.onCloseTable(clusterState, relationName));
    }

    public ClusterState onCloseTablePartition(ClusterState currentState, PartitionName partitionName) {
        return applyOnAllModifiers(currentState,
            (modifier, clusterState) -> modifier.onCloseTablePartition(clusterState, partitionName));
    }

    ClusterState onOpenTable(ClusterState currentState, RelationName relationName) {
        return applyOnAllModifiers(currentState,
            (modifier, clusterState) -> modifier.onOpenTable(clusterState, relationName));
    }

    ClusterState onOpenTablePartition(ClusterState currentState, PartitionName partitionName) {
        return applyOnAllModifiers(currentState,
            (modifier, clusterState) -> modifier.onOpenTablePartition(clusterState, partitionName));
    }

    public ClusterState onDropTable(ClusterState currentState, RelationName relationName) {
        return applyOnAllModifiers(currentState,
            (modifier, clusterState) -> modifier.onDropTable(clusterState, relationName));
    }

    ClusterState onRenameTable(ClusterState currentState,
                               RelationName sourceRelationName,
                               RelationName targetRelationName,
                               boolean isPartitioned) {
        return applyOnAllModifiers(currentState,
            (modifier, clusterState) -> modifier.onRenameTable(clusterState, sourceRelationName, targetRelationName, isPartitioned));
    }

    public ClusterState onDropView(ClusterState currentState, List<RelationName> relationNames) {
        return applyOnAllModifiers(currentState,
            (modifier, clusterState) -> modifier.onDropView(clusterState, relationNames));
    }

    public ClusterState onSwapRelations(ClusterState state, RelationName source, RelationName target) {
        return applyOnAllModifiers(state, (modifier, currentState) -> modifier.onSwapRelations(currentState, source, target));
    }

    private ClusterState applyOnAllModifiers(ClusterState currentState,
                                             BiFunction<DDLClusterStateModifier, ClusterState, ClusterState> function) {
        for (DDLClusterStateModifier modifier : clusterStateModifiers) {
            currentState = function.apply(modifier, currentState);
        }
        return currentState;
    }
}
