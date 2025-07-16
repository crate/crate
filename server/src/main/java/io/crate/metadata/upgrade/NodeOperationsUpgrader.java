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

package io.crate.metadata.upgrade;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.Version;
import org.elasticsearch.cluster.metadata.IndexMetadata;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.index.IndexNotFoundException;

import com.carrotsearch.hppc.IntIndexedContainer;

import io.crate.exceptions.RelationUnknown;
import io.crate.execution.dsl.phases.CountPhase;
import io.crate.execution.dsl.phases.ExecutionPhase;
import io.crate.execution.dsl.phases.ExecutionPhaseVisitor;
import io.crate.execution.dsl.phases.NodeOperation;
import io.crate.execution.dsl.phases.RoutedCollectPhase;
import io.crate.metadata.IndexName;
import io.crate.metadata.IndexParts;
import io.crate.metadata.IndexUUID;
import io.crate.metadata.PartitionName;
import io.crate.metadata.Routing;
import io.crate.metadata.information.InformationSchemaInfo;
import io.crate.metadata.pgcatalog.PgCatalogSchemaInfo;
import io.crate.metadata.sys.SysSchemaInfo;

/**
 * Utility class to upgrade or downgrade the routing of phases in a query execution plan to support
 * reading/writing jobs from/to older nodes in a mixed version cluster.
 * <p>
 * This class provides methods to convert between index names and index UUIDs in the routing information of phases.
 * It is used to ensure compatibility with different versions of CrateDB that may use either index names or UUIDs.
 */
public class NodeOperationsUpgrader {

    private static final Logger LOGGER = LogManager.getLogger(NodeOperationsUpgrader.class);

    private static final PhaseUpgrader PHASE_UPGRADER = new PhaseUpgrader();
    private static final PhaseDowngrader PHASE_DOWNGRADER = new PhaseDowngrader();

    /**
     * Upgrades the routing of the operation execution phases to use index UUIDs instead of index names.
     * <p>
     *  - index UUIDs of index names are resolved using the provided metadata.
     *  - if an index name cannot be resolved to a UUID, it will be left as is.
     *  - static system tables (e.g. sys.nodes) won't be changed, as they do not operate on index UUIDs.
     */
    public static Collection<? extends NodeOperation> upgrade(Collection<? extends NodeOperation> nodeOperations,
                                                              Version sourceVersion,
                                                              Metadata metadata) {
        if (sourceVersion.onOrAfter(IndexUUID.INDICES_RESOLVED_BY_UUID_VERSION)) {
            return nodeOperations;
        }
        LOGGER.debug("Upgrading node operations from version {} to the current format", sourceVersion);
        return nodeOperations.stream()
            .map(nodeOperation -> new NodeOperation(
                nodeOperation.executionPhase().accept(PHASE_UPGRADER, metadata),
                nodeOperation.downstreamNodes(),
                nodeOperation.downstreamExecutionPhaseId(),
                nodeOperation.downstreamExecutionPhaseInputId()
            )).toList();
    }

    /**
     * Downgrades the routing of the operation execution phases to use table names instead of index UUIDs.
     * <p>
     *  - index names are resolved by their UUID using the provided metadata.
     *  - if an index UUID cannot be resolved to a name, it will be left as is.
     *  - static system tables (e.g. sys.nodes) won't be changed, as they do not operate on index UUIDs.
     */
    public static Collection<? extends NodeOperation> downgrade(Collection<? extends NodeOperation> nodeOperations,
                                                                Version targetVersion,
                                                                Metadata metadata) {
        if (targetVersion.onOrAfter(IndexUUID.INDICES_RESOLVED_BY_UUID_VERSION)) {
            return nodeOperations;
        }
        LOGGER.debug("Downgrading node operations to the target version {}", targetVersion);
        return nodeOperations.stream()
            .map(nodeOperation -> new NodeOperation(
                nodeOperation.executionPhase().accept(PHASE_DOWNGRADER, metadata),
                nodeOperation.downstreamNodes(),
                nodeOperation.downstreamExecutionPhaseId(),
                nodeOperation.downstreamExecutionPhaseInputId()
            )).toList();
    }

    private static Routing upgradeRouting(Routing routing, Metadata metadata) {
        TreeMap<String, Map<String, IntIndexedContainer>> newLocations = new TreeMap<>();
        Map<String, Map<String, IntIndexedContainer>> oldLocations = routing.locations();
        for (String nodeId : oldLocations.keySet()) {
            Map<String, IntIndexedContainer> tableLocations = oldLocations.get(nodeId);
            TreeMap<String, IntIndexedContainer> newTableLocations = new TreeMap<>();
            newLocations.put(nodeId, newTableLocations);
            for (String tableName : tableLocations.keySet()) {
                IntIndexedContainer shardIds = tableLocations.get(tableName);
                if (isSystemTable(tableName)) {
                    // static system tables do not operate on index UUIDs
                    newTableLocations.put(tableName, shardIds);
                    continue;
                }
                try {
                    IndexParts indexParts = IndexName.decode(tableName);
                    List<String> partitionValues = indexParts.isPartitioned() ? PartitionName.decodeIdent(indexParts.partitionIdent()) : List.of();
                    String indexUUID = metadata.getIndex(indexParts.toRelationName(), partitionValues, true, IndexMetadata::getIndexUUID);
                    newTableLocations.put(indexUUID, shardIds);
                } catch (Exception e) {
                    // Probably already using indexUUID, use as is
                    newTableLocations.put(tableName, shardIds);
                }
            }
        }
        return new Routing(newLocations);
    }

    private static Routing downgradeRouting(Routing routing, Metadata metadata) {
        TreeMap<String, Map<String, IntIndexedContainer>> newLocations = new TreeMap<>();
        Map<String, Map<String, IntIndexedContainer>> oldLocations = routing.locations();
        for (String nodeId : oldLocations.keySet()) {
            Map<String, IntIndexedContainer> tableLocations = oldLocations.get(nodeId);
            TreeMap<String, IntIndexedContainer> newTableLocations = new TreeMap<>();
            newLocations.put(nodeId, newTableLocations);
            for (String indexUUID : tableLocations.keySet()) {
                IntIndexedContainer shardIds = tableLocations.get(indexUUID);
                if (isSystemTable(indexUUID)) {
                    // static system tables do not operate on index UUIDs
                    newTableLocations.put(indexUUID, shardIds);
                    continue;
                }
                try {
                    PartitionName partitionName = metadata.getPartitionName(indexUUID);
                    newTableLocations.put(partitionName.asIndexName(), shardIds);
                } catch (RelationUnknown | IndexNotFoundException e) {
                    LOGGER.warn("Could not find the partition/relation for UUID: {}, using as is", indexUUID, e);
                    newTableLocations.put(indexUUID, shardIds);
                }
            }
        }
        return new Routing(newLocations);
    }

    private static boolean isSystemTable(String tableName) {
        return tableName.startsWith(SysSchemaInfo.NAME)
            || tableName.startsWith(InformationSchemaInfo.NAME)
            || tableName.startsWith(PgCatalogSchemaInfo.NAME);
    }


    private static class PhaseUpgrader extends ExecutionPhaseVisitor<Metadata, ExecutionPhase> {

        @Override
        protected ExecutionPhase visitExecutionPhase(ExecutionPhase phase, Metadata metadata) {
            return phase;
        }

        @Override
        public ExecutionPhase visitRoutedCollectPhase(RoutedCollectPhase phase, Metadata metadata) {
            return new RoutedCollectPhase(
                phase.jobId(),
                phase.phaseId(),
                phase.name(),
                upgradeRouting(phase.routing(), metadata),
                phase.maxRowGranularity(),
                phase.ignoreUnavailableIndex(),
                phase.toCollect(),
                phase.projections(),
                phase.where(),
                phase.distributionInfo()
            );
        }

        @Override
        public ExecutionPhase visitCountPhase(CountPhase phase, Metadata metadata) {
            return new CountPhase(
                phase.phaseId(),
                upgradeRouting(phase.routing(), metadata),
                phase.where(),
                phase.distributionInfo(),
                phase.ignoreUnavailableIndex()
            );
        }
    }

    private static class PhaseDowngrader extends ExecutionPhaseVisitor<Metadata, ExecutionPhase> {

        @Override
        protected ExecutionPhase visitExecutionPhase(ExecutionPhase phase, Metadata metadata) {
            return phase;
        }

        @Override
        public ExecutionPhase visitRoutedCollectPhase(RoutedCollectPhase phase, Metadata metadata) {
            return new RoutedCollectPhase(
                phase.jobId(),
                phase.phaseId(),
                phase.name(),
                downgradeRouting(phase.routing(), metadata),
                phase.maxRowGranularity(),
                phase.ignoreUnavailableIndex(),
                phase.toCollect(),
                phase.projections(),
                phase.where(),
                phase.distributionInfo()
            );
        }

        @Override
        public ExecutionPhase visitCountPhase(CountPhase phase, Metadata metadata) {
            return new CountPhase(
                phase.phaseId(),
                downgradeRouting(phase.routing(), metadata),
                phase.where(),
                phase.distributionInfo(),
                phase.ignoreUnavailableIndex()
            );
        }
    }


}
