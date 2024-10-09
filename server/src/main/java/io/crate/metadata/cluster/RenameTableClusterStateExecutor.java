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

import java.util.Locale;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.metadata.Metadata;
import org.elasticsearch.cluster.metadata.RelationMetadata;

import io.crate.execution.ddl.tables.RenameTableRequest;
import io.crate.metadata.RelationName;
import io.crate.metadata.view.ViewsMetadata;

public class RenameTableClusterStateExecutor {


    private final Logger logger;
    private final DDLClusterStateService ddlClusterStateService;

    public RenameTableClusterStateExecutor(DDLClusterStateService ddlClusterStateService) {
        logger = LogManager.getLogger(getClass());
        this.ddlClusterStateService = ddlClusterStateService;
    }

    public ClusterState execute(ClusterState currentState, RenameTableRequest request) throws Exception {
        RelationName source = request.sourceName();
        RelationName target = request.targetName();

        Metadata currentMetadata = currentState.metadata();
        Metadata.Builder newMetadata = Metadata.builder(currentMetadata);
        ViewsMetadata views = currentMetadata.custom(ViewsMetadata.TYPE);
        boolean isView = views != null && views.contains(source);

        boolean viewExists = views != null && views.contains(target);
        boolean tableExists = currentMetadata.contains(target);
        if (viewExists || tableExists) {
            throw new IllegalArgumentException(String.format(
                Locale.ENGLISH,
                "Cannot rename %s %s to %s, %s %s already exists",
                isView ? "view" : "table", source, target, viewExists ? "view" : "table", target));
        }

        if (isView) {
            ViewsMetadata updatedViewsMetadata = views.rename(source, target);
            newMetadata.putCustom(ViewsMetadata.TYPE, updatedViewsMetadata);
            return ClusterState.builder(currentState)
                .metadata(newMetadata)
                .build();
        }

        RelationMetadata relation = currentMetadata.getRelation(source);
        if (relation instanceof RelationMetadata.Table table) {
            RelationMetadata.Table newTable = table.withName(target);
            newMetadata
                .dropTable(source)
                .addTable(
                    newTable.name(),
                    newTable.columns(),
                    newTable.settings(),
                    newTable.routingColumn(),
                    newTable.columnPolicy(),
                    newTable.pkConstraintName(),
                    newTable.checkConstraints(),
                    newTable.primaryKeys(),
                    newTable.partitionedBy(),
                    newTable.state(),
                    newTable.indexUUIDs()
                );
            logger.info("renaming table '{}' to '{}'", source.fqn(), target.fqn());
            ClusterState clusterStateAfterRename = ClusterState.builder(currentState)
                .metadata(newMetadata)
                .build();

            return ddlClusterStateService.onRenameTable(
                clusterStateAfterRename,
                source,
                target,
                request.isPartitioned()
            );
        }

        return currentState;
    }
}
